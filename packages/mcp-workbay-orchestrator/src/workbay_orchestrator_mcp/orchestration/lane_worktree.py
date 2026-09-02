"""Lane worktree presence + re-materialization (single shared helper).

LANE WORKTREE RE-MATERIALIZATION CONTRACT v1

1. A missing lane worktree is RECOVERABLE whenever the lane branch ref still
   resolves in the primary repository. Recover with
   ``git worktree add <path> <branch>`` and continue. An existing *empty*
   non-worktree directory is the same recoverable shape (``git worktree add``
   accepts empty dirs; this is the leftover a worktree sweep leaves behind).
2. Recovery is IDEMPOTENT — already-present registered worktrees are a no-op
   success. A non-empty non-worktree occupier (including a dir with only
   dotfiles) remains a typed ``occupied_non_worktree`` refusal.
3. Recovery is NEVER silent — warning log + explicit result flag.
4. Impossible recovery returns a typed ``outcome`` (never bare ok:false).
5. ONE implementation; both API call sites use this helper unconditionally so
   present-vs-missing and occupied-non-worktree are owned here (live in
   production, not only unit-direct calls). [ARCH-13]
"""

from __future__ import annotations

import fcntl
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_logger = logging.getLogger(__name__)

# Typed refusal when re-materialization is impossible (contract item 4).
# Distinct from generic ``error`` so callers can branch on this mode without
# parsing free-text messages. Registered in PASS_OUTCOMES +
# PASS_OUTCOME_TO_WORKER_REPORT (maps to failed). Structural lock failure
# (unavailable / unresolved path) stays here — retry will not clear EACCES
# or a missing lock path (Release It! ch. 5 unbounded retry).
OUTCOME_WORKTREE_UNRECOVERABLE = "worktree_unrecoverable"
# Peer holds the worker flock. Flock contention cannot deadlock: the owner
# makes progress and releases. Retryable / non-terminal — not a first-failure.
OUTCOME_WORKTREE_CLAIM_HELD = "worktree_claim_held"

# Explicit result flag naming that recovery ran (contract item 3). [OBS-08]
# Operator receipt on the emitted API payload — not a recovery control plane;
# no production reader gates on this key (see LANE-REPORT).
REMATERIALIZED_FLAG = "worktree_rematerialized"

# Structured refusal modes under the single typed outcome. Distinguishes
# recovery failure branches without free-text substring matching. [NAME-05]
FAILURE_EMPTY_BRANCH = "empty_branch"
FAILURE_PRIMARY_MISSING = "primary_missing"
FAILURE_BRANCH_UNRESOLVABLE = "branch_unresolvable"
FAILURE_OCCUPIED_NON_WORKTREE = "occupied_non_worktree"
FAILURE_GIT_WORKTREE_ADD = "git_worktree_add_failed"
FAILURE_BRANCH_MISMATCH = "branch_mismatch"
# A different LIVE lane already owns this worktree_path (cross-task
# enumeration + resolved path compare). Branch-identity alone cannot catch
# same-path + same-branch dual occupancy
# (EXECMODE-REMAT-NO-SHARED-PATH-OWNERSHIP-01).
FAILURE_SHARED_PATH_OWNED = "shared_path_owned"
# Reaper holds worker-<lane>.lock as claim=reaping from the authorizing
# liveness sample through CAS close. Rematerialize must take the same flock
# and refuse while that claim is live; otherwise a missing worktree can be
# rebuilt in the recency-probe → close window.
FAILURE_REAPING_CLAIM_HELD = "reaping_claim_held"
# Distinct from claim-held: the worker flock could not be taken because the
# lock file/path was unusable (EMFILE, EACCES, …), not because a peer held it.
# Rematerialize must refuse here too; fail-open lets dispatch rebuild a lane
# the reaper is about to close.
FAILURE_WORKER_LOCK_UNAVAILABLE = "worker_lock_unavailable"
_REMAT_CLAIM_KIND = "rematerialize"
_REMAT_CLAIM_BARRIER_ENV = "WORKBAY_TEST_REMAT_CLAIM_BARRIER"

# Terminal statuses are NOT live for the ownership guard. Reclaim refuses on
# any row at a path (destructive); ensure must allow path reuse after the
# previous lane is closed/merged/closed_stale. Live = not in this set.
# Observed open vocabulary on lane rows: planned, active, blocked, review,
# merged, closed, closed_stale (LANE_STATUSES in handoff lanes_recording).
_TERMINAL_LANE_STATUSES = frozenset({"closed", "merged", "closed_stale"})

# path_share_scope values — honest about the owner universe consulted.
# Success is global resolved-path equality over a fully-paged lane sweep,
# not exact-string SQL matching.
PATH_SHARE_SCOPE_RESOLVED = "global_resolved_path_equality"
PATH_SHARE_SCOPE_LOOKUP_UNAVAILABLE = "lookup_unavailable"
# Back-compat alias for any external reader that still names the round-1 token.
PATH_SHARE_SCOPE_EXACT = PATH_SHARE_SCOPE_RESOLVED

# Keyset page size for the ownership sweep. Handoff list_lanes defaults to
# 100; a single page silently truncates repos with more lane rows.
_LIST_LANES_PAGE_SIZE = 100
# Runaway guard so a broken has_more contract cannot loop forever.
_LIST_LANES_MAX_PAGES = 10_000


def _worker_lock_path_for_lane(lane_id: str) -> Path | None:
    """Return the worker flock path, or None when there is no runtime.

    ``None`` means genuine absence of a shared runtime (unconfigured or
    unimportable), so there is no claim to coordinate against. A configured
    runtime whose path cannot be resolved must not be folded into that
    meaning: ``AttributeError``, a missing ``state_dir``, and similar bugs
    propagate so the caller can refuse the same way the reaper refuses
    ``worker_lock_path_unresolved``.
    """
    key = (lane_id or "").strip()
    if not key:
        return None
    try:
        from workbay_handoff_mcp import (  # noqa: PLC0415
            RuntimeNotConfiguredError,
            get_runtime_config,
        )
    except ImportError:
        return None
    try:
        cfg = get_runtime_config()
    except RuntimeNotConfiguredError:
        return None
    return Path(cfg.state_dir) / f"worker-{key}.lock"


def _runtime_is_configured() -> bool:
    """True when a handoff runtime is present, including a broken one.

    Used to tell "no runtime, skip coordination" from "runtime is configured
    but the lock path helper returned None" — the empty-detail hole.
    """
    try:
        from workbay_handoff_mcp import (  # noqa: PLC0415
            RuntimeNotConfiguredError,
            get_runtime_config,
        )
    except ImportError:
        return False
    try:
        get_runtime_config()
    except RuntimeNotConfiguredError:
        return False
    except Exception:  # noqa: BLE001 — configured-but-broken still needs a claim
        return True
    return True


def _write_rematerialize_claim_payload(handle: Any) -> None:
    payload = {
        "pid": os.getpid(),
        "claim": _REMAT_CLAIM_KIND,
        "heartbeat_ts": time.time(),
    }
    try:
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(payload))
        handle.flush()
        os.fsync(handle.fileno())
    except OSError:
        pass


def _rematerialize_claim_barrier() -> None:
    raw = os.environ.get(_REMAT_CLAIM_BARRIER_ENV)
    if not raw or not str(raw).strip():
        return
    barrier = Path(str(raw).strip())
    try:
        barrier.mkdir(parents=True, exist_ok=True)
        (barrier / f"ready.{os.getpid()}").touch()
        go = barrier / "go"
        deadline = time.monotonic() + 30.0
        while not go.exists():
            if time.monotonic() >= deadline:
                break
            time.sleep(0.005)
    except OSError:
        return


def _try_acquire_rematerialize_claim(lane_id: str) -> tuple[Any | None, str]:
    """Non-blocking exclusive flock of the lane worker lock.

    Empty detail means there is no shared runtime to coordinate (skip).
    ``worker_lock_held`` means a reaper or live worker owns the claim.
    ``worker_lock_path_unresolved`` matches the reaper: a runtime is
    configured but the lock path could not be resolved. Any other
    non-empty detail is lock-unavailable; the caller must refuse.
    """
    try:
        path = _worker_lock_path_for_lane(lane_id)
    except Exception:  # noqa: BLE001 — configured runtime, unresolvable path
        return (None, "worker_lock_path_unresolved")
    if path is None:
        if (lane_id or "").strip() and _runtime_is_configured():
            return (None, "worker_lock_path_unresolved")
        return (None, "")
    handle = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a+")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _write_rematerialize_claim_payload(handle)
        _rematerialize_claim_barrier()
        return (handle, "")
    except BlockingIOError:
        if handle is not None:
            try:
                handle.close()
            except OSError:
                pass
        return (None, "worker_lock_held")
    except OSError as exc:
        if handle is not None:
            try:
                handle.close()
            except OSError:
                pass
        return (None, f"worker_lock_unavailable:{exc}")


def _release_rematerialize_claim(handle: Any) -> None:
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        handle.close()
    except Exception:  # noqa: BLE001 — best-effort release
        pass


@dataclass(frozen=True)
class EnsureLaneWorktreeResult:
    """Outcome of :func:`ensure_lane_worktree`.

    ``ok=True`` means the worktree path is present and usable (already was, or
    was re-materialized this call). ``ok=False`` means recovery is impossible;
    ``outcome`` then carries the typed refusal string and ``failure_kind``
    names which recovery branch failed.

    ``path_share_scope`` / ``shared_path_lookup_error`` make the shared-path
    ownership guard observable. A lookup error deliberately does **not**
    refuse (wedging every ensure on a skewed handoff install is worse than
    a defence-in-depth check not running); the branch-identity check still
    stands. Empty ``lane_id`` with a conflicting live owner *does* refuse —
    that asymmetry is intentional.
    """

    ok: bool
    worktree_path: Path
    rematerialized: bool = False
    outcome: str | None = None
    error: str | None = None
    failure_kind: str | None = None
    path_share_scope: str | None = None
    shared_path_lookup_error: str | None = None


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _branch_resolves(primary: Path, branch: str) -> bool:
    """True when *branch* names a commit in *primary*."""
    for candidate in (branch, f"refs/heads/{branch}"):
        proc = _git(primary, "rev-parse", "--verify", f"{candidate}^{{commit}}")
        if proc.returncode == 0 and (proc.stdout or "").strip():
            return True
    return False


def _is_git_worktree(path: Path) -> bool:
    """True when *path* is a usable git working tree (regular repo or linked worktree).

    Linked worktrees store a ``.git`` *file*; regular checkouts use a ``.git``
    directory. Either must pass ``git rev-parse --is-inside-work-tree``.
    """
    if not path.is_dir():
        return False
    git_meta = path / ".git"
    if not git_meta.exists():
        return False
    proc = _git(path, "rev-parse", "--is-inside-work-tree")
    return proc.returncode == 0 and (proc.stdout or "").strip().lower() == "true"


def _is_empty_directory(path: Path) -> bool:
    """True when *path* is a directory with no entries at all (including dotfiles).

    Matches ``git worktree add`` semantics: an empty directory is accepted; a
    directory that contains only dotfiles is refused with ``path already exists``.
    """
    if not path.is_dir():
        return False
    return next(path.iterdir(), None) is None


def _worktree_root_from_gitdir_record(recorded: str, admin_entry: Path) -> Path | None:
    """Resolve the worktree root from a ``gitdir`` file value.

    The ``gitdir`` file under ``<common>/worktrees/<name>/`` holds the path of
    that worktree's ``.git`` file (usually ``<worktree>/.git``). Relative values
    are resolved against the admin entry. Returns ``None`` when unusable.
    Never raises. [RES-13]
    """
    raw = (recorded or "").strip()
    if not raw:
        return None
    try:
        gitdir_path = Path(raw)
        if not gitdir_path.is_absolute():
            gitdir_path = admin_entry / gitdir_path
        # Prefer resolve(); fall back to absolute() when the target is absent.
        try:
            gitdir_path = gitdir_path.resolve()
        except OSError:
            gitdir_path = gitdir_path.absolute()
        if gitdir_path.name == ".git":
            return gitdir_path.parent
        return gitdir_path
    except (OSError, ValueError, RuntimeError):
        return None


def _paths_equivalent(a: Path, b: Path) -> bool:
    """True when *a* and *b* name the same filesystem path (best-effort)."""
    try:
        return a.resolve() == b.resolve()
    except OSError:
        return str(a) == str(b)


def _prune_stale_worktree_registration(primary: Path, path: Path) -> None:
    """Drop only the admin registration for *path*, never sibling worktrees.

    Repo-wide ``git worktree prune`` is intentionally not used: it would drop
    every prunable registration in the primary, including a sibling lane whose
    directory is only temporarily unavailable (external volume asleep, NFS
    unmount, directory moved). That leaves the sibling's files on disk but
    makes ``_is_git_worktree`` false permanently — and an operator clearing the
    "broken" path destroys uncommitted work. [RES-13] [CON-22]

    Behaviourally required for the wiped-path incident shape: a stale admin
    entry for *path* blocks ``git worktree add``. Remove only the matching
    ``<common-dir>/worktrees/<name>/`` entry when *path* is absent or not a
    valid worktree. Missing ``worktrees/``, unreadable/truncated ``gitdir``
    files, and relative/symlinked paths must not raise — recovery paths never
    throw.
    """
    try:
        if path.exists() and _is_git_worktree(path):
            return

        proc = _git(primary, "rev-parse", "--git-common-dir")
        if proc.returncode != 0:
            return
        common_raw = (proc.stdout or "").strip()
        if not common_raw:
            return
        common = Path(common_raw)
        if not common.is_absolute():
            common = primary / common
        try:
            common = common.resolve()
        except OSError:
            common = common.absolute()

        worktrees_dir = common / "worktrees"
        if not worktrees_dir.is_dir():
            return

        target = path
        try:
            target = path.resolve()
        except OSError:
            target = path.absolute()

        try:
            entries = list(worktrees_dir.iterdir())
        except OSError:
            return

        for entry in entries:
            try:
                if not entry.is_dir():
                    continue
            except OSError:
                continue
            gitdir_file = entry / "gitdir"
            try:
                recorded = gitdir_file.read_text(encoding="utf-8")
            except OSError:
                continue
            worktree_root = _worktree_root_from_gitdir_record(recorded, entry)
            if worktree_root is None:
                continue
            if not _paths_equivalent(worktree_root, target):
                continue
            # Matched the target path's admin entry — remove only this one.
            if path.exists() and _is_git_worktree(path):
                return
            # Honour git's own lock marker (``git worktree lock``). Existence
            # only — do not read the reason, do not unlock, do not raise. A
            # locked registration must survive so ``git worktree add`` refuses
            # with the safe typed FAILURE_GIT_WORKTREE_ADD path instead of
            # orphaning uncommitted work on a temporarily-missing tree.
            try:
                if (entry / "locked").exists():
                    return
            except OSError:
                return
            shutil.rmtree(entry, ignore_errors=True)
            return
    except (OSError, ValueError, RuntimeError, TypeError):
        # Recovery path: never surface unexpected errors to the caller.
        return


def _read_checked_out_branch(path: Path) -> tuple[bool, str]:
    """Read the branch checked out at *path*.

    Returns ``(True, name)`` on success. Detached HEAD yields
    ``(True, "HEAD")`` (git ``--abbrev-ref``). Returns ``(False, detail)`` when
    the git read itself fails — callers must refuse, not fail open. [DATA-14]
    """
    proc = _git(path, "rev-parse", "--abbrev-ref", "HEAD")
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip() or f"exit {proc.returncode}"
        return False, detail
    return True, (proc.stdout or "").strip()


def _list_lanes(
    *,
    after_id: int | None = None,
    limit: int = _LIST_LANES_PAGE_SIZE,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """One keyset page of every ``worktree_lanes`` row (cross-task).

    Function-local import (PLAN0181-S2GATE2-MODULE-LEVEL-XPKG-IMPORT-01): a
    skewed handoff install must not fail the entire ``lane_worktree`` import
    surface at load time.

    Assignment directed ``list_lanes(task_ref=None)`` as the cross-task
    primitive. Verified false: ``list_lanes`` always binds
    ``WHERE task_ref = ?`` after resolving the active task
    (``lanes_recording.list_lanes``), so ``task_ref=None`` is active-task
    only. Residual (a) — same ``lane_id`` on two live tasks sharing one
    path — and blocker 2 (resolved vs stored path spellings) both require
    the full row universe. This wrapper therefore pages the whole table
    with the same keyset contract ``list_lanes`` exposes (``after_id=None``
    first page, ``has_more`` / ``next_after_id``), via the same DB helpers
    ``list_lanes_by_worktree_path`` already uses, without a path or task
    filter. Callers that need path-equality across spellings compare with
    ``_paths_equal`` after reading.

    ``has_more`` is computed from the **raw** SQLite fetch count (before the
    dict conversion filter) so a malformed conversion on a full page cannot
    silently clear the continuation bit. Rows that fail ``isinstance(..., dict)``
    are dropped here and counted in ``dropped_non_dict`` so the sweep can stamp
    rather than treat a narrowed universe as complete. Pass ``conn`` to reuse
    one connection across a multi-page sweep (cost); omit it for a one-shot page.
    """
    # Import list_lanes so a missing handoff symbol still surfaces as
    # ImportError at call time (proceed-and-stamp), even though the body
    # must not use its task-scoped filter for the ownership universe.
    from workbay_handoff_mcp.lanes_recording import list_lanes as _list_lanes_sym  # noqa: PLC0415, F401
    from workbay_handoff_mcp.shared_primitives import _row_to_dict  # noqa: PLC0415
    from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

    page_limit = max(1, int(limit))

    def _page(active_conn: sqlite3.Connection) -> dict[str, Any]:
        count_row = active_conn.execute("SELECT COUNT(*) AS count FROM worktree_lanes").fetchone()
        try:
            total = int(count_row["count"])
        except (TypeError, KeyError, IndexError):
            total = int(count_row[0])

        if after_id is None:
            fetched_rows = active_conn.execute(
                "SELECT * FROM worktree_lanes ORDER BY id DESC LIMIT ?",
                (page_limit + 1,),
            ).fetchall()
        elif type(after_id) is int:
            fetched_rows = active_conn.execute(
                "SELECT * FROM worktree_lanes WHERE id < ? ORDER BY id DESC LIMIT ?",
                (after_id, page_limit + 1),
            ).fetchall()
        else:
            return {
                "ok": False,
                "data": {"error": (f"after_id must be an int or None; got {type(after_id).__name__}.")},
            }

        # has_more from raw fetch size — pre isinstance filter [REV-WTOWN2-2].
        has_more = len(fetched_rows) > page_limit
        page_raw = fetched_rows[:page_limit]
        rows: list[dict[str, Any]] = []
        dropped_non_dict = 0
        for raw in page_raw:
            row = _row_to_dict(raw)
            if isinstance(row, dict):
                rows.append(row)
            else:
                # Drop is performed here; surface the count so the sweep can
                # stamp rather than silently narrow the owner universe.
                dropped_non_dict += 1

        next_after_id: int | None = None
        if has_more and page_raw:
            # Cursor from the last *raw* row of this page so a filtered
            # conversion cannot skip past a keyset boundary.
            last = page_raw[-1]
            try:
                next_after_id = int(last["id"])
            except (TypeError, KeyError, IndexError):
                try:
                    next_after_id = int(last[0])
                except (TypeError, KeyError, IndexError):
                    next_after_id = None

        return {
            "ok": True,
            "data": {
                "total_matching": total,
                "returned": len(rows),
                "has_more": has_more,
                "next_after_id": next_after_id,
                "lanes": rows,
                "dropped_non_dict": dropped_non_dict,
            },
        }

    if conn is not None:
        return _page(conn)
    with _get_db_connection() as owned:
        return _page(owned)


def _is_live_lane_status(status: Any) -> bool:
    """True when a lane row should block path reuse by a different caller.

    Terminal rows (``closed`` / ``merged`` / ``closed_stale``) do not block —
    path reuse after close is legitimate. Unknown/empty status is treated as
    live (fail closed on the conflict check, not on the status parse).
    """
    if not isinstance(status, str):
        return True
    token = status.strip()
    if not token:
        return True
    return token not in _TERMINAL_LANE_STATUSES


@dataclass(frozen=True)
class _SharedPathGuardOutcome:
    """Internal result of the shared-path ownership check."""

    refuse: EnsureLaneWorktreeResult | None = None
    path_share_scope: str | None = None
    shared_path_lookup_error: str | None = None


def _collect_all_lanes_for_path_guard() -> tuple[list[Any] | None, str | None]:
    """Fully page the cross-task lane universe for the ownership guard.

    Seeds keyset with ``after_id=None``, follows ``next_after_id``, and
    continues until ``has_more`` is false. A truncated or unusable sweep
    returns ``(None, reason)`` so the caller stamps ``lookup_unavailable``
    rather than treating a partial universe as proven-empty.

    One ``_get_db_connection()`` spans the whole sweep when the handoff
    runtime is live (cost). Each page commits before the next so pages are
    independent observations under WAL rather than one long-lived DEFERRED
    snapshot; ``total_matching`` on the final page is the required completeness
    oracle for concurrent insert/delete drift and for rows the reader dropped
    as non-dict — missing / non-int fails closed like ``has_more``.
    ``sqlite3.Error`` degrades to the same stamp path as
    ImportError/RuntimeError/AttributeError.

    When no handoff runtime is configured (unit pins that mock ``_list_lanes``
    without ``configure_runtime``), the shared open is skipped and the mocked
    reader is invoked without a conn — production always has a configured
    runtime and keeps the single connection.
    """
    from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

    all_lanes: list[Any] = []
    after_id: int | None = None
    conn: sqlite3.Connection | None = None
    conn_cm: Any | None = None
    try:
        try:
            conn_cm = _get_db_connection()
            conn = conn_cm.__enter__()
        except (ImportError, RuntimeError, AttributeError, sqlite3.Error):
            # No live handoff DB (mocked-reader unit pins). Real ensure paths
            # configure the runtime first; a later _list_lanes open stamps.
            conn = None
            conn_cm = None

        for _ in range(_LIST_LANES_MAX_PAGES):
            try:
                env = _list_lanes(
                    after_id=after_id,
                    limit=_LIST_LANES_PAGE_SIZE,
                    conn=conn,
                )
            except (ImportError, RuntimeError, AttributeError, sqlite3.Error) as exc:
                return None, f"{type(exc).__name__}: {exc}"

            if not isinstance(env, dict) or env.get("ok") is not True:
                detail = "envelope not ok"
                if isinstance(env, dict):
                    data = env.get("data")
                    if isinstance(data, dict) and data.get("error"):
                        detail = str(data.get("error"))
                return None, detail

            data = env.get("data") if isinstance(env, dict) else None
            if not isinstance(data, dict):
                return None, "lanes data missing or malformed"
            page = data.get("lanes")
            if not isinstance(page, list):
                return None, "lanes list missing or malformed"

            # Kill site for reader-side non-dict drops: _list_lanes filters
            # them and reports dropped_non_dict. Missing/non-int is absence-
            # as-success (REV-WTOWN3-3); require a typed int and stamp on
            # any drop count > 0. A silent filter without this stamp is the
            # production-vacuous mutant (REV-WTOWN2-4).
            dropped = data.get("dropped_non_dict")
            if type(dropped) is not int:
                return None, "dropped_non_dict missing or non-int"
            if dropped > 0:
                return None, f"malformed lane row count in page: {dropped}"
            # Non-dict entries that still appear in the page list (e.g. a
            # mocked envelope) are stamped in the ownership loop.
            all_lanes.extend(page)

            has_more = data.get("has_more")
            if not isinstance(has_more, bool):
                return None, "has_more missing or non-bool"
            if not has_more:
                # Completeness oracle: final-page total_matching is required
                # and must match collected length. Same discipline as
                # has_more — missing / None / non-int fails closed rather
                # than claiming a proven-complete resolved scope
                # (REV-WTOWN3-1).
                total = data.get("total_matching")
                if type(total) is not int:
                    return None, "total_matching missing or non-int"
                if total != len(all_lanes):
                    return None, (f"total_matching drift: collected={len(all_lanes)} total_matching={total}")
                return all_lanes, None
            if not page:
                return None, "empty page with has_more=true"
            next_cursor = data.get("next_after_id")
            # type() is int, not isinstance: bool subclasses int.
            if type(next_cursor) is not int:
                return None, "next_after_id missing or non-int while has_more"
            after_id = next_cursor
            # Release the deferred snapshot so the next page is a fresh
            # observation; total_matching (not a long BEGIN DEFERRED) is
            # the cross-page consistency signal under WAL.
            if conn is not None:
                conn.commit()

    except (ImportError, RuntimeError, AttributeError, sqlite3.Error) as exc:
        return None, f"{type(exc).__name__}: {exc}"
    finally:
        if conn_cm is not None:
            try:
                conn_cm.__exit__(None, None, None)
            except Exception:  # noqa: BLE001 — close best-effort
                pass

    return None, "lane sweep page cap exceeded with has_more remaining"


def _shared_path_ownership_guard(
    *,
    path: Path,
    worktree_path_raw: str,
    lane_id: str,
    task_ref: str,
    lane_label: str,
    primary: Path,
) -> _SharedPathGuardOutcome:
    """Refuse when a different LIVE lane owns the resolved worktree path.

    Enumerates every lane row (cross-task keyset sweep) and decides ownership
    with ``lane_reclaim._paths_equal`` so trailing-slash / relative /
    macOS ``/tmp`` vs ``/private/tmp`` spellings still match. Scope is
    recorded as ``global_resolved_path_equality`` only when the sweep
    completes; a partial or failed sweep stamps ``lookup_unavailable``.

    Self-ownership is compound ``(task_ref, lane_id)`` — schema
    ``UNIQUE(task_ref, lane_id)``. Same ``lane_id`` on two live tasks is
    not self.

    Lookup exceptions (ImportError / RuntimeError / AttributeError /
    sqlite3.Error) do **not** refuse: proceed with
    ``path_share_scope=lookup_unavailable`` and a stamped error. Empty
    ``lane_id`` or empty ``task_ref`` with a conflicting live owner *does*
    refuse.
    """
    del worktree_path_raw  # retained for call-site symmetry; equality is resolved

    lanes, sweep_error = _collect_all_lanes_for_path_guard()
    if sweep_error is not None or lanes is None:
        return _SharedPathGuardOutcome(
            path_share_scope=PATH_SHARE_SCOPE_LOOKUP_UNAVAILABLE,
            shared_path_lookup_error=sweep_error or "lane sweep failed",
        )

    # Function-local import of the path equality helper so this module does
    # not take a module-level dependency on the heavy reclaim surface.
    from workbay_orchestrator_mcp.orchestration.lane_reclaim import (  # noqa: PLC0415
        _paths_equal,
    )

    caller_id = (lane_id or "").strip()
    caller_task = (task_ref or "").strip()
    for owner in lanes:
        # Finding 3 / REV-WTOWN2-4: non-dict owners must not look like an
        # empty set. Production reader drops non-dicts and stamps via
        # dropped_non_dict in the sweep; this site still stamps mock-injected
        # non-dicts that reach the page list. Silent ``continue`` is the
        # unstamped fail-open mutant.
        if not isinstance(owner, dict):
            return _SharedPathGuardOutcome(
                path_share_scope=PATH_SHARE_SCOPE_LOOKUP_UNAVAILABLE,
                shared_path_lookup_error=(f"malformed lane row: {type(owner).__name__}"),
            )
        owner_path = owner.get("worktree_path")
        if owner_path is None or owner_path == "":
            # Empty/missing path is a census defect, not a non-owner. Silent
            # continue here re-opens an unstamped narrowing of the universe.
            return _SharedPathGuardOutcome(
                path_share_scope=PATH_SHARE_SCOPE_LOOKUP_UNAVAILABLE,
                shared_path_lookup_error=("empty or missing worktree_path on owner row"),
            )
        if not _paths_equal(path, owner_path, orchestrator_root=primary):
            continue
        if not _is_live_lane_status(owner.get("status")):
            continue
        owner_id_raw = owner.get("lane_id")
        owner_id = owner_id_raw.strip() if isinstance(owner_id_raw, str) else str(owner_id_raw or "").strip()
        owner_task_raw = owner.get("task_ref")
        owner_task = owner_task_raw.strip() if isinstance(owner_task_raw, str) else str(owner_task_raw or "").strip()
        # Compound self: both components must match. Empty caller identity
        # cannot prove it *is* the owner when a live row exists (fail closed
        # for both lane_id and task_ref — decision 3 symmetric).
        if (
            caller_id
            and caller_task
            and owner_id
            and owner_task
            and owner_id == caller_id
            and owner_task == caller_task
        ):
            continue
        task_note = f"@task:{owner_task}" if owner_task else ""
        owner_label = owner_id or "<unknown>"
        error = (
            f"Lane worktree path for lane '{lane_label}' is already owned by "
            f"live lane '{owner_label}'{task_note} ({path})"
        )
        return _SharedPathGuardOutcome(
            refuse=EnsureLaneWorktreeResult(
                ok=False,
                worktree_path=path,
                rematerialized=False,
                outcome=OUTCOME_WORKTREE_UNRECOVERABLE,
                error=error,
                failure_kind=FAILURE_SHARED_PATH_OWNED,
                path_share_scope=PATH_SHARE_SCOPE_RESOLVED,
            ),
            path_share_scope=PATH_SHARE_SCOPE_RESOLVED,
        )

    return _SharedPathGuardOutcome(path_share_scope=PATH_SHARE_SCOPE_RESOLVED)


def ensure_lane_worktree(
    *,
    primary_repo: Path | str,
    worktree_path: Path | str,
    branch: str,
    lane_id: str = "",
    task_ref: str = "",
) -> EnsureLaneWorktreeResult:
    """Ensure a lane worktree exists, re-materializing from *branch* when missing.

    Parameters
    ----------
    primary_repo:
        Primary repository root from which ``git worktree add`` is run.
    worktree_path:
        Expected lane worktree checkout path (from the lane row / manifest).
    branch:
        Lane branch ref still held by the primary object store.
    lane_id:
        Lane id for log/error messages and shared-path ownership. Empty
        identity fails closed when a different live lane owns the path.
    task_ref:
        Task ref forming the compound lane identity with ``lane_id``
        (schema ``UNIQUE(task_ref, lane_id)``). Empty identity fails closed
        when a conflicting live owner claims the path — same as empty
        ``lane_id``.
    """
    primary = Path(primary_repo).expanduser().resolve()
    worktree_path_raw = str(worktree_path)
    path = Path(worktree_path).expanduser().resolve()
    branch_name = (branch or "").strip()
    lane_label = (lane_id or "").strip() or str(path)

    # Stamps from the shared-path guard, carried onto proceed results so a
    # lookup skip is never silent (decision 4).
    path_share_scope: str | None = None
    shared_path_lookup_error: str | None = None

    def _stamp(
        result: EnsureLaneWorktreeResult,
    ) -> EnsureLaneWorktreeResult:
        if path_share_scope is None and shared_path_lookup_error is None:
            return result
        if result.path_share_scope is not None or result.shared_path_lookup_error is not None:
            return result
        return EnsureLaneWorktreeResult(
            ok=result.ok,
            worktree_path=result.worktree_path,
            rematerialized=result.rematerialized,
            outcome=result.outcome,
            error=result.error,
            failure_kind=result.failure_kind,
            path_share_scope=path_share_scope,
            shared_path_lookup_error=shared_path_lookup_error,
        )

    def _run_shared_path_guard() -> EnsureLaneWorktreeResult | None:
        nonlocal path_share_scope, shared_path_lookup_error
        outcome = _shared_path_ownership_guard(
            path=path,
            worktree_path_raw=worktree_path_raw,
            lane_id=lane_id,
            task_ref=task_ref,
            lane_label=lane_label,
            primary=primary,
        )
        path_share_scope = outcome.path_share_scope
        shared_path_lookup_error = outcome.shared_path_lookup_error
        return outcome.refuse

    if path.exists():
        # Four-way presence check (git worktree add semantics):
        # 1. valid registered worktree on the requested branch → no-op success
        # 2. empty non-worktree directory → fall through to rematerialize
        #    (sweep leftover; git accepts empty dirs)
        # 3. non-empty non-worktree (or non-directory) → typed refusal
        #    [RLSE-05] Both production call sites invoke this helper
        #    unconditionally so this branch is live for real callers.
        # 4. valid worktree on a different branch (or detached HEAD) → typed
        #    branch_mismatch refusal — never spawn a worker in the wrong tree.
        #    [DATA-14] [AGT-02]
        if _is_git_worktree(path):
            # Empty branch_name: keep historical behaviour (accept present
            # worktree). The missing-path empty-branch refusal below still
            # covers recovery without a ref.
            if not branch_name:
                # Shared-path ownership still applies: same-path dual live
                # occupancy is not a branch-identity problem.
                refused = _run_shared_path_guard()
                if refused is not None:
                    return refused
                return _stamp(EnsureLaneWorktreeResult(ok=True, worktree_path=path, rematerialized=False))
            ok_read, found = _read_checked_out_branch(path)
            if not ok_read:
                error = (
                    f"Lane worktree path for lane '{lane_label}' is a git "
                    f"worktree but the checked-out branch could not be read "
                    f"at {path}: {found}"
                )
                return EnsureLaneWorktreeResult(
                    ok=False,
                    worktree_path=path,
                    rematerialized=False,
                    outcome=OUTCOME_WORKTREE_UNRECOVERABLE,
                    error=error,
                    failure_kind=FAILURE_BRANCH_MISMATCH,
                )
            # Detached HEAD reports as "HEAD"; treat as mismatch — a detached
            # checkout is not a lane worktree for branch_name.
            if found != branch_name:
                error = (
                    f"Lane worktree path for lane '{lane_label}' is checked "
                    f"out on '{found}' but branch '{branch_name}' was requested "
                    f"({path})"
                )
                return EnsureLaneWorktreeResult(
                    ok=False,
                    worktree_path=path,
                    rematerialized=False,
                    outcome=OUTCOME_WORKTREE_UNRECOVERABLE,
                    error=error,
                    failure_kind=FAILURE_BRANCH_MISMATCH,
                )
            # Matching branch: shared-path guard catches same-path + same-
            # branch two-live-lane occupancy that branch-identity misses.
            refused = _run_shared_path_guard()
            if refused is not None:
                return refused
            return _stamp(EnsureLaneWorktreeResult(ok=True, worktree_path=path, rematerialized=False))
        if not _is_empty_directory(path):
            error = f"Lane worktree path for lane '{lane_label}' is occupied by a non-worktree path: {path}"
            return EnsureLaneWorktreeResult(
                ok=False,
                worktree_path=path,
                rematerialized=False,
                outcome=OUTCOME_WORKTREE_UNRECOVERABLE,
                error=error,
                failure_kind=FAILURE_OCCUPIED_NON_WORKTREE,
            )
        # Empty non-worktree directory: rematerialize like a missing path.

    if not branch_name:
        error = f"Lane worktree does not exist for lane '{lane_label}': {path} (no branch ref to recover from)"
        return EnsureLaneWorktreeResult(
            ok=False,
            worktree_path=path,
            rematerialized=False,
            outcome=OUTCOME_WORKTREE_UNRECOVERABLE,
            error=error,
            failure_kind=FAILURE_EMPTY_BRANCH,
        )

    if not primary.is_dir():
        error = f"Lane worktree does not exist for lane '{lane_label}': {path} (primary repo not found: {primary})"
        return EnsureLaneWorktreeResult(
            ok=False,
            worktree_path=path,
            rematerialized=False,
            outcome=OUTCOME_WORKTREE_UNRECOVERABLE,
            error=error,
            failure_kind=FAILURE_PRIMARY_MISSING,
        )

    if not _branch_resolves(primary, branch_name):
        error = (
            f"Lane worktree does not exist for lane '{lane_label}': {path} (branch ref does not resolve: {branch_name})"
        )
        return EnsureLaneWorktreeResult(
            ok=False,
            worktree_path=path,
            rematerialized=False,
            outcome=OUTCOME_WORKTREE_UNRECOVERABLE,
            error=error,
            failure_kind=FAILURE_BRANCH_UNRESOLVABLE,
        )

    # Case (b): a live lane row still claims P while P is absent on disk.
    # Refuse before re-materializing so B's fresh checkout does not become
    # the tree that A's row points at.
    refused = _run_shared_path_guard()
    if refused is not None:
        return refused

    remat_lock, remat_detail = _try_acquire_rematerialize_claim(lane_id)
    if remat_lock is None and remat_detail:
        # Any non-empty detail is a hard refuse. ``worker_lock_held`` means a
        # peer owns the flock; ``worker_lock_path_unresolved`` matches the
        # reaper's refuse-close; every other detail (unreadable path, EMFILE,
        # …) is lock-unavailable. Empty detail still means "no runtime to
        # coordinate" and proceeds.
        if remat_detail == "worker_lock_held":
            failure_kind = FAILURE_REAPING_CLAIM_HELD
            why = "reaping claim held; rematerialize refused"
            # Peer-held flock is retryable: the owner necessarily makes
            # progress (CON-11 / CON-21). Structural unavailability is not.
            outcome = OUTCOME_WORKTREE_CLAIM_HELD
        else:
            failure_kind = FAILURE_WORKER_LOCK_UNAVAILABLE
            why = f"worker lock unavailable ({remat_detail}); rematerialize refused"
            outcome = OUTCOME_WORKTREE_UNRECOVERABLE
        error = f"Lane worktree does not exist for lane '{lane_label}': {path} ({why})"
        _logger.warning(
            "refused rematerialize lane_id=%s path=%s detail=%s failure_kind=%s",
            lane_label,
            path,
            remat_detail,
            failure_kind,
        )
        return _stamp(
            EnsureLaneWorktreeResult(
                ok=False,
                worktree_path=path,
                rematerialized=False,
                outcome=outcome,
                error=error,
                failure_kind=failure_kind,
            )
        )

    try:
        # Drop only the stale admin registration for *this* path when the
        # directory was removed without ``git worktree remove`` (the live
        # incident shape). Behaviourally required: without it, rebuild after a
        # wipe fails. Must never be a repo-wide ``git worktree prune`` — that
        # collaterally drops sibling lanes whose paths are only temporarily
        # unavailable. [RES-13] [CON-22]
        _prune_stale_worktree_registration(primary, path)

        path.parent.mkdir(parents=True, exist_ok=True)
        add = _git(primary, "worktree", "add", str(path), branch_name)
        if add.returncode != 0:
            detail = (add.stderr or add.stdout or "").strip() or f"exit {add.returncode}"
            error = f"Lane worktree does not exist for lane '{lane_label}': {path} (git worktree add failed: {detail})"
            return _stamp(
                EnsureLaneWorktreeResult(
                    ok=False,
                    worktree_path=path,
                    rematerialized=False,
                    outcome=OUTCOME_WORKTREE_UNRECOVERABLE,
                    error=error,
                    failure_kind=FAILURE_GIT_WORKTREE_ADD,
                )
            )

        # Contract item 3: never silent. [OBS-08] [AGT-10]
        _logger.warning(
            "re-materialized missing lane worktree lane_id=%s path=%s branch=%s",
            lane_label,
            path,
            branch_name,
        )
        return _stamp(
            EnsureLaneWorktreeResult(
                ok=True,
                worktree_path=path,
                rematerialized=True,
            )
        )
    finally:
        if remat_lock is not None:
            _release_rematerialize_claim(remat_lock)
