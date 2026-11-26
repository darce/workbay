"""One landing predicate for every writer that makes a lane or task terminal.

``close_lane`` (closed and merged), the transition of a task to ``done``,
``archive`` and ``reap_done`` all ask the same question: does any lane of the
task hold work whose live branch tip is not on the registered integration ref?
Each of them calls :func:`unlanded_lane_tips`; none carries a second
containment implementation [DBG-10]. Ancestry reuses the reap helpers
(``_resolve_integration_sha`` / ``_reap_contained``).

Policy [SECD-01]: a lane is landed only when every tip it names (the live
``refs/heads/<branch>`` and the row ``branch_tip_sha`` when set, plus a tip the
caller is about to write) is an ancestor of the registered integration ref.
A not-landed lane is harmless only when

* it is a review lane (``lane_kind='review'``) and every commit ahead of
  integration touches only ``.review/`` (retire bundles and pins those), or
* an exact decision ``lane_discarded_<task>_<lane>_<tip40>`` exists for each
  not-landed tip, so a tip that moves after the decision needs a new one.

A probe failure, an unresolvable integration ref or an unreadable ref is
unlanded: unknown keeps the lane open [CAL-02][SECD-05]. A gone branch ref with
a NULL row tip names no commit, so it is not unlanded.

The registered integration ref is the task's ``target_branch``, else the lane
manifest's ``integration_branch``, else ``main``: one registration, answered by
the one predicate ``lane_land._registered_integration_ref`` that ``lane_land``
and the landing drain merge with [DATA-14] (:func:`registered_integration_ref`).
``main`` is always accepted as well, since landing on the release line is the
operator override ``lane_land`` also honours. A lane never proves its own
landing: when the task target is the lane branch itself, only ``main`` counts.
A landing path names the ref it actually landed on, with the root and manifest
root it resolved that ref from (:func:`landed_integration_scope`, set by
``lane_land`` around one landing, so every close that landing reaches through
the drain or the offload close port sees it). That ref counts only when the
same predicate registers it for the task; an unregistered named ref leaves
every tip unlanded. An unreadable registration narrows the accepted set to
``main`` (stricter, never wider) [CAL-02].

Git runs here; callers probe before ``BEGIN IMMEDIATE`` and fence the lane rows
with :func:`lane_roster_fingerprint` (SQL only) under the writer lock, so no
Git runs while RESERVED is held ([CON-21]). :func:`fenced_unlanded` never
re-probes: a missing or stale probe is :data:`STALE_LANDING_PROBE`, which the
task-level writers turn into a pre-lock re-probe or a typed
``landing_probe_stale`` refusal. A probe that raises (anything but a database
fault or the write-lock barrier) is unlanded with reason ``probe_failed:<type>``.
"""

from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path

from .runtime import get_runtime_config
from .shared_write_context import WriteLockHeldAcrossBlockingWorkError, assert_no_write_lock_held

_HEX40 = re.compile(r"[0-9a-f]{40}")
DISCARD_DECISION_PREFIX = "lane_discarded_"
REOPEN_DECISION_PREFIX = "lane_reopened_"
_RELEASE_LINE = "main"
_REVIEW_SCRATCH = ".review"
# Reason prefix when a probe raised instead of answering; unknown is unlanded [CAL-02].
PROBE_FAILED_REASON = "probe_failed"
# Retryable refusal of a task-level terminal write whose pre-lock probe is missing or stale.
LANDING_PROBE_STALE = "landing_probe_stale"


def discard_decision_id(task_ref: str, lane_id: str, tip: str) -> str:
    return f"{DISCARD_DECISION_PREFIX}{task_ref}_{lane_id}_{tip}"


def reopen_decision_id(task_ref: str, lane_id: str, tip: str) -> str:
    return f"{REOPEN_DECISION_PREFIX}{task_ref}_{lane_id}_{tip}"


def _text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _short_branch(value: object) -> str | None:
    branch = _text(value)
    return branch.removeprefix("refs/heads/") if branch else None


# The one registration predicate (target_branch, else manifest integration_branch, else main).
_REGISTRATION_MODULE = "workbay_orchestrator_mcp.orchestration.lane_land"
# Reason when a landing path names an integration ref the task does not register.
UNREGISTERED_INTEGRATION_REF_REASON = "integration_ref_not_registered"


@dataclass(frozen=True)
class LandedIntegration:
    """The ref one landing of ``task_ref`` merged into, and where it read the registration.

    ``root`` / ``manifest_root`` are the ``lane_land`` arguments of the same
    name; ``None`` means the runtime git workspace and no explicit manifest
    root. The proof re-asks the registration predicate with them and never
    trusts ``ref`` on its own [SECD-05].
    """

    task_ref: str
    ref: str
    root: str | Path | None = None
    manifest_root: str | Path | None = None


class _LandingSlot:
    """The landing of one :func:`landed_integration_scope`; bound once its ref is resolved."""

    __slots__ = ("landed",)

    def __init__(self, landed: LandedIntegration | None) -> None:
        self.landed = landed


_LANDING_SLOT: ContextVar[_LandingSlot | None] = ContextVar("workbay_landed_integration", default=None)


@contextmanager
def landed_integration_scope(landed: LandedIntegration | None = None) -> Iterator[None]:
    """Name the landing in progress for every landing proof run inside this block.

    :func:`bind_landed_integration` may set it later inside the block (once the
    landing has resolved its ref); leaving the block restores the outer scope.
    """
    token = _LANDING_SLOT.set(_LandingSlot(landed))
    try:
        yield
    finally:
        _LANDING_SLOT.reset(token)


def bind_landed_integration(landed: LandedIntegration | None) -> None:
    """Set the landing of the enclosing :func:`landed_integration_scope`; a no-op outside one."""
    slot = _LANDING_SLOT.get()
    if slot is not None:
        slot.landed = landed


def current_landed_integration(task_ref: str) -> LandedIntegration | None:
    """The landing in progress for ``task_ref`` (another task's landing names nothing here)."""
    slot = _LANDING_SLOT.get()
    landed = slot.landed if slot is not None else None
    return landed if landed is not None and landed.task_ref == task_ref else None


class UnregisteredIntegrationRefError(ValueError):
    """A caller-named landed ref that the registration predicate does not register for the task."""

    def __init__(self, ref: str, detail: str) -> None:
        super().__init__(f"{ref}: {detail}")
        self.ref = ref
        self.detail = detail


def registered_integration_ref(
    conn: sqlite3.Connection,
    task_ref: str,
    *,
    landed: LandedIntegration | None = None,
) -> str:
    """The task's registered integration ref: ``target_branch``, else the manifest's, else ``main``.

    A set ``target_branch`` answers from SQL alone. Otherwise, or when a
    landing names the ref it landed on (``landed``, default the
    :func:`current_landed_integration` of the task), the answer comes from
    ``lane_land._registered_integration_ref``, the predicate ``lane_land`` and
    the landing drain merge with, so landing and close share one registration
    [DATA-14]. Reads the manifest and may run ``git rev-parse``: call it before
    the writer lock only, like the rest of this proof [CON-21].

    A ``landed.ref`` the predicate does not register (or cannot check) raises
    :class:`UnregisteredIntegrationRefError`; a caller-named ref never widens
    the proof [SECD-05]. Without a named ref, an unreadable registration falls
    back to ``target_branch`` or ``main``, which only narrows it [CAL-02].
    """
    row = conn.execute("SELECT target_branch FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
    target = _short_branch(row["target_branch"] if row is not None else None)
    if landed is None:
        landed = current_landed_integration(task_ref)
    named = _short_branch(landed.ref) if landed is not None else None
    if named is None and target is not None:
        return target
    # The predicate reads the manifest and may run git: never under RESERVED [CON-21].
    assert_no_write_lock_held("landing_registration")
    try:
        predicate = import_module(_REGISTRATION_MODULE)._registered_integration_ref
        root = landed.root if landed is not None else None
        ref, error = predicate(
            root=Path(root if root is not None else get_runtime_config().git_workspace_root),
            task_ref=task_ref,
            requested_ref=named,
            manifest_root=landed.manifest_root if landed is not None else None,
        )
    except (sqlite3.Error, WriteLockHeldAcrossBlockingWorkError):
        raise
    except Exception as exc:  # noqa: BLE001 - unknown registration is never wider [CAL-02]
        ref, error = None, f"registration_unreadable:{type(exc).__name__}"
    registered = _short_branch(ref) if error is None else None
    if named is not None:
        if registered != named:
            raise UnregisteredIntegrationRefError(named, error or "integration_ref_not_task_registered")
        return named
    return registered or target or _RELEASE_LINE


def _lane_rows(conn: sqlite3.Connection, task_ref: str, lane_ids: Iterable[str] | None) -> list[sqlite3.Row]:
    rows = conn.execute(
        "SELECT * FROM worktree_lanes WHERE task_ref = ? ORDER BY lane_id",
        (task_ref,),
    ).fetchall()
    if lane_ids is None:
        return list(rows)
    wanted = set(lane_ids)
    return [row for row in rows if row["lane_id"] in wanted]


def lane_roster_fingerprint(
    conn: sqlite3.Connection, task_ref: str, lane_ids: Iterable[str] | None = None
) -> tuple[tuple[object, ...], ...]:
    """SQL-only identity of every input :func:`unlanded_lane_tips` read from the lane rows."""
    return tuple(
        (row["lane_id"], row["branch"], row["branch_tip_sha"], row["lane_kind"], row["status"])
        for row in _lane_rows(conn, task_ref, None if lane_ids is None else list(lane_ids))
    )


def read_branch_tip(root: Path, branch: str | None) -> tuple[str, str | None]:
    """``("present", sha)``, ``("missing", None)`` or ``("unreadable", None)``."""
    from .import_export import _reap_git, _reap_no_repository  # noqa: PLC0415

    if branch is None:
        return "missing", None
    ref = f"refs/heads/{branch}"
    proc = _reap_git(root, "for-each-ref", "--format=%(refname)%00%(objectname)%00%(objecttype)", ref)
    if proc is None or proc.returncode != 0:
        return ("missing", None) if _reap_no_repository(root) else ("unreadable", None)
    for line in proc.stdout.splitlines():
        name, _, rest = os.fsdecode(line).partition("\0")
        if name != ref:
            continue
        sha, _, kind = rest.partition("\0")
        if kind != "commit" or _HEX40.fullmatch(sha) is None:
            return "unreadable", None
        return "present", sha
    return "missing", None


def _review_scratch_only(root: Path, tip: str, integration_shas: list[str]) -> bool:
    """Every commit of ``tip`` not on integration touches only ``.review/``; unknown is False."""
    from .import_export import _reap_git  # noqa: PLC0415

    if _HEX40.fullmatch(tip) is None or any(_HEX40.fullmatch(sha) is None for sha in integration_shas):
        return False
    proc = _reap_git(
        root,
        "log",
        "--format=",
        "--name-only",
        "--no-renames",
        "-m",
        "-z",
        tip,
        "--not",
        *integration_shas,
        "--",
    )
    if proc is None or proc.returncode != 0:
        return False
    paths = [os.fsdecode(field).strip("\n") for field in proc.stdout.split(b"\0")]
    return all(path == _REVIEW_SCRATCH or path.startswith(f"{_REVIEW_SCRATCH}/") for path in paths if path)


def _discarded(conn: sqlite3.Connection, task_ref: str, lane_id: str, tip: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM decisions WHERE task_ref = ? AND decision = ? LIMIT 1",
        (task_ref, discard_decision_id(task_ref, lane_id, tip)),
    ).fetchone()
    return row is not None


def unlanded_lane_tips(
    conn: sqlite3.Connection,
    task_ref: str,
    lane_ids: Iterable[str] | None = None,
    *,
    extra_tips: Mapping[str, str] | None = None,
) -> list[dict[str, object]]:
    """Lanes of ``task_ref`` whose tip is neither landed nor harmless, as typed entries.

    ``extra_tips`` maps a lane id to a tip the caller is about to record on
    that row (``close_lane(branch_tip_sha=...)``), which must satisfy the same
    proof. A landing in progress for the task (:func:`landed_integration_scope`)
    names its ref; see :func:`registered_integration_ref`. Each entry names ``lane_id``,
    ``branch``, ``tip`` (alias ``branch_tip_sha``), the row ``status``, a
    ``reason``, the integration ref and the exact discard decision that would
    release it.
    """
    from .import_export import _resolve_integration_sha  # noqa: PLC0415

    lanes = _lane_rows(conn, task_ref, None if lane_ids is None else list(lane_ids))
    if not lanes:
        return []
    root = Path(get_runtime_config().git_workspace_root)
    unregistered: UnregisteredIntegrationRefError | None = None
    try:
        registered = registered_integration_ref(conn, task_ref)
    except UnregisteredIntegrationRefError as exc:
        registered, unregistered = exc.ref, exc
    resolved_cache: dict[str, str | None] = {}

    def integration_shas(own_branch: str | None) -> tuple[list[str], list[str]]:
        refs = [ref for ref in dict.fromkeys((registered, _RELEASE_LINE)) if ref != own_branch]
        for ref in refs:
            if ref not in resolved_cache:
                resolved_cache[ref] = _resolve_integration_sha(root, ref)
        return refs, [sha for ref in refs if (sha := resolved_cache[ref]) is not None]

    unlanded: list[dict[str, object]] = []
    for lane in lanes:
        lane_id = str(lane["lane_id"])
        branch = _short_branch(lane["branch"])

        def entry(tip: str | None, reason: str) -> dict[str, object]:
            return {
                "lane_id": lane_id,
                "branch": branch,
                "tip": tip,
                "branch_tip_sha": tip,
                "status": lane["status"],
                "lane_kind": lane["lane_kind"],
                "reason": reason,
                "integration_ref": registered,
                "discard_decision_expected": discard_decision_id(task_ref, lane_id, tip) if tip else None,
            }

        observed: list[str] = []
        if unregistered is not None:
            # A named landed ref the task does not register proves nothing, so
            # every lane it would close stays unlanded; no Git runs [SECD-05].
            named = [tip for tip in (_text((extra_tips or {}).get(lane_id)), _text(lane["branch_tip_sha"])) if tip]
            reason = f"{UNREGISTERED_INTEGRATION_REF_REASON}:{unregistered.detail}"
            unlanded.append(entry(named[0] if named else None, reason))
            continue
        try:
            found = _lane_unlanded_entry(
                conn, task_ref, lane, root, branch, registered, extra_tips, integration_shas, entry, observed
            )
        except (sqlite3.Error, WriteLockHeldAcrossBlockingWorkError):
            # Not evidence: a database fault or Git attempted under the writer
            # lock is a caller defect and must stay loud [CON-21].
            raise
        except Exception as exc:  # noqa: BLE001 - an unknown probe answer is unlanded [CAL-02]
            known = observed or [
                tip for tip in (_text(lane["branch_tip_sha"]), _text((extra_tips or {}).get(lane_id))) if tip
            ]
            found = entry(known[0] if known else None, f"{PROBE_FAILED_REASON}:{type(exc).__name__}")
        if found is not None:
            unlanded.append(found)
    return unlanded


def _lane_unlanded_entry(
    conn: sqlite3.Connection,
    task_ref: str,
    lane: sqlite3.Row,
    root: Path,
    branch: str | None,
    registered: str,
    extra_tips: Mapping[str, str] | None,
    integration_shas: Callable[[str | None], tuple[list[str], list[str]]],
    entry: Callable[[str | None, str], dict[str, object]],
    observed: list[str],
) -> dict[str, object] | None:
    """The unlanded entry for one lane row, or ``None`` when it is landed or harmless.

    ``observed`` receives the tips as soon as they are read, so a later probe
    failure can still name them.
    """
    from .import_export import _reap_contained  # noqa: PLC0415

    lane_id = str(lane["lane_id"])
    state, live = read_branch_tip(root, branch)
    tips = [
        tip
        for tip in dict.fromkeys((live, _text(lane["branch_tip_sha"]), _text((extra_tips or {}).get(lane_id))))
        if tip is not None
    ]
    observed.extend(tips)
    if state == "unreadable":
        return entry(tips[0] if tips else None, f"branch_ref_unreadable:{branch}")
    if not tips:
        return None
    refs, shas = integration_shas(branch)
    if not shas:
        pending = {tip: f"integration_ref_unresolved:{'|'.join(refs) or registered}" for tip in tips}
    else:
        pending = {}
        for tip in tips:
            answers = [_reap_contained(root, tip, sha) for sha in shas]
            if True in answers:
                continue
            pending[tip] = "ancestry_unreadable" if None in answers else "tip_not_on_integration"
    if not pending:
        return None
    if lane["lane_kind"] == "review" and shas and all(_review_scratch_only(root, tip, shas) for tip in pending):
        return None
    if all(_discarded(conn, task_ref, lane_id, tip) for tip in pending):
        return None
    primary = live if live in pending else next(iter(pending))
    return entry(primary, pending[primary])


def tip_is_landed(conn: sqlite3.Connection, task_ref: str, branch: str, tip: str) -> bool | None:
    """Is ``tip`` on the registered integration ref (or ``main``)? ``None`` when unknown."""
    from .import_export import _reap_contained, _resolve_integration_sha  # noqa: PLC0415

    root = Path(get_runtime_config().git_workspace_root)
    own = _short_branch(branch)
    refs = [ref for ref in dict.fromkeys((registered_integration_ref(conn, task_ref), _RELEASE_LINE)) if ref != own]
    try:
        shas = [sha for ref in refs if (sha := _resolve_integration_sha(root, ref)) is not None]
        if not shas:
            return None
        answers = [_reap_contained(root, tip, sha) for sha in shas]
    except (sqlite3.Error, WriteLockHeldAcrossBlockingWorkError):
        raise
    except Exception:  # noqa: BLE001 - a probe that raises is unknown [CAL-02]
        return None
    if True in answers:
        return True
    return None if None in answers else False


def unlanded_lanes_refusal(tool: str, task_ref: str, lanes: list[dict[str, object]]) -> dict[str, object]:
    """The shared ``unlanded_lanes`` refusal payload for task-level writers."""
    names = ", ".join(f"{lane['lane_id']}@{lane['tip'] or 'unknown'}" for lane in lanes)
    return {
        "error": (
            f"{tool}: task {task_ref} has lanes whose branch tip is not on the integration ref ({names}); "
            "land them, or record lane_discarded_<task>_<lane>_<tip40> to discard a tip"
        ),
        "error_kind": "unlanded_lanes",
        "error_code": "unlanded_lanes",
        "lanes": lanes,
    }


LandingProbe = tuple[tuple[tuple[object, ...], ...], list[dict[str, object]]]


def probe_task_landing(conn: sqlite3.Connection, task_ref: str) -> LandingProbe:
    """Observe every lane of ``task_ref`` before the writer lock: ``(fence, unlanded)``."""
    return lane_roster_fingerprint(conn, task_ref), unlanded_lane_tips(conn, task_ref)


class StaleLandingProbe:
    """Marker: the pre-lock landing probe is missing or the lane roster moved since.

    The writer holding the lock must not re-observe (that runs Git under
    RESERVED, [CON-21]); it refuses with :func:`landing_probe_stale_refusal`
    or rolls back and re-probes before the lock [GRPH-27].
    """

    __slots__ = ()

    def __repr__(self) -> str:
        return "STALE_LANDING_PROBE"


STALE_LANDING_PROBE = StaleLandingProbe()


def fenced_unlanded(
    conn: sqlite3.Connection, task_ref: str, probe: LandingProbe | None
) -> list[dict[str, object]] | StaleLandingProbe:
    """Unlanded lanes for a task-level terminal write, under the caller's lock. SQL only.

    A probe whose lane rows still match is reused. A task with no lane rows
    has nothing to land (the same early answer :func:`unlanded_lane_tips`
    gives without Git). Otherwise a missing or stale probe yields
    :data:`STALE_LANDING_PROBE`; it is never re-observed under the lock.
    """
    fingerprint = lane_roster_fingerprint(conn, task_ref)
    if probe is not None and fingerprint == probe[0]:
        return probe[1]
    if not fingerprint:
        return []
    return STALE_LANDING_PROBE


def landing_probe_stale_refusal(tool: str, task_ref: str) -> dict[str, object]:
    """Typed, retryable refusal for (stale or missing landing probe, terminal write); nothing written."""
    return {
        "error": (
            f"{tool}: the lanes of task {task_ref} changed between the landing probe and the write lock; "
            "nothing was written, retry"
        ),
        "error_kind": LANDING_PROBE_STALE,
        "error_code": LANDING_PROBE_STALE,
        "retryable": True,
    }
