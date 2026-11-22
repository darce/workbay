"""Reference monitor for retiring local lane branches.

Every call decides from a fresh ``lane_disposition`` snapshot. Apply repeats
that decision under the landing lock. Ordinary deletes require main ancestry
and use ``git branch -d``. Authorized terminal plans may use ``-D`` only after
pinning and verifying a recovery bundle of the exact authorized tip. Kept
payload branches remain outside that terminal retirement path.

Every destructive caller -- this monitor, owner retirement, cross-session
cleanup, orphan and review-stage reclaim -- binds its request to the lane's
durable row through :func:`bind_close_authority` and acts through
:func:`run_under_close_authority`, so no entry acts outside the row's current
close authority [SECD-01].
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from workbay_handoff_mcp.shared_write_context import assert_no_write_lock_held

from workbay_orchestrator_mcp.orchestration import lane_disposition
from workbay_orchestrator_mcp.orchestration.lane_reclaim import (
    _INTEGRATION_TARGET_NAMES,
)

if TYPE_CHECKING:
    from workbay_orchestrator_mcp.orchestration.lane_terminal_dispose import BranchDisposition

_ZERO_SHA = "0" * 40
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
GIT_SUBPROCESS_TIMEOUT_S = 20.0
_TERMINAL_DISPOSE_ENABLED_ENV = "WORKBAY_TERMINAL_DISPOSE_ENABLED"
_TERMINAL_DISPOSE_APPLY_ENV = "WORKBAY_TERMINAL_DISPOSE_APPLY"
_DEFAULT_TERMINAL_DISPOSE_APPLY = frozenset(
    {
        "review_persisted",
        "review_unproduced",
        "never_started",
        "superseded_tombstone",
    }
)
_ARMABLE_KINDS = frozenset(
    kind.value for rule in lane_disposition.RULES if rule.branch_action == "delete_D_after_pin" for kind in rule.kinds
)
_MISSING_REVISION_MARKERS = (
    "needed a single revision",
    "unknown revision",
)
_RECOVERY_CONFIG = (
    "gc.pruneExpire",
    "gc.reflogExpireUnreachable",
)

DeleteReason = Literal[
    "deleted",
    "already_absent",
    "would_delete",
    "invalid_authorized_sha",
    "invalid_branch",
    "invalid_reclaim_ref",
    "branch_is_integration_target",
    "recovery_precondition_failed",
    "branch_missing",
    "probe_failed",
    "authorized_sha_changed",
    "live_proof_failed",
    "pin_conflict",
    "pin_failed",
    "reference_transaction_hook_present",
    "reference_transaction_hook_unknown",
    "delete_failed",
    "snapshot_moved",
    "lock_held",
    "bundle_failed",
    "landing_policy_unavailable",
    "ref_lock_busy",
    "row_changed_after_close",
    "authority_mismatch",
    "lane_row_read_failed",
    "lane_identity_ambiguous",
    "reclaim_fence_held",
    "reclaim_fence_unavailable",
    "refused_owned",
    "refused_owner_unknown",
]


@dataclass(frozen=True)
class BranchDeleteResult:
    """Typed result from :func:`delete_authorized_branch`."""

    deleted: bool
    reason: DeleteReason
    branch: str
    authorized_sha: str
    reclaim_ref: str | None = None
    detail: str = ""
    recovery_config: dict[str, str | None] = field(default_factory=dict)
    disposition: str | None = None
    rule: int | None = None
    plan: str | None = None
    next_action: str | None = None
    work_item: str | None = None
    branch_action: str | None = None
    requested_disposition: str | None = None
    bundle_path: str | None = None
    lock_path: str | None = None
    age_s: float | None = None
    size: int | None = None
    stale_after_s: float | None = None
    lock_recovery: dict[str, object] = field(default_factory=dict)
    #: ``released``, ``not_ours`` or ``release_failed`` once the act ran under the row's reclaim fence;
    #: ``""`` when no fence was claimed. Never folded into ``deleted`` or ``reason`` [AGT-10].
    fence_release: str = ""
    #: Evidence after a failed delete is independent of that command's exit status.
    observed_ref: str | None = None
    command_error: str = ""
    effect_provenance: Literal["this_operation", "external_or_unknown_effect", "unresolved"] = "unresolved"
    deleted_by_this_operation: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "deleted_by_this_operation", self.deleted)
        if self.deleted:
            object.__setattr__(self, "effect_provenance", "this_operation")


_T = TypeVar("_T")

#: Detail prefix a successful act carries when its reclaim fence was not released (lane_reaping's convention).
FENCE_RELEASE_DETAIL = "reap_fence_release:"

#: Row columns whose values a binding without close authority re-proves immediately before its act.
_ROW_GENERATION_KEYS = (
    "id",
    "identity_seq",
    "task_ref",
    "lane_id",
    "status",
    "notes",
    "branch",
    "branch_tip_sha",
    "owner_agent",
    "worktree_path",
    "updated_at",
    "reclaim_fence",
)


@dataclass(frozen=True)
class CloseAuthorityBinding:
    """A destructive request bound to the durable row it acts on.

    ``refusal`` is non-empty when the request may not act at all. Otherwise
    ``authority`` is the row's close authority, and the act runs under that
    row's reclaim fence; or it is None for a legacy/open row or a genuinely
    rowless lane, whose observed ``generation`` (``None`` when rowless) is
    re-proved immediately before every act.
    """

    refusal: str
    task_ref: str | None = None
    lane_id: str | None = None
    branch: str | None = None
    authority: Mapping[str, Any] | None = None
    generation: tuple[object, ...] | None = None
    payload_branch_kept: bool = False


def _short_branch_name(branch: object) -> str:
    return str(branch or "").strip().removeprefix("refs/heads/")


def _row_generation(row: Mapping[str, Any] | None) -> tuple[object, ...] | None:
    return None if row is None else tuple(row.get(key) for key in _ROW_GENERATION_KEYS)


def _branch_owner_refusal(branch: str | None, *, task_ref: str | None, lane_id: str) -> str:
    """``""`` unless another lane's close-authorized row names ``branch``.

    Such a branch is that row's to retire under its own reclaim fence; acting
    on it under this identity would bypass the fence, so it is ambiguous. An
    unreadable registry never proves absence.
    """
    name = _short_branch_name(branch)
    if not name:
        return ""
    from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

    try:
        with lane_reaping._get_db_connection() as conn:
            owners = conn.execute(
                "SELECT task_ref, lane_id, notes FROM worktree_lanes WHERE branch = ? OR branch = ?",
                (name, f"refs/heads/{name}"),
            ).fetchall()
    except Exception:  # noqa: BLE001 — an unreadable registry never proves absence
        return "lane_row_read_failed"
    for owner_task, owner_lane, notes in (tuple(owner) for owner in owners):
        if owner_lane == lane_id and (task_ref is None or owner_task == task_ref):
            continue
        authority, malformed = lane_reaping._parse_close_authority(notes)
        if authority is not None or malformed:
            return "lane_identity_ambiguous"
    return ""


def bind_close_authority(
    *,
    task_ref: str | None,
    lane_id: str | None,
    branch: str | None = None,
    tip: str | None = None,
) -> CloseAuthorityBinding:
    """Bind a destructive request to the lane's actual durable row and its current close authority.

    Refuses an unreadable, ambiguous, malformed or changed row; a request whose
    branch or tip disagrees with the close authority (``authority_mismatch``);
    and a branch that another lane's row names. A row without close authority,
    or no row at all, binds to what was observed, re-proved before the act.
    """
    from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

    lane = str(lane_id or "").strip()
    if not lane:
        return CloseAuthorityBinding("lane_identity_ambiguous")
    row, authority, refusal = lane_reaping._durable_close_authority(task_ref=task_ref, lane_id=lane)
    if refusal:
        return CloseAuthorityBinding(refusal)
    bound_task = str(row["task_ref"]) if row is not None else (str(task_ref).strip() or None if task_ref else None)
    if authority is not None:
        identity = authority["closed_identity"]
        if (tip is not None and tip != authority["authorized_tip"]) or (
            branch is not None and _short_branch_name(branch) != _short_branch_name(identity.get("branch"))
        ):
            return CloseAuthorityBinding("authority_mismatch")
    owner_refusal = _branch_owner_refusal(branch, task_ref=bound_task, lane_id=lane)
    if owner_refusal:
        return CloseAuthorityBinding(owner_refusal)
    return CloseAuthorityBinding(
        "",
        task_ref=bound_task,
        lane_id=lane,
        branch=branch,
        authority=authority,
        generation=_row_generation(row),
        payload_branch_kept=(
            row is not None and "branch kept: review_payload_only_over_preserved_base" in str(row.get("notes") or "")
        ),
    )


def reprove_close_authority(binding: CloseAuthorityBinding) -> str:
    """Re-prove an unfenced binding immediately before its act; ``""`` when it still holds.

    A fenced binding needs no re-proof: the claim re-reads the row against its
    authority and the guard triggers refuse every other write while it holds.
    """
    if binding.refusal or binding.authority is not None:
        return binding.refusal
    from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

    row, authority, refusal = lane_reaping._durable_close_authority(task_ref=binding.task_ref, lane_id=binding.lane_id)
    if refusal:
        return refusal
    if authority is not None or _row_generation(row) != binding.generation:
        return "row_changed_after_close"
    return _branch_owner_refusal(binding.branch, task_ref=binding.task_ref, lane_id=str(binding.lane_id))


def run_under_close_authority(binding: CloseAuthorityBinding, actuator: Callable[[], _T]) -> tuple[str, _T | None, str]:
    """Run ``actuator`` under ``binding``; returns ``(refusal, result, release)`` like the core fence.

    With close authority the act runs inside ``lane_reaping._run_under_reclaim_fence``,
    whose Git mutations go through its supported runner. Without it the binding is
    re-proved first and the act runs unfenced (``release`` is ``""``). An actuator
    that raises is re-raised unchanged once the fence was released, carrying the
    release state as ``reclaim_fence_release`` and as a note, never masked [AGT-10].
    """
    refusal = reprove_close_authority(binding)
    if refusal:
        return (refusal, None, "")
    if binding.authority is None:
        return ("", actuator(), "")
    from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

    raised: list[BaseException] = []

    def guarded() -> _T | None:
        try:
            return actuator()
        except BaseException as exc:  # noqa: BLE001 — re-raised below, after the fence is released
            raised.append(exc)
            return None

    refusal, result, release = lane_reaping._run_under_reclaim_fence(
        task_ref=str(binding.task_ref),
        lane_id=str(binding.lane_id),
        authority=binding.authority,
        actuator=guarded,
    )
    if raised:
        exc = raised[0]
        try:
            exc.reclaim_fence_release = release  # type: ignore[attr-defined]
        except AttributeError:
            pass
        exc.add_note(f"{FENCE_RELEASE_DETAIL}{release}")
        raise exc
    return (refusal, result, release)


def lane_identity_for_branch(branch: str | None) -> tuple[str, str | None, str | None]:
    """``(refusal, task_ref, lane_id)`` of the one lane whose durable rows name ``branch``.

    ``("", None, None)`` when no row names it. More than one lane is
    ``lane_identity_ambiguous``; an unreadable registry is ``lane_row_read_failed``.
    """
    name = _short_branch_name(branch)
    if not name:
        return ("", None, None)
    from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

    try:
        with lane_reaping._get_db_connection() as conn:
            owners = conn.execute(
                "SELECT DISTINCT task_ref, lane_id FROM worktree_lanes WHERE branch = ? OR branch = ?",
                (name, f"refs/heads/{name}"),
            ).fetchall()
    except Exception:  # noqa: BLE001 — an unreadable registry never proves absence
        return ("lane_row_read_failed", None, None)
    identities = {(str(owner[0]), str(owner[1])) for owner in owners}
    if len(identities) > 1:
        return ("lane_identity_ambiguous", None, None)
    if not identities:
        return ("", None, None)
    task, lane = identities.pop()
    return ("", task, lane)


def remove_worktree_under_close_authority(
    repo_root: Path | str,
    worktree_path: str,
    binding: CloseAuthorityBinding,
    *,
    review: dict[str, object] | None = None,
    incarnation: Mapping[str, object] | None = None,
) -> tuple[bool, str]:
    """Remove a linked worktree bound to ``binding`` through the core authority-bound removal.

    A row with close authority is removed by ``lane_reaping._remove_lane_worktree``
    under its reclaim fence, held to the bound tip; a successful removal whose fence
    was not released returns ``(True, "reap_fence_release:<state>")``. A legacy or
    rowless binding is re-proved immediately before the safe ``git worktree remove``.
    """
    refusal = reprove_close_authority(binding)
    if refusal:
        return (False, refusal)
    from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

    from .review_stage_reap import review_stage_deferral  # noqa: PLC0415

    if review is not None:
        if incarnation is None or binding.branch != review.get("branch"):
            return False, "review_removal_identity_missing"
        lock, detail = lane_reaping._acquire_lane_worker_lock(binding.lane_id)
        if lock is None:
            return False, detail
        try:
            refusal, result, release = run_under_close_authority(
                binding,
                lambda: lane_reaping._remove_lane_worktree(
                    repo_root,
                    worktree_path,
                    lane_id=binding.lane_id,
                    task_ref=binding.task_ref,
                    review=review,
                    incarnation=incarnation,
                ),
            )
        finally:
            lane_reaping._release_lane_worker_lock(lock)
        removed, cause = result if result is not None else (False, refusal)
        return removed, FenceReleaseDetail(cause, release)
    deferred = review_stage_deferral(Path(repo_root), str(binding.branch or ""))
    if deferred:
        return False, deferred
    return lane_reaping._remove_lane_worktree(
        repo_root,
        worktree_path,
        expected_tip=None if binding.authority is None else str(binding.authority["authorized_tip"]),
        lane_id=binding.lane_id,
        task_ref=binding.task_ref,
    )


class FenceReleaseDetail(str):
    """A byte-compatible cause with an independent, non-clean release receipt.

    Permanent causes are matched exactly by existing callers. Keep their string
    value intact; envelope consumers extract ``fence_release`` before serializing
    the cause, using :func:`fence_release_of` just as for legacy suffixed details.
    """

    fence_release: str

    def __new__(cls, cause: str, fence_release: str = "") -> FenceReleaseDetail:
        detail = super().__new__(cls, cause)
        detail.fence_release = fence_release
        return detail


def fence_release_of(detail: object) -> str:
    """Read an independent or semicolon-delimited release receipt, on success or refusal."""
    if isinstance(detail, FenceReleaseDetail):
        return detail.fence_release
    for part in str(detail or "").split(";"):
        part = part.strip()
        if part.startswith(FENCE_RELEASE_DETAIL):
            return part[len(FENCE_RELEASE_DETAIL) :]
    return ""


def exception_fence_release(exc: BaseException) -> str:
    """The release state an act's exception carries from :func:`run_under_close_authority`, else ``""``."""
    release = getattr(exc, "reclaim_fence_release", "")
    return release if isinstance(release, str) else ""


@dataclass
class _AuthorizedDeletePlan:
    short_branch: str
    branch_ref: str
    sha: str
    reclaim_ref: str
    review_stage: bool = False
    review_subject: str = ""
    recovery: dict[str, str | None] = field(default_factory=dict)
    force_delete: bool = False
    recovery_verified: bool = False
    disposition: str | None = None
    rule: int | None = None
    plan: str | None = None
    next_action: str | None = None
    work_item: str | None = None
    branch_action: str | None = None
    requested_disposition: str | None = None
    bundle_path: str | None = None
    lock_recovery: dict[str, object] = field(default_factory=dict)
    binding: CloseAuthorityBinding | None = None
    require_unowned_task_target: bool = False
    effect_guard: Callable[[], str | None] | None = None
    supersession_effect: dict[str, str] = field(default_factory=dict)


def _decode_timeout_output(raw: object) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    return str(raw)


def _git(
    root: Path,
    *args: str,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    timeout_s = GIT_SUBPROCESS_TIMEOUT_S if timeout is None else timeout
    argv = ["git", "-C", str(root), *args]
    # Fail closed before either blocking branch. Kept outside the broad mutation-delegate
    # handler below so WriteLockHeldAcrossBlockingWorkError propagates instead of becoming rc 127.
    assert_no_write_lock_held("subprocess")
    if _is_git_mutation(args):
        try:
            from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

            result = lane_reaping._run_reclaim_command(
                argv,
                timeout=timeout_s,
                mutating=True,
                env=env,
            )
        except Exception as exc:  # noqa: BLE001 - mutation failures are returned as command failures
            return subprocess.CompletedProcess(argv, 127, "", f"{type(exc).__name__}: {exc}")
        if result is not None:
            return result
        return subprocess.CompletedProcess(argv, 127, "", "git mutation could not run")
    try:
        return subprocess.run(
            argv,
            check=False,
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = _decode_timeout_output(exc.stdout)
        stderr = _decode_timeout_output(exc.stderr).strip()
        return subprocess.CompletedProcess(
            argv,
            124,
            stdout,
            stderr or f"git timeout after {timeout_s}s",
        )


def _is_git_mutation(args: tuple[str, ...]) -> bool:
    if "update-ref" in args:
        return True
    for index, token in enumerate(args[:-1]):
        if token == "worktree" and args[index + 1] == "remove":
            return True
        if token == "bundle" and args[index + 1] == "create":
            return True
        if token == "branch" and args[index + 1] in {"-d", "-D"}:
            return True
    return False


def _detail(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stderr or proc.stdout or "").strip()


def _load_branch_disposition(value: object) -> str | None:
    """Preserve a caller label for reporting without granting it authority."""
    if value is None:
        return None
    raw = getattr(value, "value", value)
    return raw if isinstance(raw, str) else None


def _terminal_dispose_apply_values() -> frozenset[str]:
    configured = os.environ.get(_TERMINAL_DISPOSE_APPLY_ENV)
    requested = (
        _DEFAULT_TERMINAL_DISPOSE_APPLY
        if configured is None
        else frozenset(token.strip() for token in configured.split(",") if token.strip())
    )
    return requested & _ARMABLE_KINDS


def _terminal_dispose_classification_enabled() -> bool:
    configured = os.environ.get(_TERMINAL_DISPOSE_ENABLED_ENV)
    if configured is None:
        return True
    return configured.strip().casefold() in {"1", "true", "yes", "on"}


def terminal_dispose_authorized(disposition: lane_disposition.Disposition) -> tuple[bool, str]:
    """Decide whether a terminal disposition may be disposed, with the refusal reason.

    The worktree-reclaim and branch-delete gates both call this, so they cannot
    disagree about which kinds are armed: arming is configuration
    (``WORKBAY_TERMINAL_DISPOSE_APPLY``), never a second list in a caller.
    """
    if disposition.branch_action != "delete_D_after_pin":
        return (False, disposition.reason)
    if not _terminal_dispose_classification_enabled():
        return (False, "terminal_dispose_disabled")
    if disposition.kind.value not in _terminal_dispose_apply_values():
        return (False, "terminal_dispose_apply_not_allowed")
    return (True, disposition.reason)


def _branch_parts(branch: str) -> tuple[str, str] | None:
    raw = branch.strip()
    if raw.startswith("refs/heads/"):
        short = raw.removeprefix("refs/heads/")
    elif raw.startswith("refs/"):
        return None
    else:
        short = raw
    if not short or short.startswith("-"):
        return None
    return short, f"refs/heads/{short}"


def _is_conclusive_missing_revision(proc: subprocess.CompletedProcess[str]) -> bool:
    """True only for git's missing-ref diagnostic, never timeout or I/O errors."""

    if proc.returncode != 128:
        return False
    text = (proc.stderr or proc.stdout or "").casefold()
    return any(marker in text for marker in _MISSING_REVISION_MARKERS)


@dataclass(frozen=True)
class _RefProbe:
    """Result of resolving a ref: a SHA, a conclusive miss, or a failed probe."""

    sha: str | None
    proc: subprocess.CompletedProcess[str]

    @property
    def missing(self) -> bool:
        return self.sha is None and _is_conclusive_missing_revision(self.proc)

    @property
    def probe_failed(self) -> bool:
        return self.sha is None and not self.missing


def _resolve_ref(root: Path, ref: str) -> _RefProbe:
    proc = _git(root, "rev-parse", "--verify", f"{ref}^{{commit}}")
    value = (proc.stdout or "").strip().lower()
    sha = value if proc.returncode == 0 and _SHA_RE.fullmatch(value) else None
    return _RefProbe(sha, proc)


def _recovery_config(root: Path) -> tuple[dict[str, str | None], list[str]]:
    observed: dict[str, str | None] = {}
    failed: list[str] = []
    for key in _RECOVERY_CONFIG:
        proc = _git(root, "config", "--get", key)
        value = (proc.stdout or "").strip() if proc.returncode == 0 else None
        observed[key] = value
        if value is None or value.casefold() != "never":
            failed.append(key)
    return observed, failed


def _write_sha_guard_hook(directory: Path) -> None:
    """Install a command-scoped reference-transaction SHA guard.

    ``git branch -d``/``-D`` have no expected-old-value argument.  The
    reference-transaction hook runs with the ref lock held, so resolving the
    branch in the ``prepared`` phase closes the final check/delete race.  The
    hook is enabled only for the delete subprocess via ``core.hooksPath``.
    Callers opt into main ancestry. A safe delete with a preservation pin is
    a review delete and must also prove the exact reviewed subject on main.
    The shared bundle reaper relies on ``branch -d``'s upstream or HEAD check.
    """

    hook = directory / "reference-transaction"
    hook.write_text(
        """#!/bin/sh
state=$1
[ \"$state\" = prepared ] || exit 0
# Use the repository's actual commit graph for the prepared proof.
export GIT_NO_REPLACE_OBJECTS=1 GIT_GRAFT_FILE=/dev/null
unset GIT_SHALLOW_FILE
seen=0
while read -r old new ref
do
    if [ \"$ref\" = \"$WORKBAY_AUTH_REF\" ]; then
        seen=1
    fi
done
[ \"$seen\" -eq 1 ] || exit 91
actual=$(git rev-parse --verify \"$WORKBAY_AUTH_REF^{commit}\") || exit 92
[ \"$actual\" = \"$WORKBAY_AUTH_SHA\" ] || exit 93
if [ -n \"$WORKBAY_AUTH_PIN\" ]; then
    pinned=$(git rev-parse --verify \"$WORKBAY_AUTH_PIN^{commit}\") || exit 94
    [ \"$pinned\" = \"$actual\" ] || exit 94
fi
if [ \"$WORKBAY_AUTH_REQUIRE_MAIN\" = 1 ]; then
    main=$(git show-ref --verify --hash refs/heads/main) || exit 94
    git -c advice.graftFileDeprecated=false merge-base --is-ancestor \"$actual\" \"$main\" || exit 94
    if [ -n \"$WORKBAY_AUTH_PIN\" ]; then
        [ -n \"$WORKBAY_AUTH_REVIEW_SUBJECT\" ] || exit 95
        git -c advice.graftFileDeprecated=false merge-base --is-ancestor \"$WORKBAY_AUTH_REVIEW_SUBJECT\" \"$main\" || exit 95
    fi
fi
if [ -n "$WORKBAY_AUTH_SUPERSESSION_PYTHON" ]; then
    "$WORKBAY_AUTH_SUPERSESSION_PYTHON" -I - <<'PY' || exit 96
import hashlib, json, os, stat, subprocess

def file_matches(path, digest, identity=None):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        observed = os.fstat(handle.fileno())
        if (not stat.S_ISREG(observed.st_mode)
                or (identity is not None and [observed.st_dev, observed.st_ino, observed.st_ctime_ns] != identity)
                or hashlib.file_digest(handle, "sha256").hexdigest() != digest):
            return False
        after = os.lstat(path)
        return os.path.samestat(observed, after) and observed.st_ctime_ns == after.st_ctime_ns

try:
    env = os.environ
    paths = json.loads(env["WORKBAY_AUTH_REMOVED_PATHS"])
    for path in paths:
        try:
            os.lstat(path)
        except FileNotFoundError:
            continue
        raise ValueError("retirement path reappeared")
    if not file_matches(env["WORKBAY_AUTH_REFLOG"], env["WORKBAY_AUTH_REFLOG_SHA"],
                        json.loads(env["WORKBAY_AUTH_REFLOG_ID"])):
        raise ValueError("branch incarnation changed")
    if not file_matches(env["WORKBAY_AUTH_BUNDLE"], env["WORKBAY_AUTH_BUNDLE_SHA"]):
        raise ValueError("recovery bundle changed")
    if subprocess.run(["git", "symbolic-ref", "--quiet", env["WORKBAY_AUTH_PIN"]],
                      stdout=subprocess.DEVNULL).returncode != 1:
        raise ValueError("recovery pin is not a direct ref")
    registry = subprocess.check_output(["git", "worktree", "list", "--porcelain", "-z"]).split(b"\\0")
    if (not registry[0].startswith(b"worktree ")
            or b"branch " + os.fsencode(env["WORKBAY_AUTH_REF"]) in registry
            or any(row.startswith(b"worktree ") and os.path.realpath(os.fsdecode(row[9:])) == paths[0]
                   for row in registry)):
        raise ValueError("checkout registration changed")
except (OSError, ValueError, subprocess.CalledProcessError) as error:
    raise SystemExit(str(error))
PY
fi
""",
        encoding="utf-8",
    )
    hook.chmod(0o700)


def merged_branch_delete_argv(root: Path, branch: str, *, hooks_path: str | None = None) -> list[str]:
    """The ``git branch -d`` argv ``lane_reaping._delete_merged_branch`` actuates.

    The bundle reaper never force-deletes: ``-d`` keeps git's merge check as its
    final fail-safe. With ``hooks_path`` the delete runs under the command-scoped
    :func:`_write_sha_guard_hook` and the branch follows ``--``; without it this is
    the reaper's unguarded form for a bundle that recorded no tip. The caller runs
    the argv through its own reclaim runner and fence.
    """
    if hooks_path is None:
        return ["git", "-C", str(root), "branch", "-d", branch]
    return ["git", "-C", str(root), "-c", f"core.hooksPath={hooks_path}", "branch", "-d", "--", branch]


def _delete_result(
    plan: _AuthorizedDeletePlan,
    *,
    deleted: bool,
    reason: DeleteReason,
    detail: str = "",
    lock_info: dict[str, object] | None = None,
    lock_recovery: dict[str, object] | None = None,
) -> BranchDeleteResult:
    lock_info = lock_info or {}
    return BranchDeleteResult(
        deleted,
        reason,
        plan.short_branch,
        plan.sha,
        reclaim_ref=plan.reclaim_ref,
        detail=detail,
        recovery_config=plan.recovery,
        disposition=plan.disposition,
        rule=plan.rule,
        plan=plan.plan,
        next_action=plan.next_action,
        work_item=plan.work_item,
        branch_action=plan.branch_action,
        requested_disposition=plan.requested_disposition,
        bundle_path=plan.bundle_path,
        lock_path=lock_info.get("lock_path") if isinstance(lock_info.get("lock_path"), str) else None,
        age_s=lock_info.get("age_s") if isinstance(lock_info.get("age_s"), (int, float)) else None,
        size=lock_info.get("size") if isinstance(lock_info.get("size"), int) else None,
        stale_after_s=(
            lock_info.get("stale_after_s") if isinstance(lock_info.get("stale_after_s"), (int, float)) else None
        ),
        lock_recovery=lock_recovery or plan.lock_recovery,
    )


def _lock_failure_detail(proc: subprocess.CompletedProcess[str]) -> str:
    return _detail(proc)[:300]


_RetryResult = TypeVar("_RetryResult", bound=subprocess.CompletedProcess[str] | BranchDeleteResult)


def _retry_after_ref_lock_recovery(
    root: Path,
    proc: subprocess.CompletedProcess[str],
    retry: Callable[[], _RetryResult],
) -> tuple[subprocess.CompletedProcess[str] | _RetryResult, dict[str, object] | None, dict[str, object]]:
    """Recover one recognized lock failure and retry its mutation at most once."""
    from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

    lock_info = lane_reaping._ref_lock_busy_info(root, _detail(proc))
    if lock_info is None:
        return proc, None, {}
    lock_path = lock_info.get("lock_path")
    if isinstance(lock_path, str):
        recovery = lane_reaping.recover_stale_ref_lock(root, lock_path)
    else:
        recovery = {
            "recovered": False,
            "status": "lock_path_unavailable",
            "stale_after_s": lock_info.get("stale_after_s"),
        }
    if recovery.get("recovered") is not True:
        return proc, lock_info, recovery
    retried = retry()
    if isinstance(retried, BranchDeleteResult):
        return retried, None, recovery
    retry_lock_info = lane_reaping._ref_lock_busy_info(root, _detail(retried)) if retried.returncode != 0 else None
    return retried, retry_lock_info, recovery


def _parse_authorized_target(
    lane_id: str,
    branch: str,
    authorized_sha: str,
) -> _AuthorizedDeletePlan | BranchDeleteResult:
    sha = authorized_sha.strip().lower()
    if not _SHA_RE.fullmatch(sha):
        return BranchDeleteResult(False, "invalid_authorized_sha", branch, sha)
    parts = _branch_parts(branch)
    if parts is None:
        return BranchDeleteResult(False, "invalid_branch", branch, sha)
    short_branch, branch_ref = parts
    if short_branch in _INTEGRATION_TARGET_NAMES:
        return BranchDeleteResult(False, "branch_is_integration_target", short_branch, sha)
    reclaim_ref = f"refs/reclaimed/{lane_id}/{sha}"
    return _AuthorizedDeletePlan(
        short_branch=short_branch,
        branch_ref=branch_ref,
        sha=sha,
        reclaim_ref=reclaim_ref,
    )


def _validate_ref_formats(
    root: Path,
    plan: _AuthorizedDeletePlan,
) -> BranchDeleteResult | None:
    valid_branch = _git(root, "check-ref-format", "--branch", plan.short_branch)
    if valid_branch.returncode != 0:
        return _delete_result(plan, deleted=False, reason="invalid_branch", detail=_detail(valid_branch))
    valid_pin = _git(root, "check-ref-format", plan.reclaim_ref)
    if valid_pin.returncode != 0:
        return _delete_result(plan, deleted=False, reason="invalid_reclaim_ref", detail=_detail(valid_pin))
    return None


def _check_recovery_policy(root: Path, plan: _AuthorizedDeletePlan) -> BranchDeleteResult | None:
    recovery, failed_config = _recovery_config(root)
    plan.recovery = recovery
    if failed_config:
        return _delete_result(
            plan,
            deleted=False,
            reason="recovery_precondition_failed",
            detail=", ".join(failed_config),
        )
    return None


def _verify_authorized_tip(root: Path, plan: _AuthorizedDeletePlan) -> BranchDeleteResult | None:
    probe = _resolve_ref(root, plan.branch_ref)
    if probe.probe_failed:
        return _delete_result(plan, deleted=False, reason="probe_failed", detail=_detail(probe.proc))
    if probe.sha is None:
        return _delete_result(plan, deleted=False, reason="branch_missing", detail=_detail(probe.proc))
    if probe.sha != plan.sha:
        return _delete_result(
            plan,
            deleted=False,
            reason="authorized_sha_changed",
            detail=f"actual={probe.sha}",
        )
    return None


def _attach_decision(
    plan: _AuthorizedDeletePlan,
    *,
    decision: lane_disposition.Disposition,
    token: str,
) -> None:
    plan.rule = decision.rule
    plan.plan = token
    plan.next_action = decision.next_action
    plan.work_item = decision.work_item
    plan.branch_action = decision.branch_action
    plan.disposition = decision.kind.value
    plan.force_delete = decision.branch_action == "delete_D_after_pin"


def _decide(
    root: Path,
    plan: _AuthorizedDeletePlan,
    integration_ref: str,
) -> tuple[lane_disposition.Snapshot, lane_disposition.Disposition, str] | BranchDeleteResult:
    """Collect and decide from this call's read-only snapshot."""

    try:
        snapshot = lane_disposition.collect_snapshot(root, plan.short_branch, integration_ref=integration_ref)
        decision = lane_disposition.decide(snapshot)
        token = lane_disposition.plan_token(snapshot, decision)
    except Exception as exc:  # noqa: BLE001 - instrument failure must be typed
        plan.rule = 0
        plan.next_action = "repair_seam"
        plan.work_item = "unknown:probe_failed"
        plan.branch_action = "keep"
        plan.disposition = "probe_failed"
        return _delete_result(
            plan,
            deleted=False,
            reason="probe_failed",
            detail=f"{type(exc).__name__}: {exc}",
        )
    _attach_decision(plan, decision=decision, token=token)
    return snapshot, decision, token


def _apply_disposition_checks(
    plan: _AuthorizedDeletePlan,
    *,
    root: Path | None = None,
    snapshot: lane_disposition.Snapshot,
    decision: lane_disposition.Disposition,
    token: str,
    expected_plan: str | None,
) -> BranchDeleteResult | None:
    """Map the first failed disposition precondition to a typed refusal."""

    if snapshot.probes.get("branch_tip") == "branch_missing":
        return _delete_result(plan, deleted=False, reason="branch_missing", detail=snapshot.probes["branch_tip"])
    if plan.require_unowned_task_target and root is not None:
        for path in snapshot.worktree_paths:
            custody = _task_target_refusal(root, plan, worktree_path=path)
            if custody is not None:
                return custody
    if decision.rule == 0 and decision.reason == "probe_failed:landing_policy":
        return _delete_result(
            plan,
            deleted=False,
            reason="landing_policy_unavailable",
            detail=snapshot.probes.get("landing_policy", "policy_missing_or_invalid"),
        )
    if decision.rule == 0:
        seam = decision.reason.removeprefix("probe_failed:")
        return _delete_result(
            plan,
            deleted=False,
            reason="probe_failed",
            detail=f"{decision.reason}: {snapshot.probes.get(seam, 'probe_error_unavailable')}",
        )
    if snapshot.tip != plan.sha:
        return _delete_result(
            plan,
            deleted=False,
            reason="authorized_sha_changed",
            detail=f"actual={snapshot.tip}",
        )
    if expected_plan is not None and expected_plan != token:
        return _delete_result(
            plan,
            deleted=False,
            reason="snapshot_moved",
            detail=f"expected={expected_plan} actual={token}",
        )
    if decision.branch_action == "keep":
        return _delete_result(plan, deleted=False, reason="live_proof_failed", detail=decision.reason)
    if (plan.binding is not None and plan.binding.payload_branch_kept) or any(
        "branch kept: review_payload_only_over_preserved_base" in str(row.get("notes") or "")
        for row in getattr(snapshot, "lane_rows", ())
    ):
        return _delete_result(plan, deleted=False, reason="live_proof_failed", detail="payload_branch_kept")
    if decision.branch_action == "delete_D_after_pin":
        authorized, detail = terminal_dispose_authorized(decision)
        if not authorized:
            return _delete_result(plan, deleted=False, reason="would_delete", detail=detail)
        return None
    if root is None:
        return _delete_result(plan, deleted=False, reason="probe_failed", detail="main_ancestry_root_required")
    ancestry = _git(root, "merge-base", "--is-ancestor", plan.sha, "refs/heads/main")
    if ancestry.returncode != 0:
        return _delete_result(
            plan,
            deleted=False,
            reason="live_proof_failed" if ancestry.returncode == 1 else "probe_failed",
            detail="main_ancestry_required",
        )
    return None


def review_pin_matches(root: Path, ref: str, tip: str) -> bool:
    """A symbolic alias cannot retain the object after the source ref disappears."""
    return _git(root, "symbolic-ref", "--quiet", ref).returncode == 1 and _resolve_ref(root, ref).sha == tip


def review_preservation_ref(root: Path, branch: str, tip: str) -> str:
    """Reuse an exact legacy pin or select the shared collision-free review ref."""
    key = _short_branch_name(branch).removeprefix("feature/")
    legacy = f"refs/reclaimed/{key}"
    for ref in (legacy, f"{legacy}/{tip}"):
        if review_pin_matches(root, ref, tip):
            return ref
    return f"refs/reclaimed/review-stage-tips/{key}/{tip}"


def pin_review_tip(root: Path, branch: str, tip: str) -> tuple[bool, str]:
    return _pin_preserved_tip(root, branch, tip, review_preservation_ref(root, branch, tip))


def pin_task_finish_tip(root: Path, branch: str, tip: str) -> tuple[bool, str]:
    return _pin_preserved_tip(root, branch, tip, f"refs/reclaimed/task-finish/{tip}")


def _pin_preserved_tip(root: Path, branch: str, tip: str, reclaim_ref: str) -> tuple[bool, str]:
    plan = _AuthorizedDeletePlan(
        short_branch=branch,
        branch_ref=f"refs/heads/{branch}",
        sha=tip,
        reclaim_ref=reclaim_ref,
        review_stage=True,
    )
    refusal = _pin_authorized_tip(root, plan)
    if refusal is not None:
        return False, refusal.detail or refusal.reason
    return True, plan.reclaim_ref


def _pin_authorized_tip(root: Path, plan: _AuthorizedDeletePlan) -> BranchDeleteResult | None:
    pin_probe = _resolve_ref(root, plan.reclaim_ref)
    if pin_probe.probe_failed:
        return _delete_result(
            plan,
            deleted=False,
            reason="probe_failed",
            detail=_detail(pin_probe.proc),
        )
    existing_pin = pin_probe.sha
    if existing_pin is not None and existing_pin != plan.sha:
        return _delete_result(
            plan,
            deleted=False,
            reason="pin_conflict",
            detail=f"actual={existing_pin}",
        )
    expected_pin = existing_pin or _ZERO_SHA
    update_args = ("--no-deref",) if plan.review_stage else ()
    if plan.review_stage and _git(root, "symbolic-ref", "--quiet", plan.reclaim_ref).returncode != 1:
        return _delete_result(plan, deleted=False, reason="pin_conflict", detail="pin_is_symbolic_or_unreadable")
    pin = _git(root, "update-ref", *update_args, plan.reclaim_ref, plan.sha, expected_pin)
    if pin.returncode != 0:
        pin, lock_info, lock_recovery = _retry_after_ref_lock_recovery(
            root,
            pin,
            lambda: _git(root, "update-ref", *update_args, plan.reclaim_ref, plan.sha, expected_pin),
        )
        plan.lock_recovery = lock_recovery
        if lock_info is not None:
            return _delete_result(
                plan,
                deleted=False,
                reason="ref_lock_busy",
                detail=_lock_failure_detail(pin),
                lock_info=lock_info,
                lock_recovery=lock_recovery,
            )
    if pin.returncode != 0:
        return _delete_result(
            plan,
            deleted=False,
            reason="pin_failed",
            detail=_detail(pin) or _detail(pin_probe.proc),
        )
    pin_verify = _resolve_ref(root, plan.reclaim_ref)
    if pin_verify.sha != plan.sha:
        reason: DeleteReason = "probe_failed" if pin_verify.probe_failed else "pin_failed"
        return _delete_result(
            plan,
            deleted=False,
            reason=reason,
            detail=_detail(pin_verify.proc) or f"actual={pin_verify.sha}",
        )
    return None


def _refuse_if_repo_hook(root: Path, plan: _AuthorizedDeletePlan) -> BranchDeleteResult | None:
    """Refuse unless the repository's reference-transaction hook is positively absent.

    The guarded delete overrides ``core.hooksPath``, which hides any repository hook, so only a
    proven absence may proceed. A failed or timed-out location query, or a failed stat, is unknown
    and refuses as a present hook does. The verdict is ``lane_reaping``'s own probe, so the two
    cannot drift; the import is local, as at the other ``lane_reaping`` call sites here.
    """
    from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

    state, detail = lane_reaping._reference_transaction_hook_state(root)
    if state == "absent":
        return None
    reason: DeleteReason = (
        "reference_transaction_hook_present" if state == "present" else "reference_transaction_hook_unknown"
    )
    return _delete_result(plan, deleted=False, reason=reason, detail=detail)


def _recheck_authorized_sha(root: Path, plan: _AuthorizedDeletePlan) -> BranchDeleteResult | None:
    probe = _resolve_ref(root, plan.branch_ref)
    if probe.sha == plan.sha:
        return None
    if probe.probe_failed:
        return _delete_result(plan, deleted=False, reason="probe_failed", detail=_detail(probe.proc))
    return _delete_result(
        plan,
        deleted=False,
        reason="authorized_sha_changed",
        detail=_detail(probe.proc) or f"actual={probe.sha}",
    )


def _act_refusal(plan: _AuthorizedDeletePlan) -> BranchDeleteResult | None:
    """Refuse the delete when the plan's unfenced binding no longer holds, immediately before the act."""
    if plan.binding is None:
        return None
    refusal = reprove_close_authority(plan.binding)
    if not refusal:
        return None
    return _delete_result(plan, deleted=False, reason=refusal, detail=refusal)  # type: ignore[arg-type]


def _task_target_refusal(
    root: Path, plan: _AuthorizedDeletePlan, worktree_path: str | None = None
) -> BranchDeleteResult | None:
    if not plan.require_unowned_task_target:
        return None
    from workbay_orchestrator_mcp.lane_reaping import _live_task_target_refusal  # noqa: PLC0415

    refusal, detail = _live_task_target_refusal(root, plan.short_branch, worktree_path)
    if refusal is None:
        return None
    reason = "refused_owned" if refusal == "refused_owned" else "refused_owner_unknown"
    return _delete_result(plan, deleted=False, reason=reason, detail=detail)


def _run_guarded_branch_delete(
    root: Path, plan: _AuthorizedDeletePlan
) -> subprocess.CompletedProcess[str] | BranchDeleteResult:
    # Recheck on every actuator use, including lock-recovery retries (SECD-03).
    if plan.effect_guard is not None and (hold := plan.effect_guard()):
        return _delete_result(plan, deleted=False, reason="live_proof_failed", detail=hold)
    if plan.review_stage:
        from .review_stage_reap import review_branch_delete_guard  # noqa: PLC0415

        owned, hold, subject = review_branch_delete_guard(root, plan.short_branch, plan.sha)
        if not owned or hold or subject != plan.review_subject:
            return subprocess.CompletedProcess([], 1, "", hold or "review_ownership_changed")
    if plan.force_delete:
        if not plan.recovery_verified or not plan.bundle_path:
            return subprocess.CompletedProcess([], 1, "", "verified_recovery_required")
        if _resolve_ref(root, plan.reclaim_ref).sha != plan.sha:
            return subprocess.CompletedProcess([], 1, "", "recovery_pin_changed")
        verified = _git(root, "bundle", "verify", plan.bundle_path)
        if verified.returncode != 0:
            return subprocess.CompletedProcess([], 1, "", "recovery_bundle_unavailable")
    else:
        ancestry = _git(root, "merge-base", "--is-ancestor", plan.sha, "refs/heads/main")
        if ancestry.returncode != 0:
            return subprocess.CompletedProcess([], 1, "", "main_ancestry_required")
    flag = "-D" if plan.force_delete else "-d"
    with tempfile.TemporaryDirectory(prefix="workbay-reclaim-hook-") as hook_dir:
        hook_path = Path(hook_dir)
        _write_sha_guard_hook(hook_path)
        env = os.environ.copy()
        env["WORKBAY_AUTH_REF"] = plan.branch_ref
        env["WORKBAY_AUTH_SHA"] = plan.sha
        env["WORKBAY_AUTH_PIN"] = plan.reclaim_ref if plan.force_delete or plan.review_stage else ""
        env["WORKBAY_AUTH_REQUIRE_MAIN"] = "" if plan.force_delete else "1"
        env["WORKBAY_AUTH_REVIEW_SUBJECT"] = plan.review_subject
        env["WORKBAY_AUTH_SUPERSESSION_PYTHON"] = sys.executable if plan.supersession_effect else ""
        env.update(plan.supersession_effect)
        # The landing lock still spans this read and Git, including on retry.
        custody = _task_target_refusal(root, plan)
        if custody is not None:
            return custody
        return _git(
            root,
            "-c",
            f"core.hooksPath={hook_path}",
            "branch",
            flag,
            "--",
            plan.short_branch,
            env=env,
        )


def _retry_guarded_branch_delete(
    root: Path, plan: _AuthorizedDeletePlan
) -> subprocess.CompletedProcess[str] | BranchDeleteResult:
    """The lock-recovery retry is another act: re-prove the binding and the hook before it bypasses.

    A refusal returns as a failed delete, as a binding refusal does.
    """
    unbound = _act_refusal(plan)
    if unbound is not None:
        return subprocess.CompletedProcess([], 1, "", unbound.detail)
    hooked = _refuse_if_repo_hook(root, plan)
    if hooked is not None:
        return subprocess.CompletedProcess([], 1, "", hooked.reason)
    return _run_guarded_branch_delete(root, plan)


def _reconcile_failed_delete(
    root: Path,
    plan: _AuthorizedDeletePlan,
    deleted: subprocess.CompletedProcess[str],
    lock_recovery: dict[str, object],
) -> BranchDeleteResult:
    """Keep the command error, exact ref observation and effect attribution separate."""
    current = _resolve_ref(root, plan.branch_ref)
    command_error = _detail(deleted)
    detail = command_error
    if current.probe_failed:
        reason: DeleteReason = "probe_failed"
        detail = f"{command_error}; ref_probe: {_detail(current.proc)}"
    elif current.missing:
        reason = "already_absent"
    elif current.sha != plan.sha:
        reason = "authorized_sha_changed"
    else:
        reason = "delete_failed"
    return replace(
        _delete_result(plan, deleted=False, reason=reason, detail=detail, lock_recovery=lock_recovery),
        observed_ref=current.sha,
        command_error=command_error,
        effect_provenance="external_or_unknown_effect" if current.missing else "unresolved",
    )


def _effect_identity(plan: _AuthorizedDeletePlan) -> list[str | None]:
    return [plan.short_branch, plan.sha, plan.reclaim_ref, plan.requested_disposition]


def _effect_binding(binding: CloseAuthorityBinding) -> dict[str, object]:
    generation = dict(zip(_ROW_GENERATION_KEYS, binding.generation, strict=True)) if binding.generation else None
    if generation is not None and binding.authority is not None:
        # Claim/release changes these two columns itself. All owner, status and
        # close-identity columns must still match, in addition to binding anew.
        generation.pop("reclaim_fence")
        generation.pop("updated_at")
    return {"task_ref": binding.task_ref, "lane_id": binding.lane_id, "generation": generation}


def _effect_receipt_path(root: Path, plan: _AuthorizedDeletePlan) -> Path:
    key = hashlib.sha256(json.dumps(_effect_identity(plan)).encode()).hexdigest()
    located = _git(root, "rev-parse", "--git-path", f"workbay-branch-effects/{key}.json")
    if located.returncode != 0 or not (located.stdout or "").strip():
        raise OSError(f"effect_receipt_location: {_detail(located)}")
    path = Path(located.stdout.strip())
    return path if path.is_absolute() else root / path


def _record_absence(root: Path, plan: _AuthorizedDeletePlan, result: BranchDeleteResult) -> BranchDeleteResult:
    """Atomically retain the settled result, including any fence release receipt."""
    if result.reason != "already_absent" or plan.binding is None:
        return result
    temporary: str | None = None
    try:
        path = _effect_receipt_path(root, plan)
        path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "schema": 1,
            "identity": _effect_identity(plan),
            "binding": _effect_binding(plan.binding),
            "result": asdict(result),
        }
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            temporary = handle.name
            json.dump(record, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except (OSError, TypeError, ValueError) as exc:
        return replace(result, reason="probe_failed", detail=f"{result.detail}; effect_receipt: {exc}")
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)
    return result


def _replay_absence(
    root: Path,
    plan: _AuthorizedDeletePlan,
    *,
    task_ref: str | None,
    lane_id: str,
) -> BranchDeleteResult | None:
    """Replay only recorded external/unknown absence under the same durable owner."""
    try:
        path = _effect_receipt_path(root, plan)
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        if (
            not isinstance(record, dict)
            or record.get("schema") != 1
            or record.get("identity") != _effect_identity(plan)
        ):
            raise ValueError("invalid settled identity")
        result = BranchDeleteResult(**record["result"])
        if (
            result.deleted
            or result.reason != "already_absent"
            or result.observed_ref is not None
            or result.effect_provenance != "external_or_unknown_effect"
            or [result.branch, result.authorized_sha, result.reclaim_ref, result.requested_disposition]
            != _effect_identity(plan)
            or not isinstance(result.command_error, str)
            or not isinstance(result.detail, str)
        ):
            raise ValueError("invalid settled absence")
    except (OSError, KeyError, TypeError, ValueError) as exc:
        return _delete_result(plan, deleted=False, reason="probe_failed", detail=f"effect_receipt: {exc}")
    binding = bind_close_authority(task_ref=task_ref, lane_id=lane_id, branch=plan.short_branch, tip=plan.sha)
    refusal = binding.refusal or reprove_close_authority(binding)
    if not refusal and record.get("binding") != _effect_binding(binding):
        refusal = "row_changed_after_close"
    if refusal:
        return _delete_result(plan, deleted=False, reason=refusal, detail=refusal)  # type: ignore[arg-type]
    current = _resolve_ref(root, plan.branch_ref)
    if current.probe_failed:
        return replace(result, reason="probe_failed", detail=f"{result.detail}; ref_probe: {_detail(current.proc)}")
    if not current.missing:
        return replace(result, reason="authorized_sha_changed", observed_ref=current.sha)
    return result


def _apply_authorized_delete(root: Path, plan: _AuthorizedDeletePlan) -> BranchDeleteResult:
    pinned = _pin_authorized_tip(root, plan)
    if pinned is not None:
        return pinned
    hooked = _refuse_if_repo_hook(root, plan)
    if hooked is not None:
        return hooked
    raced = _recheck_authorized_sha(root, plan)
    if raced is not None:
        return raced
    return _delete_guarded_branch(root, plan)


def _delete_guarded_branch(root: Path, plan: _AuthorizedDeletePlan) -> BranchDeleteResult:
    unbound = _act_refusal(plan)
    if unbound is not None:
        return unbound
    deleted = _run_guarded_branch_delete(root, plan)
    if isinstance(deleted, BranchDeleteResult):
        return deleted
    if deleted.returncode == 0:
        return _delete_result(plan, deleted=True, reason="deleted")
    deleted, lock_info, lock_recovery = _retry_after_ref_lock_recovery(
        root,
        deleted,
        lambda: _retry_guarded_branch_delete(root, plan),
    )
    if lock_recovery:
        plan.lock_recovery = lock_recovery
    if isinstance(deleted, BranchDeleteResult):
        return replace(deleted, lock_recovery=lock_recovery)
    if lock_info is not None:
        return _delete_result(
            plan,
            deleted=False,
            reason="ref_lock_busy",
            detail=_lock_failure_detail(deleted),
            lock_info=lock_info,
            lock_recovery=lock_recovery,
        )
    if deleted.returncode == 0:
        return _delete_result(plan, deleted=True, reason="deleted", lock_recovery=lock_recovery)
    return _reconcile_failed_delete(root, plan, deleted, lock_recovery)


def _apply_force_delete(root: Path, plan: _AuthorizedDeletePlan) -> BranchDeleteResult:
    pinned = _pin_authorized_tip(root, plan)
    if pinned is not None:
        return pinned

    try:
        from workbay_orchestrator_mcp import lane_reaping  # noqa: PLC0415

        bundle = lane_reaping._bundle_before_reap(root, plan.short_branch, retention_count=0)
    except Exception as exc:  # noqa: BLE001 - archive failures must refuse deletion
        return _delete_result(
            plan,
            deleted=False,
            reason="bundle_failed",
            detail=f"{type(exc).__name__}: {exc}",
        )
    if not isinstance(bundle, dict) or bundle.get("ok") is not True:
        if isinstance(bundle, dict) and isinstance(bundle.get("bundle_path"), str):
            plan.bundle_path = bundle["bundle_path"]
        error = bundle.get("error") if isinstance(bundle, dict) else None
        detail = bundle.get("detail") if isinstance(bundle, dict) else None
        message = str(error or "bundle_failed")
        if detail:
            message = f"{message}: {detail}"
        return _delete_result(plan, deleted=False, reason="bundle_failed", detail=message)

    path = bundle.get("bundle_path")
    plan.bundle_path = path if isinstance(path, str) else None
    tip_sha = bundle.get("tip_sha")
    if tip_sha != plan.sha:
        return _delete_result(
            plan,
            deleted=False,
            reason="authorized_sha_changed",
            detail=f"actual={tip_sha}",
        )
    if not plan.bundle_path:
        return _delete_result(plan, deleted=False, reason="bundle_failed", detail="bundle_path_missing")
    listed = _git(root, "bundle", "list-heads", plan.bundle_path)
    if listed.returncode != 0 or not any(line.split()[:1] == [plan.sha] for line in (listed.stdout or "").splitlines()):
        return _delete_result(plan, deleted=False, reason="bundle_failed", detail="list_heads_missing_tip")
    plan.recovery_verified = True

    hooked = _refuse_if_repo_hook(root, plan)
    if hooked is not None:
        return hooked
    raced = _recheck_authorized_sha(root, plan)
    if raced is not None:
        return raced
    return _delete_guarded_branch(root, plan)


def _pre_mutation_guards(
    root: Path,
    plan: _AuthorizedDeletePlan,
) -> BranchDeleteResult | None:
    refused = _validate_ref_formats(root, plan)
    if refused is not None:
        return refused
    refused = _check_recovery_policy(root, plan)
    if refused is not None:
        return refused
    refused = _verify_authorized_tip(root, plan)
    if refused is not None:
        return refused
    return None


def _landing_policy_preflight(plan: _AuthorizedDeletePlan, root: Path) -> BranchDeleteResult | None:
    """Refuse a missing policy before opening the lock file or writing anything."""

    try:
        policy = lane_disposition.load_landing_policy(root)
    except Exception as exc:  # noqa: BLE001 - policy lookup is a typed refusal
        detail = f"{type(exc).__name__}: {exc}"
    else:
        if policy is not None:
            return None
        detail = "policy_missing_or_invalid"
    plan.rule = 0
    plan.next_action = "repair_seam:landing_policy"
    plan.work_item = "unknown:probe_failed:landing_policy"
    plan.branch_action = "keep"
    plan.disposition = "probe_failed"
    return _delete_result(plan, deleted=False, reason="landing_policy_unavailable", detail=detail)


def _review_plan_refusal(root: Path, plan: _AuthorizedDeletePlan) -> BranchDeleteResult | None:
    from .review_stage_reap import review_branch_delete_guard  # noqa: PLC0415
    from .supersession_retirement import supersession_deferral  # noqa: PLC0415

    if hold := supersession_deferral(root, branch=plan.short_branch, tip=plan.sha):
        return _delete_result(plan, deleted=False, reason="live_proof_failed", detail=hold)

    plan.review_stage, hold, plan.review_subject = review_branch_delete_guard(root, plan.short_branch, plan.sha)
    if hold:
        return _delete_result(plan, deleted=False, reason="live_proof_failed", detail=hold)
    if plan.review_stage:
        plan.reclaim_ref = review_preservation_ref(root, plan.short_branch, plan.sha)
    return None


def _delete_supersession_branch(
    root: Path,
    *,
    branch: str,
    tip: str,
    pin: str,
    bundle: Path,
    lane_id: str,
    task_ref: str,
    revalidate: Callable[[], str | None],
    effect: dict[str, str],
) -> BranchDeleteResult:
    """Delete under the registry caller's landing lock and Git's prepared ref guard."""
    plan = _parse_authorized_target(lane_id, branch, tip)
    if isinstance(plan, BranchDeleteResult):
        return plan
    plan.reclaim_ref = pin
    plan.bundle_path = str(bundle)
    plan.force_delete = plan.recovery_verified = True
    plan.effect_guard = revalidate
    plan.supersession_effect = effect
    refusal = _pre_mutation_guards(root, plan) or _refuse_if_repo_hook(root, plan)
    if refusal is not None:
        return refusal
    binding = bind_close_authority(task_ref=task_ref, lane_id=lane_id, branch=branch, tip=tip)
    plan.binding = binding
    refused, result, released = run_under_close_authority(binding, lambda: _delete_guarded_branch(root, plan))
    if result is None:
        return _delete_result(plan, deleted=False, reason="live_proof_failed", detail=refused)
    return replace(result, fence_release=released) if released else result


def _decide_and_act(
    root: Path,
    plan: _AuthorizedDeletePlan,
    *,
    integration_ref: str,
    expected_plan: str | None,
) -> BranchDeleteResult:
    review_hold = _review_plan_refusal(root, plan)
    if review_hold is not None:
        return review_hold
    observed = _decide(root, plan, integration_ref)
    if isinstance(observed, BranchDeleteResult):
        return observed
    snapshot, decision, token = observed
    refused = _apply_disposition_checks(
        plan,
        root=root,
        snapshot=snapshot,
        decision=decision,
        token=token,
        expected_plan=expected_plan,
    )
    if refused is not None:
        return refused
    if decision.branch_action == "delete_d":
        return _apply_authorized_delete(root, plan)
    if plan.review_stage:
        return _delete_result(plan, deleted=False, reason="live_proof_failed", detail="review_tip_requires_safe_delete")
    return _apply_force_delete(root, plan)


def _decide_and_act_bound(
    root: Path,
    plan: _AuthorizedDeletePlan,
    *,
    task_ref: str | None,
    lane_id: str,
    integration_ref: str,
    expected_plan: str | None,
) -> BranchDeleteResult:
    """Decide and act bound to the lane's durable row, under its reclaim fence when it carries close authority.

    The binding is read after the landing lock is held. A fenced act keeps the
    fence through the pin, the bundle and the delete; its release state is
    carried in ``fence_release`` and, when not ``released``, in the detail of a
    successful result too, so no success string erases it [AGT-10].
    """
    custody = _task_target_refusal(root, plan)
    if custody is not None:
        return custody
    binding = bind_close_authority(task_ref=task_ref, lane_id=lane_id, branch=plan.short_branch, tip=plan.sha)
    if binding.refusal:
        return _delete_result(plan, deleted=False, reason=binding.refusal, detail=binding.refusal)  # type: ignore[arg-type]
    plan.binding = binding
    refusal, result, release = run_under_close_authority(
        binding,
        lambda: _decide_and_act(root, plan, integration_ref=integration_ref, expected_plan=expected_plan),
    )
    if refusal or result is None:
        reason = refusal or "reclaim_fence_unavailable"
        return _delete_result(plan, deleted=False, reason=reason, detail=reason)  # type: ignore[arg-type]
    if release:
        detail = result.detail
        if release != "released":
            marker = f"{FENCE_RELEASE_DETAIL}{release}"
            detail = f"{detail};{marker}" if detail else marker
        result = replace(result, fence_release=release, detail=detail)
    return _record_absence(root, plan, result)


def delete_authorized_branch(
    *,
    orchestrator_root: Path | str,
    lane_id: str,
    branch: str,
    authorized_sha: str,
    apply: bool = False,
    task_ref: str | None = None,
    integration_ref: str = "main",
    disposition: BranchDisposition | str | None = None,
    expected_plan: str | None = None,
    _lock_held: bool = False,
    _require_unowned_task_target: bool = False,
) -> BranchDeleteResult:
    """Decide at the reference monitor and retire only the fresh plan."""

    root = Path(orchestrator_root)
    requested_disposition = _load_branch_disposition(disposition)
    parsed = _parse_authorized_target(lane_id, branch, authorized_sha)
    if isinstance(parsed, BranchDeleteResult):
        return replace(parsed, requested_disposition=requested_disposition)
    parsed.requested_disposition = requested_disposition
    parsed.require_unowned_task_target = _require_unowned_task_target
    refused = _pre_mutation_guards(root, parsed)
    if refused is not None:
        if apply and refused.reason == "branch_missing":
            settled = _replay_absence(root, parsed, task_ref=task_ref, lane_id=lane_id)
            if settled is not None:
                return settled
        return refused
    if not apply:
        custody = _task_target_refusal(root, parsed)
        if custody is not None:
            return custody
        review_hold = _review_plan_refusal(root, parsed)
        if review_hold is not None:
            return review_hold
        observed = _decide(root, parsed, integration_ref)
        if isinstance(observed, BranchDeleteResult):
            return observed
        snapshot, decision, token = observed
        refused = _apply_disposition_checks(
            parsed,
            root=root,
            snapshot=snapshot,
            decision=decision,
            token=token,
            expected_plan=expected_plan,
        )
        return refused or _delete_result(parsed, deleted=False, reason="would_delete")

    policy_refusal = _landing_policy_preflight(parsed, root)
    if policy_refusal is not None:
        return policy_refusal
    if _lock_held:
        return _decide_and_act_bound(
            root,
            parsed,
            task_ref=task_ref,
            lane_id=lane_id,
            integration_ref=integration_ref,
            expected_plan=expected_plan,
        )

    from workbay_orchestrator_mcp.orchestration import orchestrator_lanes  # noqa: PLC0415

    with orchestrator_lanes._landing_mutation_lock(root) as acquired:
        if acquired is None:
            return _delete_result(parsed, deleted=False, reason="lock_held", detail="landing_lock_unavailable")
        if acquired is False:
            return _delete_result(parsed, deleted=False, reason="lock_held", detail="mutation_lock_held")
        return _decide_and_act_bound(
            root,
            parsed,
            task_ref=task_ref,
            lane_id=lane_id,
            integration_ref=integration_ref,
            expected_plan=expected_plan,
        )


__all__ = [
    "FENCE_RELEASE_DETAIL",
    "FenceReleaseDetail",
    "BranchDeleteResult",
    "CloseAuthorityBinding",
    "DeleteReason",
    "GIT_SUBPROCESS_TIMEOUT_S",
    "bind_close_authority",
    "delete_authorized_branch",
    "exception_fence_release",
    "fence_release_of",
    "lane_identity_for_branch",
    "remove_worktree_under_close_authority",
    "reprove_close_authority",
    "run_under_close_authority",
]
