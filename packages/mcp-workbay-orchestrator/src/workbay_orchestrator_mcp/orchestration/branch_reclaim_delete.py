"""Fail-closed deletion primitive for queued lane branch reclamation.

The queue/drain owns policy and durable reconciliation.  This module owns the
small Git mutation: assert recovery policy, re-sample a live merge or content
proof, preserve the authorised tip, and delete exactly that tip.  Dry-run is
the default.  ``git branch -d`` is preferred; ``-D`` stays behind a fresh
positive ``lane_branch_reclaimable`` verdict.
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from workbay_orchestrator_mcp.orchestration.lane_reclaim import (
    _INTEGRATION_TARGET_NAMES,
)

_ZERO_SHA = "0" * 40
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
GIT_SUBPROCESS_TIMEOUT_S = 20.0
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
    "delete_failed",
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


@dataclass
class _AuthorizedDeletePlan:
    short_branch: str
    branch_ref: str
    sha: str
    reclaim_ref: str
    recovery: dict[str, str | None] = field(default_factory=dict)
    force_delete: bool = False


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
    try:
        return subprocess.run(
            ["git", "-C", str(root), *args],
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
            ["git", "-C", str(root), *args],
            124,
            stdout,
            stderr or f"git timeout after {timeout_s}s",
        )


def _detail(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stderr or proc.stdout or "").strip()


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


def _installed_reference_transaction_hook(root: Path) -> Path | None:
    proc = _git(root, "rev-parse", "--git-path", "hooks/reference-transaction")
    if proc.returncode != 0 or not (proc.stdout or "").strip():
        return None
    path = Path((proc.stdout or "").strip())
    if not path.is_absolute():
        path = root / path
    return path if path.is_file() and os.access(path, os.X_OK) else None


def _write_sha_guard_hook(directory: Path) -> None:
    """Install a command-scoped reference-transaction SHA guard.

    ``git branch -d``/``-D`` have no expected-old-value argument.  The
    reference-transaction hook runs with the ref lock held, so resolving the
    branch in the ``prepared`` phase closes the final check/delete race.  The
    hook is enabled only for the delete subprocess via ``core.hooksPath``.
    """

    hook = directory / "reference-transaction"
    hook.write_text(
        """#!/bin/sh
state=$1
[ \"$state\" = prepared ] || exit 0
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
""",
        encoding="utf-8",
    )
    hook.chmod(0o700)


def _delete_result(
    plan: _AuthorizedDeletePlan,
    *,
    deleted: bool,
    reason: DeleteReason,
    detail: str = "",
) -> BranchDeleteResult:
    return BranchDeleteResult(
        deleted,
        reason,
        plan.short_branch,
        plan.sha,
        reclaim_ref=plan.reclaim_ref,
        detail=detail,
        recovery_config=plan.recovery,
    )


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


def _authorized_sha_is_merged_ancestor(root: Path, sha: str, integration_ref: str) -> bool:
    proc = _git(root, "merge-base", "--is-ancestor", sha, integration_ref)
    return proc.returncode == 0


def _observed_identity_matches_plan(observed: object, plan: _AuthorizedDeletePlan) -> bool:
    """True only when C6 evaluated the authorized branch tip, not a later lane row."""

    if not isinstance(observed, dict):
        return False
    observed_sha = str(observed.get("branch_sha") or "").strip().lower()
    if observed_sha != plan.sha:
        return False
    parts = _branch_parts(str(observed.get("branch") or ""))
    return parts is not None and parts[0] == plan.short_branch


def _fresh_reclaimable_verdict(
    *,
    root: Path,
    plan: _AuthorizedDeletePlan,
    lane_id: str,
    task_ref: str | None,
    integration_ref: str,
) -> bool:
    if not task_ref:
        return False
    from workbay_orchestrator_mcp.orchestration.lane_reclaim import (  # noqa: PLC0415
        lane_branch_reclaimable,
    )

    verdict = lane_branch_reclaimable(
        orchestrator_root=root,
        task_ref=task_ref,
        lane_id=lane_id,
        integration_ref=integration_ref,
    )
    if verdict.reclaimable is not True:
        return False
    if not _observed_identity_matches_plan(verdict.observed, plan):
        return False
    # Compare-and-set on the live ref named by the queue item, never the lane row.
    probe = _resolve_ref(root, plan.branch_ref)
    return probe.sha == plan.sha


def _require_live_delete_proof(
    root: Path,
    plan: _AuthorizedDeletePlan,
    *,
    lane_id: str,
    task_ref: str | None,
    integration_ref: str,
    apply: bool,
) -> BranchDeleteResult | None:
    if _authorized_sha_is_merged_ancestor(root, plan.sha, integration_ref):
        plan.force_delete = False
        return None
    if not apply:
        # C6/C7 is not a pure function: sampling it writes reclaim pins.
        return _delete_result(plan, deleted=False, reason="live_proof_failed")
    if _fresh_reclaimable_verdict(
        root=root,
        plan=plan,
        lane_id=lane_id,
        task_ref=task_ref,
        integration_ref=integration_ref,
    ):
        plan.force_delete = True
        return None
    return _delete_result(plan, deleted=False, reason="live_proof_failed")


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
    pin = _git(root, "update-ref", plan.reclaim_ref, plan.sha, expected_pin)
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
    existing_hook = _installed_reference_transaction_hook(root)
    if existing_hook is None:
        return None
    return _delete_result(
        plan,
        deleted=False,
        reason="reference_transaction_hook_present",
        detail=str(existing_hook),
    )


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


def _run_guarded_branch_delete(root: Path, plan: _AuthorizedDeletePlan) -> subprocess.CompletedProcess[str]:
    flag = "-D" if plan.force_delete else "-d"
    with tempfile.TemporaryDirectory(prefix="workbay-reclaim-hook-") as hook_dir:
        hook_path = Path(hook_dir)
        _write_sha_guard_hook(hook_path)
        env = os.environ.copy()
        env["WORKBAY_AUTH_REF"] = plan.branch_ref
        env["WORKBAY_AUTH_SHA"] = plan.sha
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
    deleted = _run_guarded_branch_delete(root, plan)
    if deleted.returncode == 0:
        return _delete_result(plan, deleted=True, reason="deleted")
    current = _resolve_ref(root, plan.branch_ref)
    if current.probe_failed:
        return _delete_result(plan, deleted=False, reason="probe_failed", detail=_detail(current.proc) or _detail(deleted))
    reason: DeleteReason = "authorized_sha_changed" if current.sha not in (None, plan.sha) else "delete_failed"
    return _delete_result(plan, deleted=False, reason=reason, detail=_detail(deleted))


def _pre_mutation_guards(
    root: Path,
    plan: _AuthorizedDeletePlan,
    *,
    lane_id: str,
    task_ref: str | None,
    integration_ref: str,
    apply: bool,
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
    return _require_live_delete_proof(
        root,
        plan,
        lane_id=lane_id,
        task_ref=task_ref,
        integration_ref=integration_ref,
        apply=apply,
    )


def delete_authorized_branch(
    *,
    orchestrator_root: Path | str,
    lane_id: str,
    branch: str,
    authorized_sha: str,
    apply: bool = False,
    task_ref: str | None = None,
    integration_ref: str = "main",
) -> BranchDeleteResult:
    """Pin and delete a lane branch only after a live merge or content proof."""

    root = Path(orchestrator_root)
    parsed = _parse_authorized_target(lane_id, branch, authorized_sha)
    if isinstance(parsed, BranchDeleteResult):
        return parsed
    refused = _pre_mutation_guards(
        root,
        parsed,
        lane_id=lane_id,
        task_ref=task_ref,
        integration_ref=integration_ref,
        apply=apply,
    )
    if refused is not None:
        return refused
    if not apply:
        return _delete_result(parsed, deleted=False, reason="would_delete")
    return _apply_authorized_delete(root, parsed)


__all__ = [
    "BranchDeleteResult",
    "DeleteReason",
    "GIT_SUBPROCESS_TIMEOUT_S",
    "delete_authorized_branch",
]
