"""Reference monitor for retiring local lane branches.

Every call decides from a fresh ``lane_disposition`` snapshot. Apply repeats
that decision under the landing lock, and a force deletion pins and bundles
the authorized tip before running ``git branch -D``.
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal

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
    "snapshot_moved",
    "lock_held",
    "bundle_failed",
    "landing_policy_unavailable",
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


@dataclass
class _AuthorizedDeletePlan:
    short_branch: str
    branch_ref: str
    sha: str
    reclaim_ref: str
    recovery: dict[str, str | None] = field(default_factory=dict)
    force_delete: bool = False
    disposition: str | None = None
    rule: int | None = None
    plan: str | None = None
    next_action: str | None = None
    work_item: str | None = None
    branch_action: str | None = None
    requested_disposition: str | None = None
    bundle_path: str | None = None


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
        disposition=plan.disposition,
        rule=plan.rule,
        plan=plan.plan,
        next_action=plan.next_action,
        work_item=plan.work_item,
        branch_action=plan.branch_action,
        requested_disposition=plan.requested_disposition,
        bundle_path=plan.bundle_path,
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
    snapshot: lane_disposition.Snapshot,
    decision: lane_disposition.Disposition,
    token: str,
    expected_plan: str | None,
) -> BranchDeleteResult | None:
    """Map the first failed disposition precondition to a typed refusal."""

    if snapshot.probes.get("branch_tip") == "branch_missing":
        return _delete_result(plan, deleted=False, reason="branch_missing", detail=snapshot.probes["branch_tip"])
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
    if decision.branch_action == "delete_D_after_pin":
        if not _terminal_dispose_classification_enabled():
            return _delete_result(plan, deleted=False, reason="would_delete", detail="terminal_dispose_disabled")
        if decision.kind.value not in _terminal_dispose_apply_values():
            return _delete_result(
                plan, deleted=False, reason="would_delete", detail="terminal_dispose_apply_not_allowed"
            )
    return None


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
        return _delete_result(
            plan, deleted=False, reason="probe_failed", detail=_detail(current.proc) or _detail(deleted)
        )
    reason: DeleteReason = "authorized_sha_changed" if current.sha not in (None, plan.sha) else "delete_failed"
    return _delete_result(plan, deleted=False, reason=reason, detail=_detail(deleted))


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
        return _delete_result(
            plan, deleted=False, reason="probe_failed", detail=_detail(current.proc) or _detail(deleted)
        )
    reason: DeleteReason = "authorized_sha_changed" if current.sha not in (None, plan.sha) else "delete_failed"
    return _delete_result(plan, deleted=False, reason=reason, detail=_detail(deleted))


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


def _decide_and_act(
    root: Path,
    plan: _AuthorizedDeletePlan,
    *,
    integration_ref: str,
    expected_plan: str | None,
) -> BranchDeleteResult:
    observed = _decide(root, plan, integration_ref)
    if isinstance(observed, BranchDeleteResult):
        return observed
    snapshot, decision, token = observed
    refused = _apply_disposition_checks(
        plan,
        snapshot=snapshot,
        decision=decision,
        token=token,
        expected_plan=expected_plan,
    )
    if refused is not None:
        return refused
    if decision.branch_action == "delete_d":
        return _apply_authorized_delete(root, plan)
    return _apply_force_delete(root, plan)


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
) -> BranchDeleteResult:
    """Decide at the reference monitor and retire only the fresh plan."""

    root = Path(orchestrator_root)
    requested_disposition = _load_branch_disposition(disposition)
    parsed = _parse_authorized_target(lane_id, branch, authorized_sha)
    if isinstance(parsed, BranchDeleteResult):
        return replace(parsed, requested_disposition=requested_disposition)
    parsed.requested_disposition = requested_disposition
    refused = _pre_mutation_guards(root, parsed)
    if refused is not None:
        return refused
    if not apply:
        observed = _decide(root, parsed, integration_ref)
        if isinstance(observed, BranchDeleteResult):
            return observed
        snapshot, decision, token = observed
        refused = _apply_disposition_checks(
            parsed,
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
        return _decide_and_act(root, parsed, integration_ref=integration_ref, expected_plan=expected_plan)

    from workbay_orchestrator_mcp.orchestration import orchestrator_lanes  # noqa: PLC0415

    with orchestrator_lanes._landing_mutation_lock(root) as acquired:
        if acquired is None:
            return _delete_result(parsed, deleted=False, reason="lock_held", detail="landing_lock_unavailable")
        if acquired is False:
            return _delete_result(parsed, deleted=False, reason="lock_held", detail="mutation_lock_held")
        return _decide_and_act(root, parsed, integration_ref=integration_ref, expected_plan=expected_plan)


__all__ = [
    "BranchDeleteResult",
    "DeleteReason",
    "GIT_SUBPROCESS_TIMEOUT_S",
    "delete_authorized_branch",
]
