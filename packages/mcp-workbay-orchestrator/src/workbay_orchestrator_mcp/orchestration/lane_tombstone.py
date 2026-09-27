"""Write first-parent tombstones for lane branches superseded by landed work."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .landing_log import (
    find_carrier,
    format_tombstone,
    scan_first_parent,
    verify_tombstone,
)

_GIT_TIMEOUT_S = 20.0
_SHA40 = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class SupersedeResult:
    """Typed preview, success, or refusal from :func:`supersede`."""

    ok: bool
    reason: str
    branch: str
    superseded_by: str | None
    integration_ref: str
    tip: str | None = None
    trailer: str | None = None
    integration_tip: str | None = None
    tombstone_commit: str | None = None
    detail: str = ""
    proof: dict[str, Any] = field(default_factory=dict)

    @property
    def tombstone(self) -> str | None:
        """Compatibility alias for the formatted trailer line."""
        return self.trailer


def _git(repo: Path, *args: str, timeout_s: float = _GIT_TIMEOUT_S) -> subprocess.CompletedProcess[str]:
    command = ["git", "-C", str(repo), *args]
    try:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = exc.stderr.decode("utf-8", "replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        return subprocess.CompletedProcess(command, 124, "", stderr or f"git timeout after {timeout_s}s")
    except OSError as exc:
        return subprocess.CompletedProcess(command, 127, "", str(exc))


def _detail(proc: subprocess.CompletedProcess[str]) -> str:
    return (proc.stderr or proc.stdout or f"git_failed:{proc.returncode}").strip()


def _local_branch_ref(value: object) -> tuple[str, str] | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if raw.startswith("refs/heads/"):
        short = raw.removeprefix("refs/heads/")
    elif raw.startswith("refs/"):
        return None
    else:
        short = raw
    if not short:
        return None
    return short, f"refs/heads/{short}"


def _failure(
    *,
    reason: str,
    branch: str,
    superseded_by: str | None,
    integration_ref: str,
    tip: str | None = None,
    tombstone: str | None = None,
    integration_tip: str | None = None,
    detail: str = "",
    proof: dict[str, Any] | None = None,
) -> SupersedeResult:
    return SupersedeResult(
        False,
        reason,
        branch,
        superseded_by,
        integration_ref,
        tip,
        tombstone,
        integration_tip,
        detail=detail,
        proof=proof or {},
    )


def _resolve_commit(repo: Path, revision: str) -> tuple[str | None, str | None]:
    proc = _git(repo, "rev-parse", "--verify", "--quiet", "--end-of-options", f"{revision}^{{commit}}")
    if proc.returncode != 0:
        if proc.returncode == 1 and not (proc.stderr or proc.stdout):
            return None, "revision_missing"
        return None, _detail(proc)
    value = (proc.stdout or "").strip().splitlines()
    if len(value) != 1 or not _SHA40.fullmatch(value[0]):
        return None, "malformed_commit_sha"
    return value[0], None


def _ref_is_valid(repo: Path, ref: str) -> tuple[bool | None, str]:
    proc = _git(repo, "check-ref-format", ref)
    if proc.returncode == 0:
        return True, ""
    if proc.returncode == 1:
        return False, _detail(proc)
    return None, _detail(proc)


def _is_first_parent_carrier(repo: Path, carrier_sha: str, integration_ref: str) -> tuple[bool | None, str]:
    result = find_carrier(repo, carrier_sha, main_ref=integration_ref, timeout_s=_GIT_TIMEOUT_S)
    if result.error:
        return None, result.error
    valid = result.contained and result.lineage == "first_parent" and result.carrier == result.commit
    return valid, "" if valid else "carrier_not_on_first_parent_lineage"


def _is_contained(repo: Path, tip: str, integration_ref: str) -> tuple[bool | None, str]:
    result = find_carrier(repo, tip, main_ref=integration_ref, timeout_s=_GIT_TIMEOUT_S)
    if result.error:
        return None, result.error
    return result.contained, ""


def _verified_existing(
    repo: Path,
    *,
    branch: str,
    tip: str,
    integration_ref: str,
) -> tuple[str | None, str | None]:
    scan = scan_first_parent(repo, main_ref=integration_ref, timeout_s=_GIT_TIMEOUT_S)
    if not scan.complete:
        return None, scan.error or "first_parent_scan_incomplete"
    for commit, tombstone in scan.tombstones:
        if (
            tombstone.branch != branch
            or tombstone.tip != tip
            or tombstone.disposition != "superseded"
            or tombstone.kind != "superseded_by_integration"
        ):
            continue
        verdict = verify_tombstone(repo, tombstone, main_ref=integration_ref, current_tip=tip)
        if verdict.failed_check == "probe_failed":
            return None, verdict.detail or "tombstone_probe_failed"
        if verdict.verified:
            return commit, None
    return None, None


def supersede(
    repo: str | Path,
    *,
    branch: str,
    superseded_by: str,
    integration_ref: str,
    apply: bool = False,
    allow_main: bool = False,
) -> SupersedeResult:
    """Preview or record verified supersession of a local branch.

    A successful preview performs read-only Git probes and returns the planned
    trailer plus the evidence that authorizes it. Apply writes one commit and
    advances only the selected local integration branch with compare-and-set.
    """

    root = Path(repo)
    branch_value = branch.strip() if isinstance(branch, str) else ""
    integration_value = integration_ref.strip() if isinstance(integration_ref, str) else ""
    branch_parts = _local_branch_ref(branch_value)
    integration_parts = _local_branch_ref(integration_value)
    if branch_parts is None:
        return _failure(
            reason="invalid_branch",
            branch=branch_value,
            superseded_by=None,
            integration_ref=integration_value,
        )
    if integration_parts is None:
        return _failure(
            reason="invalid_integration_ref",
            branch=branch_parts[0],
            superseded_by=None,
            integration_ref=integration_value,
        )
    branch_name, branch_ref = branch_parts
    integration_name, integration_branch_ref = integration_parts
    if integration_name == "main" and not allow_main:
        return _failure(
            reason="main_ref_requires_allow",
            branch=branch_name,
            superseded_by=None,
            integration_ref=integration_name,
        )

    for label, ref in (("branch", branch_ref), ("integration_ref", integration_branch_ref)):
        valid, ref_error = _ref_is_valid(root, ref)
        if valid is None:
            return _failure(
                reason="probe_failed",
                branch=branch_name,
                superseded_by=None,
                integration_ref=integration_name,
                detail=ref_error,
            )
        if not valid:
            return _failure(
                reason=f"invalid_{label}",
                branch=branch_name,
                superseded_by=None,
                integration_ref=integration_name,
                detail=ref_error,
            )

    tip, tip_error = _resolve_commit(root, branch_ref)
    if tip is None:
        reason = "branch_missing" if tip_error == "revision_missing" else "probe_failed"
        return _failure(
            reason=reason,
            branch=branch_name,
            superseded_by=None,
            integration_ref=integration_name,
            detail=tip_error or "branch_tip_unavailable",
        )
    carrier_sha, carrier_error = _resolve_commit(root, superseded_by)
    if carrier_sha is None:
        reason = "carrier_missing" if carrier_error == "revision_missing" else "probe_failed"
        return _failure(
            reason=reason,
            branch=branch_name,
            superseded_by=None,
            integration_ref=integration_name,
            tip=tip,
            detail=carrier_error or "carrier_unavailable",
        )
    integration_tip, integration_error = _resolve_commit(root, integration_branch_ref)
    if integration_tip is None:
        reason = "integration_ref_missing" if integration_error == "revision_missing" else "probe_failed"
        return _failure(
            reason=reason,
            branch=branch_name,
            superseded_by=carrier_sha,
            integration_ref=integration_name,
            tip=tip,
            detail=integration_error or "integration_tip_unavailable",
        )

    carrier_valid, carrier_detail = _is_first_parent_carrier(root, carrier_sha, integration_branch_ref)
    base_proof: dict[str, Any] = {
        "branch_ref": branch_ref,
        "tip": tip,
        "integration_ref": integration_branch_ref,
        "integration_tip": integration_tip,
        "carrier": carrier_sha,
        "carrier_on_first_parent": carrier_valid,
    }
    if carrier_valid is None:
        return _failure(
            reason="probe_failed",
            branch=branch_name,
            superseded_by=carrier_sha,
            integration_ref=integration_name,
            tip=tip,
            integration_tip=integration_tip,
            detail=carrier_detail,
            proof=base_proof,
        )
    if not carrier_valid:
        return _failure(
            reason="carrier_not_landed",
            branch=branch_name,
            superseded_by=carrier_sha,
            integration_ref=integration_name,
            tip=tip,
            integration_tip=integration_tip,
            detail=carrier_detail,
            proof=base_proof,
        )

    contained, contained_error = _is_contained(root, tip, integration_branch_ref)
    base_proof["branch_tip_contained"] = contained
    if contained is None:
        return _failure(
            reason="probe_failed",
            branch=branch_name,
            superseded_by=carrier_sha,
            integration_ref=integration_name,
            tip=tip,
            integration_tip=integration_tip,
            detail=contained_error,
            proof=base_proof,
        )
    if contained:
        return _failure(
            reason="branch_already_landed",
            branch=branch_name,
            superseded_by=carrier_sha,
            integration_ref=integration_name,
            tip=tip,
            integration_tip=integration_tip,
            detail="branch tip is already contained in the integration ref",
            proof=base_proof,
        )

    try:
        tombstone_line = format_tombstone(
            branch=branch_name,
            tip=tip,
            disposition="superseded",
            kind="superseded_by_integration",
            ref=carrier_sha,
        )
    except ValueError as exc:
        return _failure(
            reason="invalid_branch",
            branch=branch_name,
            superseded_by=carrier_sha,
            integration_ref=integration_name,
            tip=tip,
            integration_tip=integration_tip,
            detail=str(exc),
            proof=base_proof,
        )

    existing, existing_error = _verified_existing(
        root,
        branch=branch_name,
        tip=tip,
        integration_ref=integration_branch_ref,
    )
    if existing_error:
        return _failure(
            reason="probe_failed",
            branch=branch_name,
            superseded_by=carrier_sha,
            integration_ref=integration_name,
            tip=tip,
            tombstone=tombstone_line,
            integration_tip=integration_tip,
            detail=existing_error,
            proof=base_proof,
        )
    if existing:
        base_proof["existing_tombstone_commit"] = existing
        return SupersedeResult(
            True,
            "already_recorded",
            branch_name,
            carrier_sha,
            integration_name,
            tip,
            tombstone_line,
            integration_tip,
            existing,
            proof=base_proof,
        )
    if not apply:
        return SupersedeResult(
            True,
            "would_record",
            branch_name,
            carrier_sha,
            integration_name,
            tip,
            tombstone_line,
            integration_tip,
            proof=base_proof,
        )

    commit = _git(
        root,
        "commit-tree",
        f"{integration_tip}^{{tree}}",
        "-p",
        integration_tip,
        "-m",
        f"Record supersession of {branch_name}",
        "-m",
        tombstone_line,
    )
    new_tip = (commit.stdout or "").strip()
    if commit.returncode != 0 or not _SHA40.fullmatch(new_tip):
        return _failure(
            reason="probe_failed",
            branch=branch_name,
            superseded_by=carrier_sha,
            integration_ref=integration_name,
            tip=tip,
            tombstone=tombstone_line,
            integration_tip=integration_tip,
            detail=_detail(commit) or "commit_tree_failed",
            proof=base_proof,
        )

    update = _git(root, "update-ref", integration_branch_ref, new_tip, integration_tip)
    if update.returncode != 0:
        detail = _detail(update)
        reason = "cas_miss" if "expected" in detail.casefold() else "probe_failed"
        return _failure(
            reason=reason,
            branch=branch_name,
            superseded_by=carrier_sha,
            integration_ref=integration_name,
            tip=tip,
            tombstone=tombstone_line,
            integration_tip=integration_tip,
            detail=detail,
            proof={**base_proof, "planned_commit": new_tip},
        )

    return SupersedeResult(
        True,
        "recorded",
        branch_name,
        carrier_sha,
        integration_name,
        tip,
        tombstone_line,
        integration_tip,
        new_tip,
        proof={**base_proof, "new_integration_tip": new_tip},
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Record a verified lane supersession tombstone.")
    parser.add_argument("--branch", required=True)
    parser.add_argument("--superseded-by", required=True, help="landed carrier commit SHA or revision")
    parser.add_argument("--integration-ref", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--allow-main", action="store_true")
    args = parser.parse_args(argv)
    result = supersede(
        Path.cwd(),
        branch=args.branch,
        superseded_by=args.superseded_by,
        integration_ref=args.integration_ref,
        apply=args.apply,
        allow_main=args.allow_main,
    )
    print(json.dumps(asdict(result), sort_keys=True))
    return 0 if result.ok else 2


if __name__ == "__main__":
    sys.exit(main())
