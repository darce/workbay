"""Compose durable contracts, observed identity, snapshot and completion receipt.

No mutation policy lives here. Consumers supply the repository and policy action,
branch on the typed result, and keep their own row/custody mutation fences.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import workbay_handoff_mcp as handoff
from workbay_handoff_mcp import lane_completion_attention as journal
from workbay_handoff_mcp.lane_completion_recording import ContractRefusal, RefusalCode, load_contract
from workbay_handoff_mcp.shared_schema import _get_db_connection

from . import lane_completion as lc
from . import lane_completion_snapshot as snapshots
from .lane_completion_receipt import CompletionReceipt, build_completion_receipt


class CompletionGateKind(StrEnum):
    READY = "ready"
    HELD_UNKNOWN = "held_unknown"


@dataclass(frozen=True)
class CompletionGateResult:
    kind: CompletionGateKind
    missing_evidence: tuple[str, ...] = ()
    assessment: lc.CompletionAssessment | None = None
    receipt: CompletionReceipt | None = None
    obligation_fingerprint: str | None = None


def _hold(*missing: str) -> CompletionGateResult:
    return CompletionGateResult(CompletionGateKind.HELD_UNKNOWN, tuple(sorted(set(missing))))


def compose_completion(
    *,
    task_ref: str,
    lane_id: str,
    lane: Mapping[str, Any],
    repo_root: Path,
    phase: lc.CompletionPhase,
    policy_action: str,
) -> CompletionGateResult:
    """Return an assessment/receipt or explicit unknown; never infer N.

    Pins absent from the lane schema come only from the recorded contract.
    Current row identity, cycle, versions and any captured tip must agree.
    Query aliases are ignored, as in the caller's full-row CAS.
    """
    if not isinstance(phase, lc.CompletionPhase):
        return _hold("phase_unsupported")
    try:
        record = load_contract(task_ref, lane_id)
    except ContractRefusal as exc:
        return _hold("contract_absent" if exc.code == RefusalCode.CONTRACT_ABSENT else "contract_invalid")
    except Exception:  # Unreadable/corrupt contracts never become empty scope.
        return _hold("contract_invalid", "read_unavailable")

    try:
        declared = lc.LaneIdentity(**record.contract["identity"])
        with _get_db_connection() as conn:
            current = conn.execute(
                "SELECT * FROM worktree_lanes WHERE task_ref=? AND lane_id=?", (task_ref, lane_id)
            ).fetchone()
            task = conn.execute("SELECT revision FROM handoff_state WHERE task_ref=?", (task_ref,)).fetchone()
        if current is None or task is None:
            return _hold("identity_mismatch", "read_unavailable")
        row = dict(current)
        root = Path(repo_root).resolve()
        cycle = str(lane["cycle_epoch"]) if lane.get("cycle_epoch") is not None else None
        identity = lc.LaneIdentity(
            repository=str(root),
            task_ref=task_ref,
            lane_id=lane_id,
            lane_row_id=lane["id"],
            identity_seq=lane["identity_seq"],
            cycle_id=cycle,
            branch=lane["branch"],
            admission_base=declared.admission_base,
            product_tip=lane.get("branch_tip_sha") or declared.product_tip,
            lane_revision=lane["updated_at"],
            task_revision=str(task["revision"]),
        )
        if identity != declared or any(key in row and row[key] != value for key, value in lane.items()):
            return _hold("identity_mismatch")
        contract = lc.CompletionContract(
            **{
                **record.contract,
                "identity": identity,
                "acceptance_verifiers": tuple(
                    lc.AcceptanceVerifier(**v) for v in record.contract["acceptance_verifiers"]
                ),
            }
        )
        fingerprint = journal.get_obligation_fingerprint(task_ref, lane_id)
    except (KeyError, TypeError, ValueError):
        return _hold("identity_mismatch")
    except Exception:
        return _hold("read_unavailable")

    try:
        deadline = time.monotonic() + 20
        candidate_sha = snapshots._resolve(root, identity.branch, deadline)
        integration_sha = snapshots._resolve(root, record.integration_ref, deadline)
        if candidate_sha != identity.product_tip:
            return _hold("identity_mismatch", "product_tip_changed")
        target = lc.CompletionTarget(
            phase,
            record.integration_ref,
            integration_sha,
            candidate_sha,
            landing_sha=lane.get("landing_commit_sha") or None,
        )
    except Exception:
        return _hold("git_unavailable")

    try:
        snapshot = snapshots.build_snapshot(
            root,
            contract=contract,
            identity=identity,
            target=target,
            phase=phase,
            handoff_reader=handoff,
        )
        assessment = lc.evaluate(snapshot)
        receipt = build_completion_receipt(
            assessment,
            identity,
            record,
            policy_action=policy_action,
            snapshot=snapshot,
        )
        missing = set(receipt["missing_evidence"])
        # The collector deliberately returns unavailable evidence on IO failure.
        # Partial can tolerate absent success receipts, but never a failed read.
        if snapshot.lane.identity_scope.state != lc.EvidenceState.PRESENT:
            missing.update(("snapshot_unavailable", "git_unavailable", "read_unavailable"))
        elif not snapshot.lane.tests.evidence_ids or not snapshot.lane.findings.evidence_ids:
            missing.add("read_unavailable")
        if journal.get_obligation_fingerprint(task_ref, lane_id) != fingerprint:
            missing.add("completion_snapshot_changed")
        io_hold = missing.intersection(("snapshot_unavailable", "read_unavailable", "completion_snapshot_changed"))
        kind = (
            CompletionGateKind.HELD_UNKNOWN
            if assessment.verdict == lc.CompletionVerdict.UNKNOWN or io_hold
            else CompletionGateKind.READY
        )
        return CompletionGateResult(kind, tuple(sorted(missing)), assessment, receipt, fingerprint)
    except Exception:
        return _hold("snapshot_unavailable", "read_unavailable")


def validate_completion_authority(result: CompletionGateResult, *, repo_root: Path) -> None:
    """Recheck obligation/ref pins outside the consumer's write transaction."""
    receipt = result.receipt
    try:
        if result.kind != CompletionGateKind.READY or receipt is None:
            raise ValueError("completion gate has no authority")
        if journal.get_obligation_fingerprint(receipt["task_ref"], receipt["lane_id"]) != result.obligation_fingerprint:
            raise ValueError("completion obligations changed")
        record = load_contract(receipt["task_ref"], receipt["lane_id"])
        deadline = time.monotonic() + 20
        if (
            snapshots._resolve(repo_root, record.contract["identity"]["branch"], deadline) != receipt["candidate_sha"]
            or snapshots._resolve(repo_root, receipt["integration_ref"], deadline) != receipt["integration_sha"]
        ):
            raise ValueError("completion target changed")
    except Exception as exc:
        raise journal.CompletionRefusal(
            journal.CompletionRefusalCode.AUTHORITY_STALE, "completion evidence changed before mutation"
        ) from exc
