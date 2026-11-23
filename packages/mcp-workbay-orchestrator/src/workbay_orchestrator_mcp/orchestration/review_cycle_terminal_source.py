"""Read the native close producer's terminal source for an absent branch."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from workbay_handoff_mcp.import_export import registration_history
from workbay_handoff_mcp.shared_schema import _open_bounded_roster_read_connection
from workbay_protocol.resource_lifecycle import (
    EvidenceRef,
    GitSource,
    LifecycleEvent,
    Registration,
    WorkBayIdentity,
    canonical_digest,
    event_identity,
)

from . import review_cycle_dispatch as cycle
from .lane_close_observation import _same, unique_json
from .resource_events import resource_key
from .review_ratchet import _same_worktree, _short_branch, review_source_ref

_OID = re.compile(r"[0-9a-f]{40}")
_IDENTITY = ("id", "task_ref", "lane_id", "identity_seq", "branch", "worktree_path")
_CLOSE = (*_IDENTITY, "cycle_epoch", "status", "branch_tip_sha", "landing_commit_sha")


@dataclass(frozen=True)
class TerminalSourceWitness:
    status: Literal["landed", "empty", "unlanded", "unknown"]
    tip: str | None = None
    operation_id: str | None = None
    reason: str | None = None


def _native_close_source(row: sqlite3.Row, lane: dict) -> GitSource:
    """Verify the snapshot, R3 event and registration retained by one close."""
    event = LifecycleEvent.model_validate_json(row["payload_json"])
    registration = Registration.model_validate_json(row["registration_json"])
    operation = row["operation_id"]
    digest = "sha256:" + hashlib.sha256(row["snapshot_json"].encode()).hexdigest()
    if not re.fullmatch(r"lane:(?:[0-9a-f]{32}|[0-9a-f]{64})", operation):
        raise ValueError("terminal_close_evidence_invalid")
    ref = "sqlite:lane-mutation/" + operation.split(":", 1)[1]
    evidence = (EvidenceRef(ref=ref, digest=digest),)
    if not (
        row["snapshot_ref"] == event.transaction_ref == ref
        and row["snapshot_digest"] == digest
        and event.evidence == registration.evidence == evidence
        and row["payload_digest"] == canonical_digest(event)
        and row["registration_digest"] == canonical_digest(registration)
        and event.operation_id == operation
        and event.phase == "registered"
        and event.event_id == row["event_id"] == event_identity(event.resource, operation, event.phase)
        and row["synthetic_tag"] is None
        and event.synthetic_tag is None
        and event.resource.resource_kind == "git_worktree"
        and _same_worktree(event.resource.identity, lane["worktree_path"])
        and row["resource_key"] == resource_key(event.resource)
        and row["owner_repository"] == event.resource.owner.repository
        and event.correlation.workbay == WorkBayIdentity(task=lane["task_ref"], lane=lane["lane_id"])
        and event.correlation.orca is None
        and event.policy_version == "consumer-retention-v1"
        and registration.operation == "withdraw_materialization"
        and all(
            getattr(event, key) == getattr(registration, key)
            for key in ("resource", "source", "policy_version", "correlation")
        )
        and isinstance(event.source, GitSource)
    ):
        raise ValueError("terminal_close_evidence_invalid")
    return event.source


def _retained_terminal_tip(db_path: Path, lane: dict) -> tuple[GitSource, str]:
    expected = {key: lane[key] for key in _CLOSE}
    with closing(_open_bounded_roster_read_connection(db_path, busy_timeout_ms=50)) as conn:
        conn.execute("BEGIN")
        rows = registration_history(conn, lane["task_ref"])
        if not any(_same(row, expected) for row in rows):
            raise ValueError("terminal_registration_changed")
        events = conn.execute(
            """SELECT e.*, p.payload_json AS snapshot_json, p.digest AS snapshot_digest, p.ref AS snapshot_ref
            FROM resource_events e JOIN lane_resource_producer_evidence p USING(operation_id)
            WHERE e.wb_task=? AND e.wb_lane=? ORDER BY e.id""",
            (lane["task_ref"], lane["lane_id"]),
        ).fetchall()

    source, operation = None, None
    for row in events:
        snapshot = json.loads(row["snapshot_json"], object_pairs_hook=unique_json)
        if snapshot["operation"] != "close_lane":
            continue
        closed = snapshot["lane"]
        if closed["id"] != lane["id"]:
            continue
        if not _same(closed, expected) or any(snapshot.get(key) != lane[key] for key in ("task_ref", "lane_id")):
            # A second closure of this row could belong to a different opening.
            raise ValueError("terminal_close_incarnation_ambiguous")
        observed = _native_close_source(row, lane)
        if source is not None and source != observed:
            raise ValueError("terminal_close_sources_conflict")
        if source is None:
            source, operation = observed, row["operation_id"]
    if source is None or operation is None:
        raise ValueError("terminal_close_evidence_missing")
    return source, operation


def _git(repo: Path, *args: str) -> str:
    result = cycle._subject_probe(repo, "--no-replace-objects", "-c", "advice.graftFileDeprecated=true", *args)
    if result.returncode or result.stderr:
        raise ValueError("terminal_source_git_unproven")
    return result.stdout.strip()


def _resolve_commit(repo: Path, value: str) -> str:
    return _git(repo, "rev-parse", "--verify", "--end-of-options", value + "^{commit}")


def _is_ancestor(repo: Path, older: str, newer: str) -> bool:
    result = cycle._subject_probe(
        repo, "--no-replace-objects", "-c", "advice.graftFileDeprecated=true",
        "merge-base", "--is-ancestor", "--", older, newer,
    )
    returncode: int = result.returncode
    if returncode not in {0, 1} or result.stderr:
        raise ValueError("terminal_source_git_unproven")
    return returncode == 0


def _require_ancestor(repo: Path, older: str, newer: str, *, reason: str = "terminal_source_git_unproven") -> None:
    if not _is_ancestor(repo, older, newer):
        raise ValueError(reason)


def terminal_source_witness(
    repo: Path, db_path: Path, receipt: dict, owner: cycle._ReceiptHolder, proof: dict
) -> TerminalSourceWitness:
    """Resolve the terminal tip for a previously authenticated original receipt.

    The caller owns reservation authentication, source-time scope, and the
    admission decision. ``empty`` means all terminal source commits are in
    the incoming integration base. ``landed`` requires the witnessed tip to
    reach the native landing and that landing to reach the named integration.
    ``unlanded`` preserves a verified closed tip outside the incoming base for
    the caller's path comparison. All three verify that the incoming Git
    source inherits no holder commit outside its integration base.
    """
    lane = owner.lane
    if lane is None or owner.mismatch or owner.owner_unproven or owner.competing:
        return TerminalSourceWitness("unknown", reason="terminal_issuer_unproven")
    try:
        branch = _short_branch(receipt["source_branch"])
        head, tree = receipt["source_head"], receipt["source_tree"]
        base, incoming = proof["base_sha"], proof["tip_sha"]
        integration = _short_branch(proof["integration_branch"])
        identity_seq = lane["identity_seq"]
        # A pre-sequence registration can still have a producer-stamped close
        # epoch. Its original issuer must already be authenticated by the caller.
        if (
            not isinstance(branch, str)
            or not integration
            or branch == integration
            or _short_branch(lane["branch"]) != branch
            or lane["branch_tip_sha"] is not None
            or (identity_seq is not None and (type(identity_seq) is not int or identity_seq <= 0))
            or any(type(lane[key]) is not int or lane[key] <= 0 for key in ("id", "cycle_epoch"))
        ):
            raise ValueError("terminal_issuer_unproven")
        if lane["status"] not in {"closed", "merged"}:
            return TerminalSourceWitness("unknown", reason="terminal_close_evidence_missing")
        ref, integration_ref = f"refs/heads/{branch}", f"refs/heads/{integration}"
        for name in (ref, integration_ref):
            _git(repo, "check-ref-format", name)
        if cycle._ref_exists(repo, ref):
            raise ValueError("terminal_source_ref_present")
        source, operation = _retained_terminal_tip(db_path, lane)
        tip = source.revision
        if (
            not all(
                isinstance(oid, str) and _OID.fullmatch(oid) for oid in (head, tree, base, incoming, tip, source.tree)
            )
            or _git(repo, "rev-parse", "--is-shallow-repository") != "false"
            or any(_resolve_commit(repo, oid) != oid for oid in (head, base, incoming, tip))
            or _git(repo, "rev-parse", "--verify", head + "^{tree}") != tree
            or _git(repo, "rev-parse", "--verify", tip + "^{tree}") != source.tree
        ):
            raise ValueError("terminal_source_git_unproven")
        _require_ancestor(repo, head, tip)
        integration_tip = _resolve_commit(repo, integration_ref)
        _require_ancestor(repo, base, integration_tip)
        landing = lane["landing_commit_sha"]
        if lane["status"] == "merged":
            if not isinstance(landing, str) or not _OID.fullmatch(landing) or _resolve_commit(repo, landing) != landing:
                raise ValueError("terminal_landing_unproven")
            _require_ancestor(repo, tip, landing, reason="terminal_landing_unproven")
            _require_ancestor(repo, landing, integration_tip, reason="terminal_landing_unproven")
            status: Literal["landed", "empty", "unlanded"] = "landed"
        else:
            if landing is not None:
                raise ValueError("terminal_landing_unproven")
            status = "empty" if _is_ancestor(repo, tip, base) else "unlanded"

        for candidate in {incoming, _resolve_commit(repo, review_source_ref(repo))}:
            _require_ancestor(repo, base, candidate)
            _require_ancestor(repo, head, candidate)
            # Git returns every maximal shared ancestor. Checking the terminal
            # tip alone would miss a partial inheritance through an earlier commit.
            shared = _git(repo, "merge-base", "--all", tip, candidate).splitlines()
            if not shared or any(not _OID.fullmatch(commit) for commit in shared):
                raise ValueError("terminal_source_git_unproven")
            for commit in shared:
                _require_ancestor(repo, commit, base, reason="terminal_source_inherited_outside_base")
        if cycle._ref_exists(repo, ref):
            raise ValueError("terminal_source_ref_present")
        return TerminalSourceWitness(status, tip, operation)
    except sqlite3.Error:
        return TerminalSourceWitness("unknown", reason="terminal_close_evidence_unavailable")
    except (KeyError, TypeError, ValueError, OSError) as exc:
        reason = str(exc)
        if not re.fullmatch(r"(?:terminal|review_cycle)_[a-z_]+", reason):
            reason = "terminal_close_evidence_invalid"
        return TerminalSourceWitness("unknown", reason=reason)
