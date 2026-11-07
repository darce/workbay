"""Deterministic lane-branch + worktree materialization for dispatch.

``ensure_lane_worktree`` can rebuild a checkout only when the lane branch
already exists in the primary repository. This module is the missing
pre-flight: create the branch at the manifest ``base_sha`` when absent,
verify ancestry when present, then hand off to ``ensure_lane_worktree``.

Manifest + ``worktree_lanes`` row are the system of record; git state is a
derived view that is reconciled, never guessed (DDIA). Typed refusals make
the safe path the only path (Release It!). Dry-run is the default; apply is
opt-in. Never checkout in the primary repo. Never delete. Never force-move
a branch.

Citations
---------
``[Release It!]`` / make the safe path the only path
    Short, unknown, or off-base SHAs refuse with a named kind. There is no
    "just create it anyway" branch.

``[DDIA]`` / derived state vs source of record
    Branch name and base come from the manifest; worktree path from the
    ``worktree_lanes`` row when present. Git is updated to match, not the
    other way around.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from workbay_orchestrator_mcp.lanes_support import TERMINAL_LANE_STATUSES
from workbay_orchestrator_mcp.orchestration import worktree_root

_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_GIT_TIMEOUT_SECONDS = 30

REFUSAL_NO_MANIFEST_ENTRY = "no_manifest_entry"
REFUSAL_NO_ROW = "no_row"
REFUSAL_EMPTY_WORKTREE_PATH = "empty_worktree_path"
REFUSAL_SHORT_BASE_SHA = "short_base_sha"
REFUSAL_BASE_SHA_UNKNOWN = "base_sha_unknown"
REFUSAL_BRANCH_NOT_FROM_BASE = "branch_not_from_base"
REFUSAL_BRANCH_CREATE_FAILED = "branch_create_failed"
REFUSAL_GIT_TIMEOUT = "git_timeout"
REFUSAL_TERMINAL_LANE = "terminal_lane"

BRANCH_CREATED = "created"
BRANCH_PRESENT = "present"
BRANCH_PLANNED = "planned"
BRANCH_REFUSED = "refused"

WORKTREE_CREATED = "created"
WORKTREE_PRESENT = "present"
WORKTREE_PLANNED = "planned"
WORKTREE_SKIPPED = "skipped"

LoadManifest = Callable[[str], Mapping[str, Any]]
GetRow = Callable[[str, str], Mapping[str, Any] | None]


class RunGit(Protocol):
    def __call__(
        self,
        args: Sequence[str],
        *,
        cwd: Path | str,
    ) -> subprocess.CompletedProcess[str]: ...


class EnsureWorktree(Protocol):
    def __call__(
        self,
        *,
        primary_repo: Path | str,
        worktree_path: Path | str,
        branch: str,
        lane_id: str = "",
        task_ref: str = "",
    ) -> Any: ...


@dataclass(frozen=True)
class MaterializeResult:
    """Typed outcome of :func:`materialize_lane`.

    ``branch_outcome`` is one of ``created``, ``present``, ``planned``,
    ``refused``. ``worktree_outcome`` is ``planned`` / ``present`` /
    ``created`` / ``skipped``, or the ``EnsureLaneWorktreeResult.outcome``
    string when ensure refuses. ``applied`` is True only when *apply* ran
    far enough to mutate git or invoke ensure.
    """

    lane_id: str
    branch: str
    base_sha: str
    worktree_path: str
    branch_outcome: str
    worktree_outcome: str
    refusal_kind: str | None
    detail: str
    applied: bool


def _text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def _row_is_terminal_or_landed(row: Mapping[str, Any]) -> bool:
    """True when durable lane state proves this row must not be rematerialized."""
    status = _text(row.get("status")).lower()
    if status in TERMINAL_LANE_STATUSES:
        return True
    landing_sha = row.get("landing_commit_sha")
    if landing_sha is None:
        return False
    if isinstance(landing_sha, str):
        return bool(landing_sha.strip())
    # A malformed non-null landing value is unknown durable state. Refuse
    # reconciliation rather than recreating a branch from stale manifest data.
    return True


def _resolve_worktree_path(raw: str, *, root: Path) -> Path:
    path = Path(worktree_root.expand_lane_path(raw, orchestrator_root=root)).expanduser()
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def _paths_match(left: str, right: str) -> bool:
    try:
        return Path(left).expanduser().resolve() == Path(right).expanduser().resolve()
    except OSError:
        return os.path.normpath(left) == os.path.normpath(right)


def _cmd_tokens(cmd: object) -> list[str]:
    if isinstance(cmd, str):
        return [cmd]
    if isinstance(cmd, Sequence) and not isinstance(cmd, (bytes, bytearray)):
        return [str(part) for part in cmd]
    return []


def _refuse_git_timeout(
    *,
    lane_id: str,
    exc: subprocess.TimeoutExpired,
    args: Sequence[str] | None = None,
    branch: str = "",
    base_sha: str = "",
    worktree_path: str = "",
) -> MaterializeResult:
    fallback = ["git", *[str(part) for part in args]] if args is not None else ["git"]
    cmd = _cmd_tokens(exc.cmd) or fallback
    timeout = exc.timeout if exc.timeout is not None else _GIT_TIMEOUT_SECONDS
    detail = f"git timed out after {timeout}s: {cmd}"
    verbs = cmd[1:] if cmd[:1] == ["git"] else cmd
    if branch and verbs[:1] == ["branch"]:
        detail = f"{detail}; leftover branch {branch!r} was not deleted"
    return _refuse(
        lane_id=lane_id,
        kind=REFUSAL_GIT_TIMEOUT,
        detail=detail,
        branch=branch,
        base_sha=base_sha,
        worktree_path=worktree_path,
    )


def _default_run_git(args: Sequence[str], *, cwd: Path | str) -> subprocess.CompletedProcess[str]:
    cmd = ["git", *list(args)]
    try:
        return subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
            timeout=_GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        # Call sites convert this into REFUSAL_GIT_TIMEOUT; keep cmd + timeout.
        raise subprocess.TimeoutExpired(
            cmd=_cmd_tokens(exc.cmd) or cmd,
            timeout=exc.timeout if exc.timeout is not None else _GIT_TIMEOUT_SECONDS,
            output=exc.output,
            stderr=exc.stderr,
        ) from exc


def _default_load_manifest(task_ref: str) -> Mapping[str, Any]:
    from workbay_orchestrator_mcp.orchestration.lane_manifest import load_manifest

    return load_manifest(task_ref)


def _default_get_row(task_ref: str, lane_id: str) -> Mapping[str, Any] | None:
    """Look up the ``worktree_lanes`` row; None when missing or unreadable."""
    try:
        from workbay_orchestrator_mcp.lanes import list_worktree_lanes
    except ImportError:
        return None
    offset = 0
    limit = 100
    for _ in range(10_000):
        try:
            listed = list_worktree_lanes(task_ref=task_ref, status="all", limit=limit, offset=offset)
        except Exception:  # noqa: BLE001 — unreadable row store is a missing row
            return None
        if isinstance(listed, str):
            return None
        if not isinstance(listed, dict) or listed.get("ok") is not True:
            return None
        lanes = listed.get("lanes")
        if not isinstance(lanes, list):
            return None
        for row in lanes:
            if isinstance(row, dict) and str(row.get("lane_id") or "") == lane_id:
                return row
        if listed.get("has_more") is not True or not lanes:
            return None
        offset += len(lanes)
    return None


def _default_ensure_worktree(**kwargs: Any) -> Any:
    from workbay_orchestrator_mcp.orchestration.lane_worktree import ensure_lane_worktree

    return ensure_lane_worktree(**kwargs)


def _lane_entry(manifest: Mapping[str, Any] | object, lane_id: str) -> Mapping[str, Any] | None:
    if not isinstance(manifest, Mapping):
        return None
    lanes = manifest.get("lanes")
    if not isinstance(lanes, Mapping):
        return None
    entry = lanes.get(lane_id)
    return entry if isinstance(entry, Mapping) else None


def _registered_worktree_on_branch(
    *,
    run_git: RunGit,
    root: Path,
    worktree_path: str,
    branch: str,
) -> bool:
    if not worktree_path:
        return False
    worktree_list = ["worktree", "list", "--porcelain"]
    try:
        proc = run_git(worktree_list, cwd=root)
    except subprocess.TimeoutExpired as exc:
        raise subprocess.TimeoutExpired(
            cmd=_cmd_tokens(exc.cmd) or ["git", *worktree_list],
            timeout=exc.timeout if exc.timeout is not None else _GIT_TIMEOUT_SECONDS,
            output=exc.output,
            stderr=exc.stderr,
        ) from exc
    if proc.returncode != 0:
        return False
    want_branch = f"refs/heads/{branch}"
    current_path: str | None = None
    current_branch: str | None = None

    def _flush() -> bool:
        return bool(
            current_path is not None and _paths_match(current_path, worktree_path) and current_branch == want_branch
        )

    for raw in (proc.stdout or "").split("\n"):
        line = raw[:-1] if raw.endswith("\r") else raw
        if line.startswith("worktree "):
            if _flush():
                return True
            current_path = line[len("worktree ") :]
            current_branch = None
        elif line.startswith("branch "):
            current_branch = line[len("branch ") :]
        elif line == "":
            if _flush():
                return True
            current_path = None
            current_branch = None
    return _flush()


def _refuse(
    *,
    lane_id: str,
    kind: str,
    detail: str,
    branch: str = "",
    base_sha: str = "",
    worktree_path: str = "",
) -> MaterializeResult:
    return MaterializeResult(
        lane_id=lane_id,
        branch=branch,
        base_sha=base_sha,
        worktree_path=worktree_path,
        branch_outcome=BRANCH_REFUSED,
        worktree_outcome=WORKTREE_SKIPPED,
        refusal_kind=kind,
        detail=detail,
        applied=False,
    )


class _AdmissionRefused(RuntimeError):
    def __init__(self, reason: str, detail: str):
        self.reason = reason
        super().__init__(detail)


def _production_history(
    binding: Any,
    admission_id: str,
    *,
    historical: bool = False,
    allow_open: bool = False,
) -> dict[str, Any]:
    """Authenticate a bounded, coherent collection; partial history cannot link."""
    from workbay_handoff_mcp import lane_completion_production as production

    def collect() -> dict[str, Any]:
        data = production.get_lane_production_history(binding, admission_id, historical=historical)["data"]
        rows = list(data["history"])
        if data["next_cursor"] is not None:
            following = production.get_lane_production_history(
                binding, admission_id, historical=historical, cursor=data["next_cursor"]
            )["data"]
            if following["head"] != data["head"]:
                raise _AdmissionRefused("production_admission_mismatch", "production head changed during collection")
            rows.extend(following["history"])
            data = following
        if data["has_more"] or data["truncated"] or (data["open_operation_count"] and not allow_open):
            raise _AdmissionRefused("production_admission_mismatch", "incomplete or open production history")
        return {**data, "history": rows}

    first = collect()
    second = collect()
    if first != second:
        raise _AdmissionRefused("production_admission_mismatch", "production history changed during verification")
    return second


def _admission_writer(task_ref: str) -> str:
    """Resolve host provenance with no caller actor and no open write transaction."""
    import json
    from dataclasses import asdict

    from workbay_handoff_mcp.shared_schema import _get_db_connection
    from workbay_handoff_mcp.shared_write_context import _resolve_write_actor

    with _get_db_connection() as conn:
        context = _resolve_write_actor(conn, None, task_ref=task_ref)
    if not context.agent or not context.branch or not context.commit_sha:
        raise _AdmissionRefused("production_admission_unverifiable", "host write context is incomplete")
    return "host-write-context:" + json.dumps(asdict(context), sort_keys=True, separators=(",", ":"))


@dataclass
class _Admission:
    root: Any
    binding: dict[str, Any]
    admission_id: str
    contract: Any
    head: Any
    settlement: Any = None


def _append_admission_receipt(writer: Callable[..., Any], request: Any) -> Any:
    """An uncertain acknowledgement retries exactly the same operation bytes."""
    import sqlite3

    try:
        return writer(request)
    except (OSError, sqlite3.Error):
        return writer(request)


def _refresh_authorization(entry: Mapping[str, Any], contract_hash: str) -> Mapping[str, Any]:
    auth = entry.get("production_identity_refresh_authorization")
    if (
        not isinstance(auth, Mapping)
        or auth.get("approved") is not True
        or auth.get("exhaustive") is not True
        or not _text(auth.get("authority_id"))
        or auth.get("contract_hash") != contract_hash
    ):
        raise _AdmissionRefused(
            "production_admission_mismatch", "explicit owner identity-refresh authorization required"
        )
    return auth


def _record_from_history(row: Mapping[str, Any]) -> Any:
    import json

    from workbay_handoff_mcp import lane_completion_production as production

    return production.ProductionRecord(
        row["evidence_id"],
        row["sequence"],
        row["previous_evidence_id"],
        row["evidence_kind"],
        json.dumps(row["payload"], sort_keys=True, separators=(",", ":")),
    )


def _request_from_receipt(record: Any) -> Any:
    import json

    from workbay_handoff_mcp import lane_completion_production as production

    payload = json.loads(record.payload_json)
    return production.ProductionAppend(
        **{
            key: production.ProductionHead(**value) if key == "expected_head" else value
            for key, value in payload.items()
            if key in production.ProductionAppend.__dataclass_fields__
        }
    )


def _reapprove_lane_contract(
    admission: _Admission, current: Any, identity: dict[str, Any], entry: Mapping[str, Any]
) -> tuple[Any, str]:
    """Preserve the existing delegation for unchanged lane lifecycle obligations."""
    import json

    from workbay_handoff_mcp import lane_completion_recording as contracts

    root_binding = json.loads(admission.root.payload_json)["binding"]
    auth = _refresh_authorization(entry, root_binding["contract_hash"])
    retained = contracts.load_contract(
        identity["task_ref"], identity["lane_id"], admission.binding["contract_revision"]
    )

    def scope(record: Any) -> Any:
        return {
            key: value for key, value in record.contract.items() if key not in {"identity", "revision", "contract_hash"}
        }, (
            record.assigned_findings,
            record.empty_assignment_declared,
            record.objective,
            record.integration_ref,
            record.transfers,
            record.non_test_reason,
            record.test_target_policy,
        )

    declared = current.contract
    # The existing delegation covers native lane lifecycle metadata only. A
    # changed tip, task revision or scope still needs an exact owner contract.
    delegated_identity = dict(
        identity,
        cycle_id=declared["identity"]["cycle_id"],
        lane_revision=declared["identity"]["lane_revision"],
    )
    if (
        scope(current) != scope(retained)
        or declared["identity"] != admission.binding["identity"]
        or declared["identity"] != delegated_identity
    ):
        raise _AdmissionRefused("production_admission_mismatch", "identity_mismatch: exact approved contract required")
    refreshed = contracts.record_contract(
        identity=identity,
        objective=retained.objective,
        integration_ref=retained.integration_ref,
        acceptance_ids=declared["acceptance_ids"],
        acceptance_verifiers=declared["acceptance_verifiers"],
        approved=auth["approved"],
        exhaustive=auth["exhaustive"],
        evidence_ids=declared["evidence_ids"],
        assigned_findings=retained.assigned_findings,
        empty_assignment_declared=retained.empty_assignment_declared,
        required_commands=declared["required_commands"],
        non_test_acceptance_id=declared["non_test_acceptance_id"],
        transfers=retained.transfers,
        non_test_reason=retained.non_test_reason,
        test_target_policy=retained.test_target_policy,
    )
    return refreshed, auth["authority_id"]


def _refresh_admission(
    admission: _Admission,
    *,
    row: Mapping[str, Any],
    entry: Mapping[str, Any],
    primary: Path,
    task_ref: str,
    lane_id: str,
    branch: str,
    base_sha: str,
    git: RunGit,
) -> None:
    """Derive the complete current binding, then link any authorized change."""
    import hashlib
    import json

    from workbay_handoff_mcp import lane_completion_production as production
    from workbay_handoff_mcp import lane_completion_recording as contracts
    from workbay_handoff_mcp.handoff_state import get_handoff_state

    from . import lane_manifest

    before = admission.binding
    root_binding = json.loads(admission.root.payload_json)["binding"]
    task = get_handoff_state(task_ref=task_ref, read_profile="identity")
    active = task.get("data", {}).get("active")
    if (
        not task.get("ok")
        or not isinstance(active, Mapping)
        or active.get("task_ref") != task_ref
        or type(active.get("revision")) is not int
        or row.get("cycle_epoch") is None
        or row.get("branch") != branch
        or base_sha != root_binding["identity"]["admission_base"]
    ):
        raise _AdmissionRefused("production_admission_mismatch", "identity_mismatch: durable identity unavailable")
    tip = git(["rev-parse", "--verify", f"refs/heads/{row['branch']}^{{commit}}"], cwd=primary)
    product_tip = (tip.stdout or "").strip()
    if tip.returncode != 0 or not _FULL_SHA_RE.fullmatch(product_tip):
        raise _AdmissionRefused("production_admission_mismatch", "identity_mismatch: product tip unavailable")
    identity = {
        "repository": str(primary),
        "task_ref": task_ref,
        "lane_id": lane_id,
        "lane_row_id": row.get("id"),
        "identity_seq": row.get("identity_seq"),
        "cycle_id": str(row["cycle_epoch"]),
        "branch": row["branch"],
        "admission_base": root_binding["identity"]["admission_base"],
        "product_tip": product_tip,
        "lane_revision": row.get("updated_at"),
        "task_revision": str(active["revision"]),
    }
    current = contracts.load_contract(task_ref, lane_id)

    def binding(record: Any) -> dict[str, Any]:
        return dict(
            identity=identity,
            contract_revision=record.revision,
            contract_hash=record.contract_hash,
            target=before["target"],
        )

    after = binding(current)
    if after != before:
        declared = current.contract
        if declared["approved"] is not True or declared["exhaustive"] is not True or not declared["evidence_ids"]:
            raise _AdmissionRefused("production_admission_mismatch", "approved exhaustive contract evidence required")
        authority_id = declared["evidence_ids"][0]
        if declared["identity"] != identity:
            current, authority_id = _reapprove_lane_contract(admission, current, identity, entry)
        # The contract writer (owner or existing lifecycle delegation) completes
        # before the link; projections and claims follow verification (RES-18).
        declared = current.contract
        if declared["identity"] != identity:
            raise _AdmissionRefused("production_admission_mismatch", "identity_mismatch: exact contract required")
        after = binding(current)
        operation_id = (
            "identity-refresh:"
            + hashlib.sha256(
                json.dumps(
                    [admission.admission_id, before, after, authority_id], sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest()
        )
        request = production.ProductionAppend(
            binding=after,
            admission_id=admission.admission_id,
            operation_id=operation_id,
            producer_kind="lane_materialize",
            producer_version="1",
            writer_authority_ref=_admission_writer(task_ref),
            expected_head=admission.head,
            facts={
                "before_binding": before,
                "after_binding": after,
                "root_binding": root_binding,
                "before_identity": before["identity"],
                "after_identity": identity,
                "before_contract_hash": before["contract_hash"],
                "after_contract_hash": after["contract_hash"],
                "predecessor_head": {"sequence": admission.head.sequence, "evidence_id": admission.head.evidence_id},
                "authority_id": authority_id,
            },
        )
        _append_admission_receipt(production.record_production_contract_link, request)
        admission.binding = after
    admission.contract = current
    # Only the public current lookup grants dispatch/write authority, including
    # when recovering a contract/link/projection interrupted by a host restart.
    data = production.get_lane_production_admission(task_ref=task_ref, lane_id=lane_id)["data"]
    if data["admission_id"] != admission.admission_id or data["binding"] != admission.binding:
        raise _AdmissionRefused("production_admission_mismatch", "retained admission changed during materialization")
    admission.head = production.ProductionHead(
        **_production_history(data["binding"], data["admission_id"], allow_open=True)["head"]
    )
    path = lane_manifest.manifest_dir(orchestrator_root=str(primary)) / f"{task_ref}.json"
    lane_manifest.project_completion_contract_ref(path, lane_id, admission.contract)
    lane_manifest.project_production_admission_ref(path, lane_id, admission.root, current=data)


def _prepare_admission(
    *,
    task_ref: str,
    lane_id: str,
    primary: Path,
    entry: Mapping[str, Any],
    row: Mapping[str, Any],
    branch: str,
    base_sha: str,
    branch_exists: bool,
    git: RunGit,
) -> _Admission | None:
    import uuid
    from dataclasses import replace

    from workbay_handoff_mcp import lane_completion_production as production
    from workbay_handoff_mcp import lane_completion_recording as contracts

    if "completion_contract_ref" not in entry:
        if "production_admission_ref" in entry:
            raise _AdmissionRefused("production_admission_mismatch", "production reference without completion contract")
        return None
    contract = contracts.load_contract(task_ref, lane_id)
    predecessor = entry.get("production_admission_ref")
    retained = None
    if predecessor is not None:
        # The projection is a locator only. Historical storage verifies the
        # immutable root, contracts and receipt chain even when rows changed.
        root_binding = predecessor.get("root_binding", predecessor["binding"])
        root_identity = root_binding["identity"]
        if root_identity["lane_row_id"] == row.get("id") and root_identity["identity_seq"] == row.get("identity_seq"):
            # A same-lineage link does not decide an open offload operation.
            # Replacement still requires closed history through the default.
            retained = _production_history(root_binding, predecessor["admission_id"], historical=True, allow_open=True)
        else:
            retained = _production_history(root_binding, predecessor["admission_id"], historical=True)
        point = retained["history"][-1]["payload"]["binding"]
        before = point["identity"]
        if any(
            before[key] != value
            for key, value in {"repository": str(primary), "task_ref": task_ref, "lane_id": lane_id}.items()
        ):
            raise _AdmissionRefused("production_admission_mismatch", "foreign predecessor identity")
        if before["lane_row_id"] == row.get("id") and before["identity_seq"] == row.get("identity_seq"):
            root = _record_from_history(retained["history"][0])
            state = _Admission(
                root, point, predecessor["admission_id"], contract, production.ProductionHead(**retained["head"])
            )
            settled = any(
                r["evidence_kind"] == "production_settle"
                and r["operation_id"] == retained["history"][0]["operation_id"]
                for r in retained["history"]
            )
            if not settled:
                raise _AdmissionRefused("production_admission_mismatch", "materialization intent was never settled")
            if not branch_exists:
                raise _AdmissionRefused(
                    "production_admission_mismatch", "admitted branch disappeared; replacement needs authorization"
                )
            _refresh_admission(
                state,
                row=row,
                entry=entry,
                primary=primary,
                task_ref=task_ref,
                lane_id=lane_id,
                branch=branch,
                base_sha=base_sha,
                git=git,
            )
            return state

    if entry["completion_contract_ref"] != {"revision": contract.revision, "contract_hash": contract.contract_hash}:
        raise _AdmissionRefused("production_admission_mismatch", "contract_mismatch: stale manifest contract")
    identity = contract.contract["identity"]
    observed = {
        "repository": str(primary),
        "task_ref": task_ref,
        "lane_id": lane_id,
        "lane_row_id": row.get("id"),
        "identity_seq": row.get("identity_seq"),
        "cycle_id": str(row["cycle_epoch"]) if row.get("cycle_epoch") is not None else None,
        "branch": branch,
        "admission_base": base_sha,
        "lane_revision": row.get("updated_at"),
    }
    if any(identity[key] != value for key, value in observed.items()) or row.get("branch") != branch:
        raise _AdmissionRefused("production_admission_mismatch", "identity_mismatch: full observed identity differs")
    if row.get("status") == "planned":
        _refresh_authorization(entry, contract.contract_hash)
    binding = dict(
        identity=identity, contract_revision=contract.revision, contract_hash=contract.contract_hash, target=None
    )
    try:
        existing = production.get_lane_production_admission(task_ref=task_ref, lane_id=lane_id)["data"]
    except production.ProductionRefusal as exc:
        if exc.code != production.RefusalCode.ADMISSION_ABSENT:
            raise
    else:
        # Recover the original operation after a lost acknowledgement or a
        # crash before projection. Dispatch still holds until it is projected.
        data = _production_history(existing["binding"], existing["admission_id"], allow_open=True)
        root = _record_from_history(data["history"][0])
        original = _request_from_receipt(root)
        state = _Admission(
            root, existing["binding"], existing["admission_id"], contract, production.ProductionHead(**data["head"])
        )
        if any(
            r["evidence_kind"] == "production_settle" and r["operation_id"] == original.operation_id
            for r in data["history"]
        ):
            if not branch_exists:
                raise _AdmissionRefused("production_admission_mismatch", "admitted branch disappeared")
            return state
        if any(
            r["operation_id"] != original.operation_id
            or r["evidence_kind"] not in {"production_admission", "production_gap", "production_prepare"}
            for r in data["history"]
        ):
            raise _AdmissionRefused("production_admission_mismatch", "unexpected unfinished admission history")
        if original.facts["branch_preexisting"] and not branch_exists:
            raise _AdmissionRefused("production_admission_mismatch", "preexisting branch disappeared during admission")
        if original.facts["branch_preexisting"] and not any(
            r["evidence_kind"] == "production_gap" for r in data["history"]
        ):
            state.head = _append_admission_receipt(
                production.record_production_gap,
                replace(
                    original,
                    expected_head=state.head,
                    facts={
                        "reason": "preexisting_branch_history_unavailable",
                        "existing_tip_oid": original.facts["initial_tip_oid"],
                        "existing_tip_tree_oid": original.facts["initial_tip_tree_oid"],
                    },
                ),
            ).head
        prepared = next((r for r in data["history"] if r["evidence_kind"] == "production_prepare"), None)
        if prepared is None:
            state.head = _append_admission_receipt(
                production.record_production_prepare, replace(original, expected_head=state.head)
            ).head
        state.settlement = replace(original, expected_head=state.head)
        return state

    def oid(ref: str) -> str:
        probe = git(["rev-parse", "--verify", ref], cwd=primary)
        value = (probe.stdout or "").strip()
        if probe.returncode != 0 or not _FULL_SHA_RE.fullmatch(value):
            raise _AdmissionRefused("production_admission_unreadable", f"cannot resolve {ref}")
        return value

    tip = oid(f"refs/heads/{branch}^{{commit}}") if branch_exists else base_sha
    if identity["product_tip"] != tip:
        raise _AdmissionRefused("production_admission_mismatch", "identity_mismatch: initial product tip differs")
    facts = {
        "phase": "intent",
        "base_oid": base_sha,
        "base_tree_oid": oid(f"{base_sha}^{{tree}}"),
        "initial_tip_oid": tip,
        "initial_tip_tree_oid": oid(f"{tip}^{{tree}}"),
        "branch_preexisting": branch_exists,
        "owner_decision_ids": list(contract.contract["evidence_ids"]),
        "coverage_policy_version": production.FORMAT_VERSION,
        "source_cursors": {},
        "source_coverage": "unavailable",
    }
    if retained is not None:
        # A replacement starts a new lineage. Keep the cross-lineage predecessor
        # on the root, outside the within-lineage contract-transition chain.
        facts["predecessor"] = {
            "predecessor_admission_id": predecessor["admission_id"],
            "predecessor_binding": point,
            "predecessor_root_binding": root_binding,
            "predecessor_head": retained["head"],
            "before_identity": point["identity"],
            "after_identity": identity,
            "before_contract_hash": point["contract_hash"],
            "after_contract_hash": binding["contract_hash"],
        }
    request = production.ProductionAppend(
        binding=binding,
        admission_id=str(uuid.uuid4()),
        operation_id=str(uuid.uuid4()),
        producer_kind="lane_materialize",
        producer_version="1",
        writer_authority_ref=_admission_writer(task_ref),
        expected_head=production.ProductionHead(0, None),
        facts=facts,
    )
    root = _append_admission_receipt(production.record_production_admission, request)
    head = root.head
    if branch_exists:
        head = _append_admission_receipt(
            production.record_production_gap,
            replace(
                request,
                expected_head=head,
                facts={
                    "reason": "preexisting_branch_history_unavailable",
                    "existing_tip_oid": tip,
                    "existing_tip_tree_oid": facts["initial_tip_tree_oid"],
                },
            ),
        ).head
    prepared = _append_admission_receipt(production.record_production_prepare, replace(request, expected_head=head))
    return _Admission(
        root, binding, request.admission_id, contract, prepared.head, replace(request, expected_head=prepared.head)
    )


def _settle_admission(
    admission: _Admission | None, *, primary: Path, task_ref: str, lane_id: str, branch: str, git: RunGit
) -> None:
    from dataclasses import replace

    from workbay_handoff_mcp import lane_completion_production as production

    from . import lane_manifest

    if admission is None:
        return
    request = admission.settlement
    if request is not None:

        def oid(ref: str) -> str:
            probe = git(["rev-parse", "--verify", ref], cwd=primary)
            value = (probe.stdout or "").strip()
            if probe.returncode != 0 or not _FULL_SHA_RE.fullmatch(value):
                raise _AdmissionRefused("production_admission_unreadable", f"cannot settle {ref}")
            return value

        tip = oid(f"refs/heads/{branch}^{{commit}}")
        facts = dict(
            request.facts,
            phase="settled",
            initial_tip_oid=tip,
            initial_tip_tree_oid=oid(f"{tip}^{{tree}}"),
            empty_branch_created=not request.facts["branch_preexisting"] and tip == request.facts["base_oid"],
        )
        if tip != request.facts["initial_tip_oid"]:
            raise _AdmissionRefused("production_admission_mismatch", "branch changed during materialization")
        settled = _append_admission_receipt(production.record_production_settle, replace(request, facts=facts))
        admission.head = settled.head
    path = lane_manifest.manifest_dir(orchestrator_root=str(primary)) / f"{task_ref}.json"
    data = production.get_lane_production_admission(task_ref=task_ref, lane_id=lane_id)["data"]
    lane_manifest.project_production_admission_ref(path, lane_id, admission.root, current=data)


def materialize_lane(
    task_ref: str,
    lane_id: str,
    *,
    root: Path | str,
    apply: bool = False,
    load_manifest: LoadManifest | None = None,
    get_row: GetRow | None = None,
    run_git: RunGit | None = None,
    ensure_worktree: EnsureWorktree | None = None,
) -> MaterializeResult:
    """Materialize under the existing mutation fence; append before access.

    Legacy manifests without a completion contract keep their existing behavior
    and remain N-unavailable. Contract-bound materialization must journal first.
    No SQLite transaction spans Git or worktree/worker execution.
    """
    import sqlite3
    from contextlib import nullcontext

    from workbay_handoff_mcp import lane_completion_production as production
    from workbay_handoff_mcp import lane_completion_recording as contracts

    from .orchestrator_lanes import _landing_mutation_lock

    primary = Path(root).expanduser().resolve()
    fence = _landing_mutation_lock(primary, operation="materialize", lane=lane_id) if apply else nullcontext(True)
    try:
        with fence as acquired:
            if acquired is not True:
                return _refuse(
                    lane_id=lane_id, kind="production_admission_busy", detail="lane mutation fence unavailable"
                )
            return _materialize_lane(
                task_ref,
                lane_id,
                root=primary,
                apply=apply,
                load_manifest=load_manifest,
                get_row=get_row,
                run_git=run_git,
                ensure_worktree=ensure_worktree,
            )
    except _AdmissionRefused as exc:
        return _refuse(lane_id=lane_id, kind=exc.reason, detail=str(exc))
    except (production.ProductionRefusal, contracts.ContractRefusal) as exc:
        return _refuse(lane_id=lane_id, kind="production_admission_mismatch", detail=f"{exc.code.value}: {exc}")
    except (OSError, sqlite3.Error) as exc:
        return _refuse(lane_id=lane_id, kind="production_admission_unreadable", detail=str(exc))


def _materialize_lane(
    task_ref: str,
    lane_id: str,
    *,
    root: Path | str,
    apply: bool = False,
    load_manifest: LoadManifest | None = None,
    get_row: GetRow | None = None,
    run_git: RunGit | None = None,
    ensure_worktree: EnsureWorktree | None = None,
) -> MaterializeResult:
    """Reconcile the lane branch and worktree to the manifest + row.

    Dry-run by default (``apply=False``): reports ``planned`` / ``present``
    without creating refs or worktrees. ``apply=True`` creates a missing
    branch at ``base_sha`` (never force-updates an existing one) and then
    calls ``ensure_lane_worktree``. An empty ``worktree_path`` on both the
    row and the manifest refuses before any branch write; resolving an
    empty path would otherwise aim ensure at cwd.
    """
    primary = Path(root).expanduser().resolve()
    if load_manifest is None:
        from .lane_manifest import load_manifest as read_manifest

        def loader(ref: str) -> Mapping[str, Any]:
            return read_manifest(ref, orchestrator_root=str(primary))
    else:
        loader = load_manifest
    row_lookup = get_row if get_row is not None else _default_get_row
    git = run_git if run_git is not None else _default_run_git

    try:
        manifest = loader(task_ref)
    except FileNotFoundError:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_NO_MANIFEST_ENTRY,
            detail=f"no manifest entry for lane {lane_id!r} on task {task_ref!r}",
        )

    entry = _lane_entry(manifest, lane_id)
    if entry is None:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_NO_MANIFEST_ENTRY,
            detail=f"no manifest entry for lane {lane_id!r} on task {task_ref!r}",
        )

    branch = _text(entry.get("branch"))
    base_sha = _text(entry.get("base_sha"))
    manifest_path = _text(entry.get("worktree_path"))
    if not branch:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_NO_MANIFEST_ENTRY,
            detail=f"manifest entry for lane {lane_id!r} is missing a branch",
            base_sha=base_sha,
            worktree_path=manifest_path,
        )

    contract_bound = "completion_contract_ref" in entry
    row = _default_get_row(task_ref, lane_id) if contract_bound else row_lookup(task_ref, lane_id)
    if not isinstance(row, Mapping):
        if contract_bound:
            raise _AdmissionRefused("production_admission_mismatch", "lane row unavailable before materialization")
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_NO_ROW,
            detail=f"no worktree_lanes row for lane {lane_id!r} on task {task_ref!r}",
            branch=branch,
            base_sha=base_sha,
            worktree_path=manifest_path,
        )

    if _row_is_terminal_or_landed(row):
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_TERMINAL_LANE,
            detail=(
                f"worktree_lanes row for lane {lane_id!r} is terminal or has durable landing evidence; "
                "refusing stale manifest rematerialization"
            ),
            branch=branch,
            base_sha=base_sha,
            worktree_path=_text(row.get("worktree_path")) or manifest_path,
        )

    row_path = _text(row.get("worktree_path"))
    worktree_raw = row_path or manifest_path
    if not worktree_raw:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_EMPTY_WORKTREE_PATH,
            detail=(
                "worktree_path is empty on both the worktree_lanes row and the "
                "manifest entry; refusing to create a branch or ensure a worktree at cwd"
            ),
            branch=branch,
            base_sha=base_sha,
            worktree_path="",
        )
    worktree_path = str(_resolve_worktree_path(worktree_raw, root=primary))

    if not _FULL_SHA_RE.fullmatch(base_sha):
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_SHORT_BASE_SHA,
            detail="base_sha must be a 40-character lowercase hex commit",
            branch=branch,
            base_sha=base_sha,
            worktree_path=worktree_path,
        )

    def _git(args: Sequence[str]) -> subprocess.CompletedProcess[str] | MaterializeResult:
        try:
            return git(args, cwd=primary)
        except subprocess.TimeoutExpired as exc:
            return _refuse_git_timeout(
                lane_id=lane_id,
                exc=exc,
                args=args,
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
            )

    cat = _git(["cat-file", "-e", f"{base_sha}^{{commit}}"])
    if isinstance(cat, MaterializeResult):
        return cat
    if cat.returncode != 0:
        return _refuse(
            lane_id=lane_id,
            kind=REFUSAL_BASE_SHA_UNKNOWN,
            detail=f"base_sha {base_sha} does not name a commit in the primary repo",
            branch=branch,
            base_sha=base_sha,
            worktree_path=worktree_path,
        )

    show = _git(["show-ref", "--verify", f"refs/heads/{branch}"])
    if isinstance(show, MaterializeResult):
        return show
    branch_exists = show.returncode == 0
    admission = None

    def prepare_admission() -> Any:
        return _prepare_admission(
            task_ref=task_ref,
            lane_id=lane_id,
            primary=primary,
            entry=entry,
            row=row,
            branch=branch,
            base_sha=base_sha,
            branch_exists=branch_exists,
            git=git,
        )

    def _dry_worktree_outcome() -> str | MaterializeResult:
        try:
            registered = _registered_worktree_on_branch(
                run_git=git,
                root=primary,
                worktree_path=worktree_path,
                branch=branch,
            )
        except subprocess.TimeoutExpired as exc:
            return _refuse_git_timeout(
                lane_id=lane_id,
                exc=exc,
                args=["worktree", "list", "--porcelain"],
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
            )
        if registered:
            return WORKTREE_PRESENT
        return WORKTREE_PLANNED

    if not branch_exists:
        if not apply:
            worktree_outcome = _dry_worktree_outcome()
            if isinstance(worktree_outcome, MaterializeResult):
                return worktree_outcome
            return MaterializeResult(
                lane_id=lane_id,
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
                branch_outcome=BRANCH_PLANNED,
                worktree_outcome=worktree_outcome,
                refusal_kind=None,
                detail=f"dry-run: would create {branch!r} at {base_sha} and ensure worktree {worktree_path}",
                applied=False,
            )
        admission = prepare_admission()
        created = _git(["branch", "--", branch, base_sha])
        if isinstance(created, MaterializeResult):
            return created
        if created.returncode != 0:
            detail = (created.stderr or created.stdout or "").strip() or f"exit {created.returncode}"
            return _refuse(
                lane_id=lane_id,
                kind=REFUSAL_BRANCH_CREATE_FAILED,
                detail=f"git branch {branch!r} {base_sha} failed: {detail}",
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
            )
        branch_outcome = BRANCH_CREATED
    else:
        ancestor = _git(["merge-base", "--is-ancestor", base_sha, branch])
        if isinstance(ancestor, MaterializeResult):
            return ancestor
        if ancestor.returncode != 0:
            return _refuse(
                lane_id=lane_id,
                kind=REFUSAL_BRANCH_NOT_FROM_BASE,
                detail=(
                    f"lane branch {branch!r} does not contain base_sha {base_sha} as an ancestor; "
                    "refusing to move or force the branch"
                ),
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
            )
        if not apply:
            worktree_outcome = _dry_worktree_outcome()
            if isinstance(worktree_outcome, MaterializeResult):
                return worktree_outcome
            return MaterializeResult(
                lane_id=lane_id,
                branch=branch,
                base_sha=base_sha,
                worktree_path=worktree_path,
                branch_outcome=BRANCH_PRESENT,
                worktree_outcome=worktree_outcome,
                refusal_kind=None,
                detail=f"dry-run: branch {branch!r} already present from {base_sha}",
                applied=False,
            )
        admission = prepare_admission()
        branch_outcome = BRANCH_PRESENT

    _settle_admission(
        admission,
        primary=primary,
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        git=git,
    )
    ensurer = ensure_worktree if ensure_worktree is not None else _default_ensure_worktree
    try:
        ensured = ensurer(
            primary_repo=primary,
            worktree_path=worktree_path,
            branch=branch,
            lane_id=lane_id,
            task_ref=task_ref,
        )
    except subprocess.TimeoutExpired as exc:
        return _refuse_git_timeout(
            lane_id=lane_id,
            exc=exc,
            args=["worktree"],
            branch=branch,
            base_sha=base_sha,
            worktree_path=worktree_path,
        )
    if not getattr(ensured, "ok", False):
        outcome = getattr(ensured, "outcome", None) or "refused"
        failure_kind = getattr(ensured, "failure_kind", None)
        error = _text(getattr(ensured, "error", None)) or f"ensure_lane_worktree refused ({failure_kind or outcome})"
        return MaterializeResult(
            lane_id=lane_id,
            branch=branch,
            base_sha=base_sha,
            worktree_path=worktree_path,
            branch_outcome=branch_outcome,
            worktree_outcome=str(outcome),
            refusal_kind=str(failure_kind) if failure_kind else str(outcome),
            detail=error,
            applied=True,
        )

    if admission is not None:
        current_row = _default_get_row(task_ref, lane_id)
        if not isinstance(current_row, Mapping):
            raise _AdmissionRefused(
                "production_admission_mismatch", "current lane row unavailable after materialization"
            )
        _refresh_admission(
            admission,
            row=current_row,
            entry=entry,
            primary=primary,
            task_ref=task_ref,
            lane_id=lane_id,
            branch=branch,
            base_sha=base_sha,
            git=git,
        )

    rematerialized = bool(getattr(ensured, "rematerialized", False))
    worktree_outcome = WORKTREE_CREATED if rematerialized else WORKTREE_PRESENT
    ensured_path = getattr(ensured, "worktree_path", None)
    if ensured_path:
        worktree_path = str(Path(ensured_path).expanduser().resolve())
    return MaterializeResult(
        lane_id=lane_id,
        branch=branch,
        base_sha=base_sha,
        worktree_path=worktree_path,
        branch_outcome=branch_outcome,
        worktree_outcome=worktree_outcome,
        refusal_kind=None,
        detail=f"lane {lane_id!r} branch {branch_outcome}, worktree {worktree_outcome}",
        applied=True,
    )
