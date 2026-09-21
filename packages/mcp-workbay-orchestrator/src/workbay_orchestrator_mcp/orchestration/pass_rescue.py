"""Deterministic recovery for terminal offload passes.

The recovery order is deliberately strict: inspect the remote sandbox, record
any salvage patches, hand over a stale shared path, and only then reopen the
original dispatch.  Ambiguous probes never mutate host state.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, get_args

from workbay_orchestrator_mcp.orchestration.remote_sandbox_reap import _derive_lane_key

SSH_TIMEOUT_SECONDS = 30.0
GIT_TIMEOUT_SECONDS = 10.0
TERMINAL_LANE_STATUSES = frozenset({"closed", "merged", "closed_stale"})
NON_TERMINAL_PASS_OUTCOMES = frozenset({"still_running"})
_LS_TREE_RE = re.compile(r"^([0-7]{6}) (?:blob|commit) ([0-9a-f]{40})\t(.+)$", re.S)
_SAFE_ID_RE = re.compile(r"[^A-Za-z0-9._-]+")

SandboxState = Literal["absent", "equal_to_host", "behind_host", "ahead_of_host", "dirty", "probe_failed"]
Classification = Literal[
    "shared_path_owned",
    "host_verification_required",
    "rearm_eligible",
    "not_terminal",
    "pass_not_found",
    "unsupported_failure",
    "probe_failed",
    "already_rescued",
    "review_recorded",
    "record_pending",
    "deadline_killed",
    "auth_failed",
    "auth_evidence_missing",
]


@dataclass(frozen=True)
class RescueReport:
    pass_id: str
    lane_id: str | None
    classification: Classification
    sandbox_state: SandboxState
    differing_paths: list[str] = field(default_factory=list)
    salvage_paths: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    rearm_ready: bool = False
    refused_reason: str | None = None
    lane_spec: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _SandboxProbe:
    state: SandboxState
    head: str | None = None
    host_tip: str | None = None
    differing_paths: tuple[str, ...] = ()
    dirty_paths: tuple[str, ...] = ()
    dirty_patch: bytes = b""
    committed_patch: bytes = b""
    lease_live: bool = False
    lock_held: bool = False
    error: str | None = None


def rescue_pass(
    pass_id: str,
    *,
    root: Path | str,
    ssh_runner: Callable[..., Any],
    git: Callable[..., Any],
    list_rows: Callable[[], Sequence[Mapping[str, Any]]] | Any,
    now: Callable[[], Any],
    apply: bool = False,
) -> RescueReport:
    """Recover at most once per pass, with a nonblocking local recovery claim.

    A durable intent precedes external ownership/dispatch writes. If the process
    dies across those writes, replay returns recovery_incomplete for host
    reconciliation instead of repeating a possibly completed side effect.
    """
    normalized_id = str(pass_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", normalized_id):
        return _report(normalized_id, None, "pass_not_found", refused="pass_not_found")
    kwargs = dict(root=root, ssh_runner=ssh_runner, git=git, list_rows=list_rows, now=now, apply=apply)
    try:
        if not apply or not Path(root).is_dir():
            return _rescue_pass(normalized_id, **kwargs)
        with _rescue_state_path(Path(root), normalized_id).with_suffix(".lock").open("a") as claim:
            try:
                fcntl.flock(claim, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return _report(normalized_id, None, "already_rescued", refused="rescue_in_progress")
            try:
                return _rescue_pass(normalized_id, **kwargs)
            finally:
                fcntl.flock(claim, fcntl.LOCK_UN)
    except OSError:
        return _report(normalized_id, None, "probe_failed", refused="rescue_persistence_failed")


def _rescue_pass(
    pass_id: str,
    *,
    root: Path | str,
    ssh_runner: Callable[..., Any],
    git: Callable[..., Any],
    list_rows: Callable[[], Sequence[Mapping[str, Any]]] | Any,
    now: Callable[[], Any],
    apply: bool = False,
) -> RescueReport:
    """Classify and optionally recover one persisted terminal offload pass.

    ``root`` is the orchestrator state directory.  Collaborators are injected
    so the operation is testable and so all network calls have an explicit
    timeout.  Mutation methods on ``list_rows`` are used only after every probe
    and salvage write succeeds.
    """
    normalized_id = str(pass_id or "").strip()
    if not normalized_id:
        return _report("", None, "pass_not_found", refused="pass_not_found")
    state_dir = Path(root)
    rescue_path = _rescue_state_path(state_dir, normalized_id)
    receipt_path = state_dir / f"review-receipt-{normalized_id}.json"
    # A runner may die after publishing its carrier but before the engine
    # harvests it. Promote only the exact location persisted before review.
    if not receipt_path.exists():
        current = _load_pass_record(state_dir, normalized_id)
        carrier_path = current.get("review_carrier_path") if current else None
        if carrier_path:
            from workbay_orchestrator_mcp.orchestration.offload_pass import _review_identity_complete

            identity = {**current, "pass_id": normalized_id}
            if not _review_identity_complete(identity):
                return _report(normalized_id, None, "probe_failed", refused="identity_incomplete")
            try:
                carrier = json.loads(Path(carrier_path).read_text())
            except (OSError, ValueError, TypeError):
                carrier = {}
            if carrier.get("review_result_carrier") and carrier.get("review_operation_token"):
                if not apply:
                    return _report(normalized_id, current.get("lane_id"), "record_pending",
                                   refused="review_harvest_required")
                _atomic_write(receipt_path, json.dumps({
                    "identity": identity, "result": carrier, "report": None,
                    "operation_token": carrier["review_operation_token"], "status": "record_pending",
                    "kwargs": {"task_ref": current["task_ref"], "lane_id": current["lane_id"],
                               "session": current.get("session"),
                               "orchestrator_root": current.get("orchestrator_root", str(state_dir))},
                }))
    if receipt_path.exists():
        from workbay_orchestrator_mcp.orchestration.offload_pass import (
            _review_identity_complete, _review_identity_matches,
        )

        try:
            receipt = json.loads(receipt_path.read_text())
            saved_identity = {**receipt["identity"], "task_ref": receipt["kwargs"]["task_ref"],
                              "lane_id": receipt["kwargs"].get("lane_id")}
            current = _load_pass_record(state_dir, normalized_id)
            if current is None:
                return _report(normalized_id, None, "probe_failed", refused="review_receipt_subject_unavailable")
            if not _review_identity_complete(saved_identity) or not _review_identity_complete({**current, "pass_id": normalized_id}):
                return _report(normalized_id, None, "probe_failed", refused="identity_incomplete")
            if not _review_identity_matches(saved_identity, {**current, "pass_id": normalized_id}):
                return _report(normalized_id, None, "probe_failed", refused="review_receipt_identity_mismatch")
        except (OSError, ValueError, KeyError, TypeError):
            return _report(normalized_id, None, "probe_failed", refused="review_receipt_invalid")
    stored = _load_rescue_report(rescue_path)
    if stored is not None:
        if stored.pass_id != normalized_id:
            return _report(normalized_id, None, "probe_failed", refused="rescue_record_invalid")
        return stored
    if rescue_path.exists():
        return _report(normalized_id, None, "probe_failed", refused="rescue_record_invalid")

    receipt_path = state_dir / f"review-receipt-{normalized_id}.json"
    if receipt_path.exists():
        try:
            receipt = json.loads(receipt_path.read_text())
            identity = receipt["identity"]
            if identity.get("pass_id") != normalized_id:
                raise ValueError("receipt pass mismatch")
            lane_id = receipt["kwargs"].get("lane_id")
            if not apply:
                return _report(normalized_id, lane_id, "record_pending", refused="review_harvest_required")
            from workbay_orchestrator_mcp.orchestration.offload_pass import (
                _harvest_block_parsed,
                _harvest_review_findings,
            )

            harvest = _harvest_review_findings(
                receipt["result"],
                receipt["report"],
                **receipt["kwargs"],
                receipt_path=receipt_path,
                receipt_identity=identity,
            )
            if harvest.get("status") == "record_pending" or harvest.get("reason") == "record_pending":
                return _report(normalized_id, lane_id, "record_pending", refused="review_record_pending")
            if not _harvest_block_parsed(harvest):
                return _report(normalized_id, lane_id, "record_pending", refused="review_verdict_unavailable")
            report = RescueReport(normalized_id, lane_id, "review_recorded", "absent")
            _store_rescue_report(rescue_path, report, now)
            return report
        except (OSError, ValueError, KeyError, TypeError):
            return _report(normalized_id, None, "probe_failed", refused="review_receipt_invalid")

    pass_record = _load_pass_record(state_dir, normalized_id)
    if pass_record is None:
        return _report(normalized_id, None, "pass_not_found", refused="pass_not_found")
    lane_id = _text(pass_record.get("lane_id"))
    if not _pass_is_terminal(pass_record):
        return _report(normalized_id, lane_id, "not_terminal", refused="not_terminal")

    try:
        pass_record = _enrich_from_lane_row(pass_record, list_rows)
    except Exception as exc:  # noqa: BLE001 - incomplete ownership universe fails closed
        return RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification="probe_failed",
            sandbox_state="probe_failed",
            refused_reason="row_lookup_failed",
            actions=[f"row_lookup_failed:{type(exc).__name__}"],
        )

    branch = _text(pass_record.get("branch"))
    if branch is None or branch.startswith("-") or ".." in branch or "\n" in branch:
        return _report(normalized_id, lane_id, "probe_failed", refused="probe_failed")

    probe = _probe_sandbox(branch, root=state_dir, ssh_runner=ssh_runner, git=git, apply=apply)
    if probe.state == "probe_failed":
        return RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification="probe_failed",
            sandbox_state="probe_failed",
            refused_reason="probe_failed",
            actions=[f"probe_error:{probe.error or 'unknown'}"],
        )

    classification = _classify_pass(pass_record, ssh_runner=ssh_runner)
    if classification in {"unsupported_failure", "auth_failed", "auth_evidence_missing"}:
        return RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification=classification,
            sandbox_state=probe.state,
            differing_paths=list(probe.differing_paths[:20]),
            refused_reason=classification,
        )

    actions: list[str] = []
    if probe.lease_live:
        actions.append("sandbox_lease:live")
    if probe.lock_held:
        actions.append("sandbox_lock:held")
    if apply and (probe.lease_live or probe.lock_held):
        return RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification=classification,
            sandbox_state=probe.state,
            differing_paths=list(probe.differing_paths[:20]),
            actions=actions,
            refused_reason="sandbox_live",
        )
    salvage_paths: list[str] = []
    if classification == "shared_path_owned":
        owner_result = _handover_shared_path(pass_record, list_rows, git, state_dir, apply=False)
        if owner_result != "ok":
            return RescueReport(
                pass_id=normalized_id,
                lane_id=lane_id,
                classification=classification,
                sandbox_state=probe.state,
                differing_paths=list(probe.differing_paths[:20]),
                actions=actions,
                refused_reason=owner_result,
            )
    if apply and probe.state in {"ahead_of_host", "dirty"}:
        try:
            salvage_paths = _write_salvage(state_dir, branch, normalized_id, probe)
        except OSError as exc:
            return RescueReport(
                pass_id=normalized_id,
                lane_id=lane_id,
                classification="probe_failed",
                sandbox_state=probe.state,
                differing_paths=list(probe.differing_paths[:20]),
                refused_reason="salvage_write_failed",
                actions=[f"salvage_write_failed:{type(exc).__name__}"],
            )
        actions.extend(f"salvage_recorded:{path}" for path in salvage_paths)
        decision_recorded = _record_decision(
            list_rows,
            pass_record,
            f"pass_rescue:{normalized_id}:salvage",
            "Remote sandbox changes preserved without applying them: " + ", ".join(salvage_paths),
        )
        if not decision_recorded:
            return RescueReport(
                pass_id=normalized_id,
                lane_id=lane_id,
                classification=classification,
                sandbox_state=probe.state,
                differing_paths=list(probe.differing_paths[:20]),
                salvage_paths=salvage_paths,
                actions=actions,
                refused_reason="decision_record_failed",
            )
    elif probe.state in {"ahead_of_host", "dirty"}:
        actions.append("salvage_required")

    if classification == "host_verification_required":
        test_cmd = _test_cmd(pass_record, list_rows)
        actions.append(f"host_gate:{test_cmd}" if test_cmd else "host_gate:missing_test_cmd")
        report = RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification=classification,
            sandbox_state=probe.state,
            differing_paths=list(probe.differing_paths[:20]),
            salvage_paths=salvage_paths,
            actions=actions,
            refused_reason=None if test_cmd else "test_cmd_missing",
        )
        if apply and test_cmd:
            _store_rescue_report(rescue_path, report, now)
        return report

    if not apply:
        actions.append("rearm_planned")
        return RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification=classification,
            sandbox_state=probe.state,
            differing_paths=list(probe.differing_paths[:20]),
            actions=actions,
        )

    dispatch_id = _text(pass_record.get("dispatch_id"))
    task_ref = _text(pass_record.get("task_ref"))
    if dispatch_id is None or lane_id is None or task_ref is None:
        return RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification=classification,
            sandbox_state=probe.state,
            differing_paths=list(probe.differing_paths[:20]),
            salvage_paths=salvage_paths,
            actions=actions,
            refused_reason="dispatch_identity_missing",
        )
    lane_spec = _lane_spec(pass_record)
    if lane_spec is None:
        return RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification=classification,
            sandbox_state=probe.state,
            differing_paths=list(probe.differing_paths[:20]),
            salvage_paths=salvage_paths,
            actions=actions,
            refused_reason="lane_spec_missing",
        )
    _store_rescue_report(
        rescue_path,
        RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification=classification,
            sandbox_state=probe.state,
            differing_paths=list(probe.differing_paths[:20]),
            salvage_paths=salvage_paths,
            actions=[*actions, f"dispatch_intent:{dispatch_id}"],
            refused_reason="recovery_incomplete",
        ),
        now,
    )
    if classification == "shared_path_owned":
        owner_result = _handover_shared_path(pass_record, list_rows, git, state_dir)
        if owner_result != "ok":
            report = RescueReport(
                pass_id=normalized_id,
                lane_id=lane_id,
                classification=classification,
                sandbox_state=probe.state,
                differing_paths=list(probe.differing_paths[:20]),
                salvage_paths=salvage_paths,
                actions=actions,
                refused_reason=owner_result,
            )
            _store_rescue_report(rescue_path, report, now)
            return report
        actions.append("shared_path_handed_over")
    if not _mutate(list_rows, "reopen_dispatch", dispatch_id, task_ref, lane_id):
        report = RescueReport(
            pass_id=normalized_id,
            lane_id=lane_id,
            classification=classification,
            sandbox_state=probe.state,
            differing_paths=list(probe.differing_paths[:20]),
            salvage_paths=salvage_paths,
            actions=actions,
            refused_reason="dispatch_reopen_failed",
        )
        _store_rescue_report(rescue_path, report, now)
        return report
    actions.append(f"dispatch_reopened:{dispatch_id}")
    report = RescueReport(
        pass_id=normalized_id,
        lane_id=lane_id,
        classification=classification,
        sandbox_state=probe.state,
        differing_paths=list(probe.differing_paths[:20]),
        salvage_paths=salvage_paths,
        actions=actions,
        rearm_ready=True,
        lane_spec=lane_spec,
    )
    _store_rescue_report(rescue_path, report, now)
    return report


def _report(pass_id: str, lane_id: str | None, classification: Classification, *, refused: str) -> RescueReport:
    return RescueReport(
        pass_id=pass_id,
        lane_id=lane_id,
        classification=classification,
        sandbox_state="absent",
        refused_reason=refused,
    )


def _load_pass_record(state_dir: Path, pass_id: str) -> dict[str, Any] | None:
    path = state_dir / f"offload-pass-{pass_id}.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    result = raw.get("result")
    record = dict(result) if isinstance(result, dict) else dict(raw)
    for key in ("task_ref", "lane_id", "dispatch_id", "lane_spec", "agent_stream_path", "branch", "worktree_path",
                "backend", "model", "reasoning_effort", "base_sha", "tip_sha"):
        if not record.get(key) and raw.get(key):
            record[key] = raw[key]
    record["_persisted_status"] = raw.get("status")
    return record


def _enrich_from_lane_row(record: Mapping[str, Any], registry: Any) -> dict[str, Any]:
    enriched = dict(record)
    lane_id = _text(record.get("lane_id"))
    task_ref = _text(record.get("task_ref"))
    matches = [
        row
        for row in _rows(registry)
        if _text(row.get("lane_id")) == lane_id and (task_ref is None or _text(row.get("task_ref")) == task_ref)
    ]
    if len(matches) > 1:
        raise ValueError("ambiguous lane row")
    if matches:
        for key in ("task_ref", "branch", "worktree_path", "test_cmd"):
            if not enriched.get(key) and matches[0].get(key):
                enriched[key] = matches[0][key]
    return enriched


def _pass_is_terminal(record: Mapping[str, Any]) -> bool:
    persisted_status = _text(record.get("_persisted_status"))
    if persisted_status is not None and persisted_status != "done":
        return False
    outcome = _text(record.get("outcome"))
    return outcome is not None and outcome not in NON_TERMINAL_PASS_OUTCOMES


def _lane_spec(record: Mapping[str, Any]) -> dict[str, Any] | None:
    from workbay_orchestrator_mcp.orchestration.wave_dispatch import LaneSpec, LaneSpecError

    raw = record.get("lane_spec")
    if not isinstance(raw, dict) or raw.get("lane_id") != record.get("lane_id"):
        return None
    try:
        return asdict(LaneSpec(**raw))
    except (TypeError, ValueError, LaneSpecError):
        return None


def _classify_pass(record: Mapping[str, Any], *, ssh_runner: Callable[..., Any]) -> Classification:
    outcome = _text(record.get("outcome")) or ""
    failure_kind = _text(record.get("failure_kind")) or ""
    error = _text(record.get("error")) or ""
    failed_stage = _text(record.get("failed_stage")) or ""
    landed = record.get("commit_landed") is True
    if failure_kind == "shared_path_owned" or "shared_path_owned" in error:
        return "shared_path_owned"
    expiry_text = " ".join(
        filter(
            None,
            (
                error.lower(),
                (_text(record.get("failure_reason")) or "").lower(),
                (_text(record.get("execute_stop_reason")) or "").lower(),
            ),
        )
    )
    wall_clock_expired = "wall-clock" in expiry_text or "wall_clock" in expiry_text
    if landed and (outcome in {"self_verify_inconclusive", "timeout"} or wall_clock_expired):
        return "host_verification_required"
    if _positive_auth_error(error):
        return "auth_failed"
    auth_label = "auth_failed" in (failure_kind, outcome, record.get("failure_reason")) or "auth_failed" in error
    if auth_label:
        stream_state = _auth_stream_evidence(record)
        if stream_state == "auth_failed":
            return "auth_failed"
        if stream_state == "active_changes":
            login, login_error = _ssh_text(
                ssh_runner, "# WORKBAY_PASS_RESCUE probe=auth-status\ncodex login status 2>&1"
            )
            if not login_error and login.strip().lower().startswith("logged in"):
                return "host_verification_required" if landed else "deadline_killed"
        # The wrapper's exit 6 label is not positive credential evidence.
        return "auth_evidence_missing"
    if wall_clock_expired and not landed:
        return "deadline_killed"
    if outcome == "transport_failure" or (outcome == "error" and failed_stage == "execute" and not landed):
        return "rearm_eligible"
    return "unsupported_failure"


def _positive_auth_error(text: str) -> bool:
    return bool(re.search(r"\b(?:HTTP(?: status)?[ :]+(?:401|403)|invalid_api_key|authentication_error)\b", text, re.I))


def _auth_stream_evidence(record: Mapping[str, Any]) -> str:
    """Use only this pass's explicitly attributed stream; never the latest log."""
    stream_path = _text(record.get("agent_stream_path"))
    if stream_path is None:
        return "missing"
    active = False
    try:
        path = Path(stream_path)
        if path.stat().st_size > 16 * 1024 * 1024:
            return "missing"
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                event = json.loads(line)
                if not isinstance(event, dict):
                    return "missing"
                # Do not read file contents or tool output as provider errors.
                if event.get("type") in {"error", "turn.failed", "auth.error"}:
                    if event.get("type") == "auth.error" or _positive_auth_error(json.dumps(event)):
                        return "auth_failed"
                item = event.get("item")
                if isinstance(item, dict) and item.get("type") == "file_change":
                    active = True
    except (OSError, ValueError, UnicodeError):
        return "missing"
    return "active_changes" if active else "missing"


def _probe_sandbox(
    branch: str,
    *,
    root: Path,
    ssh_runner: Callable[..., Any],
    git: Callable[..., Any],
    apply: bool,
) -> _SandboxProbe:
    key = _derive_lane_key(branch)
    remote_root = (os.environ.get("WORKBAY_REMOTE_AGENT_ROOT") or "grok-sandbox").strip()
    if not remote_root or ".." in remote_root or "\n" in remote_root:
        return _SandboxProbe("probe_failed", error="unsafe_remote_root")
    scripts = _probe_scripts(remote_root, key)
    exists, error = _ssh_text(ssh_runner, scripts["exists"])
    if error:
        return _SandboxProbe("probe_failed", error=error)
    if exists.strip() == "absent":
        liveness, error = _ssh_text(ssh_runner, scripts["liveness"])
        match = re.fullmatch(r"lease=(idle|live) lock=(free|held)\n?", liveness)
        if error or match is None:
            return _SandboxProbe("probe_failed", error=error or "unparseable_liveness")
        return _SandboxProbe("absent", lease_live=match.group(1) == "live", lock_held=match.group(2) == "held")
    if exists.strip() != "present":
        return _SandboxProbe("probe_failed", error="unparseable_exists")
    head, error = _ssh_text(ssh_runner, scripts["head"])
    if error or not re.fullmatch(r"[0-9a-f]{40}", head.strip()):
        return _SandboxProbe("probe_failed", error=error or "unparseable_head")
    tree_text, error = _ssh_text(ssh_runner, scripts["tree"])
    remote_tree = _parse_tree(tree_text)
    if error or remote_tree is None:
        return _SandboxProbe("probe_failed", error=error or "unparseable_tree")
    status, error = _ssh_text(ssh_runner, scripts["status"])
    dirty_paths = _parse_status_paths(status)
    if error or dirty_paths is None:
        return _SandboxProbe("probe_failed", error=error or "unparseable_status")
    liveness, error = _ssh_text(ssh_runner, scripts["liveness"])
    liveness_match = re.fullmatch(r"lease=(idle|live) lock=(free|held)\n?", liveness)
    if error or liveness_match is None:
        return _SandboxProbe("probe_failed", error=error or "unparseable_liveness")

    host_tip_result = _git_text(git, root, "rev-parse", "--verify", f"{branch}^{{commit}}")
    host_tip = host_tip_result[0].strip()
    if host_tip_result[1] or not re.fullmatch(r"[0-9a-f]{40}", host_tip):
        return _SandboxProbe("probe_failed", error=host_tip_result[1] or "host_branch_missing")
    host_tree_text, git_error = _git_text(git, root, "ls-tree", "-r", host_tip, "-z")
    host_tree = _parse_tree(host_tree_text)
    if git_error or host_tree is None:
        return _SandboxProbe("probe_failed", error=git_error or "unparseable_host_tree")
    baseline_text, error = _ssh_text(ssh_runner, scripts["baseline-tree"].replace("HOST_TIP", host_tip))
    baseline_tree = _parse_tree(baseline_text)
    if error or baseline_tree is None:
        return _SandboxProbe("probe_failed", error=error or "unparseable_baseline_tree")
    # The exported snapshot can omit host paths. Only a path present at the
    # sandbox baseline can establish a subsequent committed deletion (F2).
    compared_paths = remote_tree.keys() | baseline_tree.keys()
    differing = tuple(sorted(path for path in compared_paths if host_tree.get(path) != remote_tree.get(path)))
    sandbox_changes = {path for path in compared_paths if baseline_tree.get(path) != remote_tree.get(path)}

    dirty_patch = b""
    committed_patch = b""
    if dirty_paths:
        dirty_patch, error = _ssh_bytes(ssh_runner, scripts["dirty-diff"])
        if error:
            return _SandboxProbe("probe_failed", error=error)
    state: SandboxState
    if dirty_paths:
        state = "dirty"
    elif not differing:
        state = "equal_to_host"
    elif sandbox_changes.intersection(differing):
        # A revert/recombination is new work even when each resulting blob
        # occurs somewhere in host history (F3).
        state = "ahead_of_host"
    else:
        history = _all_blobs_in_history(remote_tree, differing, host_tip, root, git)
        if history is None:
            return _SandboxProbe("probe_failed", error="host_history_unavailable")
        state = "behind_host" if history else "ahead_of_host"
    if apply and state in {"ahead_of_host", "dirty"}:
        committed_patch, error = _ssh_bytes(ssh_runner, scripts["committed-diff"].replace("HOST_TIP", host_tip))
        if error:
            return _SandboxProbe("probe_failed", error=error)
        if state == "ahead_of_host" and not committed_patch.strip():
            return _SandboxProbe("probe_failed", error="empty_committed_patch")
    if dirty_paths and not dirty_patch.strip():
        return _SandboxProbe("probe_failed", error="empty_dirty_patch")
    if apply:
        # Independent SSH calls must describe one stable snapshot before any
        # durable decision or ownership write can rely on it.
        checks = [("head", head), ("status", status), ("liveness", liveness)]
        for name, expected in checks:
            observed, error = _ssh_text(ssh_runner, scripts[name])
            if error or observed != expected:
                return _SandboxProbe("probe_failed", error=error or "sandbox_changed_during_probe")
        if dirty_paths:
            observed_patch, error = _ssh_bytes(ssh_runner, scripts["dirty-diff"])
            if error or observed_patch != dirty_patch:
                return _SandboxProbe("probe_failed", error=error or "sandbox_changed_during_probe")
        current_tip, error = _git_text(git, root, "rev-parse", "--verify", f"{branch}^{{commit}}")
        if error or current_tip.strip() != host_tip:
            return _SandboxProbe("probe_failed", error=error or "host_changed_during_probe")
    return _SandboxProbe(
        state,
        head=head.strip(),
        host_tip=host_tip,
        differing_paths=differing,
        dirty_paths=tuple(dirty_paths),
        dirty_patch=dirty_patch,
        committed_patch=committed_patch,
        lease_live=liveness_match.group(1) == "live",
        lock_held=liveness_match.group(2) == "held",
    )


def _probe_scripts(remote_root: str, key: str) -> dict[str, str]:
    root_q = _shell_quote(remote_root)
    key_q = _shell_quote(key)
    setup = f'ROOT={root_q}; case "$ROOT" in /*) ;; *) ROOT="$HOME/$ROOT" ;; esac; KEY={key_q}; SD="$ROOT/$KEY"; '
    # remote_agent.sh records its export as one synthetic root commit. Prefer
    # that baseline over merge-base: history-stripped exports share no ancestry
    # with the host. Never infer the baseline from the current VM path set.
    base_subject = _shell_quote(f"sandbox base ({key}, history-stripped, remote-severed)")
    baseline = (
        'set -e; roots=$(git -C "$SD" rev-list --max-parents=0 HEAD); '
        'case "$roots" in ""|*"\n"*) exit 1 ;; esac; '
        f'if [ "$(git -C "$SD" show -s --format=%s "$roots")" = {base_subject} ]; then base=$roots; '
        'else base=$(git -C "$SD" merge-base HEAD HOST_TIP 2>/dev/null) || '
        'base=$(git -C "$SD" hash-object -t tree /dev/null); fi; '
    )
    return {
        "exists": "# WORKBAY_PASS_RESCUE probe=exists\n"
        + setup
        + "[ -d \"$SD\" ] && printf 'present\\n' || printf 'absent\\n'",
        "head": "# WORKBAY_PASS_RESCUE probe=head\n" + setup + 'git -C "$SD" rev-parse HEAD',
        "tree": "# WORKBAY_PASS_RESCUE probe=tree\n" + setup + 'git -C "$SD" ls-tree -r HEAD -z',
        "baseline-tree": "# WORKBAY_PASS_RESCUE probe=baseline-tree\n"
        + setup
        + baseline
        + 'git -C "$SD" ls-tree -r "$base" -z',
        "status": "# WORKBAY_PASS_RESCUE probe=status\n" + setup + 'git -C "$SD" status --porcelain=v1 -z',
        "liveness": (
            "# WORKBAY_PASS_RESCUE probe=liveness\n"
            + setup
            + 'lease=idle; if [ -f "$ROOT/.lane-live-$KEY" ]; then lease=live; expiry=; issued=; '
            'while IFS= read -r line || [ -n "$line" ]; do case "$line" in '
            "expiry=*) expiry=${line#expiry=} ;; issued=*) issued=${line#issued=} ;; esac; "
            'done < "$ROOT/.lane-live-$KEY"; '
            'case "$issued" in ""|*[!0-9]*) ;; *) case "$expiry" in ""|*[!0-9]*) ;; '
            '*) [ "$(date +%s)" -ge "$expiry" ] && lease=idle ;; esac ;; esac; fi; lock=free; '
            '[ -f "$ROOT/.lane-lock-$KEY" ] && ! flock -n "$ROOT/.lane-lock-$KEY" true 2>/dev/null && lock=held; '
            'printf \'lease=%s lock=%s\\n\' "$lease" "$lock"'
        ),
        "dirty-diff": (
            "# WORKBAY_PASS_RESCUE probe=dirty-diff\n"
            + setup
            + 'set -eo pipefail; cd -- "$SD"; git diff HEAD --binary --no-ext-diff --no-textconv; '
            "git ls-files --others --exclude-standard -z | "
            "while IFS= read -r -d '' file; do "
            'git diff --binary --no-ext-diff --no-textconv --no-index -- /dev/null "$file" || [ $? -eq 1 ]; done'
        ),
        "committed-diff": (
            "# WORKBAY_PASS_RESCUE probe=committed-diff\n"
            + setup
            + baseline
            + 'git -C "$SD" diff --binary --no-ext-diff --no-textconv "$base" HEAD'
        ),
    }


def _all_blobs_in_history(
    remote_tree: Mapping[str, str], differing: Sequence[str], branch: str, root: Path, git: Callable[..., Any]
) -> bool | None:
    revisions_text, error = _git_text(git, root, "rev-list", branch)
    if error:
        return None
    revisions = [line.strip() for line in revisions_text.splitlines() if line.strip()]
    if not revisions:
        return None
    unmatched = set(differing)
    for revision in revisions:
        tree_text, tree_error = _git_text(git, root, "ls-tree", "-r", revision, "-z")
        tree = _parse_tree(tree_text)
        if tree_error or tree is None:
            return None
        unmatched = {path for path in unmatched if tree.get(path) != remote_tree.get(path)}
        if not unmatched:
            return True
    return False


def _parse_tree(text: str) -> dict[str, str] | None:
    tree: dict[str, str] = {}
    for line in text.split("\0") if "\0" in text else text.splitlines():
        if not line:
            continue
        match = _LS_TREE_RE.fullmatch(line)
        if match is None or "\x00" in match.group(3) or match.group(3) in tree:
            return None
        tree[match.group(3)] = f"{match.group(1)}:{match.group(2)}"
    return tree


def _parse_status_paths(text: str) -> list[str] | None:
    if not text:
        return []
    nul_delimited = "\0" in text
    records = iter(text.split("\0") if nul_delimited else text.splitlines())
    paths: list[str] = []
    for record in records:
        if not record:
            continue
        if len(record) < 4 or record[2] != " " or any(char not in " MADRCU?!T" for char in record[:2]):
            return None
        path = record[3:]
        if not path or "\x00" in path:
            return None
        paths.append(path if nul_delimited else path.split(" -> ", 1)[-1])
        if nul_delimited and any(char in record[:2] for char in "RC"):
            source = next(records, None)
            if not source:
                return None
            paths.append(source)
    return paths


def _ssh_text(runner: Callable[..., Any], script: str) -> tuple[str, str | None]:
    raw, error = _ssh_bytes(runner, script)
    try:
        return raw.decode("utf-8"), error
    except UnicodeDecodeError:
        return "", "ssh_invalid_utf8"


def _ssh_bytes(runner: Callable[..., Any], script: str) -> tuple[bytes, str | None]:
    try:
        raw = runner(script, timeout=SSH_TIMEOUT_SECONDS)
    except (OSError, TimeoutError, subprocess.TimeoutExpired) as exc:
        return b"", f"ssh_{type(exc).__name__}"
    except TypeError:
        return b"", "ssh_runner_missing_timeout"
    if isinstance(raw, (str, bytes)):
        stdout = raw
    else:
        if getattr(raw, "returncode", 0) not in (0, None):
            return b"", f"ssh_exit_{raw.returncode}"
        stdout = getattr(raw, "stdout", b"") or b""
    if isinstance(stdout, str):
        return stdout.encode("utf-8"), None
    if isinstance(stdout, bytes):
        return stdout, None
    return b"", "ssh_invalid_output"


def _git_text(git: Callable[..., Any], root: Path, *args: str) -> tuple[str, str | None]:
    try:
        raw = git(root, *args, timeout=GIT_TIMEOUT_SECONDS)
    except TypeError:
        return "", "git_runner_missing_timeout"
    except (OSError, TimeoutError, subprocess.TimeoutExpired) as exc:
        return "", f"git_{type(exc).__name__}"
    return _coerce_result(raw, "git")


def _coerce_result(raw: Any, kind: str) -> tuple[str, str | None]:
    if isinstance(raw, str):
        return raw, None
    stdout = getattr(raw, "stdout", "") or ""
    stderr = getattr(raw, "stderr", "") or ""
    returncode = getattr(raw, "returncode", 0)
    if isinstance(stdout, bytes):
        stdout = stdout.decode(errors="replace")
    if isinstance(stderr, bytes):
        stderr = stderr.decode(errors="replace")
    if returncode not in (0, None):
        return "", f"{kind}_exit_{returncode}:{str(stderr)[:120]}"
    return str(stdout), None


def _write_salvage(state_dir: Path, branch: str, pass_id: str, probe: _SandboxProbe) -> list[str]:
    directory = state_dir / "salvage-patches"
    directory.mkdir(parents=True, exist_ok=True)
    # Include content identity as well as pass identity: even a failed rescue
    # retry must not replace a patch already named by a handoff decision.
    digest = hashlib.sha256(probe.committed_patch + b"\0" + probe.dirty_patch).hexdigest()[:16]
    key = f"{_derive_lane_key(branch)}.{_SAFE_ID_RE.sub('-', pass_id)}.{digest}"
    written: list[str] = []
    if probe.committed_patch:
        path = directory / f"{key}.committed.patch"
        _atomic_write(path, probe.committed_patch)
        written.append(str(path))
    if probe.state == "dirty" and probe.dirty_patch:
        path = directory / f"{key}.dirty.patch"
        _atomic_write(path, probe.dirty_patch)
        written.append(str(path))
    return written


def _handover_shared_path(
    record: Mapping[str, Any], registry: Any, git: Callable[..., Any], root: Path, *, apply: bool = True
) -> str:
    lane_id = _text(record.get("lane_id"))
    task_ref = _text(record.get("task_ref"))
    target_path = _resolved_path(record.get("worktree_path"))
    if target_path is None:
        return "worktree_path_missing"
    rows = _rows(registry)
    failed_row = next(
        (row for row in rows if _text(row.get("lane_id")) == lane_id and _text(row.get("task_ref")) == task_ref),
        None,
    )
    owners = [
        row
        for row in rows
        if (_text(row.get("task_ref")), _text(row.get("lane_id"))) != (task_ref, lane_id)
        and _resolved_path(row.get("worktree_path")) == target_path
    ]
    if len(owners) > 1:
        return "owner_ambiguous"
    if owners:
        owner = owners[0]
        if _row_live(owner):
            return "owner_live"
        status = (_text(owner.get("status")) or "").lower()
        if status in TERMINAL_LANE_STATUSES:
            if apply and not _mutate(registry, "close_lane", owner, "pass rescue shared_path_owned handoff"):
                return "owner_close_failed"
        else:
            owner_branch = _text(owner.get("branch"))
            failed_branch = _text(record.get("branch"))
            if owner_branch != failed_branch:
                return "owner_tip_mismatch"
            tip, error = _git_text(git, root, "rev-parse", "--verify", f"{failed_branch}^{{commit}}")
            observed_tip = _text(owner.get("branch_tip_sha")) or _text(owner.get("commit_sha"))
            if error or observed_tip is None or observed_tip != tip.strip():
                return "owner_tip_mismatch"
            if apply and not _mutate(registry, "close_lane", owner, "pass rescue shared_path_owned handoff"):
                return "owner_close_failed"
    if not apply:
        return "ok"
    if failed_row is None:
        failed_row = dict(record)
    if not _mutate(registry, "ensure_lane_worktree", failed_row):
        return "worktree_reclaim_failed"
    return "ok"


def _row_live(row: Mapping[str, Any]) -> bool:
    pid = row.get("worker_pid")
    return (
        any(row.get(key) is True for key in ("worker_live", "lease_live", "lock_held"))
        or (isinstance(pid, int) and pid > 0)
        or bool(_text(pid))
    )


def _rows(registry: Any) -> list[Mapping[str, Any]]:
    raw = registry() if callable(registry) else registry.list_rows()
    if isinstance(raw, dict):
        for key in ("lanes", "rows", "data"):
            candidate = raw.get(key)
            if isinstance(candidate, list):
                raw = candidate
                break
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise TypeError("list_rows did not return a row sequence")
    return [row for row in raw if isinstance(row, Mapping)]


def _mutate(registry: Any, method: str, *args: Any) -> bool:
    fn = getattr(registry, method, None)
    if not callable(fn):
        return False
    try:
        result = fn(*args)
    except Exception:  # noqa: BLE001 - mutation failure is a typed refusal
        return False
    if isinstance(result, Mapping):
        return result.get("ok", True) is not False and result.get("updated", True) is not False
    return result is not False


def _record_decision(registry: Any, record: Mapping[str, Any], decision: str, rationale: str) -> bool:
    fn = getattr(registry, "record_decision", None)
    if not callable(fn):
        return False
    try:
        result = fn(decision, rationale, record)
    except Exception:  # noqa: BLE001 - patch remains durable; mutation stays refused
        return False
    return result is not False and (not isinstance(result, Mapping) or result.get("ok", True) is not False)


def _test_cmd(record: Mapping[str, Any], registry: Any) -> str | None:
    direct = _text(record.get("test_cmd"))
    if direct:
        return direct
    lane_id = _text(record.get("lane_id"))
    task_ref = _text(record.get("task_ref"))
    try:
        for row in _rows(registry):
            if _text(row.get("lane_id")) == lane_id and _text(row.get("task_ref")) == task_ref:
                return _text(row.get("test_cmd"))
    except Exception:  # noqa: BLE001 - caller receives typed test_cmd_missing
        return None
    return None


def _rescue_state_path(state_dir: Path, pass_id: str) -> Path:
    safe = _SAFE_ID_RE.sub("-", pass_id).strip(".-") or "pass"
    return state_dir / f"pass-rescue-{safe}.json"


def _load_rescue_report(path: Path) -> RescueReport | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        report = raw["report"]
        if not isinstance(report, dict):
            return None
        if report.get("classification") not in get_args(Classification) or report.get("sandbox_state") not in get_args(
            SandboxState
        ):
            return None
        if not isinstance(report.get("pass_id"), str) or type(report.get("rearm_ready", False)) is not bool:
            return None
        for key in ("lane_id", "refused_reason"):
            if report.get(key) is not None and not isinstance(report[key], str):
                return None
        for key in ("differing_paths", "salvage_paths", "actions"):
            values = report.get(key, [])
            if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
                return None
        if report.get("lane_spec") is not None and _lane_spec(report) is None:
            return None
        return RescueReport(**report)
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _store_rescue_report(path: Path, report: RescueReport, now: Callable[[], Any]) -> None:
    payload = {"recorded_at": str(now()), "report": report.to_dict()}
    _atomic_write(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _atomic_write(path: Path, content: str | bytes) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("wb") as stream:
        stream.write(content.encode("utf-8") if isinstance(content, str) else content)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _resolved_path(value: Any) -> str | None:
    text = _text(value)
    return str(Path(text).expanduser().resolve(strict=False)) if text else None


def _text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _shell_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"
