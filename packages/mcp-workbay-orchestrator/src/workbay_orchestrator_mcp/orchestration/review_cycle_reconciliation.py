"""Bounded host-only preview/apply of a historical pre-dispatch false charge.

Run with JSON on stdin: preview takes operation and original dispatch_id; apply
also requires the preview's expected_ledger_sha256/expected_observation_sha256.
No authority locators, snapshots, routes or counters are request parameters.

Canon v0.26.2: DATA-14 No dual writes (DDIA, ch. 11); REF-09 Mutable/
derived-data drift (Refactoring, 2nd ed., ch. 9); SECD-03 Check every access at
the reference monitor / SECD-05 Fail-safe defaults (Security Engineering).
The private canonical producers are the trust boundary. Exports are evidence
for humans only. Neither a false model marker nor an absent modern source row
establishes that no historical model ran.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
from pathlib import Path
from urllib.parse import quote

from . import review_cycle_dispatch as cycle
from .review_cycle_ledger import (
    LedgerDecision,
    ReviewCycleLedger,
    _digest,
    _encoded,
    _Hold,
    correction_effects,
    correction_eligibility,
)

_SHA = re.compile(r"[0-9a-f]{64}")


class EvidenceHold(_Hold):
    """A missing/contradictory named producer relation, with no secret bytes."""

    def __init__(self, reason: str, needed: str):
        super().__init__(reason)
        self.needed = needed


def _require(condition: bool, reason: str, needed: str) -> None:
    if not condition:
        raise EvidenceHold(reason, needed)


def _read(path: Path, root: Path) -> tuple[dict, str]:
    """Read canonical host bytes; never follow a worker-selected/export path."""
    _require(path.is_relative_to(root) and path.resolve() == path, "authority_path_invalid", str(path))
    try:
        info = path.stat()
        _require(
            stat.S_ISREG(info.st_mode)
            and info.st_uid == os.getuid()
            and not info.st_mode & 0o022
            and info.st_size <= 32 * 1024 * 1024,
            "authority_custody_invalid",
            str(path),
        )
        raw = path.read_bytes()
        value = json.loads(raw)
        _require(isinstance(value, dict), "authority_shape_invalid", str(path))
        return value, _digest(raw)
    except (OSError, ValueError) as exc:
        raise EvidenceHold("authority_unavailable", str(path)) from exc


def _locator(value: object, root: Path, prefix: str) -> Path:
    _require(isinstance(value, str), "missing_producer_link", f"{root}/{prefix}* original producer locator")
    path = Path(value)
    _require(
        path.is_absolute() and path.parent == root and path.name.startswith(prefix),
        "producer_link_invalid",
        f"{root}/{prefix}* original producer locator",
    )
    return path


@contextmanager
def _dispatch_lock(path: Path):
    # The producer creates this stable lock at preparation. Preview never
    # creates it (nor a ledger, activation, migration, pass or proof).
    try:
        with path.with_suffix(".lock").open("r+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)
    except BlockingIOError as exc:
        raise EvidenceHold("dispatch_busy", str(path)) from exc


def _host(repo: Path) -> tuple[Path, Path]:
    authority = cycle.authority(repo).resolve()
    # Git's common directory belongs to the primary worktree, not the caller's
    # cwd or a request-provided state directory. Bare repos are unsupported.
    primary = authority.parent.parent
    _require((primary / ".git").resolve() == authority.parent, "canonical_primary_unavailable", "primary Git worktree")
    return primary, authority


# Any real model/job, judgment, source artifact or commit dominates refusal.
_ACTUAL_WORK_KEYS = (
    "job_id",
    "model_process_started",
    "model_started",
    "commit_landed",
    "checkpoint_commits",
    "changed_files",
    "findings",
    "source_provenance",
    "consumed_source",
    "merge_ready",
    "patch",
    "tests_run",
)


def _verified_native_refusal(
    *,
    receipt: dict,
    native: object,
    records: tuple[tuple[Path, str], ...],
    state: dict,
    result: dict,
    row: dict,
    binding: dict,
    identity: dict,
) -> dict:
    """Host-native pre-model proof, judged by settlement's own shared predicate.

    The proof was written under the dispatch lock before settlement and binds
    this receipt's identity, reservation, managed identity and token. No
    adapter carrier exists for host phases. The constructor phase may have a
    temporary carrier, but its durable native proof survives that file's cleanup.
    Any retained carrier's model/job/judgment/artifact/commit fact, or in-flight
    model call in the pass record, dominates (SECD-05). The caller joined pass, lane
    cycle, source and route to the same reservation.
    """
    (receipt_path, _), (pass_path, _) = records
    reason = cycle._native_pre_model_reason(receipt, native)
    _require(reason is not None, "native_pre_model_refusal_unproven", str(receipt_path))
    if native["phase"] == "constructor_route":
        if state.get("review_carrier_path"):
            carrier_root = Path(state["orchestrator_root"]) / ".task-state"
            carrier_path = _locator(state["review_carrier_path"], carrier_root, "lane-exec-")
            if carrier_path.exists() or carrier_path.is_symlink():
                carrier, carrier_sha = _read(carrier_path, carrier_root)
                raw = carrier.get("raw_payload")
                _require(isinstance(raw, dict), "adapter_evidence_missing", str(carrier_path))
                for evidence in (carrier, raw):
                    _require(
                        cycle._model_or_unknown_evidence(evidence) is None
                        and not any(evidence.get(key) for key in _ACTUAL_WORK_KEYS)
                        and not evidence.get("token_usage"),
                        "actual_model_or_judgment_evidence",
                        str(carrier_path),
                    )
                if "result_path" in raw:
                    model_path = Path(raw["result_path"])
                    _require(
                        model_path.is_absolute()
                        and model_path.parent.parent == carrier_root
                        and model_path.parent.name.startswith("remote-exec-")
                        and model_path.name == "result.json",
                        "missing_model_artifact_link",
                        str(carrier_path),
                    )
                    for name in (
                        "result.json",
                        "agent-stream.jsonl",
                        "turn.patch",
                        "source-provenance.json",
                        "selfverify.json",
                    ):
                        artifact = model_path.parent / name
                        _require(
                            not artifact.is_symlink() and (not artifact.exists() or artifact.stat().st_size == 0),
                            "actual_model_or_judgment_evidence",
                            str(artifact),
                        )
                records += ((carrier_path, carrier_sha),)
    else:
        _require(not state.get("review_carrier_path"), "actual_model_or_judgment_evidence", str(pass_path))
    for evidence in (state, result):
        _require(
            not any(evidence.get(key) for key in _ACTUAL_WORK_KEYS),
            "actual_model_or_judgment_evidence",
            str(pass_path),
        )
        _require(
            all(key not in evidence or evidence[key] is False for key in ("model_started", "model_process_started")),
            "actual_model_or_judgment_evidence",
            str(pass_path),
        )
    executed = result.get("dispatch_receipt") or {}
    _require(
        not any(
            executed.get(key)
            for key in ("process_ran", "served_model", "observed_model", "turns", "rounds", "observed_tokens")
        )
        and not (state.get("progress") or {}).get("model_call_in_flight"),
        "actual_model_or_judgment_evidence",
        str(pass_path),
    )
    _require(all(_digest(p.read_bytes()) == digest for p, digest in records), "stale_observation", str(pass_path))
    return dict(
        schema="workbay/verified-native-pre-model-refusal@1",
        dispatch_id=row["dispatch_id"],
        subject_branch=row["subject_branch"],
        task_ref=identity["task_ref"],
        lane_id=identity["lane_id"],
        pass_id=identity["pass_id"],
        row_id=binding["row_id"],
        cycle_epoch=binding["cycle_epoch"],
        source_head=identity["source_head"],
        source_tree=receipt["managed_source"]["head_tree"],
        outcome="dispatch_refused",
        reason=reason,
        phase=native["phase"],
        proof_sha256=_digest(_encoded(native)),
        producer_records={str(p): digest for p, digest in records},
    )


def verified_refusal(repo: Path, data: dict, dispatch_id: str) -> dict:
    """Host legacy evidence producer: join existing records without upgrading them.

    Old prepared receipts need no fabricated managed_source, nonce, model-start
    or modern envelope. The canonical pass's reservation and frozen lane cycle
    join the old receipt to the exact retained adapter carrier. Missing carrier
    custody remains an explicit hold; a copied exec-result/export cannot fill it.
    A receipt carrying a host-native pre-model proof is judged by the same
    predicate settlement uses (``cycle._native_pre_model_reason``) instead of a
    carrier; model-start/execution facts refuse either path.
    """
    primary, authority = _host(repo)
    from workbay_handoff_mcp.config import RuntimeConfig

    runtime = RuntimeConfig.for_repo(primary)
    state_dir = runtime.state_dir
    carrier_root = primary / ".task-state"

    path = authority / "review_cycle_dispatch" / (_digest(dispatch_id.encode()) + ".json")
    receipt, receipt_sha = _read(path, authority)
    row = data["rows"][dispatch_id]
    identity = {k: row[k] for k in ("dispatch_id", "subject_branch", "kind")}
    _require(
        receipt.get("identity") == identity
        and receipt.get("settlement_owner") == "engine"
        and receipt.get("state") == "prepared"
        and isinstance(receipt.get("token"), str)
        and re.fullmatch(r"[0-9a-f]{48}", receipt["token"]) is not None,
        "dispatch_authority_mismatch",
        str(path),
    )
    decision = receipt.get("decision")
    _require(isinstance(decision, dict) and decision.get("status") == "reserved", "reservation_link_invalid", str(path))
    original = decision.get("row") or {}
    _require(
        all(
            original.get(k) == row.get(k)
            for k in original
            if k not in {"completion", "outcome_class", "settled_sequence", "settlement_status"}
        )
        and original.get("completion") is None
        and original.get("outcome_class") == "pending"
        and all(original.get(k) == row[k] for k in identity),
        "reservation_link_invalid",
        str(path),
    )
    # Lifetime history is not an authority condition. Keep only the target
    # record, with _read's per-record bounds, and finish the scan for duplicates.
    needed = f"{state_dir}/offload-pass-*.json: review_cycle_dispatch_id={dispatch_id}"
    match = None
    for candidate in state_dir.glob("offload-pass-*.json"):
        value, digest = _read(candidate, state_dir)
        if value.get("review_cycle_dispatch_id") == dispatch_id:
            if match is not None:
                raise EvidenceHold("missing_or_ambiguous_pass_link", needed)
            match = (candidate, value, digest)
    if match is None:
        raise EvidenceHold("missing_or_ambiguous_pass_link", needed)
    pass_path, state, pass_sha = match
    config, binding, result = (
        state.get("completion_config") or {},
        state.get("lane_cycle_binding") or {},
        state.get("result") or {},
    )
    pass_id, task, lane = config.get("pass_id"), state.get("task_ref"), state.get("lane_id")
    source_branch = state.get("branch")
    _require(
        isinstance(pass_id, str)
        and pass_path.name == f"offload-pass-{pass_id}.json"
        and all(isinstance(x, str) and x for x in (task, lane))
        and state.get("review_cycle") == decision
        and isinstance(source_branch, str)
        and bool(source_branch)
        and state.get("review_cycle_subject_branch") == row["subject_branch"]
        and state.get("status") == "done"
        and state.get("terminal_at")
        and not state.get("superseded_invocations")
        and not state.get("job_id")
        and Path(state.get("orchestrator_root", "")).resolve() == primary,
        "pass_identity_or_state_mismatch",
        str(pass_path),
    )
    _require(
        binding.get("v") == 1
        and type(binding.get("row_id")) is int
        and type(binding.get("cycle_epoch")) is int
        and (binding.get("task_ref"), binding.get("lane_id"), binding.get("branch"), binding.get("worktree_path"))
        == (task, lane, source_branch, state.get("worktree_path")),
        "missing_lane_cycle_link",
        f"{pass_path}: first-publication lane_cycle_binding",
    )
    # Read-only SQLite avoids schema initialization or a current-task write in
    # preview. The DB path comes from the same deployment config as publication.
    try:
        with sqlite3.connect("file:" + quote(str(runtime.db_path)) + "?mode=ro", uri=True, timeout=0.05) as conn:
            conn.row_factory = sqlite3.Row
            records = conn.execute("SELECT * FROM worktree_lanes WHERE id=?", (binding["row_id"],)).fetchall()
    except sqlite3.Error as exc:
        raise EvidenceHold("lane_cycle_unavailable", str(runtime.db_path)) from exc
    _require(len(records) == 1, "lane_cycle_unavailable", f"canonical worktree_lanes id={binding['row_id']}")
    registered = dict(records[0])
    _require(
        all(
            registered.get(k) == binding.get(k)
            for k in ("task_ref", "lane_id", "branch", "worktree_path", "cycle_epoch")
        ),
        "lane_cycle_changed",
        f"canonical worktree_lanes id={binding['row_id']} epoch={binding['cycle_epoch']}",
    )
    completed_cycle = result.get("review_cycle") or {}
    _require(
        completed_cycle.get("reservation") == decision and (completed_cycle.get("settlement") or {}).get("row") == row,
        "completion_ledger_link_invalid",
        str(pass_path),
    )
    # The engine's own actual/unknown execution observation outlives any
    # temporary carrier; no surviving refusal proof can forgive it.
    _require(not completed_cycle.get("observed_execution"), "actual_model_or_judgment_evidence", str(pass_path))
    head = config.get("start_head_ref")
    # A host-native pre-model exit (history/attestation) may stop before the
    # engine records base/tip; absence is allowed only for that positive proof.
    native = receipt.get("native_pre_model_refusal")
    _require(
        isinstance(head, str)
        and re.fullmatch(r"[0-9a-f]{40}", head) is not None
        and cycle._git(primary, "rev-parse", head + "^{commit}") == head
        and (
            state.get("base_sha") == state.get("tip_sha") == head
            or (native is not None and state.get("base_sha") in (None, head) and state.get("tip_sha") in (None, head))
        ),
        "source_or_commit_contradiction",
        str(pass_path),
    )
    worktree = Path(state["worktree_path"]).resolve()
    _require(
        cycle.authority(worktree).resolve() == authority
        and cycle._git(worktree, "symbolic-ref", "--short", "HEAD") == source_branch
        and cycle._git(worktree, "rev-parse", "HEAD") == head,
        "source_or_commit_contradiction",
        str(worktree),
    )
    managed = receipt.get("managed_identity")
    if managed is not None:
        _require(
            managed == dict(task_ref=task, lane_id=lane, pass_id=pass_id, worktree_path=str(worktree))
            and receipt.get("managed_source")
            == dict(
                branch=source_branch,
                head_sha=head,
                head_tree=cycle._git(primary, "rev-parse", head + "^{tree}"),
            ),
            "managed_source_mismatch",
            str(path),
        )
    else:
        base = binding.get("source_base_sha")
        _require(
            isinstance(base, str)
            and re.fullmatch(r"[0-9a-f]{40}", base) is not None
            and cycle._git(primary, "merge-base", base, head) == base,
            "missing_legacy_source_base_link",
            f"{pass_path}: original first-publication source_base_sha",
        )
        if source_branch != row["subject_branch"]:
            # An old carrier cannot declare its own original subject. Reuse
            # the supported registry/host-receipt join; absent custody holds.
            _require(
                cycle.canonical_subject(
                    worktree,
                    state_dir,
                    task_ref=task,
                    branch=source_branch,
                    pass_id=dispatch_id,
                    orchestrator_root=primary,
                )
                == row["subject_branch"],
                "original_source_alias_unproven",
                str(path),
            )
    route = row["required_route"]
    constructor = (
        native is not None
        and cycle._native_pre_model_reason(receipt, native) is not None
        and (native["phase"] == "constructor_route")
    )
    if constructor:
        route = native["constructor"]["requested_route"]
    if route is None:
        route = dict(model=state.get("model"), effort=state.get("reasoning_effort"))
    # The same spelling normalization enforce_route applied to the executed
    # argv: a genuine claude-opus-5-5 run IS the reserved opus-5.5 route.
    required = (cycle.normalized_model(route["model"]), route["effort"])
    _require(
        all(isinstance(value, str) and value for value in required)
        and (cycle.normalized_model(state.get("model")), state.get("reasoning_effort")) == required
        and (cycle.normalized_model(config.get("model")), config.get("reasoning_effort")) == required,
        "route_contradiction",
        str(pass_path),
    )
    if constructor:
        _require(
            state.get("backend") == config.get("backend") == route["backend"], "route_contradiction", str(pass_path)
        )
    if native is not None:
        _require(managed is not None, "managed_source_mismatch", str(path))
        return _verified_native_refusal(
            receipt=receipt,
            native=native,
            records=((path, receipt_sha), (pass_path, pass_sha)),
            state=state,
            result=result,
            row=row,
            binding=binding,
            identity=dict(task_ref=task, lane_id=lane, pass_id=pass_id, source_head=head),
        )
    carrier_path = _locator(state.get("review_carrier_path"), carrier_root, "lane-exec-")
    carrier, carrier_sha = _read(carrier_path, carrier_root)
    raw = carrier.get("raw_payload") or {}
    _require(isinstance(raw, dict), "adapter_evidence_missing", str(carrier_path))
    # The raw constructor record is retained by the host receiver. A worker's
    # nested raw_payload is never consulted, and false alone grants nothing.
    refusal = raw.get("pre_dispatch_refusal") or {}
    _require(
        raw.get("adapter_refusal") is True
        and raw.get("typed_outcome") is True
        and raw.get("retryable") is False
        and raw.get("refusal_cause") == "policy_or_classifier_refusal"
        and refusal == {"outcome": "dispatch_refused", "reason": "capability_missing"}
        and raw.get("failure_reason") == "capability_missing"
        and raw.get("attestation") == {"status": "failed", "reason": "capability_missing"}
        and raw.get("probe_reason") == "script_unparseable"
        and raw.get("consumed_source") is None
        and raw.get("uncommitted_remainder_present") is False
        and raw.get("review_cycle") == decision,
        "adapter_refusal_unproven",
        str(carrier_path),
    )
    source = raw.get("selected_source") or {}
    _require(
        (source.get("branch"), source.get("head_sha"), source.get("head_tree"))
        == (source_branch, head, cycle._git(primary, "rev-parse", head + "^{tree}"))
        and source.get("transport_source") == "worktree"
        and source.get("transport_path") == str(worktree / "scripts/remote_agent.sh")
        and isinstance(source.get("transport_digest"), str)
        and _SHA.fullmatch(source["transport_digest"]),
        "adapter_source_link_invalid",
        str(carrier_path),
    )
    # Authenticate immutable source bytes, never today's repaired script or a
    # supplied/guessed SHA. A dirty generated script needs its retained producer
    # custody record; absent that relation this implementation deliberately holds.
    script = subprocess.run(
        ["git", "-C", str(primary), "show", head + ":scripts/remote_agent.sh"],
        capture_output=True,
        check=True,
        timeout=5,
        env={
            k: v
            for k, v in os.environ.items()
            if k not in {"GIT_DIR", "GIT_COMMON_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"}
        },
    ).stdout
    _require(
        _digest(script) == source["transport_digest"],
        "missing_immutable_transport_link",
        f"{head}:scripts/remote_agent.sh matching retained adapter transport_digest",
    )
    preflight = raw.get("preflight_source") or {}
    consumed = preflight.get("consumed_source") or {}
    _require(
        (preflight.get("selected_source") or {}).get("transport_digest") == source["transport_digest"]
        and consumed.get("transport_digest") == source["transport_digest"]
        and isinstance(consumed.get("transport_path"), str)
        and Path(consumed["transport_path"]).parent == worktree / "scripts"
        and Path(consumed["transport_path"]).name.startswith(".workbay-transport-"),
        "missing_probe_custody_link",
        str(carrier_path),
    )
    # Any real model/job, judgment, source artifact or commit dominates refusal.
    for evidence in (state, result, carrier, raw):
        _require(
            not any(evidence.get(k) for k in _ACTUAL_WORK_KEYS),
            "actual_model_or_judgment_evidence",
            str(pass_path),
        )
    executed = result.get("dispatch_receipt") or {}
    _require(
        not any(executed.get(key) for key in ("served_model", "turns", "rounds", "observed_tokens"))
        and not (state.get("progress") or {}).get("model_call_in_flight"),
        "actual_model_or_judgment_evidence",
        str(pass_path),
    )
    # The same pre-model predicate settlement's reader applies: in the retained
    # constructor record, model start is absent or exactly False and no
    # execution/unknown dispatch facts exist. A false-or-missing marker alone
    # still proves nothing; the positive constructor conjunction above does.
    _require(
        cycle._model_or_unknown_evidence(carrier) is None and cycle._model_or_unknown_evidence(raw) is None,
        "actual_model_or_judgment_evidence",
        str(carrier_path),
    )
    model_path = Path(raw.get("result_path", ""))
    _require(
        model_path.is_absolute()
        and model_path.parent.parent == carrier_root
        and model_path.parent.name.startswith("remote-exec-")
        and model_path.name == "result.json",
        "missing_model_artifact_link",
        str(carrier_path),
    )
    # No absence-by-glob. The adapter's exact named spool is needed, and its
    # model result/stream/patch/provenance must not contain actual output.
    _require(model_path.parent.is_dir(), "missing_model_artifact_link", str(model_path.parent))
    for name in ("result.json", "agent-stream.jsonl", "turn.patch", "source-provenance.json", "selfverify.json"):
        artifact = model_path.parent / name
        _require(
            not artifact.is_symlink() and (not artifact.exists() or artifact.stat().st_size == 0),
            "actual_model_or_judgment_evidence",
            str(artifact),
        )
    # A retained modern observation, when present, must match as well. Legacy
    # receipts are never modified to pretend they carried this field originally.
    modern = receipt.get("adapter_pre_dispatch_refusal")
    if modern is not None:
        _require(
            modern.get("envelope_sha256") == carrier_sha
            and _digest(modern.get("envelope_json", "").encode()) == carrier_sha
            and cycle._adapter_refusal_reason(receipt, raw) == "capability_missing",
            "retained_observation_mismatch",
            str(path),
        )
    _require(
        all(
            _digest(p.read_bytes()) == digest
            for p, digest in ((path, receipt_sha), (pass_path, pass_sha), (carrier_path, carrier_sha))
        ),
        "stale_observation",
        str(pass_path),
    )
    return dict(
        schema="workbay/verified-legacy-pre-dispatch-refusal@1",
        dispatch_id=dispatch_id,
        subject_branch=row["subject_branch"],
        task_ref=task,
        lane_id=lane,
        pass_id=pass_id,
        row_id=binding["row_id"],
        cycle_epoch=binding["cycle_epoch"],
        source_head=head,
        source_tree=source["head_tree"],
        transport_sha256=source["transport_digest"],
        outcome="dispatch_refused",
        reason="capability_missing",
        probe_reason="script_unparseable",
        producer_records={str(path): receipt_sha, str(pass_path): pass_sha, str(carrier_path): carrier_sha},
    )


def _bootstrap_producer_custody(repo: Path, primary: Path, runtime, receipt: dict, row: dict) -> tuple[dict, dict]:
    """Observe an original managed producer join; completion alone cannot release.

    No reconstruction of source_task_subject occurs here. A missing source-time
    scope is still missing even when the managed source and native lane join.
    """
    from .review_cycle_terminal_recovery import _recovery_path, validated_record

    recovery_path = _recovery_path(repo, row["dispatch_id"])
    if recovery_path.exists() or recovery_path.is_symlink():
        recovery = validated_record(repo, receipt)
        observation = recovery["observation"]
        return dict(status="authenticated", managed_identity=observation["managed_identity"],
            managed_source=observation["source"], lane_cycle_binding=observation["lane_cycle_binding"],
            pass_status="done", native_settlement_matches=True,
            recovered_source_scope=observation["source_scope"],
            native_terminal_recovery=str(recovery_path), execution=observation["execution"],
            required_host_evidence=None), {**observation["producer_records"],
                                         str(recovery_path): _digest(recovery_path.read_bytes())}
    managed, source = receipt.get("managed_identity"), receipt.get("managed_source")
    unknown = dict(status="unproven", required_host_evidence="original managed_identity/managed_source and first-publication native pass lane_cycle_binding")
    if not isinstance(managed, dict) or not isinstance(source, dict):
        return unknown, {}
    pass_id = managed.get("pass_id")
    if not isinstance(pass_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", pass_id) or pass_id.startswith("."):
        return unknown, {}
    path = runtime.state_dir / f"offload-pass-{pass_id}.json"
    if not path.exists():
        return {**unknown, "required_host_evidence": str(path) + ": original native producer record"}, {}
    state, digest = _read(path, runtime.state_dir)
    binding, config = state.get("lane_cycle_binding") or {}, state.get("completion_config") or {}
    expected = dict(branch=receipt.get("source_branch"), head_sha=receipt.get("source_head"), head_tree=receipt.get("source_tree"))
    valid = (
        source == expected and state.get("review_cycle_dispatch_id") == row["dispatch_id"]
        and state.get("review_cycle") == receipt["decision"]
        and state.get("review_cycle_subject_branch") == row["subject_branch"]
        and state.get("branch") == expected["branch"]
        and state.get("orchestrator_root") == str(primary)
        and config.get("pass_id") == pass_id and config.get("start_head_ref") == expected["head_sha"]
        and all(state.get(k) == managed.get(k) for k in ("task_ref", "lane_id", "worktree_path"))
        and binding.get("v") == 1 and type(binding.get("row_id")) is int
        and type(binding.get("cycle_epoch")) is int
        and all(binding.get(k) == state.get(k) for k in ("task_ref", "lane_id", "branch", "worktree_path"))
    )
    if not valid:
        return {**unknown, "required_host_evidence": str(path) + ": matching original managed source, reservation, completion identity and lane-cycle join"}, {str(path): digest}
    from .review_cycle_bootstrap import _lane
    registered = _lane(runtime, binding["row_id"])
    if registered is None or any(registered.get(k) != binding.get(k)
                                 for k in ("task_ref", "lane_id", "branch", "worktree_path", "cycle_epoch")):
        return {**unknown, "required_host_evidence": "unchanged native producer row id=" + str(binding["row_id"]) + " and original cycle_epoch"}, {str(path): digest}
    settlement = ((state.get("result") or {}).get("review_cycle") or {}).get("settlement") or {}
    return dict(status="authenticated", managed_identity=managed, managed_source=source,
                lane_cycle_binding=binding, pass_status=state.get("status"),
                native_settlement_matches=row.get("completion") is not None and settlement.get("row") == row,
                required_host_evidence=None), {str(path): digest}


def _bootstrap_receipts(repo: Path, data: dict) -> tuple[dict, dict]:
    """Every native dispatch receipt, custody-read and linked to its ledger row."""
    private = cycle.authority(repo)
    directory = private / "review_cycle_dispatch"
    records, receipts = {}, {}
    paths = sorted(directory.iterdir()) if directory.exists() else []
    for path in paths:
        if path.suffix != ".json":
            continue
        receipt, digest = _read(path, private)
        identity = receipt.get("identity") or {}
        dispatch_id = identity.get("dispatch_id")
        _require(isinstance(dispatch_id, str) and path.stem == _digest(dispatch_id.encode()),
                 "holder_custody_unavailable", str(path) + ": original native dispatch identity")
        row = data["rows"].get(dispatch_id)
        original = (receipt.get("decision") or {}).get("row") or {}
        mutable = {"completion", "outcome_class", "settled_sequence", "settlement_status"}
        _require(isinstance(row, dict) and identity == {k: row[k] for k in ("dispatch_id", "subject_branch", "kind")}
                 and (receipt.get("decision") or {}).get("status") == "reserved"
                 and original.get("dispatch_id") == dispatch_id
                 and all(row.get(k) == v for k, v in original.items() if k not in mutable),
                 "holder_custody_unavailable", str(path) + ": original reservation linked to canonical ledger row")
        if receipt.get("source_head"):
            _require(cycle._git(repo, "rev-parse", receipt["source_head"] + "^{tree}") == receipt.get("source_tree"),
                     "holder_source_mismatch", str(path) + ": immutable native source commit/tree")
        records[str(path)] = digest
        receipts[path.name] = (receipt, row)
    return records, receipts


def bootstrap_charged_subject(repo: Path, primary: Path, proof: dict, data: dict) -> tuple[dict, dict]:
    """The subject native dispatch would charge for this source, after custody.

    Reuses ``retained_subject``, including its direct-match precedence, over
    receipts whose custody and ledger rows were all just authenticated. The
    incoming proof stays the source identity; the derived subject owns the grant
    and counters. Several applicable subjects stay a typed hold, never a choice.
    Canon v0.26.2: GRPH-14 Record provenance as a walkable lineage graph
    (Building Knowledge Graphs, ch. 8 + Mental Models, ch. 8); SECD-03 Check
    every access at the reference monitor (Security Engineering).
    """
    records, receipts = _bootstrap_receipts(repo, data)
    derived = dict(incoming_branch=proof["subject_branch"], relation="review_cycle_dispatch.retained_subject")
    try:
        subject = cycle.retained_subject(repo, proof["subject_branch"], proof, orchestrator_root=primary)
    except cycle.SubjectAmbiguous as exc:
        return dict(status="ambiguous", subjects=sorted({row["subject"] for row in exc.holding}),
                    receipts=sorted(row["receipt"] for row in exc.holding), **derived), records
    # A retained subject other than the proof's must be a custody-read native charge.
    _require(subject == proof["subject_branch"]
             or any(row["subject_branch"] == subject for _, row in receipts.values()),
             "charged_subject_unproven", "custody-linked native receipt and ledger row for the retained subject")
    return dict(status="derived", subject_branch=subject, **derived), records


def bootstrap_holder_report(repo: Path, primary: Path, runtime, proof: dict, data: dict) -> dict:
    """Authenticate custody before consuming the existing holder applicability rules.

    GRPH-09 Colour the conflict graph (Graph Theory with Applications, ch. 8),
    Canon v0.26.2: resource/producer relations are checked, not branch labels.
    An empty branch disables the report's historical direct-match short circuit
    (empty source branches cannot be holders), so every ancestry holder is read.
    This is an observation, never a release event or a second settlement ledger.
    """
    directory = cycle.authority(repo) / "review_cycle_dispatch"
    records, receipts = _bootstrap_receipts(repo, data)
    # Missing native source receipts cannot be replaced by a current registry
    # snapshot. Imported rows retain their separate original producer evidence.
    for row in data["rows"].values():
        if row.get("decision") == "reserved" and not row.get("legacy_source"):
            name = _digest(row["dispatch_id"].encode()) + ".json"
            _require(name in receipts, "holder_custody_unavailable",
                     str(directory / name) + ": original native dispatch receipt")
    holding = cycle.subject_hold_report(repo, "", proof, orchestrator_root=primary)
    named = {item["receipt"] for item in holding}
    custody_by_receipt = {}
    # Historical discovery can release on task completion. Maintenance must
    # additionally retain an unstamped original whose native producer/harvest
    # is unknown. A finished task or pass is not native settlement evidence.
    for name, (receipt, row) in receipts.items():
        head, branch = receipt.get("source_head"), receipt.get("source_branch")
        if name in named or receipt.get("source_task_subject") is not None or not head or not branch:
            continue
        if cycle._git(repo, "merge-base", head, "HEAD") != head:
            continue
        custody, linked = _bootstrap_producer_custody(repo, primary, runtime, receipt, row)
        records.update(linked)
        custody_by_receipt[name] = custody
        if custody.get("native_terminal_recovery") and custody.get("native_settlement_matches"):
            # Native subject_hold_report just rechecked this recovery's source
            # and release predicate. Do not re-add a diagnostic phantom holder.
            continue
        if (row.get("completion") is None or custody["status"] != "authenticated"
                or not custody.get("native_settlement_matches")):
            holding.append(dict(receipt=name, subject=row["subject_branch"], source_branch=branch, source_head=head,
                                has_source_task_subject=False,
                                predicates_tried=["subject_hold_report", "original_native_producer_custody", "native_settlement"]))
    holders = []
    for item in sorted(holding, key=lambda item: item["receipt"]):
        receipt, row = receipts[item["receipt"]]
        custody = custody_by_receipt.get(item["receipt"])
        if custody is None:
            custody, linked = _bootstrap_producer_custody(repo, primary, runtime, receipt, row)
            records.update(linked)
        snapshot = receipt.get("source_task_subject") or custody.get("recovered_source_scope")
        if (row.get("completion") is not None and row["subject_branch"] == proof["subject_branch"]
                and isinstance(snapshot, dict) and snapshot == proof):
            # This task's authenticated, settled history retains its charge,
            # but is not a foreign producer claiming this source.
            continue
        pending = row.get("completion") is None
        needed = []
        if custody["status"] != "authenticated":
            needed.append(custody["required_host_evidence"])
        if snapshot is None:
            needed.append("original source-time scope producer evidence; current notes/finished pass cannot retroactively stamp source_task_subject")
        if pending:
            needed.append("authoritative native harvest/settlement for dispatch " + row["dispatch_id"] + "; pass status done alone is insufficient")
        else:
            needed.append("existing authoritative subject release predicate: authenticated source scope and typed producer landing/resource independence")
        holders.append({**item, "dispatch_id": row["dispatch_id"], "native_round": row["round"],
                        "state": "unharvested" if pending else "retained",
                        "native_outcome_class": row["outcome_class"], "producer_custody": custody,
                        "required_host_evidence": needed})
    return dict(holders=holders, producer_records=records)


def reconcile(request: dict, *, repo: Path | None = None) -> LedgerDecision:
    """Typed public seam; preview is read-only and apply reauthenticates under CAS."""
    try:
        if isinstance(request, dict) and request.get("operation") in {"terminal_preview", "terminal_apply", "source_preview", "source_apply"}:
            from .review_cycle_terminal_recovery import recover

            return recover(request, repo=Path(repo or Path.cwd()).resolve())
        if isinstance(request, dict) and request.get("operation") == "bootstrap_check":
            from .review_cycle_bootstrap import check
            receipt = check({**request, "operation": "check"}, repo=repo)
            return LedgerDecision(receipt["status"], receipt["reason"], receipt)
        operation, dispatch_id = request["operation"], request["dispatch_id"]
        allowed = {"operation", "dispatch_id", "expected_ledger_sha256", "expected_observation_sha256"}
        _require(
            operation in {"preview", "apply"}
            and not set(request) - allowed
            and isinstance(dispatch_id, str)
            and bool(dispatch_id),
            "invalid_protocol",
            "original dispatch_id and CAS digests",
        )
        repo = Path(repo or Path.cwd()).resolve()
        _, authority = _host(repo)
        ledger = ReviewCycleLedger(authority / "review_cycle_ledger.json", cycle.trusted_policy())
        if not ledger.path.exists():
            return LedgerDecision("not_applicable", "no_ledger_row")
        path = authority / "review_cycle_dispatch" / (_digest(dispatch_id.encode()) + ".json")
        # Global lock -> dispatch lock, both NB. No reverse producer holds the
        # dispatch lock while settling. All proof reads are bounded local work.
        if operation == "apply":
            current = ledger._read()
            if dispatch_id not in current["rows"]:
                return LedgerDecision("not_applicable", "no_ledger_row")
            for key in ("expected_ledger_sha256", "expected_observation_sha256"):
                _require(isinstance(request.get(key), str) and _SHA.fullmatch(request[key]), "invalid_protocol", key)
            with ExitStack() as proof_locks:

                def authenticate(data):
                    # Keep the producer lock until the event is durable, not
                    # merely until the evidence callback returns.
                    proof_locks.enter_context(_dispatch_lock(path))
                    return verified_refusal(repo, data, dispatch_id)

                return ledger.append_outcome_correction(
                    dispatch_id=dispatch_id,
                    expected_ledger_sha256=request["expected_ledger_sha256"],
                    expected_observation_sha256=request["expected_observation_sha256"],
                    authenticate=authenticate,
                )
        raw = ledger.path.read_bytes()
        data = ledger._decode(raw)
        eligible = correction_eligibility(data, dispatch_id)
        if eligible.status != "eligible":
            return eligible
        with _dispatch_lock(path):
            observation = verified_refusal(repo, data, dispatch_id)
        _require(ledger.path.read_bytes() == raw, "stale_ledger", str(ledger.path))
        return LedgerDecision(
            "preview",
            "",
            dict(
                dispatch_id=dispatch_id,
                expected_ledger_sha256=_digest(raw),
                expected_observation_sha256=_digest(_encoded(observation)),
                observation=observation,
                effects=correction_effects(data, dispatch_id, ledger.policy),
            ),
        )
    except EvidenceHold as exc:
        return LedgerDecision("ledger_hold", str(exc), {"required_host_evidence": exc.needed})
    except (_Hold, OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.SubprocessError) as exc:
        # Do not interpolate arbitrary exception text: producer bytes may hold
        # private tokens. Named EvidenceHold errors are the usable diagnostics.
        return LedgerDecision("ledger_hold", str(exc) if isinstance(exc, _Hold) else type(exc).__name__)


def main() -> None:
    try:
        request = json.load(sys.stdin)
        result = reconcile(request)
    except (ValueError, TypeError):
        result = LedgerDecision("ledger_hold", "invalid_protocol")
    print(json.dumps(asdict(result), sort_keys=True))


if __name__ == "__main__":
    main()
