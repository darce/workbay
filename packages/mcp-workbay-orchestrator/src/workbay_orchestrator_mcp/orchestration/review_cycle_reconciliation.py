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


def verified_refusal(repo: Path, data: dict, dispatch_id: str) -> dict:
    """Host legacy evidence producer: join existing records without upgrading them.

    Old prepared receipts need no fabricated managed_source, nonce, model-start
    or modern envelope. The canonical pass's reservation and frozen lane cycle
    join the old receipt to the exact retained adapter carrier. Missing carrier
    custody remains an explicit hold; a copied exec-result/export cannot fill it.
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
    _require(
        isinstance(pass_id, str)
        and pass_path.name == f"offload-pass-{pass_id}.json"
        and all(isinstance(x, str) and x for x in (task, lane))
        and state.get("review_cycle") == decision
        and state.get("branch") == row["subject_branch"]
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
        == (task, lane, row["subject_branch"], state.get("worktree_path")),
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
    head = config.get("start_head_ref")
    _require(
        isinstance(head, str)
        and re.fullmatch(r"[0-9a-f]{40}", head) is not None
        and cycle._git(primary, "rev-parse", head + "^{commit}") == head
        and state.get("base_sha") == state.get("tip_sha") == head,
        "source_or_commit_contradiction",
        str(pass_path),
    )
    worktree = Path(state["worktree_path"]).resolve()
    _require(
        cycle.authority(worktree).resolve() == authority
        and cycle._git(worktree, "symbolic-ref", "--short", "HEAD") == row["subject_branch"]
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
                branch=row["subject_branch"],
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
    route = row["required_route"]
    _require(
        (state.get("model"), state.get("reasoning_effort")) == (route["model"], route["effort"])
        and (config.get("model"), config.get("reasoning_effort")) == (route["model"], route["effort"]),
        "route_contradiction",
        str(pass_path),
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
        == (row["subject_branch"], head, cycle._git(primary, "rev-parse", head + "^{tree}"))
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
            not any(
                evidence.get(k)
                for k in (
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
            ),
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
            and _digest(modern.get("envelope_json", "").encode()) == carrier_sha,
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


def reconcile(request: dict, *, repo: Path | None = None) -> LedgerDecision:
    """Typed public seam; preview is read-only and apply reauthenticates under CAS."""
    try:
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
