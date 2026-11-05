"""Native recovery of an old synchronous engine terminal producer.

Public via review_cycle_reconciliation: terminal_preview / terminal_apply take
only dispatch_id and (for apply) the preview's ledger/observation SHA256s.
No worker result, scope, route, outcome or authority path is an input.

The immutable recovery record is an intent, not another accounting ledger. It
becomes usable only when the canonical ledger has its charged terminal fact.
A crash before ledger commit leaves an inert, replayable intent. Existing
settled rows and their ledger bytes are preserved; pending settlement reuses
ReviewCycleLedger.settle inside the one native ledger transaction. No source
receipt, pass, grant, route, ordinal or approval threshold is rewritten.

Canon v0.26.2: SECD-03 Check every access at the reference monitor / SECD-05
Fail-safe defaults (Security Engineering); DATA-14 No dual writes (Designing
Data-Intensive Applications, ch. 11); GRPH-14 Record provenance as a walkable
lineage graph (Building Knowledge Graphs, ch. 8 + Mental Models, ch. 8).
"""

from __future__ import annotations

import re
import subprocess
from contextlib import ExitStack
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace

from . import review_cycle_dispatch as cycle
from . import review_ratchet as ratchet
from .review_cycle_ledger import LedgerDecision, ReviewCycleLedger, _digest, _encoded, _Hold, _Unchanged
from .review_cycle_reconciliation import _SHA, EvidenceHold, _dispatch_lock, _host, _read, _require

_ID = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*")
_OID = re.compile(r"[0-9a-f]{40}")
_MUTABLE = {"completion", "outcome_class", "settled_sequence", "settlement_status"}
_SCHEMA = "workbay/original-terminal-recovery@1"


def _ledger(repo: Path) -> ReviewCycleLedger:
    """Read the deployed policy without activation, migration or cache writes."""
    config = cycle._configured_review_cycle(repo)
    policy = replace(
        cycle.trusted_policy(),
        routes=config.get("routes", {}),
        approval_thresholds=config.get("approval_thresholds", {}),
    )
    return ReviewCycleLedger(cycle.authority(repo) / "review_cycle_ledger.json", policy, repo=repo)


def _recovery_path(repo: Path, dispatch_id: str) -> Path:
    return cycle.authority(repo) / "review_cycle_terminal_recovery" / (_digest(dispatch_id.encode()) + ".json")


@dataclass(frozen=True)
class _Observed:
    proof: dict
    receipt: dict
    pass_path: Path
    scope_path: Path
    state_dir: Path
    lane: dict


def _original_scope(repo: Path, private: Path, managed: dict, binding: dict, source: dict) -> tuple[dict, Path, str]:
    """Recover only an existing protected original, never current notes as proof.

    The first-publication native binding supplies the original row/base and
    worktree. The registry producer's own validator authenticates the scope
    file name, original binding and monotonic authorizations. Current notes
    may locate or describe work but cannot supply this missing edge.
    """
    matches = []
    for path in (private / "task_subject_scopes").glob("*.json"):
        record, digest = _read(path, private)
        original = record.get("original")
        if not isinstance(original, dict) or any(
            original.get(k) != v
            for k, v in {
                "task_ref": managed["task_ref"],
                "lane_id": managed["lane_id"],
                "subject_branch": source["branch"],
                "base_sha": binding["source_base_sha"],
            }.items()
        ):
            continue
        _require(
            ratchet._scope_path(repo, original) == path
            and ratchet._scope_record(repo, original) == record
            and record.get("worktree_path") == managed["worktree_path"],
            "source_scope_custody_invalid",
            "original protected scope publication and exact native worktree",
        )
        roots = ratchet._owned_roots(original.get("owned_paths"))
        head, base, tip = source["head_sha"], original.get("base_sha"), original.get("tip_sha")
        _require(
            roots is not None
            and all(isinstance(x, str) and _OID.fullmatch(x) for x in (base, tip))
            and ratchet._commit(repo, base) == base
            and ratchet._commit(repo, tip) == tip
            and ratchet._subject_is_ancestor(repo, base, tip)
            and ratchet._subject_is_ancestor(repo, tip, head)
            and isinstance(original.get("integration_branch"), str)
            and bool(original["integration_branch"]),
            "source_scope_git_mismatch",
            "original protected scope base/tip ancestral to the immutable native source",
        )
        changed = cycle._git(repo, "diff", "--name-only", "--no-renames", base, tip, "--").splitlines()
        source_changes = cycle._git(repo, "diff", "--name-only", "--no-renames", base, head, "--").splitlines()
        _require(
            sorted(changed) == original.get("changed_files")
            and all(any(p == root or p.startswith(root + "/") for root in roots) for p in source_changes),
            "source_scope_git_mismatch",
            "source Git delta inside original source-time owned roots",
        )
        matches.append((original, path, digest))
    _require(
        len(matches) == 1,
        "source_scope_original_join_unproven",
        "one original protected task_subject_scopes record matching native task/lane/base/branch/worktree; current notes are insufficient",
    )
    return matches[0]


def _authenticate(repo: Path, data: dict, dispatch_id: str, *, historical: dict | None = None) -> _Observed:
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime

    from .review_cycle_bootstrap import _lane
    from .terminal_history_repair import _expected_completion

    primary, private = _host(repo)
    runtime = RuntimeConfig.for_repo(primary)
    configure_runtime(runtime)
    path = private / "review_cycle_dispatch" / (_digest(dispatch_id.encode()) + ".json")
    receipt, receipt_sha = _read(path, private)
    row = data["rows"].get(dispatch_id)
    _require(isinstance(row, dict), "no_ledger_row", "original native reservation")
    identity = {key: row.get(key) for key in ("dispatch_id", "subject_branch", "kind")}
    decision = receipt.get("decision") or {}
    reserved = decision.get("row") or {}
    _require(
        receipt.get("identity") == identity
        and receipt.get("settlement_owner") == "engine"
        and receipt.get("state") == "transport_started"
        and decision.get("status") == "reserved"
        and reserved.get("decision") == "reserved"
        and reserved.get("outcome_class") == "pending"
        and reserved.get("completion") is None
        and all(row.get(k) == v for k, v in reserved.items() if k not in _MUTABLE)
        and all(reserved.get(k) == v for k, v in identity.items())
        and isinstance(receipt.get("token"), str)
        and re.fullmatch(r"[0-9a-f]{48}", receipt["token"]) is not None,
        "dispatch_authority_mismatch",
        "original engine transfer and immutable native reservation",
    )
    _require(
        row["kind"] in {"implement", "analysis"},
        "gate_harvest_required",
        "canonical review/gate harvest; synchronous source recovery cannot establish a review verdict",
    )
    _require(
        not any(e.get("dispatch_id") == dispatch_id for e in data.get("corrections", [])),
        "terminal_correction_conflict",
        "uncorrected full-charge original native row",
    )
    managed, source = receipt.get("managed_identity"), receipt.get("managed_source")
    _require(
        isinstance(managed, dict) and isinstance(source, dict),
        "managed_source_missing",
        "original protected managed_identity and managed_source",
    )
    pass_id = managed.get("pass_id")
    _require(
        isinstance(pass_id, str) and _ID.fullmatch(pass_id) is not None,
        "pass_identity_mismatch",
        "original native managed pass locator",
    )
    pass_path = runtime.state_dir / f"offload-pass-{pass_id}.json"
    state, pass_sha = _read(pass_path, runtime.state_dir)
    binding, config, result = state.get("lane_cycle_binding"), state.get("completion_config"), state.get("result")
    _require(
        all(isinstance(x, dict) for x in (binding, config, result)),
        "pass_identity_mismatch",
        "native first-publication lane binding, frozen completion config and terminal result",
    )
    _require(
        all(isinstance(managed.get(k), str) and managed[k] for k in ("task_ref", "lane_id", "worktree_path"))
        and all(state.get(k) == managed[k] for k in ("task_ref", "lane_id", "worktree_path"))
        and config.get("pass_id") == pass_id
        and state.get("review_cycle_dispatch_id") == dispatch_id
        and state.get("review_cycle_subject_branch") == row["subject_branch"]
        and state.get("review_cycle") == decision
        and state.get("orchestrator_root") == str(primary)
        and state.get("status") == "done"
        and bool(state.get("terminal_at"))
        and not state.get("superseded_invocations")
        and not state.get("job_id")
        and isinstance(state.get("invocation_token"), str)
        and re.fullmatch(r"[0-9a-f]{32}", state["invocation_token"]) is not None
        and all(
            result.get(k) == v
            for k, v in {"task_ref": managed["task_ref"], "lane_id": managed["lane_id"], "pass_id": pass_id}.items()
        )
        and (result.get("review_cycle") or {}).get("reservation") == decision,
        "pass_identity_mismatch",
        "unchanged original synchronous invocation, reservation and native terminal producer",
    )
    # Optional echoed producer identities may not contradict their native owner.
    _require(
        all(
            result[k] == state.get(k)
            for k in (
                "branch",
                "base_sha",
                "tip_sha",
                "worktree_path",
                "review_cycle_dispatch_id",
                "review_cycle_subject_branch",
            )
            if k in result
        ),
        "terminal_identity_mismatch",
        "terminal producer and first-publication identity agree",
    )
    _require(
        source
        == dict(
            branch=receipt.get("source_branch"),
            head_sha=receipt.get("source_head"),
            head_tree=receipt.get("source_tree"),
        )
        and all(isinstance(source.get(k), str) and _OID.fullmatch(source[k]) for k in ("head_sha", "head_tree"))
        and ratchet._commit(repo, source["head_sha"]) == source["head_sha"]
        and cycle._git(repo, "rev-parse", source["head_sha"] + "^{tree}") == source["head_tree"]
        and config.get("start_head_ref") == source["head_sha"]
        and state.get("base_sha") == source["head_sha"]
        and state.get("branch") == source["branch"],
        "source_git_mismatch",
        "original native commit/tree/branch and frozen start_head_ref",
    )
    _require(
        binding.get("v") == 1
        and type(binding.get("row_id")) is int
        and type(binding.get("cycle_epoch")) is int
        and all(binding.get(k) == state.get(k) for k in ("task_ref", "lane_id", "branch", "worktree_path"))
        and isinstance(binding.get("source_base_sha"), str)
        and _OID.fullmatch(binding["source_base_sha"]),
        "lane_cycle_unavailable",
        "original native first-publication row/base/cycle binding",
    )
    registered = _lane(runtime, binding["row_id"])
    native_identity = (
        None
        if registered is None
        else {k: registered.get(k) for k in ("id", "identity_seq", "task_ref", "lane_id", "branch", "worktree_path")}
    )
    # A recovery authored while the original epoch was still current is a
    # native historical witness. It can survive later closure of that SAME
    # registry identity. It cannot qualify a fresh repair of an already-moved
    # epoch, a different row/identity, or a reopened active cycle.
    historical_join = (
        historical is not None
        and historical.get("lane_cycle_binding") == binding
        and historical.get("native_registry_identity") == native_identity
        and registered is not None
        and registered.get("status") in {"closed", "merged"}
    )
    _require(
        registered is not None
        and all(registered.get(k) == binding.get(k) for k in ("task_ref", "lane_id", "branch", "worktree_path"))
        and (registered.get("cycle_epoch") == binding["cycle_epoch"] or historical_join),
        "historical_lane_join_unavailable",
        "authenticated original row id="
        + str(binding["row_id"])
        + " cycle_epoch="
        + str(binding["cycle_epoch"])
        + "; advanced/retired rows require an authenticated historical join, never today's epoch",
    )
    lane_spec = state.get("lane_spec")
    route = dict(backend=state.get("backend"), model=state.get("model"), effort=state.get("reasoning_effort"))
    _require(
        isinstance(lane_spec, dict)
        and lane_spec.get("lane_id") == managed["lane_id"]
        and lane_spec.get("lane_kind") == "implement"
        and all(lane_spec.get(k) == v for k, v in route.items())
        and all(config.get(k) == state.get(k) for k in ("backend", "model", "reasoning_effort"))
        and all(isinstance(v, str) and v for v in route.values())
        and (
            row.get("required_route") is None
            or all(
                (cycle.normalized_model(route[k]) == cycle.normalized_model(v) if k == "model" else route.get(k) == v)
                for k, v in row["required_route"].items()
            )
        )
        and (
            receipt.get("executed_route") is None
            or all(
                (cycle.normalized_model(route[k]) == cycle.normalized_model(v) if k == "model" else route.get(k) == v)
                for k, v in receipt["executed_route"].items()
            )
        ),
        "route_or_spec_mismatch",
        "original native lane spec, completion config and reserved/executed route",
    )
    original, scope_path, scope_sha = _original_scope(repo, private, managed, binding, source)
    outcome = result.get("outcome")
    from .pass_outcome_class import PassOutcomeClass, classify_pass_outcome

    _require(
        isinstance(outcome, str) and bool(outcome) and classify_pass_outcome(outcome) != PassOutcomeClass.PENDING,
        "not_terminal",
        "authoritative synchronous terminal producer outcome",
    )
    telemetry = result.get("dispatch_receipt")
    telemetry = telemetry if isinstance(telemetry, dict) else {}
    actual = any(type(telemetry.get(k)) is int and telemetry[k] > 0 for k in ("turns", "observed_tokens"))
    actual |= result.get("model_started") is True or result.get("model_process_started") is True
    contradictory = any(result.get(k) is False for k in ("model_started", "model_process_started"))
    expected = _expected_completion(outcome)
    if row.get("completion") is not None:
        _require(
            row.get("outcome_class") == "verdict",
            "existing_nonverdict_settlement",
            "full-charge original settlement; this recovery never refunds or rewrites existing outcomes",
        )
        contradictory |= row["completion"] != expected
    execution = "actual" if actual and not contradictory else "UNKNOWN"
    records = {str(path): receipt_sha, str(pass_path): pass_sha, str(scope_path): scope_sha}
    proof = dict(
        schema="workbay/original-synchronous-terminal@1",
        dispatch_id=dispatch_id,
        charged_subject=row["subject_branch"],
        native_round=row["round"],
        kind=row["kind"],
        managed_identity=managed,
        source=source,
        source_scope=original,
        lane_cycle_binding=binding,
        native_registry_identity=native_identity,
        invocation_sha256=_digest(state["invocation_token"].encode()),
        transfer_sha256=_digest(receipt["token"].encode()),
        terminal_outcome=outcome,
        settlement_completion=expected,
        outcome_class="verdict",
        execution=execution,
        required_route=row["required_route"],
        requested_route=route,
        dispatch_receipt={
            k: telemetry[k]
            for k in (
                "turns",
                "rounds",
                "observed_tokens",
                "served_model",
                "resolved_model",
                "tokens_complete",
                "routing_history",
                "routing_effective",
                "pass_history",
            )
            if k in telemetry
        },
        producer_records=records,
    )
    _require(
        all(_digest(Path(p).read_bytes()) == sha for p, sha in records.items()),
        "stale_observation",
        "unchanged original native producer bytes",
    )
    lane = dict(
        row_id=binding["row_id"],
        **{k: binding[k] for k in ("task_ref", "lane_id", "branch", "worktree_path", "cycle_epoch")},
    )
    return _Observed(proof, receipt, pass_path, scope_path, runtime.state_dir, lane)


class _SettlementInTransaction(ReviewCycleLedger):
    """Reuse the native settlement transition in its caller's locked transaction.

    This adapter has no independent I/O or accounting rules. The outer native
    transaction is the sole writer and commits its state once.
    """

    def __init__(self, ledger: ReviewCycleLedger, data: dict):
        super().__init__(ledger.path, ledger.policy, repo=ledger.repo)
        self.data = data

    def _transaction(self, operation, *, activation=False):
        if activation:
            raise _Hold("recovery_activation_forbidden")
        return operation(self.data)


def _event(repo: Path, dispatch_id: str) -> dict | None:
    path = _recovery_path(repo, dispatch_id)
    if not path.exists() and not path.is_symlink():
        return None
    record, _ = _read(path, cycle.authority(repo))
    _require(
        record.get("schema") == _SCHEMA
        and record.get("dispatch_id") == dispatch_id
        and record.get("observation_sha256") == _digest(_encoded(record.get("observation"))),
        "terminal_recovery_integrity",
        "immutable original terminal recovery intent",
    )
    return record


def recover(request: dict, *, repo: Path) -> LedgerDecision:
    """Bounded local preview or CAS apply; no transport, model or admission."""
    try:
        operation, dispatch_id = request.get("operation"), request.get("dispatch_id")
        expected = {"operation", "dispatch_id"}
        if operation == "terminal_apply":
            expected |= {"expected_ledger_sha256", "expected_observation_sha256"}
        _require(
            operation in {"terminal_preview", "terminal_apply"}
            and set(request) == expected
            and isinstance(dispatch_id, str)
            and bool(dispatch_id)
            and len(dispatch_id) <= 4096,
            "invalid_protocol",
            "dispatch locator and exact CAS digests only",
        )
        ledger = _ledger(repo)
        if operation == "terminal_preview":
            raw = ledger.path.read_bytes()
            with _dispatch_lock(
                cycle.authority(repo) / "review_cycle_dispatch" / (_digest(dispatch_id.encode()) + ".json")
            ):
                observed = _authenticate(repo, ledger._decode(raw), dispatch_id)
            _require(ledger.path.read_bytes() == raw, "stale_ledger", "unchanged canonical ledger")
            return LedgerDecision(
                "preview",
                "",
                dict(
                    expected_ledger_sha256=_digest(raw),
                    expected_observation_sha256=_digest(_encoded(observed.proof)),
                    observation=observed.proof,
                    native_admission=False,
                    OrcaDispatch=None,
                ),
            )
        for field in ("expected_ledger_sha256", "expected_observation_sha256"):
            _require(isinstance(request.get(field), str) and _SHA.fullmatch(request[field]), "invalid_protocol", field)
        from .offload_pass_recovery import EvidenceHold as NativeHold
        from .terminal_history_repair import _epoch_fence, _lane_publication, _pass_publication

        with ExitStack() as locks:

            def apply(data):
                path = cycle.authority(repo) / "review_cycle_dispatch" / (_digest(dispatch_id.encode()) + ".json")
                locks.enter_context(_dispatch_lock(path))
                hint = _authenticate(repo, data, dispatch_id)
                locks.enter_context(_lane_publication(hint.state_dir, hint.lane["task_ref"], hint.lane["lane_id"]))
                locks.enter_context(_pass_publication(hint.pass_path))
                locks.enter_context(_dispatch_lock(hint.scope_path))
                locks.enter_context(_epoch_fence(repo, SimpleNamespace(lane=hint.lane)))
                observed = _authenticate(repo, data, dispatch_id)
                digest = _digest(_encoded(observed.proof))
                record = _event(repo, dispatch_id)
                existed = record is not None
                if record is not None:
                    _require(
                        record["observation_sha256"] == digest == request["expected_observation_sha256"]
                        and record.get("ledger_sha256_before") == request["expected_ledger_sha256"],
                        "terminal_recovery_conflict",
                        "same original native producer observation and intent CAS",
                    )
                else:
                    _require(
                        _digest(ledger.path.read_bytes()) == request["expected_ledger_sha256"],
                        "stale_ledger",
                        "canonical ledger bytes from preview",
                    )
                    _require(
                        digest == request["expected_observation_sha256"],
                        "stale_observation",
                        "original producer bytes from preview",
                    )
                row = data["rows"][dispatch_id]
                pending = row.get("completion") is None
                if record is not None and pending:
                    _require(
                        _digest(ledger.path.read_bytes()) == record["ledger_sha256_before"],
                        "stale_ledger",
                        "uncommitted recovery intent's unchanged canonical ledger",
                    )
                if record is None:
                    record = dict(
                        schema=_SCHEMA,
                        dispatch_id=dispatch_id,
                        observation=observed.proof,
                        observation_sha256=digest,
                        ledger_sha256_before=request["expected_ledger_sha256"],
                        pending_before=pending,
                        native_admission=False,
                        OrcaDispatch=None,
                    )
                    destination = _recovery_path(repo, dispatch_id)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    cycle._save(destination, record)
                if pending:
                    result = _SettlementInTransaction(ledger, data).settle(
                        dispatch_id,
                        row["subject_branch"],
                        row["kind"],
                        observed.proof["settlement_completion"]["outcome"],
                    )
                    _require(
                        result.status == "settled",
                        "native_settlement_refused",
                        "existing charged native settlement transition",
                    )
                response = dict(
                    recovery_path=str(_recovery_path(repo, dispatch_id)),
                    observation_sha256=digest,
                    charged_subject=row["subject_branch"],
                    native_round=row["round"],
                    native_admission=False,
                )
                status = "already_recovered" if existed and not pending else "terminal_recovered"
                decision = LedgerDecision(status, "", response)
                if not pending:
                    raise _Unchanged(decision)  # Preserve settled ledger bytes exactly.
                return decision

            try:
                return ledger._transaction(apply)
            except NativeHold as exc:
                raise EvidenceHold(
                    exc.reason, getattr(exc, "needed", None) or "native publication/cycle fence"
                ) from exc
    except EvidenceHold as exc:
        return LedgerDecision("ledger_hold", str(exc), {"required_host_evidence": exc.needed})
    except (_Hold, OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.SubprocessError) as exc:
        return LedgerDecision("ledger_hold", str(exc) if isinstance(exc, _Hold) else type(exc).__name__)


def validated_record(repo: Path, receipt: dict, data: dict | None = None) -> dict | None:
    """Every native consumer reopens original evidence; intent alone releases nothing."""
    dispatch_id = receipt["identity"]["dispatch_id"]
    record = _event(repo, dispatch_id)
    if record is None:
        return None
    ledger = _ledger(repo)
    data = ledger._read() if data is None else data
    observed = _authenticate(repo, data, dispatch_id, historical=record["observation"])
    _require(
        record["observation"] == observed.proof and record["observation_sha256"] == _digest(_encoded(observed.proof)),
        "terminal_recovery_conflict",
        "unchanged native producer evidence referenced by original recovery",
    )
    row = data["rows"][dispatch_id]
    _require(
        row.get("completion") is not None and row.get("outcome_class") == "verdict",
        "terminal_recovery_uncommitted",
        "canonical full-charge native terminal settlement; intent alone is inert",
    )
    return record


def releases(repo: Path, proof: dict, receipt: dict, orchestrator_root: Path) -> bool:
    """Recovered scope participates only in the existing resource/landing predicates.

    The charged identity remains untouched. The independently authenticated
    producer's resource subject can differ from the old retained charge; it
    cannot mint an approval or move accounting to that source branch.
    """
    record = validated_record(repo, receipt)
    if record is None or record["observation"]["execution"] != "actual":
        return False
    original = record["observation"]["source_scope"]
    view = {**receipt, "source_task_subject": original}
    return cycle._independent_baseline_source(
        repo, proof, view, orchestrator_root, source_subject=original["subject_branch"]
    ) or cycle._terminal_producer_source(
        repo, proof, view, orchestrator_root, source_subject=original["subject_branch"]
    )
