"""Bounded host-only preview/apply of a historical pre-dispatch false charge.

Run with JSON on stdin: preview takes operation and original dispatch_id; apply
also requires the preview's expected_ledger_sha256/expected_observation_sha256.
The distinct ``holder_release`` operation takes ``dispatch_id`` (the first
holder), ``dispatch_ids`` (a nonempty unique list),
``expected_receipt_sha256s`` (the exact dispatch-ID-to-digest mapping), and
``operator_decision_id``. The existing decisions authority must contain an
approval named ``review_cycle_holder_release_approved`` whose rationale
contains that exact mapping as a JSON object. The ledger appends one event only
after the current host receipt, registry and Git checks all pass. Every later
hold walk reopens those facts and the decision. No authority paths, replacement
snapshots, routes, counters, or bypass flags are request parameters.

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
from dataclasses import asdict, replace
from pathlib import Path
from urllib.parse import quote

from workbay_handoff_mcp.enums import LIVE_LANE_STATUSES, TERMINAL_LANE_STATUSES

from . import review_cycle_dispatch as cycle
from .invocation_disposition import DispositionHold
from .invocation_disposition import _identity as _file_identity
from .review_cycle_ledger import (
    LedgerDecision,
    ReviewCycleLedger,
    _digest,
    _encoded,
    _Hold,
    _issued,
    correction_effects,
    correction_eligibility,
)

_SHA = re.compile(r"[0-9a-f]{64}")
_TERMINAL = TERMINAL_LANE_STATUSES | {"landed"}


class EvidenceHold(_Hold):
    """A missing/contradictory named producer relation, with no secret bytes."""

    def __init__(self, reason: str, needed: str | dict):
        super().__init__(reason)
        self.needed = needed


def _require(condition: bool, reason: str, needed: str) -> None:
    if not condition:
        raise EvidenceHold(reason, needed)


@contextmanager
def _regular(path: Path):
    """Open an authority file without following links or listing its parents."""
    # O_PATH on Linux pins the directory without requesting read access. XNU
    # defines O_SEARCH as O_EXEC | O_DIRECTORY, even on Python builds made
    # with an older SDK that does not export the flag.
    if sys.platform == "darwin":
        search = getattr(os, "O_SEARCH", 0x40000000 | os.O_DIRECTORY)
    else:
        search = getattr(os, "O_PATH", getattr(os, "O_SEARCH", os.O_RDONLY))
    directory_flags = search | os.O_DIRECTORY | os.O_NOFOLLOW
    path = Path(os.path.abspath(path))
    parent = os.open(path.anchor, directory_flags)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, directory_flags, dir_fd=parent)
            os.close(parent)
            parent = child
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_NOATIME", 0)
        fd = os.open(path.name, flags, dir_fd=parent)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise DispositionHold("unsafe_regular_file")
            if info.st_uid != os.getuid():
                raise DispositionHold("foreign_file_owner")
            identity = _file_identity(info)

            def check_name():
                if _file_identity(os.stat(path.name, dir_fd=parent, follow_symlinks=False)) != identity:
                    raise DispositionHold("file_replaced")

            check_name()
            yield fd
            if _file_identity(os.fstat(fd)) != identity:
                raise DispositionHold("file_changed")
            check_name()
        finally:
            os.close(fd)
    finally:
        os.close(parent)


def _read(path: Path, root: Path) -> tuple[dict, str]:
    """Read canonical host bytes; never follow a worker-selected/export path."""
    _require(path.is_relative_to(root) and path.resolve() == path, "authority_path_invalid", str(path))
    try:
        with _regular(path) as fd:
            info = os.fstat(fd)
            _require(info.st_mode & 0o444 and not info.st_mode & 0o022 and info.st_size <= 32 * 1024 * 1024,
                     "authority_custody_invalid", str(path))
            with open(fd, "rb", closefd=False) as stream:
                raw = stream.read(info.st_size)
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
        with _regular(path.with_suffix(".lock")) as lock:
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


def _receipt_reservation(repo: Path, path: Path, receipt: dict, rows: dict) -> dict:
    """Authenticate the native source and the immutable portion of its original reservation."""
    identity = receipt.get("identity")
    dispatch_id = identity.get("dispatch_id") if isinstance(identity, dict) else None
    _require(isinstance(dispatch_id, str) and path.name == _digest(dispatch_id.encode()) + ".json",
             "holder_custody_unavailable", str(path) + ": original native dispatch identity")
    row = rows.get(dispatch_id)
    decision = receipt.get("decision")
    original = decision.get("row") if isinstance(decision, dict) else None
    # ReviewCycleLedger.settle writes these fields after reservation.
    mutable = {"completion", "outcome_class", "settled_sequence", "settlement_status",
               "provider_refusal", "round_charged"}
    _require(isinstance(row, dict) and row.get("decision") == "reserved"
             and identity == {k: row.get(k) for k in ("dispatch_id", "subject_branch", "kind")}
             and isinstance(original, dict) and decision.get("status") == "reserved"
             and _encoded({k: v for k, v in row.items() if k not in mutable})
             == _encoded({k: v for k, v in original.items() if k not in mutable}),
             "holder_custody_unavailable", str(path) + ": original reservation linked to canonical ledger row")
    head = receipt.get("source_head")
    if head:
        tree = receipt.get("source_tree")
        _require(isinstance(head, str) and re.fullmatch(r"[0-9a-f]{40}", head)
                 and cycle._git(repo, "rev-parse", "--verify", head + "^{tree}") == tree
                 and cycle._git(repo, "cat-file", "-t", tree) == "tree",
                 "holder_source_mismatch", str(path) + ": immutable native source commit/tree")
    return row


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
        row = _receipt_reservation(repo, path, receipt, data["rows"])
        records[str(path)] = digest
        receipts[path.name] = (receipt, row)
    return records, receipts


def bootstrap_charged_subject(repo: Path, primary: Path, proof: dict, data: dict) -> tuple[dict, dict]:
    """The subject native dispatch would charge for this source, after custody.

    Reuses ``retained_subject`` over receipts whose custody and ledger rows were
    all just authenticated. The incoming proof stays the source identity; a
    derived subject owns the grant and counters. Unproved or conflicting holders
    produce a typed hold before any subject is chosen.
    Canon v0.26.2: GRPH-14 Record provenance as a walkable lineage graph
    (Building Knowledge Graphs, ch. 8 + Mental Models, ch. 8); SECD-03 Check
    every access at the reference monitor (Security Engineering).
    """
    records, receipts = _bootstrap_receipts(repo, data)
    derived = dict(incoming_branch=proof["subject_branch"], relation="review_cycle_dispatch.retained_subject")
    try:
        subject = cycle.retained_subject(repo, proof["subject_branch"], proof, orchestrator_root=primary)
    except (cycle.SubjectAmbiguous, cycle.SubjectResolutionHeld, cycle.SubjectScopeUnauthenticated) as exc:
        detail = cycle.subject_resolution_details(repo, exc, kind="implement", orchestrator_root=primary)
        if isinstance(exc, cycle.SubjectAmbiguous):
            # Several subjects remain a choice nobody may make; keep their names.
            detail.update(status="ambiguous", accounts=detail.pop("subjects"),
                          subjects=sorted({row["subject"] for row in exc.holding}),
                          receipts=sorted(row["receipt"] for row in exc.holding))
        return {**detail, **derived}, records
    # A retained subject other than the proof's must be a custody-read native charge.
    _require(subject == proof["subject_branch"]
             or any(row["subject_branch"] == subject for _, row in receipts.values()),
             "charged_subject_unproven", "custody-linked native receipt and ledger row for the retained subject")
    return dict(status="derived", subject_branch=subject, **derived), records


def bootstrap_holder_report(repo: Path, primary: Path, runtime, proof: dict, data: dict) -> dict:
    """Authenticate custody before consuming the existing holder applicability rules.

    GRPH-09 Colour the conflict graph (Graph Theory with Applications, ch. 8),
    Canon v0.26.2: resource/producer relations are checked, not branch labels.
    An empty branch selects the ancestry walk for every nonempty source branch.
    This is an observation, never a release event or a second settlement ledger.
    """
    from .lane_close_release import verified_close_release

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
    release_walk = HolderReleaseWalk(repo)
    # Historical discovery can release on task completion. Maintenance must
    # additionally retain an unstamped original whose native producer/harvest
    # is unknown. A finished task or pass is not native settlement evidence.
    for name, (receipt, row) in receipts.items():
        head, branch = receipt.get("source_head"), receipt.get("source_branch")
        if name in named or receipt.get("source_task_subject") is not None or not head or not branch:
            continue
        if cycle._git(repo, "merge-base", head, "HEAD") != head:
            continue
        path = directory / name
        if (receipt.get("source_issuer") is not None
                and verified_close_release(repo, path, receipt, records[str(path)], proof) is not None):
            continue
        if release_walk.landed_self_source_releases(path, receipt, records[str(path)], proof):
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



def _holder_ledger(repo: Path) -> ReviewCycleLedger:
    """Open existing authority without activating a ledger or writing a lock."""
    config = cycle._configured_review_cycle(repo)
    policy = replace(cycle.trusted_policy(), routes=config.get("routes", {}),
                     approval_thresholds=config.get("approval_thresholds", {}))
    return ReviewCycleLedger(cycle.authority(repo) / "review_cycle_ledger.json", policy, repo=repo)


def _decision_authorizes(rationale: object, digests: dict[str, str]) -> bool:
    """Require an exact JSON mapping, not the occurrence of unrelated substrings."""
    if not isinstance(rationale, str):
        return False
    decoder = json.JSONDecoder()
    for index, char in enumerate(rationale):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(rationale[index:])
        except ValueError:
            continue
        if value == digests:
            return True
    return False


def _holder_release_observations(repo: Path, request: dict, data: dict, locks: ExitStack) -> dict:
    """Check the exact ledger, protected receipts, operator record and current Git facts.

    Receipt locks remain held by the caller until the ledger append completes.
    The same checks are made on every later hold walk. The recorded decision and
    machine observations are distinct controls; a decision ID is not itself
    proof of a separately authenticated human identity.
    """
    from workbay_handoff_mcp.config import RuntimeConfig

    primary, authority = _host(repo)
    ids = request.get("dispatch_ids")
    digests = request.get("expected_receipt_sha256s")
    operator = request.get("operator_decision_id")
    _require(isinstance(ids, list) and bool(ids) and all(isinstance(i, str) and i for i in ids)
             and len(set(ids)) == len(ids), "holder_release_invalid_ids", "distinct holder dispatch ids")
    _require(isinstance(digests, dict) and set(digests) == set(ids)
             and all(isinstance(v, str) and _SHA.fullmatch(v) for v in digests.values()),
             "holder_release_invalid_digests", "exact receipt SHA-256 for every holder")
    _require(type(operator) is int and operator > 0, "holder_release_operator_required", "recorded operator decision")
    runtime = RuntimeConfig.for_repo(primary)
    try:
        uri = "file:" + quote(str(runtime.db_path), safe="/") + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        try:
            decision = connection.execute("SELECT decision, rationale FROM decisions WHERE id = ?", (operator,)).fetchone()
        finally:
            connection.close()
    except sqlite3.Error as exc:
        raise EvidenceHold("holder_release_operator_unavailable", "recorded operator decision") from exc
    _require(decision is not None and decision[0] == "review_cycle_holder_release_approved"
             and _decision_authorizes(decision[1], digests),
             "holder_release_operator_unverified", "approval decision for the exact dispatch-to-digest mapping")
    observations = {}
    for dispatch_id in sorted(ids):
        path = authority / "review_cycle_dispatch" / (_digest(dispatch_id.encode()) + ".json")
        locks.enter_context(_dispatch_lock(path))
        receipt, digest = _read(path, authority)
        _require(digest == digests[dispatch_id], "holder_release_receipt_changed", dispatch_id)
        try:
            holder = cycle._receipt_holder(primary, receipt, receipt.get("source_branch"))
        except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, subprocess.SubprocessError) as exc:
            raise EvidenceHold("holder_release_lane_unavailable", dispatch_id) from exc
        observations[dispatch_id] = _terminal_holder_facts(primary, dispatch_id, receipt,
                                                           data["rows"].get(dispatch_id), holder)
    return observations


def _holder_integration(primary: Path, lane: dict, dispatch_id: str) -> str:
    from .review_ratchet import _manifest_lane, _short_branch, task_record

    manifest = _manifest_lane(primary, lane["task_ref"], lane["lane_id"])
    if manifest is not None:
        integration_branch = _short_branch(manifest[0].get("integration_branch", "main"))
    else:
        # Legacy lanes may have no manifest. A task explicitly targeting
        # main still names the integration branch; absence alone does not.
        try:
            task = task_record(lane["task_ref"])
        except (OSError, sqlite3.Error) as exc:
            raise EvidenceHold("holder_release_integration_unverified", dispatch_id) from exc
        integration_branch = _short_branch(task.get("target_branch")) if task else None
        if integration_branch != "main":
            integration_branch = None
    _require(integration_branch is not None, "holder_release_integration_unverified", dispatch_id)
    return integration_branch


def _ref_created_after_receipt(primary: Path, ref: str, receipt_path: Path) -> bool:
    """Witness the birth of the current ref. A checkout can adopt even a different worktree's ref."""
    try:
        observed_ns = receipt_path.stat().st_mtime_ns
        log = Path(cycle._git(primary, "rev-parse", "--path-format=absolute", "--git-path", "logs/" + ref))
        if not log.is_file() or log.is_symlink():
            return False
        with log.open("rb") as stream:
            first = stream.readline(4096)
            created = re.fullmatch(rb"0{40} ([0-9a-f]{40}) [^\n]+ (\d+) [+-]\d{4}\tbranch: Created from [^\n]+\n", first)
            if created is None:
                return False
            second = int(created[2])
            receipt_second = observed_ns // 1_000_000_000
            if second != receipt_second:
                return second > receipt_second and second <= os.fstat(stream.fileno()).st_mtime_ns // 1_000_000_000

            # Git dates have whole-second precision. Only the original, still
            # single-entry creation transaction has more precise write times
            # on both the loose ref and its reflog. Later updates cannot serve
            # as a subsecond creation witness.
            if stream.read(1):
                return False
            ref_file = Path(cycle._git(primary, "rev-parse", "--path-format=absolute", "--git-path", ref))
            if not ref_file.is_file() or ref_file.is_symlink():
                return False
            with ref_file.open("rb") as loose:
                times = (os.fstat(stream.fileno()).st_mtime_ns, os.fstat(loose.fileno()).st_mtime_ns)
                return (loose.read(42) == created[1] + b"\n"
                        and all(observed_ns < ns and ns // 1_000_000_000 == second for ns in times))
    except (ValueError, OSError, subprocess.SubprocessError):
        return False


def _ref_has_later_registration(primary: Path, ref: str, holder: cycle._ReceiptHolder, receipt_path: Path) -> bool:
    """A live checkout of this ref must belong to a distinct, later registration."""
    from .review_ratchet import _same_worktree

    listing = cycle._git(primary, "worktree", "list", "--porcelain", "-z")
    checkouts = []
    for record in listing.split("\0\0"):
        fields = record.split("\0")
        if "branch " + ref in fields:
            path = fields[0].removeprefix("worktree ")
            if not Path(path).is_dir() or any(field.split(" ", 1)[0] == "prunable" for field in fields):
                return False
            checkouts.append(path)
    if len(checkouts) != 1:
        return False
    (checkout,) = checkouts
    issuer_checkout = holder.lane.get("worktree_path")
    # A later row can adopt the issuer's checkout without retiring its ref.
    # Require positive evidence that the old checkout was removed.
    if not issuer_checkout or not Path(issuer_checkout).is_absolute() or _same_worktree(issuer_checkout, checkout):
        return False
    try:
        Path(issuer_checkout).lstat()
    except FileNotFoundError:
        pass
    except OSError:
        return False
    else:
        return False
    owners = [row for row in holder.competing if row["status"] in LIVE_LANE_STATUSES
              and _same_worktree(row["worktree_path"], checkout)]
    return (len(owners) == 1 and owners[0]["id"] > holder.lane["id"]
            and _ref_created_after_receipt(primary, ref, receipt_path))


def _terminal_holder_facts(primary: Path, dispatch_id: str, receipt: dict, row: dict | None,
                           holder: cycle._ReceiptHolder, *, stamped_receipt: Path | None = None) -> dict:
    """Live terminal evidence for approved historical releases and bound issuers."""
    identity, unproven = receipt.get("identity"), receipt.get("source_task_subject_unproven")
    _require(isinstance(identity, dict) and isinstance(row, dict)
             and identity == {key: row.get(key) for key in ("dispatch_id", "subject_branch", "kind")}
             and identity.get("dispatch_id") == dispatch_id and row.get("decision") == "reserved"
             and row.get("kind") in {"implement", "gate"},
             "holder_release_ledger_join_unverified", dispatch_id)
    _require(receipt.get("state") == "transport_started" and receipt.get("source_task_subject") is None
             and isinstance(unproven, dict) and unproven.get("reason") == "no_producer_proof"
             and unproven.get("head_on_integration") is True,
             "holder_release_unproven_record_invalid", dispatch_id)
    source, head, original_integration = (receipt.get("source_branch"), receipt.get("source_head"),
                                           unproven.get("integration_sha"))
    _require(isinstance(source, str) and bool(source) and isinstance(head, str)
             and re.fullmatch(r"[0-9a-f]{40}", head) is not None
             and isinstance(original_integration, str)
             and re.fullmatch(r"[0-9a-f]{40}", original_integration) is not None,
             "holder_release_source_invalid", dispatch_id)
    if holder.owner_unproven:
        candidates = holder.competing
        blocking = [cycle._issuer_description(r) for r in candidates if r.get("status") not in _TERMINAL]
        if holder.mismatch or not candidates or blocking:
            raise EvidenceHold("holder_release_owner_unproven",
                               dict(dispatch_id=dispatch_id, missing_proof=cycle.ISSUANCE_OWNER_UNPROVEN,
                                    possible_issuers=blocking))
    else:
        # The protected stamp identifies the issuer. Later registrations on
        # its old branch are checked for receipts without that binding.
        _require(holder.lane is not None and not holder.mismatch and holder.lane.get("status") in _TERMINAL,
                 "holder_release_lane_not_terminal", dispatch_id)
        candidates = (holder.lane,)
    ref = source if source.startswith("refs/heads/") else "refs/heads/" + source
    try:
        valid_ref = cycle._subject_probe(primary, "check-ref-format", ref)
        branch_exists = cycle._ref_exists(primary, ref)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise EvidenceHold("holder_release_branch_present_or_unknown", dispatch_id) from exc
    branch_absent = not branch_exists
    _require(valid_ref.returncode == 0 and (branch_absent or (
                 stamped_receipt is not None and not holder.owner_unproven
                 and cycle._subject_probe(primary, "rev-parse", "--verify", "--quiet", ref + "^{commit}").returncode == 0
                 and _ref_has_later_registration(primary, ref, holder, stamped_receipt))),
             "holder_release_branch_present_or_unknown", dispatch_id)
    integrations = {}
    for integration_branch in sorted({_holder_integration(primary, lane, dispatch_id) for lane in candidates}):
        integration_ref = "refs/heads/" + integration_branch
        try:
            valid_integration = cycle._subject_probe(primary, "check-ref-format", integration_ref)
            _require(valid_integration.returncode == 0, "holder_release_integration_unverified", dispatch_id)
            landed = cycle._git(primary, "rev-parse", "--verify", integration_ref + "^{commit}")
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise EvidenceHold("holder_release_integration_unverified", dispatch_id) from exc
        try:
            original_ancestry = cycle._ancestry(primary, head, original_integration)
            current_ancestry = cycle._ancestry(primary, head, landed)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise EvidenceHold("holder_release_head_ancestry_unverified", dispatch_id) from exc
        _require(original_ancestry == "ancestor" and current_ancestry == "ancestor",
                 "holder_release_head_not_on_integration", dispatch_id)
        integrations[integration_branch] = landed
    # Keep the scalar witness for existing ledger readers. Replay looks up
    # the recorded name in the complete live map, even when that map grows.
    integration_branch = min(integrations)
    result = dict(lane_terminal=True, branch_absent=branch_absent, head_on_integration=True, receipt_sha256_matches=True,
                  unproven_record=True, task_ref=holder.task_ref or "unknown", lane_id=holder.lane_id or "unknown",
                  subject_branch=identity["subject_branch"], source_branch=source, source_head=head,
                  integration_branch=integration_branch, integration_head=integrations[integration_branch],
                  integration_heads=integrations, final_tip="unknown")
    if holder.owner_unproven:
        result["possible_issuers"] = [cycle._issuer_description(r) for r in candidates]
    return result


def _earlier_issuer_integration(repo: Path, primary: Path, lane: dict, receipt: dict, rows: dict) -> str | None:
    """Recover the issuer's pinned integration from a protected, earlier native admission.

    A ledger sequence dates the producer stamp before this unstamped dispatch.
    Current notes, file times and a commit shared by two integrations cannot do so.
    """
    from . import review_ratchet as ratchet

    sequence = _issued(rows[receipt["identity"]["dispatch_id"]])
    if sequence is None:
        return None
    private = cycle.authority(repo)
    names = set()
    for row in rows.values():
        issued = _issued(row)
        if (row["subject_branch"] != lane["branch"] or row["decision"] != "reserved"
                or issued is None or issued >= sequence):
            continue
        path = private / "review_cycle_dispatch" / (_digest(row["dispatch_id"].encode()) + ".json")
        previous, _ = _read(path, private)
        stamp = previous.get("source_task_subject")
        if (not isinstance(stamp, dict) or previous.get("source_branch") != lane["branch"]
                or stamp.get("subject_branch") != lane["branch"]):
            continue
        _receipt_reservation(repo, path, previous, rows)
        issuer = cycle._receipt_holder(primary, previous, lane["branch"])
        if (issuer.mismatch or issuer.lane is None or issuer.lane["id"] != lane["id"]
                or previous.get("state") != "transport_started"):
            return None
        record = ratchet._scope_record(repo, stamp)
        if (record is None or not ratchet._same_worktree(record["worktree_path"], lane["worktree_path"])
                or cycle._ancestry(repo, stamp["tip_sha"], previous["source_head"]) != "ancestor"
                or cycle._ancestry(repo, stamp["tip_sha"], receipt["source_head"]) != "ancestor"):
            return None
        names.add(stamp["integration_branch"])
    return names.pop() if len(names) == 1 else None


def _landed_self_source(repo: Path, primary: Path, receipt: dict, rows: dict, proof: dict) -> bool:
    """The issuer's typed final tip covers this source and landed on its own integration."""
    from . import review_ratchet as ratchet

    branch, head = receipt["source_branch"], receipt["source_head"]
    holder = cycle._receipt_holder(primary, receipt, branch)
    lane = holder.lane
    if (holder.mismatch or holder.owner_unproven or holder.competing or lane is None
            or lane["branch"] != branch or lane["status"] != "merged" or lane["task_ref"] == proof["task_ref"]):
        return False
    managed = receipt.get("managed_identity")
    if managed is not None and (
            not isinstance(managed, dict) or not ratchet._same_worktree(managed.get("worktree_path"), lane["worktree_path"])
            or receipt.get("managed_source") != dict(branch=branch, head_sha=head, head_tree=receipt["source_tree"])):
        return False
    task = ratchet.task_record(lane["task_ref"])
    located = ratchet._manifest_lane(primary, lane["task_ref"], lane["lane_id"])
    if task is None or task.get("status") not in {"done", "in_progress"} or located is None:
        return False
    manifest, member = located
    integration = proof["integration_branch"]
    target = ratchet._short_branch(task.get("target_branch"))
    if (branch == integration or target not in {branch, integration}
            or (target == branch and not ratchet._same_worktree(task.get("target_worktree_path"), lane["worktree_path"]))
            or ratchet._short_branch(manifest.get("integration_branch", "main")) != integration
            or ratchet._short_branch(member.get("branch")) != branch
            or not ratchet._same_worktree(member.get("worktree_path"), lane["worktree_path"])):
        return False

    landed = ratchet._commit(repo, "refs/heads/" + integration)
    if (landed is None or cycle._landed_continuation_tip(repo, head, [lane], landed) is None
            or not cycle._terminal_refs_landed(repo, {branch}, landed)):
        return False
    binding = receipt.get("source_task_subject")
    if binding is None:
        observed = receipt["source_task_subject_unproven"]
        cut = observed.get("integration_sha")
        if (not isinstance(cut, str) or not re.fullmatch("[0-9a-f]{40}", cut)
                or ratchet._commit(repo, cut) != cut or cycle._ancestry(repo, head, cut) != "not_ancestor"
                or cycle._ancestry(repo, cut, lane["landing_commit_sha"]) != "ancestor"):
            return False
        if "source_integration_branch" in receipt:
            if receipt["source_integration_branch"] != integration:
                return False
        elif _earlier_issuer_integration(repo, primary, lane, receipt, rows) != integration:
            return False
    else:
        if (binding.get("integration_branch") != integration or binding.get("base_sha") != member.get("base_sha")
                or not ratchet._same_worktree((ratchet._scope_record(repo, binding) or {}).get("worktree_path"),
                                              lane["worktree_path"])
                or cycle._ancestry(repo, binding["base_sha"], binding["tip_sha"]) != "ancestor"
                or cycle._ancestry(repo, binding["tip_sha"], head) != "ancestor"):
            return False

    # A feature-target task which is still live also owns its other lanes.
    if (task["status"] == "in_progress" and target == branch
            and not cycle._feature_members_landed(repo, primary, task, lane, subject=branch, source=branch,
                                                  head=head, integration=integration, landed=landed)):
        return False
    scope = ratchet._scope_record(repo, proof)
    return (scope is not None and cycle._authorized_roots(repo, primary, proof) is not None
            and cycle._git(repo, "symbolic-ref", "--quiet", "HEAD") == "refs/heads/" + proof["subject_branch"]
            and ratchet._same_worktree(scope.get("worktree_path"), cycle._git(repo, "rev-parse", "--show-toplevel")))


class HolderReleaseWalk:
    """A fresh ledger index and live observations for one subject resolution."""

    def __init__(self, repo: Path) -> None:
        self.repo = repo
        self._ledger_path = cycle.authority(repo) / "review_cycle_ledger.json"
        self._ledger_identity = None
        self._data = {}
        try:
            if self._ledger_path.exists() or self._ledger_path.is_symlink():
                with _regular(self._ledger_path) as fd:
                    info = os.fstat(fd)
                    if not info.st_mode & 0o444:
                        raise ValueError("unreadable ledger")
                    with open(fd, "rb", closefd=False) as stream:
                        data = _holder_ledger(repo)._decode(stream.read())
                    # A legacy ledger can carry ordinary charges without supplying
                    # an authenticated reservation or event to this release walk.
                    if not info.st_mode & 0o022:
                        self._data = data
                        self._ledger_identity = (info.st_dev, info.st_ino, info.st_ctime_ns)
                    elif data.get("holder_releases"):
                        raise ValueError("unprotected holder release events")
        except (_Hold, OSError, ValueError, KeyError, TypeError, sqlite3.Error, subprocess.SubprocessError) as exc:
            raise cycle.HolderLedgerUnavailable("review_cycle_holder_release_ledger_unavailable") from exc
        self.events = self._data.get("holder_releases", [])
        # The ledger validates unique membership and strictly increasing event sequences.
        self._by_dispatch = {item: event for event in self.events for item in event["dispatch_ids"]}
        self._observations: dict[int, dict | None] = {}
        self.unproven_owners: dict[str, list[dict]] = {}

    def has_reservation(self, identity: dict) -> bool:
        row = self._data.get("rows", {}).get(identity.get("dispatch_id"))
        return (isinstance(row, dict) and row.get("decision") == "reserved" and row.get("kind") in {"implement", "gate"}
                and all(row.get(key) == identity.get(key) for key in ("dispatch_id", "subject_branch", "kind")))

    def landed_self_source_releases(self, path: Path, receipt: dict, digest: str, proof: dict) -> bool:
        """One automatic landing proof for dispatch and reconciliation; no settlement is inferred."""
        identity = receipt["identity"]
        branch = receipt.get("source_branch")
        stamp = receipt.get("source_task_subject")
        unproven = receipt.get("source_task_subject_unproven")
        if (branch != identity.get("subject_branch") or branch == proof.get("subject_branch")
                or receipt.get("state") != "transport_started" or not self.has_reservation(identity)):
            return False
        if stamp is None:
            if (not isinstance(unproven, dict) or unproven.get("reason") != "no_producer_proof"
                    or unproven.get("head_on_integration") is not False):
                return False
        elif not isinstance(stamp, dict) or stamp.get("subject_branch") != branch or unproven is not None:
            return False
        try:
            primary, private = _host(self.repo)
            if path.parent != private / "review_cycle_dispatch":
                return False
            with _dispatch_lock(path):
                protected, observed = _read(path, private)
                if observed != digest or protected != receipt:
                    return False
                _receipt_reservation(self.repo, path, protected, self._data["rows"])
                if not _landed_self_source(self.repo, primary, protected, self._data["rows"], proof):
                    return False
                # A ledger replacement during the walk invalidates its row and any release events.
                info = self._ledger_path.lstat()
                return (self._ledger_path.resolve() == self._ledger_path
                        and (info.st_dev, info.st_ino, info.st_ctime_ns) == self._ledger_identity)
        except (EvidenceHold, OSError, ValueError, KeyError, TypeError, sqlite3.Error, subprocess.SubprocessError):
            return False

    def terminal_issuer_releases(self, path: Path, receipt: dict, digest: str,
                                 holder: cycle._ReceiptHolder, proof: dict) -> bool:
        """A protected issuance binding supplies the owner without an operator decision."""
        if holder.lane is None or holder.lane.get("status") not in _TERMINAL:
            return False
        dispatch_id = receipt["identity"]["dispatch_id"]
        if path.name != _digest(dispatch_id.encode()) + ".json":
            return False
        try:
            with _dispatch_lock(path):
                protected, observed_digest = _read(path, path.parent.parent)
                if observed_digest != digest:
                    return False
                primary, _ = _host(self.repo)
                facts = _terminal_holder_facts(primary, dispatch_id, protected,
                                              self._data.get("rows", {}).get(dispatch_id), holder, stamped_receipt=path)
            return proof["integration_branch"] in facts["integration_heads"]
        except (EvidenceHold, OSError, ValueError, KeyError, sqlite3.Error, subprocess.SubprocessError):
            return False

    def observations(self, event: dict) -> dict | None:
        sequence = event["recorded_sequence"]
        if sequence not in self._observations:
            request = dict(dispatch_ids=event["dispatch_ids"], expected_receipt_sha256s=event["receipt_sha256s"],
                           operator_decision_id=event["operator_decision_id"])
            fields = ("task_ref", "lane_id", "subject_branch", "source_branch", "source_head")
            try:
                with ExitStack() as locks:
                    current = _holder_release_observations(self.repo, request, self._data, locks)
                for item, recorded in event["preconditions"].items():
                    observed = current[item]
                    if (any(observed[field] != recorded[field] for field in fields)
                            or recorded["integration_branch"] not in observed["integration_heads"]
                            or not re.fullmatch(r"[0-9a-f]{40}", recorded["integration_head"])
                            or cycle._ancestry(self.repo, recorded["source_head"], recorded["integration_head"])
                            != "ancestor"):
                        current = None
                        break
            except EvidenceHold as exc:
                if str(exc) == "holder_release_owner_unproven":
                    self.unproven_owners[exc.needed["dispatch_id"]] = exc.needed["possible_issuers"]
                current = None
            except (_Hold, OSError, ValueError, KeyError, TypeError, AttributeError, sqlite3.Error, subprocess.SubprocessError):
                current = None
            self._observations[sequence] = current
        return self._observations[sequence]

    def is_current(self, receipt: dict, receipt_sha256: str, *, integration_branch: str) -> bool | None:
        dispatch_id = receipt.get("identity", {}).get("dispatch_id")
        event = self._by_dispatch.get(dispatch_id)
        if event is None:
            return None
        current = self.observations(event)
        return (current is not None and receipt_sha256 == event["receipt_sha256s"][dispatch_id]
                and integration_branch in current[dispatch_id]["integration_heads"])


def holder_release_is_current(repo: Path, receipt: dict, receipt_sha256: str, *, integration_branch: str) -> bool | None:
    try:
        return HolderReleaseWalk(repo).is_current(receipt, receipt_sha256, integration_branch=integration_branch)
    except (ValueError, OSError, subprocess.SubprocessError):
        return False


def reconcile(request: dict, *, repo: Path | None = None) -> LedgerDecision:
    """Typed public seam; preview is read-only and apply reauthenticates under CAS."""
    try:
        if isinstance(request, dict) and request.get("operation") == "holder_release":
            allowed = {"operation", "dispatch_id", "dispatch_ids", "expected_receipt_sha256s", "operator_decision_id"}
            _require(not set(request) - allowed and isinstance(request.get("dispatch_ids"), list)
                     and bool(request["dispatch_ids"]) and request.get("dispatch_id") == request["dispatch_ids"][0],
                     "holder_release_invalid_protocol", "holder ids, receipt digests and operator decision")
            ids, digests = request["dispatch_ids"], request.get("expected_receipt_sha256s")
            _require(all(isinstance(item, str) and item for item in ids) and len(set(ids)) == len(ids),
                     "holder_release_invalid_ids", "distinct holder dispatch ids")
            _require(isinstance(digests, dict) and set(digests) == set(ids)
                     and all(isinstance(value, str) and _SHA.fullmatch(value) for value in digests.values()),
                     "holder_release_invalid_digests", "exact receipt SHA-256 for every holder")
            _require(type(request.get("operator_decision_id")) is int and request["operator_decision_id"] > 0,
                     "holder_release_operator_required", "recorded operator decision")
            selected = Path(repo or Path.cwd()).resolve()
            ledger = _holder_ledger(selected)
            _require(ledger.path.exists(), "holder_release_ledger_unavailable", "canonical ledger")
            with ExitStack() as locks:
                return ledger._append_holder_release(
                    dispatch_ids=request["dispatch_ids"], receipt_sha256s=request.get("expected_receipt_sha256s"),
                    operator_decision_id=request["operator_decision_id"],
                    authenticate=lambda data: _holder_release_observations(selected, request, data, locks),
                )
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
