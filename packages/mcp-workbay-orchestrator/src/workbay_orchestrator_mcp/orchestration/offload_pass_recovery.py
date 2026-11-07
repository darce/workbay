"""Native terminal-evidence derivation shared by completion, repair and history.

One derivation authenticates the retained artifacts of a detached remote job
(named by its RemoteJobLedger spec argv) against the host-retained review-cycle
dispatch receipt, and projects them onto a terminal receipt. Three callers use
it: first publication of a nonzero exit in ``offload_pass._complete_remote_pass``,
the public ``terminal_history_repair`` preview/apply, and the cached readers
(``complete_remote_pass`` on a completed job, ``_matching_terminal_passes``).

Canon 0.26.2: GRPH-14 provenance lineage (every enriched fact names the bytes
it came from); SECD-05 fail-safe defaults (anything unauthenticated is a typed
hold, unknown token usage stays ``None``, never 0); DATA-14 no dual writes (the
original receipt is never rewritten; an approved enrichment is one
content-addressed intent in protected custody, and every reader re-derives its
view from the native records it names -- see ``terminal_history_contract``).

This module has no import-time dependency on ``offload_pass`` or the review
cycle modules, so the engine can import it lazily without a cycle.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from workbay_handoff_mcp import lane_completion_production as production

EVIDENCE_SCHEMA = "workbay/native-terminal-evidence@1"
# Version 2 is the complete native authority contract (terminal_history_contract);
# version 1 records were self-hashed caller-chosen views and are never served.
INTENT_SCHEMA = "workbay/terminal-evidence-enrichment@2"
INTENT_DIRNAME = "terminal-evidence"
# Result keys owned by this projection. A thin receipt carries none of the
# first three; ``terminal_evidence`` may record why first publication held.
ENRICHMENT_KEYS = frozenset({"dispatch_receipt", "tokens", "token_accounting", "terminal_evidence"})
# Retained native artifacts that must exist for an authentic enrichment.
REQUIRED_ARTIFACTS = ("provenance", "stream")
_JOB_ID = re.compile(r"j[0-9a-f]{20}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MAX_ARTIFACT_BYTES = 256 * 1024 * 1024


class OffloadProductionHistory:
    """One pass's storage authority; a projection never decides legacy status.

    Canon v0.26.2: DATA-14, No dual writes (Designing Data-Intensive
    Applications, ch. 11); DATA-04, Expand→migrate→contract (Release It!,
    ch. 18; Continuous Delivery, ch. 12). Only the public storage API is used.
    Worker access cannot establish complete source coverage, even at zero diff.
    """

    def __init__(self, *, task_ref: str, lane_id: str, pass_id: str, job_id: str | None):
        self.task_ref, self.lane_id, self.pass_id, self.job_id = task_ref, lane_id, pass_id, job_id
        self.admission: dict[str, Any] | None = None
        self.rows: list[dict[str, Any]] = []
        self.head = production.ProductionHead(0, None)
        self.current = False
        self.prepared = False
        self.resuming = False
        self.access_started = False
        self.hold = False
        self.failed = False
        self.annotation: dict[str, Any] = {"state": "unrecorded", "production": "unavailable"}

    def unavailable(self, reason: str, exc: Exception | str, *, hold: bool = False) -> None:
        detail = {"error": str(exc)}
        if isinstance(exc, production.ProductionRefusal):
            detail["refusal_code"] = exc.code.value
        elif isinstance(exc, Exception):
            detail["exception_type"] = type(exc).__name__
        self.hold, self.failed = hold, True
        self.annotation = {
            **self.annotation,
            "operation_id": self.pass_id,
            "state": "held" if hold else "pending",
            "production": "unavailable",
            "reason": reason,
            "detail": detail,
        }

    def open(self, ref: Any) -> None:
        """Mirror wave_dispatch._production_admission_checks, including names."""
        detail = {"task_ref": self.task_ref, "manifest_ref": ref}
        try:
            admission = production.get_lane_production_admission(task_ref=self.task_ref, lane_id=self.lane_id)["data"]
        except production.ProductionRefusal as exc:
            code = exc.code.value
            if code in {"identity_mismatch", "contract_mismatch"} and isinstance(ref, dict):
                # Historical reads grant no write authority. They only identify
                # this exact pre-existing intent so recovery cannot run a worker.
                try:
                    self._read(ref, historical=True)
                    previous = self._pass_prepare()
                    if previous is not None:
                        self.admission = ref
                        self.annotation.update(admission_id=ref["admission_id"])
                        self.identity_pending(exc)
                        self.failed = False
                        return
                except Exception as read_exc:  # noqa: BLE001 — incomplete evidence cannot authorize recovery
                    self.unavailable("production_history_unavailable", read_exc, hold=True)
                    return
            detail["refusal_code"] = code
            if ref is None and code in {"admission_absent", "identity_mismatch", "contract_mismatch", "invalid_record"}:
                self.annotation.update(
                    reason="production_admission_absent"
                    if code == "admission_absent"
                    else "production_admission_unverifiable",
                    detail=detail,
                )
                return
            reason = "production_admission_missing" if code == "admission_absent" else "production_admission_mismatch"
            self.unavailable(reason, exc, hold=True)
            self.annotation["detail"].update(detail)
            return
        except Exception as exc:  # noqa: BLE001 — unreadable authority must stop before access
            self.unavailable("production_admission_unreadable", exc, hold=True)
            return
        latest_ref = {"admission_id": admission["admission_id"], "binding": admission["binding"]}
        if "root_binding" in admission:
            latest_ref["root_binding"] = admission["root_binding"]
        if ref != latest_ref:
            self.unavailable("production_admission_mismatch", "Manifest admission differs from the DB root", hold=True)
            self.annotation["detail"].update(detail, latest_ref=latest_ref)
            return
        self.admission = admission
        self.current = True
        self.annotation = {
            "state": "pending",
            "production": "unavailable",
            "admission_id": admission["admission_id"],
            "operation_id": self.pass_id,
        }
        try:
            self._read(admission)
        except Exception as exc:  # noqa: BLE001 — no CAS from an incomplete/unreadable collection
            self.unavailable("production_history_unavailable", exc, hold=True)

    def _read(self, admission: Mapping[str, Any], *, historical: bool = False) -> None:
        binding = admission.get("root_binding", admission["binding"]) if historical else admission["binding"]
        data = production.get_lane_production_history(binding, admission["admission_id"], historical=historical)["data"]
        if data["has_more"] or data["truncated"]:
            raise ValueError("production history is truncated")
        self.head = production.ProductionHead(**data["head"])
        self.rows = data["history"]

    def _pass_prepare(self) -> dict[str, Any] | None:
        return next(
            (
                r
                for r in self.rows
                if r["operation_id"] == self.pass_id
                and r["evidence_kind"] == "production_prepare"
                and r["payload"]["producer_kind"] == "offload_pass"
                and r["payload"]["facts"].get("pass_id") == self.pass_id
            ),
            None,
        )

    def identity_pending(self, exc: Exception) -> None:
        self.unavailable("production_settle_identity_pending", exc)

    def refresh_current(self) -> None:
        """Only the DB can supply the binding for a post-harvest write.

        Canon v0.26.2 DATA-23, Do not guess the final decision (Database
        Internals, ch. 13). Owner contract refresh and its link precede recovery.
        """
        admission = production.get_lane_production_admission(task_ref=self.task_ref, lane_id=self.lane_id)["data"]
        if self.admission is None or admission["admission_id"] != self.admission["admission_id"]:
            raise ValueError("recovery admission differs from original prepare")
        self._read(admission)
        if self._pass_prepare() is None:
            raise ValueError("original pass prepare is unavailable")
        self.admission = admission
        self.current = True

    def _append(self, kind: str, operation_id: str, facts: Mapping[str, Any]) -> None:
        assert self.admission is not None
        request = production.ProductionAppend(
            binding=self.admission["binding"],
            admission_id=self.admission["admission_id"],
            operation_id=operation_id,
            producer_kind="offload_pass",
            producer_version="1",
            writer_authority_ref=f"offload:{self.task_ref}:{self.lane_id}:{self.pass_id}",
            expected_head=self.head,
            facts=facts,
        )
        record = getattr(production, f"record_production_{kind}")(request)
        self.head = record.head

    def prepare(self, facts: Mapping[str, Any]) -> None:
        if self.hold or self.admission is None:
            return
        previous = self._pass_prepare()
        if previous is not None:
            self.prepared, self.resuming, self.access_started = True, True, True
            if previous["payload"]["facts"].get("source_reservation", {}).get("job_id") != self.job_id:
                self.unavailable("production_history_unavailable", "pass/job reservation differs", hold=True)
            return
        try:
            self._append("prepare", self.pass_id, facts)
            self.prepared = True
        except Exception as exc:  # noqa: BLE001 — failed write-ahead append forbids worker access
            self.unavailable("production_history_unavailable", exc, hold=True)

    def prepare_checkpoint(self, facts: Mapping[str, Any]) -> str | None:
        operation_id = f"{self.pass_id}:checkpoint:{facts['oid']}"
        try:
            if self.failed:
                return None
            self._append("prepare", operation_id, {"pass_id": self.pass_id, "kind": "checkpoint", **facts})
            return operation_id
        except Exception as exc:  # noqa: BLE001 — never publish an unprepared checkpoint
            self.unavailable("production_history_unavailable", exc)
            return None

    def settle_checkpoint(self, operation_id: str, facts: Mapping[str, Any]) -> None:
        if not self.current:
            return
        try:
            self._append(
                "settle",
                operation_id,
                {"pass_id": self.pass_id, "kind": "checkpoint", "production": "positive", **facts},
            )
        except Exception as exc:  # noqa: BLE001 — retain product; leave checkpoint and pass pending
            self.unavailable("production_history_unavailable", exc)

    def checkpoint_receipts(self) -> list[dict[str, Any]]:
        return [
            r
            for r in self.rows
            if r["evidence_kind"] == "production_prepare"
            and r["payload"]["facts"].get("kind") == "checkpoint"
            and r["payload"]["facts"].get("pass_id") == self.pass_id
        ]

    def settled(self, operation_id: str) -> dict[str, Any] | None:
        return next(
            (r for r in self.rows if r["operation_id"] == operation_id and r["evidence_kind"] == "production_settle"),
            None,
        )

    def finish(
        self,
        result: dict[str, Any],
        *,
        clean_ref: bool,
        native_artifacts: Mapping[str, Any],
        artifacts_complete: bool,
        native_commits: list[str],
    ) -> None:
        """Called after terminal persistence, checkpoint/salvage and harvest.

        Canon v0.26.2 RES-18, Order durable writes: payload then commit;
        pointed-to then pointer (Operating Systems: Three Easy Pieces, ch. 42).
        A missing artifact or unmediated worker with no product remains pending.
        """
        if self.prepared and not self.hold and not self.failed:
            existing = self.settled(self.pass_id)
            harvest = result.get("findings_harvest") or {}
            incomplete = isinstance(harvest, dict) and (
                harvest.get("unrecorded_ids") or harvest.get("reason") in {"record_failed", "record_pending"}
            )
            checkpoints = result.get("checkpoint_commits") or []
            positive = bool(
                checkpoints or native_commits or (isinstance(harvest, dict) and harvest.get("recorded_ids"))
            )
            zero = not self.access_started and clean_ref and not native_artifacts and not positive
            terminal = result.get("outcome") != "remote_job_pending" and result.get("terminal") is not False
            if existing:
                facts = existing["payload"]["facts"]
                self._mark_settled(facts, production.ProductionHead(existing["sequence"], existing["evidence_id"]))
            elif (
                terminal
                and artifacts_complete
                and not incomplete
                and (positive or zero)
                and result.get("outcome") != "uncommitted_work"
                and not str(result.get("error", "")).startswith("offload pass crashed:")
            ):
                facts = {
                    "pass_id": self.pass_id,
                    "job_id": self.job_id,
                    "production": "positive" if positive else "verified_zero",
                    "worker_source_access_started": self.access_started,
                    "source_complete": zero,
                    "checkpoint_commits": checkpoints,
                    "native_commits": native_commits,
                    "native_artifacts": dict(native_artifacts),
                    "findings_harvest": harvest,
                    "outcome": result.get("outcome"),
                }
                # Persist these facts in protected pass custody even if authority
                # moved. The prepare remains the sentinel; this is not a settle.
                self.annotation["settlement_facts"] = facts
                try:
                    self.refresh_current()
                    existing = self.settled(self.pass_id)
                    if existing is None:
                        self._append("settle", self.pass_id, facts)
                        self._mark_settled(facts, self.head)
                    else:
                        self._mark_settled(
                            existing["payload"]["facts"],
                            production.ProductionHead(existing["sequence"], existing["evidence_id"]),
                        )
                except production.ProductionRefusal as exc:
                    if exc.code.value in {"identity_mismatch", "contract_mismatch"}:
                        self.identity_pending(exc)
                    else:
                        self.unavailable("production_history_unavailable", exc)
                except Exception as exc:  # noqa: BLE001 — evidence failure cannot discard existing product
                    self.unavailable("production_history_unavailable", exc)
        self.project(result)

    def project(self, result: dict[str, Any]) -> None:
        result["production_history"] = dict(self.annotation)
        if self.annotation.get("state") == "pending":
            result["production_history_unavailable"] = True
        elif self.annotation.get("state") == "settled":
            result.pop("production_history_unavailable", None)

    def _mark_settled(self, facts: Mapping[str, Any], head: production.ProductionHead) -> None:
        self.annotation = {
            "state": "settled",
            "production": facts["production"],
            "admission_id": self.admission["admission_id"],
            "operation_id": self.pass_id,
            "head": {"sequence": head.sequence, "evidence_id": head.evidence_id},
        }

    def reconcile(self, result: dict[str, Any]) -> None:
        """Replay retained terminal facts, without worker access or new intent.

        Canon v0.26.2 RES-19, Recovery redo must be idempotent (Operating
        Systems: Three Easy Pieces, ch. 42): a retained settle is a no-op.
        """
        facts = result.get("production_history", {}).get("settlement_facts")
        if not isinstance(facts, dict) or facts.get("pass_id") != self.pass_id:
            self.unavailable("production_history_unavailable", "retained terminal facts unavailable")
            result["production_history"] = dict(self.annotation)
            result["production_history_unavailable"] = True
            return
        try:
            for artifact in facts["native_artifacts"].values():
                contents = regular_file_bytes(Path(artifact["path"]))
                if len(contents) != artifact["bytes"] or sha256_hex(contents) != artifact["sha256"]:
                    raise ValueError("retained native artifact differs")
        except Exception as exc:  # noqa: BLE001 — missing or replaced source is pending, never zero
            self.unavailable("production_history_unavailable", exc)
        self.finish(
            result,
            clean_ref=False,
            native_artifacts=facts["native_artifacts"],
            artifacts_complete=not self.failed,
            native_commits=facts["native_commits"],
        )

    def recover_terminal(
        self,
        result: dict[str, Any],
        *,
        seed: Mapping[str, Any] | None = None,
        detail: str = "terminal has no retained settlement facts",
    ) -> None:
        """Reconcile this pass's open prepare from a terminal read, in place.

        Custody refusals and cached detached receipts never re-enter the engine,
        so they authenticate the ORIGINAL operation through the current public
        lookup binding, then settle it or keep it typed pending. No worker runs,
        no intent or operation_id is minted, and a settled operation is a no-op.
        Without a retained pending projection, only an open prepare found in the
        DB adds one; legacy and unrecorded results stay byte-for-byte unchanged.
        Canon v0.26.2 RES-19 and AGT-10, Degrade loudly (The Art of UNIX
        Programming, ch. 1).
        """
        retained = result.get("production_history")
        seed = retained if isinstance(retained, dict) else seed
        if seed is not None and (seed.get("state") != "pending" or seed.get("operation_id") != self.pass_id):
            return
        if seed is not None:
            self.annotation = dict(seed)
        try:
            admission = production.get_lane_production_admission(task_ref=self.task_ref, lane_id=self.lane_id)["data"]
            if seed is not None and admission["admission_id"] != seed.get("admission_id"):
                raise ValueError("terminal admission differs from original prepare")
            self._read(admission)
            prepare = self._pass_prepare()
            if prepare is None:
                if seed is None:
                    return
                raise ValueError("original pass prepare is unavailable")
        except production.ProductionRefusal as exc:
            if seed is None:
                return
            if exc.code.value in {"identity_mismatch", "contract_mismatch"}:
                self.identity_pending(exc)
            else:
                self.unavailable("production_history_unavailable", exc)
            self.project(result)
            return
        except Exception as exc:  # noqa: BLE001 — unreadable authority keeps the operation pending
            if seed is None:
                return
            self.unavailable("production_history_unavailable", exc)
            self.project(result)
            return
        if seed is None:
            self.annotation = {
                "state": "pending",
                "production": "unavailable",
                "admission_id": admission["admission_id"],
                "operation_id": self.pass_id,
            }
        self.admission, self.current = admission, True
        self.prepared = self.resuming = self.access_started = True
        reserved = prepare["payload"]["facts"].get("source_reservation", {}).get("job_id")
        existing = self.settled(self.pass_id)
        if isinstance(reserved, str) and isinstance(self.job_id, str) and reserved != self.job_id:
            self.unavailable("production_history_unavailable", "pass/job reservation differs")
        elif existing is not None:
            self._mark_settled(
                existing["payload"]["facts"], production.ProductionHead(existing["sequence"], existing["evidence_id"])
            )
        elif isinstance(self.annotation.get("settlement_facts"), dict):
            self.project(result)
            self.reconcile(result)
            return
        else:
            self.unavailable("production_history_unavailable", detail)
        self.project(result)


_MAX_INTENT_BYTES = 16 * 1024 * 1024


class EvidenceHold(Exception):
    """A typed refusal (AGT-21); ``reason`` is the stable machine-readable code."""

    def __init__(self, reason: str, needed: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.needed = needed


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_sha256(value: Any) -> str:
    return sha256_hex(canonical_bytes(value))


def regular_file_bytes(path: Path, *, root: Path | None = None, limit: int = _MAX_ARTIFACT_BYTES) -> bytes:
    """Read one producer-owned regular file without following a link.

    ``FileNotFoundError`` propagates (callers map absence to their own hold).
    A symlink, non-regular file, foreign owner, world-writable mode, or a
    parent outside ``root`` is ``evidence_path_identity_invalid``; more than
    ``limit`` bytes is ``retained_evidence_oversized`` (RES-02 bounded read).
    """
    path = Path(path)
    if not path.is_absolute():
        raise EvidenceHold("evidence_path_identity_invalid", str(path))
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o002:
        raise EvidenceHold("evidence_path_identity_invalid", str(path))
    if root is not None and not Path(os.path.realpath(path.parent)).is_relative_to(Path(os.path.realpath(root))):
        raise EvidenceHold("evidence_path_identity_invalid", str(path))
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(fd)
        if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
            raise EvidenceHold("evidence_path_identity_invalid", str(path))
        with os.fdopen(fd, "rb") as stream:
            fd = -1
            data = stream.read(limit + 1)
    finally:
        if fd >= 0:
            os.close(fd)
    if len(data) > limit:
        raise EvidenceHold("retained_evidence_oversized", str(path))
    return data


# ---------------------------------------------------------------------------
# Review-cycle dispatch receipt (host-retained request), read-only
# ---------------------------------------------------------------------------


def dispatch_receipt_path(repo: Path, dispatch_id: str) -> Path:
    """The native receipt locator, without creating its directory.

    ``review_cycle_dispatch._receipt_path`` names the file but also creates the
    authority directory; a read must never create it, so call it only once the
    directory is known to exist (the call is then a no-op).
    """
    from . import review_cycle_dispatch as dispatch  # noqa: PLC0415

    root = dispatch.authority(Path(repo)) / "review_cycle_dispatch"
    if not root.is_dir() or root.is_symlink():
        raise EvidenceHold("dispatch_receipt_missing", str(root))
    return dispatch._receipt_path(Path(repo), dispatch_id)


def read_dispatch_receipt(repo: Path, dispatch_id: str) -> tuple[Path, dict[str, Any], bytes]:
    path = dispatch_receipt_path(repo, dispatch_id)
    try:
        raw = regular_file_bytes(path, limit=_MAX_INTENT_BYTES)
        receipt = json.loads(raw)
    except FileNotFoundError as exc:
        raise EvidenceHold("dispatch_receipt_missing", str(path)) from exc
    except (OSError, ValueError) as exc:
        raise EvidenceHold("dispatch_receipt_unreadable", str(path)) from exc
    if not isinstance(receipt, dict):
        raise EvidenceHold("dispatch_receipt_unreadable", str(path))
    return path, receipt, raw


# ---------------------------------------------------------------------------
# Shared derivation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NativeTerminalEvidence:
    """Authenticated facts of one paid native attempt; every field names its source."""

    job_id: str
    rc: int
    backend: str
    route: dict[str, Any]
    review_cycle: dict[str, Any]
    native_dispatch: dict[str, str]
    source: dict[str, Any]
    stream: dict[str, int]
    turns: int
    turns_complete: bool
    artifacts: dict[str, Any]
    evidence_sha256: str


def stream_turn_facts(raw: bytes) -> dict[str, int]:
    """Count native ``turn.*`` events in a retained JSONL stream (no model call)."""
    facts = {"turn_completed": 0, "turn_started": 0, "turn_failed": 0, "malformed_lines": 0}
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError):
            facts["malformed_lines"] += 1
            continue
        kind = event.get("type") if isinstance(event, dict) else None
        if kind == "turn.completed":
            facts["turn_completed"] += 1
        elif kind == "turn.started":
            facts["turn_started"] += 1
        elif kind == "turn.failed":
            facts["turn_failed"] += 1
    return facts


def _enforced_route(spec: Mapping[str, Any], row: Mapping[str, Any], completion_config: Mapping[str, Any]) -> dict:
    """Routing facts the reservation enforced before transport started.

    ``shell_reserve`` moves a receipt to ``transport_started`` only after
    ``enforce_route`` accepted the executed spec. Without an operator requirement,
    the job argv or recorded completion config supplies the route. All stated
    routes must agree. This is the requested route, never served identity.
    """
    from . import review_cycle_dispatch as dispatch  # noqa: PLC0415

    required = row.get("required_route")
    backend = spec.get("backend")
    if not isinstance(backend, str) or not backend:
        raise EvidenceHold("route_unavailable")
    argv = [str(item) for item in spec.get("argv") or []] if isinstance(spec.get("argv"), list) else []
    stated: list[tuple[list[str], Any]] = []
    has_route = any(item in {"-m", "--model", "--effort", "--reasoning-effort"}
                    or item.startswith("model_reasoning_effort=") for item in argv)
    if required is None:
        try:
            required = dispatch.executed_route(dict(argv=argv, backend_id=backend)) if has_route else dict(
                model=completion_config.get("model"), effort=completion_config.get("reasoning_effort"))
        except ValueError as exc:
            raise EvidenceHold("route_unavailable") from exc
        if not all(isinstance(required.get(key), str) and required[key] for key in ("model", "effort")):
            raise EvidenceHold("route_unavailable")
        # Compare execution facts even though the operator imposed no constraint.
        required = {**required, "backend": backend}
    if not isinstance(required, dict):
        raise EvidenceHold("route_unavailable")
    if has_route:
        stated.append((argv, backend))
    model, effort = completion_config.get("model"), completion_config.get("reasoning_effort")
    model = model if isinstance(model, str) and model else None
    effort = effort if isinstance(effort, str) and effort else None
    if model is not None or effort is not None:
        # An unset (adapter-default) part states nothing; a stated part must agree.
        stated.append((
            ["-m", model or str(required.get("model")), "--effort", effort or str(required.get("effort"))],
            completion_config.get("backend") or backend,
        ))
    try:
        for executed, executed_backend in stated:
            dispatch.enforce_route({"argv": executed, "backend_id": executed_backend}, {"required_route": required})
    except (ValueError, KeyError, TypeError) as exc:
        raise EvidenceHold("route_mismatch") from exc
    return {"backend": backend, "model": required.get("model"), "effort": required.get("effort"),
            "source": "review_cycle_reservation_enforced"}


def derive_native_terminal_evidence(
    *,
    job_id: str,
    rc: int,
    spec: Mapping[str, Any],
    receipt: Mapping[str, Any],
    artifact_paths: Mapping[str, Path],
    evidence_root: Path,
    completion_config: Mapping[str, Any],
    resolve_tree: Callable[[str], str],
) -> NativeTerminalEvidence:
    """Authenticate retained native evidence of one detached job, or hold.

    Identity: the receipt is this job's ``transport_started`` reservation and
    its source branch/head are the spec's. Evidence: the required retained
    artifacts are producer-owned regular files under ``evidence_root``; the
    source provenance passes the native validator against the receipt source;
    its nonce/ref is the host-retained ``native_dispatch``; it proves the model
    process started. Turns come from ``turn.completed`` events only.
    """
    from .adapters.remote_exec import _source_provenance_from_json  # noqa: PLC0415
    from .review_cycle_dispatch import normalized_model  # noqa: PLC0415

    identity = receipt.get("identity")
    decision = receipt.get("decision")
    row = decision.get("row") if isinstance(decision, dict) else None
    if (
        not isinstance(identity, dict)
        or identity.get("dispatch_id") != job_id
        or receipt.get("state") != "transport_started"
        or not isinstance(decision, dict)
        or decision.get("status") != "reserved"
        or not isinstance(row, dict)
        or row.get("dispatch_id") != job_id
        or (row.get("subject_branch"), row.get("kind")) != (identity.get("subject_branch"), identity.get("kind"))
    ):
        raise EvidenceHold("dispatch_identity_mismatch")
    branch, head, tree = receipt.get("source_branch"), receipt.get("source_head"), receipt.get("source_tree")
    if not all(isinstance(value, str) and value for value in (branch, head, tree)) or branch != spec.get("branch"):
        raise EvidenceHold("dispatch_identity_mismatch")
    start_head = completion_config.get("start_head_ref")
    if start_head is not None and start_head != head:
        raise EvidenceHold("pass_identity_mismatch")
    route = _enforced_route(spec, row, completion_config)
    executed = receipt.get("executed_route")
    if executed is not None and (
        not isinstance(executed, dict)
        or any(dispatch_value != recorded_value for dispatch_value, recorded_value in (
            (executed.get("backend"), route["backend"]),
            (normalized_model(executed.get("model")), normalized_model(route["model"])),
            (executed.get("effort"), route["effort"]),
        ))
    ):
        raise EvidenceHold("route_mismatch")

    # Retained artifacts named by the persisted build argv (GRPH-14 lineage).
    producer = receipt.get("producer_artifacts")
    if isinstance(producer, dict):
        for name in ("result", "provenance", "patch"):
            named = producer.get(name)
            if named and (name not in artifact_paths or Path(named) != Path(artifact_paths[name])):
                raise EvidenceHold("evidence_path_identity_invalid", str(named))
    contents: dict[str, bytes] = {}
    manifest: dict[str, Any] = {}
    for name in sorted(artifact_paths):
        path = Path(artifact_paths[name])
        try:
            data = regular_file_bytes(path, root=evidence_root)
        except FileNotFoundError:
            manifest[name] = None
            continue
        except OSError as exc:
            raise EvidenceHold("evidence_path_identity_invalid", str(path)) from exc
        contents[name] = data
        manifest[name] = {"path": str(path), "sha256": sha256_hex(data), "bytes": len(data)}
    for name in REQUIRED_ARTIFACTS:
        if name not in contents:
            raise EvidenceHold("retained_evidence_missing", str(artifact_paths.get(name, f"--{name}-out")))

    # The native source validator, bound to the host receipt and the object store.
    if resolve_tree(head) != tree:
        raise EvidenceHold("source_provenance_identity_mismatch")
    proof, failure = _source_provenance_from_json(
        Path(artifact_paths["provenance"]), branch=branch, head_sha=head, head_tree=tree
    )
    if proof is None:
        raise EvidenceHold(
            "retained_evidence_missing" if failure == "missing" else f"source_provenance_{failure or 'malformed'}"
        )
    try:
        retained = json.loads(contents["provenance"])
    except ValueError as exc:
        raise EvidenceHold("stale_evidence_sha256") from exc
    if retained != proof:
        # The validated bytes are not the bytes this derivation hashed.
        raise EvidenceHold("stale_evidence_sha256")
    native = receipt.get("native_dispatch")
    if not isinstance(native, dict) or native != {
        "dispatch_nonce": proof.get("dispatch_nonce"),
        "dispatch_ref": proof.get("dispatch_ref"),
    }:
        raise EvidenceHold("native_dispatch_mismatch")
    if proof.get("model_process_started") is not True:
        raise EvidenceHold("model_process_not_started")

    stream = stream_turn_facts(contents["stream"])
    turns = stream["turn_completed"]
    turns_complete = (
        stream["malformed_lines"] == 0 and stream["turn_failed"] == 0 and stream["turn_started"] <= turns
    )
    artifacts = {"schema": EVIDENCE_SCHEMA, "artifacts": manifest}
    return NativeTerminalEvidence(
        job_id=job_id,
        rc=rc,
        backend=route["backend"],
        route=route,
        review_cycle={
            "dispatch_id": job_id,
            "subject_branch": identity["subject_branch"],
            "kind": identity["kind"],
            "round": row.get("round"),
        },
        native_dispatch={"dispatch_nonce": native["dispatch_nonce"], "dispatch_ref": native["dispatch_ref"]},
        source={
            "branch": branch,
            "head_sha": head,
            "head_tree": tree,
            "remote_source_commit": proof["remote_source_commit"],
            "sandbox_base_commit": proof["sandbox_base_commit"],
            "history_stripped": proof["history_stripped"],
        },
        stream=stream,
        turns=turns,
        turns_complete=turns_complete,
        artifacts=manifest,
        evidence_sha256=canonical_sha256(artifacts),
    )


def thin_terminal_base(result: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in result.items() if key not in ENRICHMENT_KEYS}


def enriched_terminal_result(base: Mapping[str, Any], evidence: NativeTerminalEvidence) -> dict[str, Any]:
    """Project authenticated history onto a terminal receipt; never certify more.

    Paid usage is explicitly unknown (``token_accounting: "unknown"``,
    ``cumulative_total: None``, ``tokens_complete: False``). The requested route
    is recorded as requested, never as served model identity. No review ran on
    an execute-stage failure, so rounds/high findings are zero; the outcome,
    ``ok`` and merge facts of the original receipt are untouched.
    """
    counts = {"turns": evidence.turns, "rounds": 0, "high_finding_count": 0}
    pass_history: dict[str, Any] = {**counts, "routing_history": []}
    if not evidence.turns_complete:
        pass_history["counts_complete"] = False
    receipt = {
        "backend_id": evidence.backend,
        "resolved_model": None,
        "served_model": None,
        "tracked_pin": None,
        "pin_source": "unavailable",
        "gate_host": None,
        "observed_backend": evidence.backend,
        "tier": None,
        "originating_tier": None,
        "routing_effective": None,
        "routing_requested": dict(evidence.route),
        **counts,
        "escalated_from": None,
        "cost_per_accepted_output": None,
        "observed_tokens": None,
        "routing_history": [],
        "tokens_complete": False,
        "pass_history": pass_history,
        "model_process_started": True,
        "process_start_source": "native_source_provenance",
    }
    return {
        **thin_terminal_base(base),
        "dispatch_receipt": receipt,
        "tokens": {
            "cumulative_total": None,
            "tokens_complete": False,
            "usage_source": "unavailable",
            "reason": "native_usage_not_retained",
        },
        "token_accounting": "unknown",
        "terminal_evidence": {
            "status": "authenticated",
            "schema": EVIDENCE_SCHEMA,
            "job_id": evidence.job_id,
            "rc": evidence.rc,
            "evidence_sha256": evidence.evidence_sha256,
            "artifacts": evidence.artifacts,
            "model_process_started": True,
            "native_dispatch": dict(evidence.native_dispatch),
            "source": dict(evidence.source),
            "stream": dict(evidence.stream),
            "review_cycle": dict(evidence.review_cycle),
        },
    }


# ---------------------------------------------------------------------------
# Durable enrichment intent (one content-addressed file per approved job)
# ---------------------------------------------------------------------------


def intent_dir(state_dir: Path) -> Path:
    """Under the native RemoteJobLedger root, beside ``ledger/``."""
    return Path(state_dir) / "remote-jobs" / INTENT_DIRNAME


def intent_path(state_dir: Path, job_id: str, digest: str) -> Path:
    return intent_dir(state_dir) / f"{job_id}.{digest}.json"


def read_enrichment_intents(state_dir: Path, job_id: str) -> list[tuple[Path, str, dict[str, Any]]] | None:
    """Every intent for ``job_id``; ``None`` when any candidate is invalid (fail closed).

    A candidate must be in protected producer custody (no link, this uid, no
    group/world bits), content-addressed, canonical, and the complete
    versioned shape. Shape is not authority: readers still authenticate the
    native records it names (``terminal_history_contract.authenticated_result``).
    """
    from . import terminal_history_contract as contract  # noqa: PLC0415

    if not isinstance(job_id, str) or _JOB_ID.fullmatch(job_id) is None:
        return None
    root = intent_dir(state_dir)
    if not root.is_dir():
        return []
    found: list[tuple[Path, str, dict[str, Any]]] = []
    try:
        candidates = sorted(root.glob(f"{job_id}.*.json"))
    except OSError:
        return None
    for path in candidates:
        digest = path.name[len(job_id) + 1 : -len(".json")]
        if not contract.protected_custody(path, root):
            return None
        try:
            raw = regular_file_bytes(path, root=root, limit=_MAX_INTENT_BYTES)
            intent = json.loads(raw)
        except (OSError, ValueError, EvidenceHold):
            return None
        if (
            _SHA256.fullmatch(digest) is None
            or sha256_hex(raw) != digest
            or raw != canonical_bytes(intent)
            or not contract.well_formed(intent, job_id)
        ):
            return None
        found.append((path, digest, intent))
    return found


def applied_enrichment(
    state_dir: Path, job_id: str, *, receipt_sha256: str
) -> tuple[Path, str, dict[str, Any]] | None:
    """The single valid intent bound to the exact original receipt bytes, else None."""
    found = read_enrichment_intents(state_dir, job_id)
    if not found or len(found) != 1:
        return None
    path, digest, intent = found[0]
    if intent["original_receipt"].get("sha256") != receipt_sha256:
        return None
    return path, digest, intent


def approved_terminal_result(
    state_dir: Path, *, pass_id: str, job_id: str, recorded: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Read-only cached view: the authenticated enrichment of ``recorded``, else None.

    The single intent for the job is an index only. The view is served when
    every native record it names re-validates (exact original pass bytes and
    identity, first-publication lane cycle, terminal job record, dispatch
    receipt nonce/source/route, the native settlement row, retained artifact
    digests) and the result re-derives exactly. Anything else keeps the thin
    receipt and its unknown usage (SECD-05). A read never writes.
    """
    from . import terminal_history_contract as contract  # noqa: PLC0415

    if (
        not isinstance(job_id, str)
        or _JOB_ID.fullmatch(job_id) is None
        or not isinstance(recorded, Mapping)
        or not intent_dir(state_dir).is_dir()
    ):
        return None
    found = read_enrichment_intents(state_dir, job_id)
    if not found or len(found) != 1:
        return None
    try:
        return contract.authenticated_result(Path(state_dir), found[0][2], pass_id=pass_id, job_id=job_id,
                                             recorded=dict(recorded))
    except (EvidenceHold, OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
    except Exception:  # noqa: BLE001 — an unverifiable authority (e.g. git/canon unavailable) is no authority
        return None
