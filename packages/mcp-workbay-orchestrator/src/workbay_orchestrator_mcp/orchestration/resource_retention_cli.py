"""R4 retention command boundary, with no installed entrypoint or default effects.

R5 supplies qualified R1/R2 probes and one R3/R5 ledger authority. The ledger
persists plans, CAS intents, terminal dispositions and events in its own atomic
transactions outside candidate resources. There is deliberately no second event
writer here. R0 Allow is pure validation and always effects_authorized=False.

QualifiedCallerAuthority and RetentionExecutor are trusted, out-of-band Python
integration dependencies, never parsed from JSON. Authority must independently
qualify the caller, operation, owner, source, policy and evidence; a wire Proof
with state=verified cannot supply it. Executor reconciliation is READ ONLY and
must identify this exact operation's effect, not infer success from absence.
A pending intent never invokes execute again, including after a process crash.

Canon v0.26.2: SECD-05 Fail-safe defaults (security.md:268, Security Engineering);
DATA-14 No dual writes (engineering.md:232, Designing Data-Intensive Applications,
ch. 11); DOM-06 Outbox or it didn't happen (engineering.md:684, Learning
Domain-Driven Design, ch. 9). Original distilled corpus is unavailable.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Literal, Protocol, TextIO

from pydantic import Field, StrictInt, ValidationError
from workbay_protocol.convergence import Sha256Digest
from workbay_protocol.resource_lifecycle import (
    Admission,
    ApplyRequest,
    ByteAccounting,
    ContractModel,
    CustodyAcknowledgement,
    DependencySnapshot,
    EvidenceRef,
    Held,
    Owner,
    Registration,
    ResourceIdentity,
    RetentionPlan,
    RetentionProbe,
    Text,
    Timestamp,
    admit_retention,
    canonical_digest,
    event_identity,
)

MAX_PAGE_SIZE = 100
MAX_INPUT_BYTES = 1024 * 1024
MAX_OUTPUT_BYTES = 64 * 1024
CommandName = Literal["plan", "probe", "apply", "status", "history"]
BoundaryCode = Literal[
    "authority_missing",
    "authority_denied",
    "authority_unknown",
    "executor_missing",
    "cas_changed",
    "plan_unknown",
    "plan_conflict",
    "receipt_storage_unsafe",
    "lock_timeout",
    "probe_failed",
    "ledger_failed",
    "effect_interrupted",
    "reconciliation_unknown",
    "receipt_invalid",
    "invalid_command",
    "invalid_packet",
    "output_too_large",
    "provider_page_invalid",
    "resource_path_invalid",
    "symlink_resource_unsupported",
    "provider_unconfigured",
]


class CommandHold(ContractModel):
    """Adapter holds; these do not extend R0's frozen Hold code vocabulary."""

    code: BoundaryCode
    subject: Text


class EffectReceipt(ContractModel):
    """Qualified executor observation, not a permission or a guessed absence."""

    resource: ResourceIdentity
    operation_id: Text
    plan_digest: Sha256Digest
    evidence: tuple[EvidenceRef, ...] = Field(min_length=1)
    completed_at: Timestamp
    accounting: ByteAccounting = Field(default_factory=ByteAccounting)


class OperationRecord(ContractModel):
    """Ledger-owned CAS intent and terminal record, with the original request."""

    request: ApplyRequest
    state: Literal["pending", "applied"]
    effect: EffectReceipt | None = None
    receipt: EvidenceRef | None = None


class RecordSummary(ContractModel):
    sequence: StrictInt = Field(ge=1)
    resource: ResourceIdentity
    operation_id: Text
    plan_digest: Sha256Digest
    state: Literal["pending", "applied", "held", "planned", "probed", "failed"]
    receipt: EvidenceRef | None = None


class RecordPage(ContractModel):
    items: tuple[RecordSummary, ...] = Field(max_length=MAX_PAGE_SIZE)
    next_cursor: StrictInt | None = Field(default=None, ge=1)


class PageQuery(ContractModel):
    cursor: StrictInt = Field(default=0, ge=0)
    limit: StrictInt = Field(default=25, ge=1, le=MAX_PAGE_SIZE)


class CommandResult(ContractModel):
    command: CommandName
    status: Literal["planned", "probed", "dry_run", "applied", "replayed", "pending", "held", "page"]
    plan_digest: Sha256Digest | None = None
    outcome: Admission | None = None
    holds: tuple[CommandHold, ...] = ()
    receipt: EvidenceRef | None = None
    page: RecordPage | None = None


class ProbeProvider(Protocol):
    def probe(self, plan: RetentionPlan) -> RetentionProbe:
        """Independently re-observe source, dependency, custody, use and policy.

        R1/R2 must cover reachable consumers/SCCs and privileged unknowns. Do not
        echo the plan as an observed clearance. Called while resource lock held.
        """
        ...


class QualifiedCallerAuthority(Protocol):
    def authorize(self, plan: RetentionPlan, probe: RetentionProbe, *, now: datetime) -> bool:
        """Literal True only for independently qualified effect authority."""
        ...

    def authorize_recovery(self, record: OperationRecord, *, now: datetime) -> bool:
        """Separate permission for read-only reconciliation of a pending intent."""
        ...


class RetentionExecutor(Protocol):
    def execute(self, plan: RetentionPlan, probe: RetentionProbe, *, operation_key: str) -> EffectReceipt:
        """One bounded operation; provider must make its effect reconcilable.

        The provider owns target containment/descriptor and identity pinning,
        durability, and defenses against mutations by noncooperating processes.
        All cooperating users must use this service's SAME resource lock root.
        """
        ...

    def reconcile(self, record: OperationRecord, *, operation_key: str) -> EffectReceipt | None:
        """READ ONLY: exact operation-specific evidence, never absence alone."""
        ...


class RetentionLedger(Protocol):
    """One qualified durable authority, implemented/wired by R3/R5.

    storage_root must describe the actual private authority location, outside
    every candidate and any containing retired checkout. Every mutator commits
    state and its immutable lifecycle event atomically (or via its own outbox).
    No lane identity is synthesized. page() uses owner-scoped keyset pagination.
    These are integration obligations, not claims about an unlanded R3 adapter.
    """

    storage_root: Path

    def save_plan(self, plan: RetentionPlan) -> bool:
        """CAS by (owner, plan_id): identical replay True, changed bytes False."""
        ...

    def read_plan(self, owner: Owner, plan_id: str) -> RetentionPlan | None: ...

    def lookup(self, resource: ResourceIdentity, operation_id: str) -> OperationRecord | None: ...

    def reserve(self, request: ApplyRequest) -> bool:
        """Durably insert pending intent iff operation key absent; no overwrite."""
        ...

    def complete(self, record: OperationRecord, effect: EffectReceipt) -> OperationRecord:
        """CAS pending digest -> applied + terminal event, retaining first receipt.

        Return only after durable commit; if commit raises/has unknown outcome,
        lookup/reconciliation on replay must recover without another effect.
        """
        ...

    def page(self, owner: Owner, *, cursor: int, limit: int, history: bool) -> RecordPage: ...


class _BoundaryError(Exception):
    def __init__(self, code: BoundaryCode, subject: str):
        super().__init__(code)
        self.code, self.subject = code, subject


class FileResourceLocks:
    """Bounded Linux flock shared by all processes using this private root.

    Lock files are stable and never unlinked: replacing them splits lock domains.
    The qualified caller provisions/trusts the directory; no permission changes.
    """

    def __init__(self, root: Path, *, timeout_seconds: float = 5.0):
        if not 0 < timeout_seconds <= 60:
            raise ValueError("lock timeout must be in (0, 60]")
        self.root = Path(root).resolve()
        self.timeout_seconds = timeout_seconds

    @contextmanager
    def hold(self, resource: ResourceIdentity) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True)
        key = canonical_digest(resource).removeprefix("sha256:")
        fd = os.open(self.root / (key + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            deadline = time.monotonic() + self.timeout_seconds
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise _BoundaryError("lock_timeout", "resource") from None
                    time.sleep(0.01)
            try:
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def build_plan(
    registration: Registration,
    dependencies: DependencySnapshot,
    custody: CustodyAcknowledgement,
    *,
    plan_id: str,
    operation_id: str,
    now: datetime,
) -> RetentionPlan:
    """Construct the exact frozen R0 plan; no authority or proofs are minted."""
    return RetentionPlan(
        registration=registration,
        dependencies=dependencies,
        custody=custody,
        plan_id=plan_id,
        operation_id=operation_id,
        created_at=now,
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RetentionCommands:
    """Importable orchestration seam for R5; no service is created on import."""

    def __init__(
        self,
        *,
        owner: Owner,
        probes: ProbeProvider,
        ledger: RetentionLedger,
        locks: FileResourceLocks,
        executor: RetentionExecutor | None = None,
        authority: QualifiedCallerAuthority | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ):
        self.owner = Owner.model_validate(owner.model_dump(mode="json"))
        self.probes, self.ledger, self.locks = probes, ledger, locks
        self.executor, self.authority, self.clock = executor, authority, clock

    def _locations(self, plan: RetentionPlan) -> None:
        resource = plan.registration.resource
        if resource.resource_kind in ("docker_image", "docker_volume"):
            return  # R2's qualified executor owns Docker target containment.
        path = Path(resource.identity)
        if not path.is_absolute() or ".." in path.parts:
            raise _BoundaryError("resource_path_invalid", "resource.identity")
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise _BoundaryError("symlink_resource_unsupported", "resource.identity")
        candidate = path.resolve()
        for root in (self.locks.root, Path(self.ledger.storage_root).resolve()):
            if root == candidate or root.is_relative_to(candidate):
                raise _BoundaryError("receipt_storage_unsafe", "storage_root")

    def _owner_hold(self, command: CommandName, plan: RetentionPlan) -> CommandResult | None:
        if plan.registration.resource.owner != self.owner:
            from workbay_protocol.resource_lifecycle import Hold

            return CommandResult(
                command=command,
                status="held",
                outcome=Held(
                    scope="retention",
                    holds=(Hold(code="foreign_owner", subject="resource.owner"),),
                ),
            )
        return None

    def _stored(self, plan: RetentionPlan) -> None:
        stored = self.ledger.read_plan(self.owner, plan.plan_id)
        if stored is None:
            raise _BoundaryError("plan_unknown", "plan_id")
        stored = RetentionPlan.model_validate(stored.model_dump(mode="json"))
        if stored != plan:
            raise _BoundaryError("plan_conflict", "plan_id")

    def _fresh(self, plan: RetentionPlan) -> RetentionProbe:
        try:
            observed = self.probes.probe(plan)
            if not isinstance(observed, RetentionProbe):
                raise TypeError("probe must return frozen R0 RetentionProbe")
            return RetentionProbe.model_validate(observed.model_dump(mode="json"))
        except Exception:
            raise _BoundaryError("probe_failed", "fresh_probe") from None

    def _record(self, record: OperationRecord, plan: RetentionPlan) -> OperationRecord:
        record = OperationRecord.model_validate(record.model_dump(mode="json"))
        if record.request.plan != plan or record.request.expected_plan_digest != canonical_digest(plan):
            raise _BoundaryError("cas_changed", "operation_id")
        if record.state == "applied":
            if record.effect is None or record.receipt is None:
                raise _BoundaryError("receipt_invalid", "terminal_record")
            self._effect(record.effect, plan)
        elif record.effect is not None or record.receipt is not None:
            raise _BoundaryError("receipt_invalid", "pending_record")
        return record

    def _effect(self, effect: EffectReceipt, plan: RetentionPlan) -> EffectReceipt:
        if not isinstance(effect, EffectReceipt):
            raise _BoundaryError("receipt_invalid", "executor")
        effect = EffectReceipt.model_validate(effect.model_dump(mode="json"))
        if (
            effect.resource != plan.registration.resource
            or effect.operation_id != plan.operation_id
            or effect.plan_digest != canonical_digest(plan)
            or effect.completed_at > self.clock()
        ):
            raise _BoundaryError("receipt_invalid", "executor")
        return effect

    def _permission(self, plan: RetentionPlan, probe: RetentionProbe | None, record: OperationRecord | None) -> None:
        if self.authority is None:
            raise _BoundaryError("authority_missing", "qualified_caller")
        if self.executor is None:
            raise _BoundaryError("executor_missing", "executor")
        try:
            permitted = (
                self.authority.authorize_recovery(record, now=self.clock())
                if record is not None
                else self.authority.authorize(plan, probe, now=self.clock())
            )
        except Exception:
            raise _BoundaryError("authority_unknown", "qualified_caller") from None
        if permitted is not True:
            raise _BoundaryError("authority_denied", "qualified_caller")

    def plan(self, plan: RetentionPlan) -> CommandResult:
        if held := self._owner_hold("plan", plan):
            return held
        self._locations(plan)
        with self.locks.hold(plan.registration.resource):
            self._locations(plan)
            if self.ledger.save_plan(plan) is not True:
                raise _BoundaryError("plan_conflict", "plan_id")
        return CommandResult(command="plan", status="planned", plan_digest=canonical_digest(plan))

    def probe(self, plan: RetentionPlan) -> CommandResult:
        if held := self._owner_hold("probe", plan):
            return held
        self._locations(plan)
        with self.locks.hold(plan.registration.resource):
            self._locations(plan)
            self._stored(plan)
            probe = self._fresh(plan)
            request = ApplyRequest(plan=plan, expected_plan_digest=canonical_digest(plan), probe=probe)
            outcome = admit_retention(request, owner=self.owner, now=self.clock())
        return CommandResult(
            command="probe",
            status="held" if isinstance(outcome, Held) else "probed",
            plan_digest=canonical_digest(plan),
            outcome=outcome,
        )

    def apply(self, request: ApplyRequest) -> CommandResult:
        plan, digest = request.plan, canonical_digest(request.plan)
        if held := self._owner_hold("apply", plan):
            return held
        # Validate caller CAS even on terminal replay, without imposing age on history.
        if request.expected_plan_digest != digest:
            from workbay_protocol.resource_lifecycle import Hold

            return CommandResult(
                command="apply",
                status="held",
                outcome=Held(
                    scope="retention",
                    holds=(Hold(code="plan_changed", subject="plan"),),
                ),
            )
        self._locations(plan)
        with self.locks.hold(plan.registration.resource):
            self._locations(plan)
            self._stored(plan)
            previous = self.ledger.lookup(plan.registration.resource, plan.operation_id)
            submitted = admit_retention(request, owner=self.owner, now=self.clock())
            if previous is not None and not request.dry_run:
                previous = self._record(previous, plan)
                # Historical lookup/reconciliation cannot execute. Old observation
                # windows may expire, but changed bindings, unknown/denied facts,
                # and every other R0 refusal still reject the replay request.
                if isinstance(submitted, Held):
                    replay_holds = tuple(hold for hold in submitted.holds if hold.code != "proof_stale")
                    if replay_holds:
                        return CommandResult(
                            command="apply",
                            status="held",
                            plan_digest=digest,
                            outcome=Held(scope="retention", holds=replay_holds),
                        )
                if previous.state == "applied":
                    return CommandResult(
                        command="apply", status="replayed", plan_digest=digest, receipt=previous.receipt
                    )
                self._permission(plan, None, previous)
                key = event_identity(plan.registration.resource, plan.operation_id, "applied")
                try:
                    effect = self.executor.reconcile(previous, operation_key=key)
                except Exception:
                    raise _BoundaryError("reconciliation_unknown", "pending_operation") from None
                if effect is None:
                    raise _BoundaryError("reconciliation_unknown", "pending_operation")
                return self._complete(previous, self._effect(effect, plan), "replayed")
            # Submitted JSON is only validation input. A fresh provider is mandatory.
            if isinstance(submitted, Held):
                return CommandResult(command="apply", status="held", plan_digest=digest, outcome=submitted)
            probe = self._fresh(plan)
            fresh = ApplyRequest(
                plan=plan, expected_plan_digest=request.expected_plan_digest, probe=probe, dry_run=request.dry_run
            )
            outcome = admit_retention(fresh, owner=self.owner, now=self.clock())
            if isinstance(outcome, Held) or request.dry_run:
                return CommandResult(
                    command="apply",
                    status="held" if isinstance(outcome, Held) else "dry_run",
                    plan_digest=digest,
                    outcome=outcome,
                )
            self._permission(plan, probe, None)
            # Intent is durable BEFORE execute; failed/unknown reservation forbids effects.
            if self.ledger.reserve(fresh) is not True:
                raise _BoundaryError("cas_changed", "operation_id")
            record = OperationRecord(request=fresh, state="pending")
            # Re-probe AFTER the durable CAS as well: CAS/authority callbacks can wait
            # or observe races. Nothing but pure validation separates this probe/effect.
            self._locations(plan)
            immediate = self._fresh(plan)
            final = admit_retention(
                ApplyRequest(plan=plan, expected_plan_digest=digest, probe=immediate, dry_run=False),
                owner=self.owner,
                now=self.clock(),
            )
            if isinstance(final, Held):
                return CommandResult(command="apply", status="held", plan_digest=digest, outcome=final)
            try:
                effect = self.executor.execute(
                    plan,
                    immediate,
                    operation_key=event_identity(plan.registration.resource, plan.operation_id, "applied"),
                )
            except Exception:
                raise _BoundaryError("effect_interrupted", "pending_operation") from None
            return self._complete(record, self._effect(effect, plan), "applied")

    def _complete(
        self, record: OperationRecord, effect: EffectReceipt, status: Literal["applied", "replayed"]
    ) -> CommandResult:
        terminal = self._record(self.ledger.complete(record, effect), record.request.plan)
        if terminal.state != "applied":
            raise _BoundaryError("receipt_invalid", "terminal_record")
        return CommandResult(command="apply", status=status, plan_digest=effect.plan_digest, receipt=terminal.receipt)

    def page(self, command: Literal["status", "history"], query: PageQuery) -> CommandResult:
        page = self.ledger.page(self.owner, cursor=query.cursor, limit=query.limit, history=command == "history")
        page = RecordPage.model_validate(page.model_dump(mode="json"))
        seqs = [item.sequence for item in page.items]
        if (
            len(page.items) > query.limit
            or seqs != sorted(set(seqs))
            or any(item.resource.owner != self.owner or item.sequence <= query.cursor for item in page.items)
            or (page.next_cursor is not None and (not seqs or page.next_cursor != seqs[-1]))
        ):
            raise _BoundaryError("provider_page_invalid", "page")
        return CommandResult(command=command, status="page", page=page)


def run_command(command: str, payload: Mapping[str, object], *, commands: RetentionCommands) -> CommandResult:
    """Closed JSON/result boundary; wire input cannot inject services or authority."""
    name: CommandName = command if command in ("plan", "probe", "apply", "status", "history") else "status"
    try:
        if command in ("plan", "probe"):
            plan = RetentionPlan.model_validate(payload)
            return getattr(commands, command)(plan)
        if command == "apply":
            return commands.apply(ApplyRequest.model_validate(payload))
        if command in ("status", "history"):
            return commands.page(command, PageQuery.model_validate(payload))
        raise _BoundaryError("invalid_command", "command")
    except _BoundaryError as exc:
        return CommandResult(command=name, status="held", holds=(CommandHold(code=exc.code, subject=exc.subject),))
    except (ValidationError, TypeError, ValueError):
        return CommandResult(command=name, status="held", holds=(CommandHold(code="invalid_packet", subject="packet"),))
    except Exception:
        # Never echo arbitrary provider exception text/private payloads to stdout.
        return CommandResult(
            command=name, status="held", holds=(CommandHold(code="ledger_failed", subject="authority_store"),)
        )


def main(
    argv: Sequence[str] | None = None,
    *,
    commands: RetentionCommands | None = None,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Importable CLI for R5. JSON packet on stdin; no default provider/executor.

    stdout is one typed JSON object, at most MAX_OUTPUT_BYTES. It carries digest
    and private receipt references, not unbounded plans/manifests/evidence. Full
    original packets remain in the external ledger. Exit 2 means a named hold.
    """
    parser = argparse.ArgumentParser(prog="workbay-retention")
    parser.add_argument("command", choices=("plan", "probe", "apply", "status", "history"))
    args = parser.parse_args(argv)
    stdin, stdout, stderr = stdin or sys.stdin, stdout or sys.stdout, stderr or sys.stderr
    if commands is None:
        result = CommandResult(
            command=args.command,
            status="held",
            holds=(CommandHold(code="provider_unconfigured", subject="R5_integration"),),
        )
    else:
        try:
            raw = stdin.read(MAX_INPUT_BYTES + 1)
            if len(raw.encode("utf-8")) > MAX_INPUT_BYTES:
                raise ValueError("input limit")
            payload = {} if args.command in ("status", "history") and not raw.strip() else json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("packet must be an object")
        except (ValueError, UnicodeError):
            result = CommandResult(
                command=args.command, status="held", holds=(CommandHold(code="invalid_packet", subject="stdin"),)
            )
        else:
            result = run_command(args.command, payload, commands=commands)
    encoded = result.model_dump_json() + "\n"
    if len(encoded.encode("utf-8")) > MAX_OUTPUT_BYTES:
        result = CommandResult(
            command=args.command,
            status="held",
            holds=(CommandHold(code="output_too_large", subject="private_manifest_required"),),
        )
        encoded = result.model_dump_json() + "\n"
    stdout.write(encoded)
    if result.status in ("held", "pending"):
        codes = [hold.code for hold in result.holds]
        if isinstance(result.outcome, Held):
            codes.extend(hold.code for hold in result.outcome.holds)
        stderr.write("retention held: " + ",".join(dict.fromkeys(codes))[:1024] + "\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
