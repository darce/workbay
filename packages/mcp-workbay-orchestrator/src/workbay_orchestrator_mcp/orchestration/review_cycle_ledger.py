"""Durable D3A reservations; this helper does not enforce live dispatch paths.

Consumer contract: trusted runtime supplies ONE absolute authority path, calls
activate once at rollout, reserve before spawn, and settle with the actual D2
outcome and already-classified canonical blocking IDs/fingerprints after harvest.
Only ``reserved`` permits dispatch. Replay returns the stored decision; consumers
must reconcile the same job, never spawn it twice. Pending attempts never expire.

D3B must bind TrustedPolicy to config SSOT and a verified published-canon manifest
and rule index, canonicalize logical subjects across branch renames, emit loud
proposal decisions, enforce returned routes/scope, and cover common submit|build
plus run_offload_pass exactly once. No caller threshold arguments are accepted.
Proposal files are JSON: rule_ids: nonempty list[str], change_from_previous:
nonempty str. Their bytes, digest, pin and correction are retained. A new attempt
needs a new correction, including after infrastructure failure. Runtime alone
asserts Claude exhaustion at construction; this module never chooses a fallback.

Kinds: ``implement`` (source), ``gate`` (merge judgment) and ``analysis``, a
read-only classification/diagnosis role whose native consumer must refuse any
source or HEAD change. Analysis has its own bound and never touches the
subject's fix, gate, repeated or terminal state, so it is never an approval,
a source pass or budget forgiveness. Routes below apply to new rows only; a
replay returns the stored row, so historical ``required_route`` is immutable.

Pre-activation sources enter only through ``import_legacy_source_history``,
which consumes the A17 verified lineage producer's ``list[dict]`` and keeps
original IDs, routes and charges. Once a subject has rows, this ledger is its
system of record: it is never rescanned, merged or reset (DATA-14).

Historical outcome corrections never rewrite a row. A trusted host producer
appends one idempotent event per dispatch to the append-only ``corrections``
list (``append_outcome_correction``); the original completion, round, route
and settled sequence stay byte-equal. ``effective_outcome_class`` is the one
fold over rows plus events: reserve's charged-row count and the derived
subject ``fixes`` view both read it, so a ledger without corrections behaves
exactly as before (DATA-14 single authority; REF-09 derived, not patched).

Issuance order is native and immutable: every NEW row stores
``issued_sequence`` from the ledger-level ``issuance_sequence`` counter at
reservation. Replay, settlement and correction never change it. Rounds
(reusable charge counts), ``settled_sequence`` (completion order), timestamps
and absent tokens are not issuance proof. Rows written before the counter
existed, and imported legacy rows, stay unproven; no value is fabricated for
them. Decode accepts such ledgers unchanged and fails closed on a malformed,
duplicated or out-of-range issuance value (SECD-05; GRPH-14 lineage).

D1 and release0173 remain integration prerequisites. No handoff DB/history reads,
model spawning, severity classification, or live infinite-loop enforcement here.
"""

from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from workbay_orchestrator_mcp.orchestration.pass_outcome_class import PassOutcomeClass, classify_pass_outcome


def _encoded(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


KINDS = frozenset({"implement", "gate", "analysis"})
LEGACY_PRODUCER = "review_ratchet.verified_legacy_source_history"
_GIT_ID = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _legacy_order(source: object, subject_branch: str) -> tuple | None:
    """Verified order key of one actual charged source record, else None.

    The record is the producer's evidence, not a request: the ordinal is
    derived from this order, so a supplied ``round`` is refused. A pending,
    refunded or uncharged call is not settled source history.
    """
    if not isinstance(source, dict) or "round" in source:
        return None
    route, producer, refusal = source.get("required_route"), source.get("producer"), source.get("refusal_reason")
    if (
        not all(
            isinstance(source.get(key), str) and source[key]
            for key in ("dispatch_id", "pass_id", "task_ref", "lane_id", "outcome", "started_at")
        )
        or source.get("subject_branch") != subject_branch
        or source.get("kind", "implement") != "implement"
        or source.get("charged") is not True
        or not isinstance(route, dict)
        or set(route) != {"model", "effort"}
        or not all(isinstance(value, str) and value for value in route.values())
        or not all(
            isinstance(source.get(key), str) and _GIT_ID.fullmatch(source[key])
            for key in ("source_head", "source_tree")
        )
        or not isinstance(producer, dict)
        or not isinstance(producer.get("provenance"), str)
        or not producer["provenance"]
        or not isinstance(producer.get("provenance_sha256"), str)
        or not _SHA256.fullmatch(producer["provenance_sha256"])
        or not isinstance(producer.get("dispatch_nonce"), str)
        or not producer["dispatch_nonce"]
        or not (refusal is None or isinstance(refusal, str))
        or classify_pass_outcome(source["outcome"], refusal_reason=refusal) != PassOutcomeClass.VERDICT
    ):
        return None
    try:
        started = datetime.fromisoformat(source["started_at"])
    except ValueError:
        return None
    return (started, source["dispatch_id"]) if started.tzinfo is not None else None


def _imported(rows: list[dict]) -> list[dict]:
    return sorted((row for row in rows if row.get("legacy_source")), key=lambda row: row["round"])


def _issued(row: dict) -> int | None:
    """The row's authentic native issuance order, else None (unproven).

    Only a native reservation carries it; decode has already rejected any
    malformed or duplicated value. Imported legacy rows are never proven.
    """
    value = row.get("issued_sequence")
    if row.get("legacy_source") is not None or type(value) is not int or value <= 0:
        return None
    return value


def _validate_issuance(data: dict) -> None:
    """Fail closed on issuance proof that is not unique, positive and counted.

    A ledger without ``issuance_sequence`` predates native issuance; it is
    accepted unchanged only while no row claims an ``issued_sequence``.
    """
    counter = data.get("issuance_sequence")
    if "issuance_sequence" in data and (type(counter) is not int or counter < 0):
        raise _Hold("ledger_issuance_integrity")
    seen = set()
    for row in data["rows"].values():
        if not isinstance(row, dict):
            raise _Hold("ledger_shape")
        if "issued_sequence" not in row:
            continue
        value = row["issued_sequence"]
        if (
            counter is None
            or type(value) is not int
            or not 0 < value <= counter
            or value in seen
            or row.get("legacy_source") is not None
        ):
            raise _Hold("ledger_issuance_integrity")
        seen.add(value)


@dataclass(frozen=True)
class TrustedPolicy:
    """Construction-only trusted context, never populated from dispatch arguments."""

    canon_version: str
    manifest_digest: str
    rule_ids: frozenset[str]
    dispatch_stop_at: int = 3
    claude_exhausted: bool = False

    def __post_init__(self):
        object.__setattr__(self, "rule_ids", frozenset(self.rule_ids))
        if not self.canon_version or not self.manifest_digest or not self.rule_ids:
            raise ValueError("verified canon pin/index required")
        if type(self.dispatch_stop_at) is not int or self.dispatch_stop_at < 3:
            raise ValueError("dispatch_stop_at must be at least three")
        if not all(isinstance(rule, str) and rule for rule in self.rule_ids):
            raise ValueError("invalid trusted rule index")

    def binding(self) -> dict:
        value = asdict(self)
        value["rule_ids"] = sorted(self.rule_ids)
        # Availability can change between restarts; it is captured on each row.
        value.pop("claude_exhausted")
        return value


@dataclass(frozen=True)
class LedgerDecision:
    """Typed result; a hold/stop must never be treated as permission to spawn."""

    status: str
    reason: str = ""
    row: dict | None = None


class _Hold(Exception):
    pass


class _Unchanged(_Hold):
    """Typed transaction result that must not write (replay, stale, refusal)."""

    def __init__(self, decision: LedgerDecision):
        super().__init__(decision.reason or decision.status)
        self.decision = decision


CORRECTION_SCHEMA = "workbay/review-cycle-outcome-correction@1"
_ORIGINAL_FIELDS = ("completion", "outcome_class", "round", "required_route", "settled_sequence", "settlement_status")


def _row_digest(row: dict) -> str:
    return _digest(_encoded(row))


def _correction_id(dispatch_id: str, row_sha256: str, observation_sha256: str) -> str:
    return _digest(
        _encoded({"dispatch_id": dispatch_id, "row_sha256": row_sha256, "observation_sha256": observation_sha256})
    )


def _valid_correction(event: object, row: dict) -> bool:
    """An event forgives only the exact, unmodified VERDICT implement row it snapshots."""
    if not isinstance(event, dict) or not isinstance(event.get("original"), dict):
        return False
    original = event["original"]
    digest = _row_digest(row)
    return (
        event.get("schema") == CORRECTION_SCHEMA
        and event.get("effective_outcome_class") == PassOutcomeClass.INFRA_STOP.value
        and (event.get("dispatch_id"), event.get("subject_branch"), event.get("kind"))
        == (row["dispatch_id"], row["subject_branch"], "implement")
        and row["kind"] == "implement"
        and row["outcome_class"] == PassOutcomeClass.VERDICT.value
        and original.get("row_sha256") == digest
        and all(original.get(key) == row.get(key) for key in _ORIGINAL_FIELDS)
        and event.get("producer") == "review_cycle_reconciliation.verified_refusal"
        and event.get("observation_sha256") == _digest(_encoded(event.get("observation")))
        and event.get("correction_id") == _correction_id(row["dispatch_id"], digest, event["observation_sha256"])
    )


def effective_outcome_class(data: dict, row: dict) -> str:
    """The single accounting fold: the stored class unless one valid event corrects it.

    Two events for one dispatch are ambiguous and forgive nothing (SECD-05).
    """
    corrections = data.get("corrections")
    if corrections:
        events = [event for event in corrections if event.get("dispatch_id") == row["dispatch_id"]]
        if len(events) == 1 and _valid_correction(events[0], row):
            return PassOutcomeClass.INFRA_STOP.value
    return row["outcome_class"]


def _subject_fixes(data: dict, subject_branch: str) -> int:
    """Completed source fixes, exactly the rows settle/import charge as a fix."""
    return sum(
        1
        for row in data["rows"].values()
        if row["subject_branch"] == subject_branch
        and row["kind"] == "implement"
        and row["decision"] == "reserved"
        and row["completion"] is not None
        and effective_outcome_class(data, row) == PassOutcomeClass.VERDICT.value
    )


def _required_route(policy: TrustedPolicy, kind: str, number: int) -> dict:
    # Operator roles: first source Sol high, gates Sol xhigh, analysis
    # Haiku xhigh, or Luna max only once the runtime asserts exhaustion.
    model, effort = "gpt-6.1-sol", "xhigh"
    if kind == "implement":
        if number == 1:
            effort = "high"
        elif number % 2 == 0 and not policy.claude_exhausted:
            model = "opus-5.5"
    elif kind == "analysis":
        model, effort = ("gpt-6-luna", "max") if policy.claude_exhausted else ("claude-haiku-5-5", "xhigh")
    return {"model": model, "effort": effort}


def subject_accounting(data: dict, subject_branch: str, policy: TrustedPolicy) -> dict:
    """Read-only view of what the fold implies for the subject's next reservation."""
    rows = [row for row in data["rows"].values() if row["subject_branch"] == subject_branch]
    charged = {
        kind: sum(
            1
            for row in rows
            if row["kind"] == kind
            and row["decision"] == "reserved"
            and effective_outcome_class(data, row) != PassOutcomeClass.INFRA_STOP.value
        )
        for kind in sorted(KINDS)
    }
    state = data["subjects"].get(subject_branch) or {}
    number = charged["implement"] + 1
    return {
        "charged_rows": charged,
        "fixes": _subject_fixes(data, subject_branch),
        "fixes_fold": _subject_fixes(data, subject_branch),
        "continuation": state.get("continuation"),
        "repeated": state.get("repeated", False),
        "terminal": state.get("terminal", False),
        "next_implement_round": number,
        "next_implement_route": _required_route(policy, "implement", number),
        "next_implement_needs_canon_correction": number >= policy.dispatch_stop_at or bool(state.get("repeated")),
    }


def correction_eligibility(data: dict, dispatch_id: str) -> LedgerDecision:
    """Whether the live ledger may carry a correction for this row; ``eligible`` or typed.

    The whole subject history is checked. A source reserved after the target
    counted its charge. Rounds are reusable charge counts (refunds let a
    dependent reuse or undercut the target's round) and settled_sequence is
    completion order, so neither proves issuance. Only native
    ``issued_sequence`` on BOTH rows proves that a pending same-kind source was
    issued before the target, so it never counted the target's charge and is
    the original, not a dependent. Every other source row still holds, as does
    unsettled work of another kind, later-issued work and any row lacking
    issuance proof (SECD-05; legacy and imported rows by construction). Gate
    and analysis rounds count only their own kind; they must be provably
    settled before the target.
    """
    row = data["rows"].get(dispatch_id)
    if row is None:
        return LedgerDecision("not_applicable", "no_ledger_row")
    if any(event.get("dispatch_id") == dispatch_id for event in data.get("corrections") or ()):
        return LedgerDecision("not_applicable", "correction_already_recorded")
    if row["decision"] != "reserved":
        return LedgerDecision("not_applicable", "dispatch_was_not_reserved")
    if row["kind"] != "implement":
        return LedgerDecision("ledger_hold", "correction_kind_unsupported")
    if row.get("legacy_source") is not None:
        return LedgerDecision("not_applicable", "imported_legacy_source_row")
    if row["completion"] is None:
        return LedgerDecision("not_applicable", "dispatch_pending")
    if row["outcome_class"] != PassOutcomeClass.VERDICT.value:
        return LedgerDecision("not_applicable", "row_not_verdict_charged", {"outcome_class": row["outcome_class"]})
    sequence = row.get("settled_sequence")
    if type(sequence) is not int or type(row.get("round")) is not int:
        return LedgerDecision("ledger_hold", "row_settlement_unordered")
    subject = row["subject_branch"]
    state = data["subjects"].get(subject)
    if not isinstance(state, dict) or state.get("fixes") != _subject_fixes(data, subject):
        return LedgerDecision("ledger_hold", "subject_fixes_fold_mismatch")
    later, unordered = [], []
    issued = _issued(row)
    for other_id, other in sorted(data["rows"].items()):
        if other_id == dispatch_id or other["subject_branch"] != subject:
            continue
        other_sequence = other.get("settled_sequence")
        if other["decision"] == "reserved" and other.get("completion") is None:
            other_issued = _issued(other)
            if other["kind"] == row["kind"] and None not in (issued, other_issued) and other_issued < issued:
                continue  # proven earlier original source: its reservation preceded the target's charge
            later.append(other_id)  # pending/unsettled work is never forgiven around
        elif other["kind"] == row["kind"]:
            unordered.append(other_id)  # its round may have counted the target; nothing proves otherwise
        elif type(other_sequence) is int:
            if other_sequence > sequence:
                later.append(other_id)
        else:
            unordered.append(other_id)
    if later:
        return LedgerDecision("contradicted", "later_dependent_subject_row", {"dependent_dispatch_ids": later})
    if unordered:
        return LedgerDecision("ledger_hold", "subject_row_order_unprovable", {"dispatch_ids": unordered})
    last_gate = state.get("last_gate")
    if state.get("terminal") or (isinstance(last_gate, dict) and last_gate.get("fixes", 0) >= state["fixes"]):
        return LedgerDecision("contradicted", "gate_state_depends_on_row")
    return LedgerDecision("eligible", "", copy.deepcopy(row))


def _append_correction(
    data: dict,
    observation: dict,
    observation_sha256: str,
    ledger_sha256_before: str,
    producer: str,
    recorded_at: str,
    dispatch_id: str,
) -> dict:
    """Append one event and refresh only the views derived from it; the row is untouched."""
    row = data["rows"][dispatch_id]
    digest = _row_digest(row)
    data["sequence"] += 1
    event = {
        "schema": CORRECTION_SCHEMA,
        "correction_id": _correction_id(dispatch_id, digest, observation_sha256),
        "dispatch_id": dispatch_id,
        "subject_branch": row["subject_branch"],
        "kind": row["kind"],
        "recorded_at": recorded_at,
        "recorded_sequence": data["sequence"],
        "original": {**{key: copy.deepcopy(row.get(key)) for key in _ORIGINAL_FIELDS}, "row_sha256": digest},
        "effective_outcome_class": PassOutcomeClass.INFRA_STOP.value,
        "observation": copy.deepcopy(observation),
        "observation_sha256": observation_sha256,
        "ledger_sha256_before": ledger_sha256_before,
        "producer": producer,
    }
    data.setdefault("corrections", []).append(event)
    state = data["subjects"][row["subject_branch"]]
    state["fixes"] = _subject_fixes(data, row["subject_branch"])
    # Same derived effect as an infra_stop settlement of the continuation.
    if state["continuation"] == dispatch_id:
        state["continuation"] = None
    return event


def correction_effects(data: dict, dispatch_id: str, policy: TrustedPolicy) -> dict:
    """Proposed accounting effect of a correction, simulated on a copy (no writes)."""
    subject = data["rows"][dispatch_id]["subject_branch"]
    after = copy.deepcopy(data)
    _append_correction(
        after, {}, _digest(_encoded({})), "0" * 64, "review_cycle_reconciliation.verified_refusal", "", dispatch_id
    )
    others = sorted(name for name in data["subjects"] if name != subject)
    return {
        "subject_branch": subject,
        "before": subject_accounting(data, subject, policy),
        "after": subject_accounting(after, subject, policy),
        "rows_unchanged": after["rows"] == data["rows"],
        "unrelated_subjects": len(others),
        "unrelated_subjects_unchanged": all(
            after["subjects"][name] == data["subjects"][name]
            and subject_accounting(after, name, policy) == subject_accounting(data, name, policy)
            for name in others
        ),
    }


class ReviewCycleLedger:
    def __init__(self, authority_path: Path, policy: TrustedPolicy):
        self.path = Path(authority_path)
        if not self.path.is_absolute():
            raise ValueError("trusted absolute ledger authority path required")
        self.policy = policy

    def _read(self) -> dict:
        try:
            # Explicit mode check also refuses unreadable files under privileged tests.
            if not self.path.stat().st_mode & 0o444:
                raise _Hold("ledger_unreadable")
            return self._decode(self.path.read_bytes())
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise _Hold("ledger_unreadable_or_corrupt") from exc

    def _decode(self, raw: bytes) -> dict:
        """Verify already-read ledger bytes exactly as ``_read`` does (no I/O)."""
        try:
            envelope = json.loads(raw)
            data = envelope["data"]
            if envelope["sha256"] != _digest(_encoded(data)):
                raise _Hold("ledger_integrity")
            if data["version"] != 1 or data["policy"] != self.policy.binding():
                raise _Hold("ledger_policy_or_version_mismatch")
            if not isinstance(data["activated_at"], str) or not data["activated_at"]:
                raise _Hold("ledger_activation_invalid")
            if not isinstance(data["rows"], dict) or not isinstance(data["subjects"], dict):
                raise _Hold("ledger_shape")
            _validate_issuance(data)
            corrections = data.get("corrections", [])
            if not isinstance(corrections, list) or not all(isinstance(event, dict) for event in corrections):
                raise _Hold("ledger_shape")
            seen = set()
            for event in corrections:
                dispatch_id = event.get("dispatch_id")
                row = data["rows"].get(dispatch_id)
                if dispatch_id in seen or row is None or not _valid_correction(event, row):
                    raise _Hold("ledger_correction_integrity")
                seen.add(dispatch_id)
            # Disposable projection: every recovery/read derives fixes from the
            # same events that reserve uses for ordinal and route selection.
            for event in corrections:
                subject = event["subject_branch"]
                data["subjects"][subject]["fixes"] = _subject_fixes(data, subject)
            return data
        except (ValueError, KeyError, TypeError) as exc:
            raise _Hold("ledger_unreadable_or_corrupt") from exc

    def _write(self, data: dict):
        raw = _encoded({"data": data, "sha256": _digest(_encoded(data))})
        # Same-directory replacement, fsync file and directory: no partial JSON.
        with tempfile.NamedTemporaryFile(dir=self.path.parent, prefix=self.path.name + ".", delete=False) as handle:
            temporary = Path(handle.name)
            try:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
                os.replace(temporary, self.path)
                directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                temporary.unlink(missing_ok=True)

    def _transaction(self, operation: Callable, *, activation: bool = False) -> LedgerDecision:
        try:
            # Stable sibling lock inode is never replaced/unlinked. Like worker_lock,
            # use kernel flock LOCK_NB; contention is an immediate typed hold.
            with self.path.with_name(self.path.name + ".lock").open("a+b") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return LedgerDecision("ledger_hold", "ledger_busy")
                try:
                    if activation and not self.path.exists():
                        from datetime import datetime, timezone

                        data = {
                            "version": 1,
                            "activated_at": datetime.now(timezone.utc).isoformat(),
                            "policy": self.policy.binding(),
                            "rows": {},
                            "subjects": {},
                            "sequence": 0,
                        }
                    else:
                        data = self._read()
                    result = operation(data)
                    self._write(data)
                    return result
                finally:
                    fcntl.flock(lock, fcntl.LOCK_UN)
        except _Unchanged as exc:
            return exc.decision
        except _Hold as exc:
            return LedgerDecision(
                "ledger_hold", str(exc), {"required_host_evidence": exc.needed} if hasattr(exc, "needed") else None
            )
        except (OSError, ValueError, KeyError, TypeError) as exc:
            return LedgerDecision("ledger_hold", type(exc).__name__)

    def activate(self) -> LedgerDecision:
        """Explicit rollout anchor; never import lifetime or review_runs counts."""
        return self._transaction(lambda data: LedgerDecision("activated", data["activated_at"]), activation=True)

    @staticmethod
    def _subject(data: dict, subject: str) -> dict:
        return data["subjects"].setdefault(
            subject,
            {
                "fixes": 0,
                "last_gate": None,
                "repeated": False,
                "continuation": None,
                "terminal": False,
            },
        )

    def _proposal(self, path: Path | None, rows: list[dict]) -> dict | None:
        if path is None:
            return None
        try:
            raw = Path(path).read_bytes()
            proposal = json.loads(raw)
            rules = proposal["rule_ids"]
            change = proposal["change_from_previous"]
            if (
                not isinstance(rules, list)
                or not rules
                or not all(isinstance(rule, str) and rule in self.policy.rule_ids for rule in rules)
                or not isinstance(change, str)
                or not change.strip()
            ):
                return None
            correction = " ".join(change.casefold().split())
            if any(row.get("proposal", {}).get("correction") == correction for row in rows):
                return None
            return {
                "path": str(path),
                "sha256": _digest(raw),
                "evidence": raw.decode(),
                "rule_ids": sorted(set(rules)),
                "correction": correction,
                "canon_version": self.policy.canon_version,
                "manifest_digest": self.policy.manifest_digest,
            }
        except (OSError, ValueError, KeyError, TypeError, UnicodeError):
            return None

    def reserve(
        self, dispatch_id: str, subject_branch: str, kind: str, *, canon_proposal_path: Path | None = None
    ) -> LedgerDecision:
        """Atomically reserve prospective verdict budget before any remote spawn."""
        if not dispatch_id or not subject_branch or kind not in KINDS:
            return LedgerDecision("ledger_hold", "invalid_identity")

        def operation(data):
            old = data["rows"].get(dispatch_id)
            if old is not None:
                if (old["subject_branch"], old["kind"]) != (subject_branch, kind):
                    return LedgerDecision("ledger_hold", "dispatch_identity_mismatch")
                return LedgerDecision(old["decision"], "replay", copy.deepcopy(old))
            state = self._subject(data, subject_branch)
            rows = [row for row in data["rows"].values() if row["subject_branch"] == subject_branch]
            charged = [
                row
                for row in rows
                if row["kind"] == kind
                and row["decision"] == "reserved"
                and effective_outcome_class(data, row) != PassOutcomeClass.INFRA_STOP.value
            ]
            number = len(charged) + 1
            proposal = None
            status, reason = "reserved", ""
            if state["terminal"] or (kind == "gate" and number > 3):
                status, reason = "review_budget_exhausted", "no_fourth_automatic_gate"
            elif kind == "analysis" and number > 3:
                status, reason = "cycle_budget_stop", "no_fourth_automatic_analysis"
            elif kind == "gate" and any(
                row["kind"] == "gate"
                and row["decision"] == "reserved"
                and row["outcome_class"] == PassOutcomeClass.PENDING.value
                for row in rows
            ):
                status, reason = "ledger_hold", "gate_judgment_pending"
            elif state["repeated"] and state["continuation"] is not None and kind == "implement":
                status, reason = "cycle_budget_stop", "continuation_awaits_gate"
            elif kind == "implement" and (number >= self.policy.dispatch_stop_at or state["repeated"]):
                proposal = self._proposal(canon_proposal_path, rows)
                if proposal is None:
                    status, reason = "cycle_budget_stop", "new_verified_canon_correction_required"
            elif kind == "gate" and state["repeated"]:
                continuation = data["rows"].get(state["continuation"])
                if (
                    continuation is None
                    or effective_outcome_class(data, continuation) != PassOutcomeClass.VERDICT.value
                ):
                    status, reason = "cycle_budget_stop", "repeated_blocking_requires_source_correction"
            # Native issuance order: assigned once here, in the same atomic
            # write as the row, and never by replay/settle/correction (DATA-14).
            data["issuance_sequence"] = data.get("issuance_sequence", 0) + 1
            row = {
                "dispatch_id": dispatch_id,
                "subject_branch": subject_branch,
                "kind": kind,
                "decision": status,
                "reason": reason,
                "round": number,
                "outcome_class": PassOutcomeClass.PENDING.value,
                "completion": None,
                "proposal": proposal or {},
                "loud_decision": proposal is not None,
                "required_route": _required_route(self.policy, kind, number),
                "listed_open_only": kind == "gate" and number == 3,
                "listed_blocking_ids": (state["last_gate"] or {}).get("blocking_ids", []),
                "activated_at": data["activated_at"],
                "issued_sequence": data["issuance_sequence"],
            }
            data["rows"][dispatch_id] = row
            if status == "reserved" and kind == "implement" and state["repeated"]:
                state["continuation"] = dispatch_id
            return LedgerDecision(status, reason, copy.deepcopy(row))

        return self._transaction(operation)

    def settle(
        self,
        dispatch_id: str,
        subject_branch: str,
        kind: str,
        outcome: str,
        *,
        refusal_reason: str | None = None,
        blocking_ids: tuple[str, ...] | None = None,
        blocking_fingerprints: tuple[str, ...] | None = None,
    ) -> LedgerDecision:
        """Settle actual D2 outcome; harvest supplies canonical, classified blockers.

        Gate verdicts must explicitly provide both canonical ID and fingerprint
        sets (empty means clear). A source verdict denotes a completed fix pass.
        Infra stops and pending outcomes never mutate gate/repeated-state history.
        """
        classification = (
            classify_pass_outcome(outcome, refusal_reason=refusal_reason) if isinstance(outcome, str) else None
        )
        if (
            kind == "gate"
            and classification == PassOutcomeClass.VERDICT
            and (blocking_ids is None or blocking_fingerprints is None)
        ):
            return LedgerDecision("ledger_hold", "gate_harvest_required")
        blocking_ids = () if blocking_ids is None else blocking_ids
        blocking_fingerprints = () if blocking_fingerprints is None else blocking_fingerprints
        if (
            not isinstance(outcome, str)
            or not outcome
            or kind not in KINDS
            or not all(isinstance(value, str) and value for value in (*blocking_ids, *blocking_fingerprints))
            or bool(blocking_ids) != bool(blocking_fingerprints)
        ):
            return LedgerDecision("ledger_hold", "invalid_completion")
        completion = {
            "outcome": outcome,
            "refusal_reason": refusal_reason,
            "blocking_ids": sorted(set(blocking_ids)),
            "blocking_fingerprints": sorted(set(blocking_fingerprints)),
        }

        def operation(data):
            row = data["rows"].get(dispatch_id)
            if row is None or (row["subject_branch"], row["kind"]) != (subject_branch, kind):
                return LedgerDecision("ledger_hold", "dispatch_identity_mismatch")
            if row["decision"] != "reserved":
                return LedgerDecision("ledger_hold", "dispatch_was_not_reserved")
            if row["completion"] is not None:
                if row["completion"] != completion:
                    return LedgerDecision("ledger_hold", "dispatch_outcome_mismatch")
                return LedgerDecision(row["settlement_status"], "replay", copy.deepcopy(row))
            if classification == PassOutcomeClass.PENDING:
                return LedgerDecision("pending", "actual_outcome_required", copy.deepcopy(row))
            row["completion"] = completion
            row["outcome_class"] = classification.value
            data["sequence"] += 1
            row["settled_sequence"] = data["sequence"]
            state = self._subject(data, subject_branch)
            status = "settled"
            if classification == PassOutcomeClass.INFRA_STOP and state["continuation"] == dispatch_id:
                state["continuation"] = None
            # An analysis verdict is charged on its own row only.
            if classification == PassOutcomeClass.VERDICT:
                if kind == "implement":
                    state["fixes"] = _subject_fixes(data, subject_branch)
                elif kind == "gate":
                    previous = state["last_gate"]
                    repeated = (
                        previous is not None
                        and state["fixes"] > previous["fixes"]
                        and bool(set(previous["blocking_fingerprints"]) & set(completion["blocking_fingerprints"]))
                    )
                    state["last_gate"] = {**completion, "fixes": state["fixes"], "dispatch_id": dispatch_id}
                    state["repeated"] = bool(repeated)
                    state["continuation"] = None
                    if row["round"] == 3 and completion["blocking_ids"]:
                        state["terminal"] = True
                        status = "review_budget_exhausted"
                    elif repeated:
                        status = "cycle_budget_stop"
            row["settlement_status"] = status
            return LedgerDecision(status, "", copy.deepcopy(row))

        return self._transaction(operation)

    def settle_unjudged(self, dispatch_id: str, *, authenticate: Callable) -> LedgerDecision:
        """Native-only charged INCOMPLETE completion; no gate judgment effects.

        The producer is re-read under the ledger transaction (CON-11). No proof
        dictionary supplied by the public caller can authorize this transition.
        """
        def operation(data):
            row = data["rows"].get(dispatch_id)
            if row is None or row["kind"] != "gate" or row["decision"] != "reserved":
                return LedgerDecision("ledger_hold", "unjudged_reservation_required")
            evidence = authenticate(copy.deepcopy(row))
            completion = dict(outcome="review_incomplete", refusal_reason=None,
                              blocking_ids=[], blocking_fingerprints=[], judgment="incomplete",
                              usage="UNKNOWN", evidence=evidence)
            if row["completion"] is not None:
                if row["completion"] != completion:
                    return LedgerDecision("ledger_hold", "dispatch_outcome_mismatch")
                raise _Unchanged(LedgerDecision("settled", "replay", copy.deepcopy(row)))
            row.update(completion=completion, outcome_class=PassOutcomeClass.VERDICT.value,
                       settlement_status="settled")
            data["sequence"] += 1
            row["settled_sequence"] = data["sequence"]
            return LedgerDecision("settled", "unjudged_incomplete", copy.deepcopy(row))
        return self._transaction(operation)

    def append_outcome_correction(
        self, *, dispatch_id: str, expected_ledger_sha256: str, expected_observation_sha256: str, authenticate: Callable
    ) -> LedgerDecision:
        """Host-only transaction. Reopen producer evidence under the native NB lock.

        ``authenticate`` is an internal producer, never deserialized input. The
        public reconciler accepts only dispatch identity and two CAS digests.
        """

        def operation(data):
            # Replays still reauthenticate the original evidence; an old preview
            # is an idempotency key, never a continuing authorization.
            observation = authenticate(data)
            digest = _digest(_encoded(observation))
            events = [e for e in data.get("corrections", []) if e["dispatch_id"] == dispatch_id]
            if events:
                event = events[0]
                if (
                    event["observation_sha256"] == expected_observation_sha256 == digest
                    and event["ledger_sha256_before"] == expected_ledger_sha256
                ):
                    raise _Unchanged(LedgerDecision("already_corrected", "replay", copy.deepcopy(event)))
                raise _Unchanged(LedgerDecision("ledger_hold", "correction_conflict"))
            if _digest(self.path.read_bytes()) != expected_ledger_sha256:
                raise _Unchanged(LedgerDecision("ledger_hold", "stale_ledger"))
            if digest != expected_observation_sha256:
                raise _Unchanged(LedgerDecision("ledger_hold", "stale_observation"))
            eligible = correction_eligibility(data, dispatch_id)
            if eligible.status != "eligible":
                raise _Unchanged(eligible)
            event = _append_correction(
                data,
                observation,
                digest,
                expected_ledger_sha256,
                "review_cycle_reconciliation.verified_refusal",
                datetime.now(timezone.utc).isoformat(),
                dispatch_id,
            )
            return LedgerDecision("corrected", "", copy.deepcopy(event))

        return self._transaction(operation)

    def import_legacy_sources(self, subject_branch: str, history: object) -> LedgerDecision:
        """Idempotently carry verified pre-activation source calls into a subject.

        ``history`` is the A17 producer's ``list[dict]``, never a caller count:
        ``[]`` is truly new work, and each record is one actual charged source
        with its original dispatch ID, stored route, outcome, Git identity and
        provenance locators, in the producer's verified (started_at,
        dispatch_id) order. Rounds derive from that order. Gates, pending or
        refunded calls and unrelated reviews are not imported. The same
        verified prefix replays after later native rows; any other difference,
        native history or reuse of an original ID/nonce is a typed conflict.
        Existing rows are never rewritten or reset.
        """
        if not subject_branch or not isinstance(history, list):
            return LedgerDecision("ledger_hold", "legacy_source_history_invalid")
        if not history:
            return LedgerDecision("not_applicable", "no_legacy_source_history")
        records, nonces, previous = [], set(), None
        for source in history:
            order = _legacy_order(source, subject_branch)
            if (
                order is None
                or (previous is not None and order <= previous)
                or (source["producer"]["dispatch_nonce"] in nonces)
            ):
                return LedgerDecision("ledger_hold", "legacy_source_history_invalid")
            previous = order
            nonces.add(source["producer"]["dispatch_nonce"])
            records.append(copy.deepcopy(source))
        imported = {"subject_branch": subject_branch, "dispatch_ids": [record["dispatch_id"] for record in records]}

        def operation(data):
            rows = list(data["rows"].values())
            prior = _imported([row for row in rows if row["subject_branch"] == subject_branch])
            if prior:
                if [row["legacy_source"]["record"] for row in prior] == records:
                    return LedgerDecision("imported", "replay", imported)
                raise _Hold("legacy_source_history_conflict")
            # Native history for the subject, or an original ID or provenance
            # nonce already held elsewhere, means another authority owns it.
            held = {
                row["legacy_source"]["record"]["producer"]["dispatch_nonce"] for row in rows if row.get("legacy_source")
            }
            if (
                any(row["subject_branch"] == subject_branch for row in rows)
                or any(record["dispatch_id"] in data["rows"] for record in records)
                or held & nonces
            ):
                raise _Hold("legacy_source_history_conflict")
            state = self._subject(data, subject_branch)
            for number, record in enumerate(records, 1):
                data["sequence"] += 1
                data["rows"][record["dispatch_id"]] = {
                    "dispatch_id": record["dispatch_id"],
                    "subject_branch": subject_branch,
                    "kind": "implement",
                    "decision": "reserved",
                    "reason": "",
                    "round": number,
                    "outcome_class": PassOutcomeClass.VERDICT.value,
                    "completion": {
                        "outcome": record["outcome"],
                        "refusal_reason": record.get("refusal_reason"),
                        "blocking_ids": [],
                        "blocking_fingerprints": [],
                    },
                    "proposal": {},
                    "loud_decision": False,
                    "required_route": dict(record["required_route"]),
                    "listed_open_only": False,
                    "listed_blocking_ids": [],
                    "activated_at": data["activated_at"],
                    "settled_sequence": data["sequence"],
                    "settlement_status": "settled",
                    "legacy_source": {"producer": LEGACY_PRODUCER, "record": record},
                }
                state["fixes"] = _subject_fixes(data, subject_branch)
            return LedgerDecision("imported", "", imported)

        return self._transaction(operation)


def import_legacy_source_history(repo: Path, ledger: ReviewCycleLedger, subject_branch: str) -> LedgerDecision:
    """Named B18 migration adapter over the A17 verified lineage producer.

    A subject that already has rows is answered from this ledger without a
    legacy scan: a native subject is ``not_applicable`` and an imported prefix
    replays, however many native calls followed it. Only a subject without
    rows reads the producer, which is late-bound and never replaced by a local
    guess; its absence or refusal is a typed hold, never a fresh bucket.
    """
    try:
        rows = [row for row in ledger._read()["rows"].values() if row["subject_branch"] == subject_branch]
    except _Hold as exc:
        return LedgerDecision("ledger_hold", str(exc))
    except (KeyError, TypeError, AttributeError):
        return LedgerDecision("ledger_hold", "ledger_shape")
    if rows:
        prior = _imported(rows)
        if not prior:
            return LedgerDecision("not_applicable", "native_ledger_history")
        return LedgerDecision(
            "imported",
            "replay",
            {"subject_branch": subject_branch, "dispatch_ids": [row["dispatch_id"] for row in prior]},
        )
    try:
        from workbay_orchestrator_mcp.orchestration.review_ratchet import verified_legacy_source_history
    except ImportError:
        return LedgerDecision("ledger_hold", "legacy_source_history_producer_unavailable")
    try:
        history = verified_legacy_source_history(Path(repo), subject_branch)
    except Exception as exc:  # any producer failure holds known work; it never proves new work
        return LedgerDecision(
            "ledger_hold",
            "legacy_source_history_unavailable",
            {"subject_branch": subject_branch, "producer_reason": str(exc) or type(exc).__name__},
        )
    return ledger.import_legacy_sources(subject_branch, history)
