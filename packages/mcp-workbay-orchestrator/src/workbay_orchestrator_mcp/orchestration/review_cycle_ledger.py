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

D1 and release0173 remain integration prerequisites. No handoff DB/history reads,
model spawning, severity classification, or live infinite-loop enforcement here.
"""

from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

from workbay_orchestrator_mcp.orchestration.pass_outcome_class import PassOutcomeClass, classify_pass_outcome


def _encoded(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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
            envelope = json.loads(self.path.read_bytes())
            data = envelope["data"]
            if envelope["sha256"] != _digest(_encoded(data)):
                raise _Hold("ledger_integrity")
            if data["version"] != 1 or data["policy"] != self.policy.binding():
                raise _Hold("ledger_policy_or_version_mismatch")
            if not isinstance(data["activated_at"], str) or not data["activated_at"]:
                raise _Hold("ledger_activation_invalid")
            if not isinstance(data["rows"], dict) or not isinstance(data["subjects"], dict):
                raise _Hold("ledger_shape")
            return data
        except (OSError, ValueError, KeyError, TypeError) as exc:
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
        except _Hold as exc:
            return LedgerDecision("ledger_hold", str(exc))
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
        if not dispatch_id or not subject_branch or kind not in {"implement", "gate"}:
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
                and row["outcome_class"] != PassOutcomeClass.INFRA_STOP.value
            ]
            number = len(charged) + 1
            proposal = None
            status, reason = "reserved", ""
            if state["terminal"] or (kind == "gate" and number > 3):
                status, reason = "review_budget_exhausted", "no_fourth_automatic_gate"
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
                if continuation is None or continuation["outcome_class"] != PassOutcomeClass.VERDICT.value:
                    status, reason = "cycle_budget_stop", "repeated_blocking_requires_source_correction"
            model, effort = "gpt-6.1-sol", "xhigh"
            if kind == "implement":
                if number == 1:
                    effort = "low"
                elif number % 2 == 0 and not self.policy.claude_exhausted:
                    model = "opus-5.5"
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
                "required_route": {"model": model, "effort": effort},
                "listed_open_only": kind == "gate" and number == 3,
                "listed_blocking_ids": (state["last_gate"] or {}).get("blocking_ids", []),
                "activated_at": data["activated_at"],
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
            or kind not in {"implement", "gate"}
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
            if classification == PassOutcomeClass.VERDICT:
                if kind == "implement":
                    state["fixes"] += 1
                else:
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
