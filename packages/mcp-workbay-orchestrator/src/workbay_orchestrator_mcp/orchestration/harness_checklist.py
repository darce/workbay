"""Display-only harness-checklist-v1 reducer over captured, read-only gate facts.

The caller resolves an exact subject and supplies observations captured by trusted
readers under its *existing* fleet deadline (absolute ``time.monotonic()``).
This module has no source callbacks, gate invocation, database initialization,
receipt-path opener, refresh, cache writer, or lane-land verdict API. Omitted
observations mean no recorded measurement, not an instruction to collect one.
``Observation.value`` is a gate-specific measured result, never an agent report
or arbitrary serialized receipt; e.g. ``landing_receipt`` is the independently
verified result of L/LR, bound to the integration ref, not a receipt-path label.
Callers must not deserialize claims into this trusted observation channel.

Contract: docs/contracts/harness-checklist-v1.md (decision 40771).
Canon 0.26.2, supplied INPUT/canon-rows.md:
* **Architecture hoisting** [ARCH-13], lexicons/engineering.md#arch-13,
  Just Enough Software Architecture, ch. 2: disjoint typed inputs and runtime
  checks; observers never receive claims or judgments.
* **Independent watcher the optimizer cannot tune** [OBS-09],
  lexicons/engineering.md#obs-09, Thinking in Systems, ch. 6: existing stored
  test records cannot authenticate their writer, regardless of supplied labels.
* **Evidence before label** [HAI-01], lexicons/interaction-ux.md#hai-01,
  Designing AI Interfaces and Guidelines for Human-AI Interaction: preserve
  measured bindings, scope, references and original observation time.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable

_CONTRACT = "docs/contracts/harness-checklist-v1.md"
_SHA = re.compile(r"[0-9a-f]{40}\Z")
_STATES = ("observed_pass", "observed_fail", "not_observed", "unknown", "no_observer")
_PAUSES = {"pass_terminal", "before_lane_land", "close", "on_demand"}
_COVERAGE = {
    "inventory_predicates": 25,
    "inventory_entries": 26,
    "included_entries": 4,
    "excluded_entries": 22,
    "incomplete_observer_predicates": 20,
}


@dataclass(frozen=True)
class Subject:
    """Exact identity and chart state from a trusted subject lookup, not status prose."""

    task_ref: str
    lane_id: str | None
    current_state: str | None
    tip_sha: str | None
    base_sha: str | None
    integration_ref: str | None = None
    commands: tuple[str, ...] = ()


@dataclass(frozen=True)
class Claim:
    kind: str
    source_id: str
    text: str
    claimed_at: str | None = None
    evidence_ref: str | None = None


@dataclass(frozen=True)
class Judgment:
    kind: str
    decision_id: int
    decision_exists: bool | None
    observed_at: str | None = None
    observed_by: str | None = None
    evidence_ref: str | None = None


def _freeze(value: Any, depth: int = 0) -> Any:
    if depth > 8:
        raise TypeError("observation data nesting exceeds capture bound")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("observation data keys must be strings")
        return MappingProxyType({key: _freeze(item, depth + 1) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item, depth + 1) for item in value)
    if value is None or type(value) in (str, bool, int):
        return value
    raise TypeError("observation data must contain JSON scalars")


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    return value


@dataclass(frozen=True)
class Evidence:
    task_ref: str
    lane_id: str | None
    sha: str | None
    base_sha: str | None
    command: str | None
    parameters: Mapping[str, Any]
    observed_at: str | None
    verified_test_id: int | None = None
    findings_count: int | None = None
    receipt_path: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.parameters, Mapping):
            raise TypeError("Evidence parameters must be a mapping")
        object.__setattr__(self, "parameters", _freeze(self.parameters))


@dataclass(frozen=True)
class Observation:
    """A captured reader outcome, with no generic row state or claim promotion API.

    Status is present/absent/unreadable/malformed/timed_out/over_budget. Source
    failures remain distinct from a completed lookup that found no record.
    ``value`` holds only the gate-local measured facts consumed by its observer.
    """

    key: str
    status: str
    evidence: Evidence
    value: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if type(self.evidence) is not Evidence:
            raise TypeError("Observation requires Evidence")
        if not isinstance(self.value, Mapping):
            raise TypeError("Observation value must be a mapping")
        object.__setattr__(self, "value", _freeze(self.value))


@dataclass(frozen=True)
class Observations:
    records: tuple[Observation, ...] = ()

    def __post_init__(self) -> None:
        if type(self.records) is not tuple or any(type(item) is not Observation for item in self.records):
            raise TypeError("Observations accepts only captured Observation records")


@dataclass(frozen=True)
class RegistryEntry:
    owners: tuple[str, ...]
    sources: tuple[str, ...]
    contract_row: str
    limit: str | None
    observer_name: str

    @property
    def observer(self) -> Callable[..., dict[str, Any]]:
        return globals()[self.observer_name]


@dataclass(frozen=True)
class Exclusion:
    reason: str
    contract_row: str
    chart_locator: str


# Owner names and anchors are contract data, not executable gate imports.
REGISTRY = MappingProxyType(
    {
        ("slice_gate_verdict", "findings_count"): RegistryEntry(
            ("plan_cursors._evaluate_clean_slice_gate",),
            ("SG", "C:97"),
            f"{_CONTRACT}:77",
            "observer_partial",
            "_observe_slice",
        ),
        ("lane_tests_green", "test_exit_code"): RegistryEntry(
            ("lane_land.land", "lane_ready_set._content_gate_refusal", "landing_gate_policy.gate_report"),
            ("L", "LP", "CG", "GR", "C:209"),
            f"{_CONTRACT}:78",
            "observer_partial",
            "_observe_tests",
        ),
        ("lane_merged_to_task_branch", "commit_ancestry"): RegistryEntry(
            ("lane_land.land", "lane_land._find_natural_landing"),
            ("L", "LR", "LC", "LP", "C:219"),
            f"{_CONTRACT}:79",
            None,
            "_observe_ancestry",
        ),
        ("fresh_full_battery_qualified", "machine_observation"): RegistryEntry(
            ("validation_battery.run_battery",),
            ("VB", "C:339"),
            f"{_CONTRACT}:80",
            "observer_over_budget",
            "_observe_battery",
        ),
    }
)

# The second class of lane_merged_to_task_branch remains excluded: ancestry
# does not establish the gated edge's additional custody requirements.
EXCLUDED = MappingProxyType(
    {
        ("scope_accepted", "findings_count"): Exclusion(
            "`no_observer`: no evaluator ties scope admission to absence of blocking findings; slice counts are not scope admission.",
            f"{_CONTRACT}:107",
            "C:54",
        ),
        ("feature_branch_created", "machine_observation"): Exclusion(
            "`fits`, but provisioning is outside this gate surface. `_branch_exists_on_disk` at `O/orchestration/lane_provision.py:181` uses show-ref; a failed probe is not proven absence.",
            f"{_CONTRACT}:108",
            "C:64",
        ),
        ("slice_admitted", "commit_ancestry"): Exclusion(
            "`fits`, but dispatch admission is outside this gate surface. `lane_dependency_satisfied` at `O/orchestration/orchestrator_lanes.py:3387` includes dependent-base and vacuous discharge alternatives.",
            f"{_CONTRACT}:109",
            "C:74",
        ),
        ("slice_work_ready_for_review", "test_exit_code"): Exclusion(
            "`no_observer`: slice-start runs one test, not the entire chart suite; lane-land accepts any passing row at tip, not the specified suite.",
            f"{_CONTRACT}:110",
            "C:84",
        ),
        ("all_lanes_terminal", "machine_terminal_outcome"): Exclusion(
            "`no_observer`: no aggregate evaluator proves every lane terminal; review-ready is not child-terminal proof.",
            f"{_CONTRACT}:111",
            "C:112",
        ),
        ("close_check_clean", "findings_count"): Exclusion(
            "`too_slow`: close-check calls review-ready, handoff augmentation, mergeability and subprocess probes; broader conditions and no verified-test write.",
            f"{_CONTRACT}:112",
            "C:127",
        ),
        ("merge_landed_on_main", "commit_ancestry"): Exclusion(
            "`no_observer`: generic ancestry and lane receipts do not jointly prove task change, main merge/squash and recorded landed SHA.",
            f"{_CONTRACT}:113",
            "C:140",
        ),
        ("upstream_landed_ancestry", "commit_ancestry"): Exclusion(
            "`fits`, but dispatch dependency admission is outside this gate surface. `lane_dependency_satisfied` at `O/orchestration/orchestrator_lanes.py:3387` includes dependent-base and vacuous discharge alternatives.",
            f"{_CONTRACT}:114",
            "C:173",
        ),
        ("collision_freedom", "machine_terminal_outcome"): Exclusion(
            "`fits`, but manifest admission is outside this gate surface. `_validate_owned_path_collision_freedom` at `O/orchestration/lane_manifest.py:833` permits explicit conflict edges with distinct colours; not a live recheck.",
            f"{_CONTRACT}:115",
            "C:178",
        ),
        ("lane_blocked_observation", "machine_observation"): Exclusion(
            "`no_observer`: no evaluator for the broad worker/preflight cause; lane status does not prove its cause.",
            f"{_CONTRACT}:116",
            "C:189",
        ),
        ("blocker_cleared", "findings_count"): Exclusion(
            "`no_observer`: no evaluator binds resolved blocking findings to resumption; slice HIGH counts are narrower.",
            f"{_CONTRACT}:117",
            "C:199",
        ),
        ("review_findings_reopen_work", "findings_count"): Exclusion(
            "`no_observer`: generic finding counts do not establish a decision to reopen work.",
            f"{_CONTRACT}:118",
            "C:229",
        ),
        ("lane_vacuous_close", "machine_terminal_outcome"): Exclusion(
            "`no_observer`: dependency no-work discharge does not decide the planned-to-closed transition.",
            f"{_CONTRACT}:119",
            "C:239",
        ),
        ("lane_abandoned", "machine_terminal_outcome"): Exclusion(
            "`no_observer`: no evaluator for an operator/policy decision to close a blocked lane.",
            f"{_CONTRACT}:120",
            "C:249",
        ),
        ("lane_cancelled", "machine_terminal_outcome"): Exclusion(
            "`no_observer`: no evaluator for cancellation with no landed commit.", f"{_CONTRACT}:121", "C:259"
        ),
        ("lane_review_rejected_close", "findings_count"): Exclusion(
            "`no_observer`: no evaluator binds review rejection to lane closure.", f"{_CONTRACT}:122", "C:269"
        ),
        ("lane_proven_stale", "machine_terminal_outcome"): Exclusion(
            "`no_observer`: stale-close handles only absent-branch cases, requires supplied liveness/process probes and mutates on CAS; not a complete observer of every stale reason.",
            f"{_CONTRACT}:123",
            "C:279",
        ),
        ("stale_lane_reopened", "machine_observation"): Exclusion(
            "`no_observer`: update_lane is a writer, not a read-only evaluator of reopen cause.",
            f"{_CONTRACT}:124",
            "C:319",
        ),
        ("tip_moved_after_close", "machine_observation"): Exclusion(
            "`no_observer`: reaper moved-row handling is an action path, not a complete independent observer.",
            f"{_CONTRACT}:125",
            "C:329",
        ),
        ("strict_review_and_holds_satisfied", "machine_observation"): Exclusion(
            "`too_slow`: finish_native may run the battery and publishes a milestone; review/gate/custody/findings checks are not a read-only snapshot call. Decision existence is shown separately as judgment.",
            f"{_CONTRACT}:126",
            "C:369",
        ),
        ("lane_merged_to_task_branch", "machine_observation"): Exclusion(
            "`no_observer`: the gated edge also requires custody checks; the ancestry receipt alone is partial for this class.",
            f"{_CONTRACT}:127",
            "C:379",
        ),
        ("validation_invalidated", "machine_observation"): Exclusion(
            "`no_observer`: policy registration mutates a milestone and does not evaluate all work/recovery/cancellation cases.",
            f"{_CONTRACT}:128",
            "C:389",
        ),
    }
)


def _guards(transition: Mapping[str, Any]) -> list[dict[str, Any]]:
    return transition.get("guards", [transition["guard"]] if "guard" in transition else [])


def validate_registry(chart: Mapping[str, Any]) -> None:
    """Fail closed for new/unclassified guards, including a new evidence class."""
    pairs = {
        (guard["predicate_id"], guard["evidence_class"])
        for machine in ("outer", "child")
        for transition in chart[machine]["transitions"]
        for guard in _guards(transition)
    }
    expected = set(REGISTRY) | set(EXCLUDED)
    if pairs != expected:
        raise ValueError(f"unclassified or missing chart guards: {sorted(pairs ^ expected)!r}")


def _timestamp(value: str | None) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None


def _freshness(ev: Evidence, subject: Subject, now: datetime, *, base_required: bool) -> tuple[str, str | None]:
    if any(
        value is not None and (not isinstance(value, str) or not _SHA.fullmatch(value))
        for value in (ev.sha, ev.base_sha)
    ):
        return "unknown", "evidence_malformed"
    observed = _timestamp(ev.observed_at)
    if observed is None or observed > now:
        return "unknown", "timestamp_invalid"
    if (
        (ev.sha is not None and subject.tip_sha is not None and ev.sha != subject.tip_sha)
        or (
            base_required
            and ev.base_sha is not None
            and subject.base_sha is not None
            and ev.base_sha != subject.base_sha
        )
        or (now - observed).total_seconds() > 60
    ):
        return "stale", "stale_evidence"
    if (
        ev.sha is None
        or subject.tip_sha is None
        or (base_required and (ev.base_sha is None or subject.base_sha is None))
    ):
        return "unknown", "binding_unavailable"
    return "current", None


def _positive_id(value: Any) -> bool:
    return type(value) is int and value > 0


def _evidence(ev: Evidence, subject: Subject, freshness: str, owner: str, *, test: bool) -> dict[str, Any]:
    observed = _timestamp(ev.observed_at)
    return {
        "sha": ev.sha if isinstance(ev.sha, str) and _SHA.fullmatch(ev.sha) else None,
        "base_sha": ev.base_sha if isinstance(ev.base_sha, str) and _SHA.fullmatch(ev.base_sha) else None,
        "verified_test_id": ev.verified_test_id if _positive_id(ev.verified_test_id) else None,
        "findings_count": ev.findings_count if type(ev.findings_count) is int and ev.findings_count >= 0 else None,
        "receipt_path": ev.receipt_path,
        "observed_at": observed.isoformat().replace("+00:00", "Z") if observed else None,
        "observed_by": None
        if test
        else (
            owner if owner in {"lane_land._find_natural_landing", "plan_cursors._evaluate_clean_slice_gate"} else None
        ),
        "writer_provenance": "unproven" if test else "not_applicable",
        "scope": {
            "task_ref": ev.task_ref,
            "lane_id": ev.lane_id,
            "command": ev.command,
            "parameters": _plain(ev.parameters),
        },
        "binding": {"current_tip_sha": subject.tip_sha, "current_base_sha": subject.base_sha, "freshness": freshness},
    }


# Fixed gate-local keys prevent client-supplied owner names or generic pass flags
# from selecting an evaluator. No key creates a new chart predicate.
_CHECKS = {
    "open_high_findings": ("plan_cursors._evaluate_clean_slice_gate", False, False),
    "recent_tests": ("plan_cursors._evaluate_clean_slice_gate", True, False),
    "verified_test_at_tip": ("lane_land.land", True, False),
    "content_gate": ("lane_ready_set._content_gate_refusal", False, True),
    "landing_report": ("landing_gate_policy.gate_report", False, True),
    "lane_identity": ("lane_land.land", False, False),
    "integration_target": ("lane_land.land", False, True),
    "merge_tree": ("lane_land.land", False, True),
    "landing_receipt": ("lane_land._find_natural_landing", False, True),
    "execution_receipt": ("validation_battery.run_battery", False, True),
}


def _check(key: str, batch: Observations, subject: Subject, now: datetime, deadline: float) -> dict[str, Any]:
    owner, test, base_required = _CHECKS[key]
    check: dict[str, Any] = {
        "key": key,
        "owner": owner,
        "state": "not_observed",
        "reason": "evidence_absent",
        "evidence": None,
    }
    matches = []
    for item in batch.records:
        if time.monotonic() >= deadline:
            return {**check, "state": "unknown", "reason": "observer_timeout"}
        if item.key == key:
            matches.append(item)
            if len(matches) > 1:
                break
    if time.monotonic() >= deadline:
        return {**check, "state": "unknown", "reason": "observer_timeout"}
    if not matches:
        return check
    if len(matches) != 1:
        return {**check, "state": "unknown", "reason": "evidence_conflicting"}
    item = matches[0]
    statuses = {
        "absent": ("not_observed", "evidence_absent"),
        "unreadable": ("unknown", "evidence_unreadable"),
        "malformed": ("unknown", "evidence_malformed"),
        "timed_out": ("unknown", "observer_timeout"),
        "over_budget": ("no_observer", "observer_over_budget"),
    }
    if item.status != "present":
        state, reason = statuses.get(item.status, ("unknown", "evidence_malformed"))
        return {**check, "state": state, "reason": reason}
    ev = item.evidence
    freshness, problem = _freshness(ev, subject, now, base_required=base_required)
    has_test = test and (key != "recent_tests" or (type(item.value.get("count")) is int and item.value["count"] > 0))
    check["evidence"] = _evidence(ev, subject, freshness, owner, test=has_test)
    if key == "content_gate" and isinstance(item.value.get("refusal"), str):
        check["evidence"]["refusal"] = item.value["refusal"]
    # A stored test dependency cannot authenticate itself, even by providing a
    # proven label, pass ID, reader name, session, imported event or actor.
    if has_test:
        return {**check, "state": "unknown", "reason": "writer_provenance_unavailable"}
    expected_lane = None if key == "recent_tests" else subject.lane_id
    if ev.task_ref != subject.task_ref or ev.lane_id != expected_lane:
        return {**check, "state": "unknown", "reason": "scope_mismatch"}
    if ev.command is not None and subject.commands and ev.command not in subject.commands:
        return {**check, "state": "unknown", "reason": "scope_mismatch"}
    if (ev.verified_test_id is not None and not _positive_id(ev.verified_test_id)) or (
        ev.findings_count is not None and (type(ev.findings_count) is not int or ev.findings_count < 0)
    ):
        return {**check, "state": "unknown", "reason": "evidence_malformed"}
    if problem:
        return {**check, "state": "not_observed" if freshness == "stale" else "unknown", "reason": problem}
    value = item.value
    if key == "recent_tests":
        if type(value.get("count")) is int and value["count"] == 0:
            return {**check, "state": "not_observed", "reason": "evidence_absent"}
        return {**check, "state": "unknown", "reason": "evidence_malformed"}
    if key == "landing_receipt":
        established = (
            value.get("verified") is True
            and isinstance(value.get("commit"), str)
            and _SHA.fullmatch(value["commit"]) is not None
            and subject.integration_ref is not None
            and value.get("integration_ref") == subject.integration_ref
        )
    elif key == "open_high_findings":
        count = value.get("count")
        valid = (
            type(count) is int
            and count >= 0
            and ev.findings_count == count
            and ev.parameters.get("severity") == "high"
            and ev.parameters.get("status") == "open"
        )
        if valid:
            return {**check, "state": "observed_pass" if count == 0 else "observed_fail", "reason": "gate_observed"}
        established = False
    elif key == "content_gate":
        # Captured return value of CG, not a supplied green label.
        established = "refusal" in value and value["refusal"] is None
    elif key == "landing_report":
        established = (
            value.get("mergeable") is True
            and value.get("recognised") is True
            and all(
                isinstance(value.get(probe), Mapping) and isinstance(value[probe].get("outcome"), str)
                for probe in ("freshness", "environment", "measurement", "diff", "scope")
            )
        )
    elif key == "execution_receipt":
        # Whitelist receipt metadata; never embed execution output or bodies.
        check["evidence"]["scope"]["parameters"]["execution_receipt"] = {
            name: _plain(value[name])
            for name in (
                "identity",
                "role",
                "qualification",
                "categories",
                "evidence_digest",
                "categories_sha256",
                "validation_digest",
            )
            if name in value
        }
        summary = check["evidence"]["scope"]["parameters"]["execution_receipt"]
        if isinstance(value.get("categories"), Mapping):
            # Category evidence includes execution bodies in native receipts;
            # detail exposes only the status/reason metadata and their digest.
            summary["categories"] = {
                name: {key: category[key] for key in ("status", "reason") if key in category}
                if isinstance(category, Mapping)
                else category
                for name, category in value["categories"].items()
            }
        # The full guard requires a trusted carrier, not this recorded label.
        return {**check, "state": "no_observer", "reason": "observer_over_budget"}
    else:
        established = value.get("satisfied") is True
    # A negative probe / malformed receipt is not an independent counterexample.
    return {
        **check,
        "state": "observed_pass" if established else "unknown",
        "reason": "gate_observed" if established else "evidence_inconclusive",
    }


def _row_result(state: str, reason: str, checks: list[dict[str, Any]], *contributing: dict[str, Any]) -> dict[str, Any]:
    # Binding qualifiers cover every dependency, independently of which check
    # supplies the state/reason. A sufficient counterexample needs only itself.
    bindings = {check["evidence"]["binding"]["freshness"] if check["evidence"] else "unknown" for check in contributing}
    freshness = "stale" if "stale" in bindings else "unknown" if not bindings or "unknown" in bindings else "current"
    return {"state": state, "reason": reason, "freshness": freshness, "checks": checks}


def _observe_ancestry(batch: Observations, subject: Subject, now: datetime, deadline: float) -> dict[str, Any]:
    checks = [
        _check(key, batch, subject, now, deadline)
        for key in ("lane_identity", "integration_target", "verified_test_at_tip", "merge_tree", "landing_receipt")
    ]
    receipt = checks[-1]
    return _row_result(receipt["state"], receipt["reason"], checks, receipt)


def _observe_tests(batch: Observations, subject: Subject, now: datetime, deadline: float) -> dict[str, Any]:
    checks = [
        _check(key, batch, subject, now, deadline) for key in ("verified_test_at_tip", "content_gate", "landing_report")
    ]
    test = checks[0]
    if test["reason"] == "writer_provenance_unavailable":
        return _row_result("unknown", test["reason"], checks, test)
    return _row_result("no_observer", "observer_partial", checks, test)


def _observe_slice(batch: Observations, subject: Subject, now: datetime, deadline: float) -> dict[str, Any]:
    checks = [_check(key, batch, subject, now, deadline) for key in ("open_high_findings", "recent_tests")]
    findings, test = checks
    if findings["state"] == "observed_fail":
        return _row_result("observed_fail", findings["reason"], checks, findings)
    if test["reason"] == "writer_provenance_unavailable":
        return _row_result("unknown", test["reason"], checks, findings, test)
    # The chart's multi-target exit includes branch_review; SG cannot establish
    # child terminals. Even a green SG is only a gate-local fact in v1.
    return _row_result("no_observer", "observer_partial", checks, findings, test)


def _observe_battery(batch: Observations, subject: Subject, now: datetime, deadline: float) -> dict[str, Any]:
    check = _check("execution_receipt", batch, subject, now, deadline)
    checks = [] if check["reason"] == "evidence_absent" else [check]
    return _row_result("no_observer", "observer_over_budget", checks, check)


def _strings_fit(value: Any) -> bool:
    if isinstance(value, str):
        return len(value) <= 1024
    if isinstance(value, dict):
        return all(_strings_fit(key) and _strings_fit(item) for key, item in value.items())
    if isinstance(value, list):
        return all(_strings_fit(item) for item in value)
    return True


def _bound_detail(report: dict[str, Any]) -> dict[str, Any]:
    """Preserve identities and required keys; omit whole items, never recount."""

    def partial() -> None:
        report["partial"] = True
        report["counts"] = None
        report["excluded_exit_count"] = None
        if "detail_cap" not in report["limits_hit"]:
            report["limits_hit"].append("detail_cap")

    for key, cap in (("rows", 64), ("claims", 32), ("judgments", 32)):
        kept: list[dict[str, Any]] = []
        for item in report[key]:
            if key == "rows":
                checks = item["checks"]
                item["checks"] = [check for check in checks[:8] if _strings_fit(check)]
                if len(checks) != len(item["checks"]):
                    # An omitted required check must not leave a passing label.
                    item.update(state="unknown", reason="detail_cap")
                    partial()
            if len(kept) < cap and _strings_fit(item):
                kept.append(item)
            else:
                partial()
        report[key] = kept

    # Bound the normal/default and compact UTF-8 serializers. Default JSON's
    # ensure_ascii expansion can be larger than literal UTF-8 for non-ASCII.
    def size() -> int:
        return max(
            len(json.dumps(report).encode("utf-8")),
            len(json.dumps(report, ensure_ascii=False, separators=(",", ":")).encode("utf-8")),
        )

    while size() > 65536:
        partial()
        for key in ("claims", "judgments", "rows"):
            if report[key]:
                report[key].pop()
                break
        else:
            raise ValueError("required envelope exceeds detail cap")
    return report


def build_checklist(
    subject: Subject,
    *,
    now: datetime,
    observations: Observations | None = None,
    claims: tuple[Claim, ...] = (),
    judgments: tuple[Judgment, ...] = (),
    chart: Mapping[str, Any] | None = None,
    pause_point: str = "on_demand",
    deadline: float | None = None,
) -> dict[str, Any]:
    """Return one bounded envelope; no field has gate or hold authority.

    ``deadline`` is the caller's absolute monotonic deadline, shared with fleet
    collection, never a second 30s window. With no deadline this pure reducer
    performs only in-memory observations and reads the shipped static chart.
    The observation batch is a trusted collector input; claims/judgments are
    separate display inputs. Unknown subject identity is a caller lookup error;
    missing/ambiguous chart state returns an unknown envelope.
    """
    if type(subject) is not Subject:
        raise TypeError("subject must be an exactly resolved Subject")
    for value in (subject.task_ref, subject.lane_id, subject.integration_ref):
        if value is not None and (not isinstance(value, str) or not value or len(value) > 1024):
            raise ValueError("invalid or oversized subject identity")
    if not subject.task_ref or subject.lane_id == "":
        raise ValueError("unresolved subject identity")
    for value in (subject.tip_sha, subject.base_sha):
        if value is not None and (not isinstance(value, str) or not _SHA.fullmatch(value)):
            raise ValueError("subject SHA must be a full git object ID")
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("now must include a timezone")
    if pause_point not in _PAUSES:
        raise ValueError("invalid pause_point")
    if observations is None:
        observations = Observations()
    if type(observations) is not Observations:
        raise TypeError("observations must be Observations, separate from claims")
    if any(type(item) is not Claim for item in claims) or any(type(item) is not Judgment for item in judgments):
        raise TypeError("claims and judgments require their separate input types")
    if any(item.kind not in {"merge_ready", "self_verify_gate", "agent_message"} for item in claims):
        raise ValueError("invalid claim kind")
    if any(
        item.kind not in {"review_verdict", "operator_hold"}
        or not _positive_id(item.decision_id)
        or (item.decision_exists is not None and type(item.decision_exists) is not bool)
        for item in judgments
    ):
        raise ValueError("invalid judgment")
    now = now.astimezone(timezone.utc)
    # No IO can consume an unbounded slice of the caller's remaining budget.
    end = deadline if deadline is not None else time.monotonic() + 30
    machine = "outer" if subject.lane_id is None else "child"
    state = (
        subject.current_state if isinstance(subject.current_state, str) and len(subject.current_state) <= 1024 else None
    )
    report: dict[str, Any] = {
        "schema": "harness-checklist-v1",
        "schema_version": 1,
        "subject": {
            "task_ref": subject.task_ref,
            "lane_id": subject.lane_id,
            "machine": machine,
            "current_state": state,
            "tip_sha": subject.tip_sha,
            "base_sha": subject.base_sha,
        },
        "selection": "unknown",
        "pause_point": pause_point,
        "collected_at": now.isoformat().replace("+00:00", "Z"),
        "partial": False,
        "limits_hit": [],
        "counts": None,
        "excluded_exit_count": None,
        "coverage": dict(_COVERAGE),
        "rows": [],
        "claims": [asdict(item) for item in claims],
        "judgments": [{**asdict(item), "pause": True} for item in judgments],
        "diagnostics": [],
    }
    try:
        if chart is None:
            chart = json.loads(Path(__file__).with_name("lifecycle_statechart.json").read_text(encoding="utf-8"))
        validate_registry(chart)
    except json.JSONDecodeError:
        report["diagnostics"].append("chart_unreadable")
        return _bound_detail(report)
    except ValueError:
        report["diagnostics"].append("unclassified_guard")
        return _bound_detail(report)
    except (OSError, KeyError, TypeError):
        report["diagnostics"].append("chart_unreadable")
        return _bound_detail(report)
    if state not in chart[machine]["states"]:
        report["diagnostics"].append("chart_state_unavailable")
        return _bound_detail(report)
    selected: dict[tuple[str, str], set[str]] = {}
    excluded = set()
    for transition in chart[machine]["transitions"]:
        if transition["from"] != state:
            continue
        for guard in _guards(transition):
            pair = (guard["predicate_id"], guard["evidence_class"])
            if pair in REGISTRY:
                selected.setdefault(pair, set()).add(transition["id"])
            else:
                excluded.add(pair)
    report["selection"] = "selected" if selected else "no_applicable_rows"
    report["excluded_exit_count"] = len(excluded)
    for pair, transition_ids in sorted(selected.items()):
        entry = REGISTRY[pair]
        result = entry.observer(observations, subject, now, end)
        report["rows"].append(
            {
                "row_id": ":".join((machine, *pair)),
                "machine": machine,
                "predicate_id": pair[0],
                "evidence_class": pair[1],
                "transition_ids": sorted(transition_ids),
                "required": True,
                **result,
            }
        )
    rows = report["rows"]
    report["counts"] = {
        "total": len(rows),
        **{state: sum(row["state"] == state for row in rows) for state in _STATES},
        "stale": sum(row["freshness"] == "stale" for row in rows),
    }
    if time.monotonic() >= end:
        report.update(partial=True, counts=None, excluded_exit_count=None)
        report["diagnostics"].append("observer_timeout")
    return _bound_detail(report)
