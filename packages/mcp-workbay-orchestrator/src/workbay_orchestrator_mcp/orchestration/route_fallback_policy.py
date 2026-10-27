"""Pure, explicit capacity fallback authorization and replay receipts."""

import json
from collections.abc import Collection, Mapping
from dataclasses import dataclass, replace
from hashlib import sha256
from typing import Literal

from workbay_orchestrator_mcp.orchestration.backend_registry import validate_backend
from workbay_orchestrator_mcp.orchestration.lane_routing import RoutingQuad

ExecutionLocation = Literal["remote_vm", "local_host"]
FAILURE_CLASSES = ("capacity", "auth", "configuration", "validation", "transport", "unknown")


def execution_location(backend: str | None, *, admitted_remote: Collection[str]) -> ExecutionLocation:
    return "remote_vm" if backend in admitted_remote else "local_host"


@dataclass(frozen=True)
class RouteFailure:
    failure_class: str
    evidence: str
    evidence_source: str


@dataclass(frozen=True)
class FallbackRule:
    rule_id: str
    from_backend: str
    from_model: str | None
    to: RoutingQuad
    on: frozenset[str] = frozenset({"capacity"})

    def __post_init__(self) -> None:
        if self.on != frozenset({"capacity"}):
            raise ValueError("on must contain only capacity")
        if not self.to.backend or not self.to.model:
            raise ValueError("target backend and model are required")


@dataclass(frozen=True)
class FallbackPolicy:
    rules: tuple[FallbackRule, ...]
    source: str

    @property
    def digest(self) -> str:
        rules = [
            dict(
                rule_id=r.rule_id,
                from_backend=r.from_backend,
                from_model=r.from_model,
                to=r.to.as_dict(),
                on=sorted(r.on),
            )
            for r in self.rules
        ]
        return sha256(json.dumps(rules, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


EMPTY_POLICY = FallbackPolicy((), "default_deny")


def _keys(data, allowed):
    if not isinstance(data, Mapping):
        raise ValueError("expected a mapping")
    unknown = set(data) - set(allowed)
    if unknown:
        raise ValueError(f"unknown keys: {sorted(unknown)}")


def _required(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be nonempty")
    return value


def _backend(value):
    value = _required(value, "backend")
    try:
        return validate_backend(value)
    except (RuntimeError, ValueError) as exc:
        raise ValueError(f"unknown backend: {value}") from exc


def policy_from_mapping(data: Mapping, *, source: str) -> FallbackPolicy:
    _keys(data, {"rules"})
    if not isinstance(data.get("rules"), list):
        raise ValueError("rules must be a list")
    rules = []
    seen = set()
    for row in data["rules"]:
        _keys(row, {"rule_id", "from_backend", "from_model", "to", "on"})
        if row.get("on") != ["capacity"]:
            raise ValueError("on must be ['capacity']")
        rule_id = _required(row.get("rule_id"), "rule_id")
        if rule_id in seen:
            raise ValueError(f"duplicate rule_id: {rule_id}")
        seen.add(rule_id)
        target = row.get("to")
        _keys(target, {"backend", "model", "effort", "speed", "tier"})
        values = dict(target)
        values["backend"] = _backend(target.get("backend"))
        values["model"] = _required(target.get("model"), "model")
        for key in ("effort", "speed", "tier"):
            if values.get(key) is not None:
                _required(values[key], key)
        from_model = row.get("from_model")
        if from_model is not None:
            _required(from_model, "from_model")
        rules.append(FallbackRule(rule_id, _backend(row.get("from_backend")), from_model, RoutingQuad(**values)))
    return FallbackPolicy(tuple(rules), source)


def classify_route_failure(result: Mapping) -> RouteFailure:
    raw = result.get("raw_payload")
    raw = raw if isinstance(raw, Mapping) else {}
    blockers = result.get("blockers") or ()
    if (
        result.get("handoff_action") == "rate_limited"
        or result.get("outcome") == "rate_limited"
        or "rate_limited" in blockers
    ):
        return RouteFailure("capacity", str(raw.get("rate_limit_detail") or "rate_limited"), "typed")
    if result.get("outcome") in {"remote_required", "routing_refused", "dispatch_refused"}:
        return RouteFailure("validation", str(result["outcome"]), "typed")
    if result.get("failure_class") == "configuration" or raw.get("failure_class") == "configuration":
        return RouteFailure("configuration", "configuration", "typed")
    if raw.get("origin") == "dispatch":
        text = json.dumps(dict(result), default=str).lower()
        for failure_class, markers in (
            ("auth", ("401", "403", "unauthorized", "not logged in", "invalid api key", "authentication")),
            (
                "configuration",
                ("resolve_effective_model", "allowed_models", "malformed backend spec", "missing binary"),
            ),
            ("capacity", ("http 402", "out of usage", "usage limit", "quota", "rate_limited", "rate limit")),
        ):
            for marker in markers:
                if marker in text:
                    return RouteFailure(failure_class, marker, "text")
    return RouteFailure("unknown", "", "none")


@dataclass(frozen=True)
class RouteDecision:
    kind: Literal["transition", "refused"]
    requested: RoutingQuad
    effective: RoutingQuad | None
    reason_code: str
    rule_id: str | None
    failure: RouteFailure
    policy_digest: str
    policy_source: str
    decided_at: str


def decide_route_transition(
    requested, failure, policy, *, execution_mode, admitted_remote, decided_at
) -> RouteDecision:
    """A transition only names a route.

    The caller still runs the full preflight, admission, budget, no-replay and
    review gates on the effective route.
    """
    decision = RouteDecision(
        "refused", requested, None, "fallback_not_capacity", None, failure, policy.digest, policy.source, decided_at
    )
    if failure.failure_class != "capacity":
        return decision
    rule = next(
        (
            r
            for r in policy.rules
            if r.from_backend == requested.backend and (r.from_model is None or r.from_model == requested.model)
        ),
        None,
    )
    if rule is None:
        return replace(decision, reason_code="fallback_not_authorized")
    decision = replace(decision, rule_id=rule.rule_id)
    if execution_mode == "remote_only" and rule.to.backend not in admitted_remote:
        return replace(decision, reason_code="remote_required")
    if all(getattr(rule.to, field) == getattr(requested, field) for field in ("backend", "model", "effort", "speed")):
        return replace(decision, reason_code="fallback_same_route")
    return replace(decision, kind="transition", effective=rule.to, reason_code="capacity_fallback_authorized")


def route_decision_record(decision: RouteDecision, *, admitted_remote: Collection[str]) -> dict:
    def route(value):
        return (
            None
            if value is None
            else {
                **value.as_dict(),
                "execution_location": execution_location(value.backend, admitted_remote=admitted_remote),
            }
        )

    return {
        "requested": route(decision.requested),
        "effective": route(decision.effective),
        "transition": {
            "kind": decision.kind,
            "reason_code": decision.reason_code,
            "rule_id": decision.rule_id,
            "failure_class": decision.failure.failure_class,
            "evidence": decision.failure.evidence,
            "evidence_source": decision.failure.evidence_source,
            "policy_digest": decision.policy_digest,
            "policy_source": decision.policy_source,
            "decided_at": decision.decided_at,
        },
    }


def replay_route_decision(
    record: Mapping,
    policy: FallbackPolicy,
    *,
    execution_mode: str,
    admitted_remote: Collection[str],
) -> RouteDecision:
    transition = record["transition"]
    requested = RoutingQuad(
        **{key: record["requested"].get(key) for key in ("backend", "model", "effort", "speed", "tier")}
    )
    failure = RouteFailure(transition["failure_class"], transition["evidence"], transition["evidence_source"])
    if transition["policy_digest"] != policy.digest:
        return RouteDecision(
            "refused",
            requested,
            None,
            "fallback_policy_changed",
            None,
            failure,
            policy.digest,
            policy.source,
            transition["decided_at"],
        )
    decision = decide_route_transition(
        requested,
        failure,
        policy,
        execution_mode=execution_mode,
        admitted_remote=admitted_remote,
        decided_at=transition["decided_at"],
    )
    if route_decision_record(decision, admitted_remote=admitted_remote)["effective"] != record["effective"]:
        return replace(decision, kind="refused", effective=None, reason_code="fallback_record_mismatch")
    return decision
