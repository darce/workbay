"""Composite landing-gate policy: closed outcomes and the merge verdict.

Kept separate from parsing, scope, and subprocess probes so the fail-closed
verdict cannot drift with extractor or environment details [REVIEW-M-07].
"""

from __future__ import annotations

from typing import Any, Sequence


class LandingGateError(RuntimeError):
    """A gate input could not be established. Never raised for a *failing* gate."""


#: Outcomes. Closed sets, checked by ``gate_report``.
FRESHNESS_OUTCOMES = frozenset({"current", "subject_behind_baseline"})
ENVIRONMENT_OUTCOMES = frozenset({"consistent", "environment_inconsistent"})
DIFF_OUTCOMES = frozenset({"zero_regression", "regressed", "ids_not_measured"})
MEASUREMENT_OUTCOMES = frozenset({"complete", "incomplete", "not_measured", "ids_mismatched"})
SCOPE_OUTCOMES = frozenset(
    {
        "comparable",
        "scope_missing",
        "scope_malformed",
        "scope_mismatched",
        "scope_unverified",
        "producer_mismatched",
    }
)


def _named_outcome(payload: dict[str, Any], *, missing: str) -> str:
    return str(payload.get("outcome", "") or missing)


def _scope_report_state(scope: dict[str, Any] | None) -> tuple[str | None, bool]:
    """Scope omission is a typed refusal for every caller, including in-process."""
    if scope is None:
        return "scope_missing", True
    return _named_outcome(scope, missing="scope_outcome_missing"), True


def _blocking_pairs(fresh: str, env: str, measure: str, diff: str, scope: str | None) -> list[tuple[str, str]]:
    pairs = [
        (fresh, "current"),
        (env, "consistent"),
        (measure, "complete"),
    ]
    if scope is not None:
        pairs.append((scope, "comparable"))
    pairs.append((diff, "zero_regression"))
    return pairs


def _first_unmet(pairs: Sequence[tuple[str, str]]) -> str:
    for actual, expected in pairs:
        if actual != expected:
            return actual
    return pairs[-1][0]


def _is_mergeable(fresh: str, env: str, measure: str, diff: str, scope: str | None) -> bool:
    ok = fresh == "current" and env == "consistent" and measure == "complete" and diff == "zero_regression"
    return ok if scope is None else ok and scope == "comparable"


def _outcomes_recognised(fresh: str, env: str, measure: str, diff: str, scope: str | None) -> bool:
    ok = (
        fresh in FRESHNESS_OUTCOMES
        and env in ENVIRONMENT_OUTCOMES
        and measure in MEASUREMENT_OUTCOMES
        and diff in DIFF_OUTCOMES
    )
    return ok if scope is None else ok and scope in SCOPE_OUTCOMES


def gate_report(
    *,
    freshness: dict[str, Any],
    environment: dict[str, Any],
    diff: dict[str, Any],
    measurement: dict[str, Any],
    scope: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Combine the probes into one typed verdict.

    Order is the contract.  A stale subject, a split checkout, a truncated
    run, or incomparable measurement scope each makes the failure-id diff
    meaningless, so those outcomes pre-empt it: reporting ``zero_regression``
    off a run that measured the wrong tree -- or a different pytest target --
    is a confident wrong answer, which is worse than no answer.

    ``measurement`` is a required argument rather than an optional one with a
    permissive default.  An optional completeness check defaults to trusting
    the evidence it was not given, which is the vacuous pass this module
    exists to remove.

    ``scope`` is optional only for in-process callers that are pinning a
    different probe.  The CLI always supplies a scope comparison, and a
    missing/malformed/mismatched receipt is a named refusal rather than an
    automatic pass.

    Fails closed.  An outcome outside the declared sets is surfaced verbatim
    with ``mergeable=False`` rather than falling out of the ``if`` chain into a
    pass -- a gate must not approve something it does not understand.
    """
    fresh_outcome = _named_outcome(freshness, missing="freshness_outcome_missing")
    env_outcome = _named_outcome(environment, missing="environment_outcome_missing")
    measure_outcome = _named_outcome(measurement, missing="measurement_outcome_missing")
    diff_outcome = _named_outcome(diff, missing="diff_outcome_missing")
    scope_outcome, scope_checked = _scope_report_state(scope)
    scoped = scope_outcome if scope_checked else None
    report = {
        "outcome": _first_unmet(_blocking_pairs(fresh_outcome, env_outcome, measure_outcome, diff_outcome, scoped)),
        "mergeable": _is_mergeable(fresh_outcome, env_outcome, measure_outcome, diff_outcome, scoped),
        "recognised": _outcomes_recognised(fresh_outcome, env_outcome, measure_outcome, diff_outcome, scoped),
        "freshness": freshness,
        "environment": environment,
        "measurement": measurement,
        "diff": diff,
    }
    report["scope"] = scope if scope is not None else {"outcome": "scope_missing"}
    return report
