"""Map pass-engine outcomes onto ``worker_reports.outcome`` enum values.

implementation note S1: ``PASS_OUTCOMES`` (offload pass engine) and
``WORKER_REPORT_OUTCOMES`` (durable report CHECK) are intentionally different
vocabularies. Every new ``worker_reports`` row must carry a non-NULL report
outcome; this module is the single total mapping.

Success is ``handoff_ready`` → ``finished`` (there is no ``completed`` member).
Transient / residual pass outcomes default to ``failed`` so a report that
reached the writer without a clean terminal is conservatively not a success.
"""

from __future__ import annotations

from workbay_orchestrator_mcp.orchestration.offload_pass import PASS_OUTCOMES

# Keep in sync with lanes.WORKER_REPORT_OUTCOMES (avoid import cycle: lanes
# imports orchestration only lazily for some helpers).
WORKER_REPORT_OUTCOMES = frozenset(
    {"finished", "failed", "exhausted", "stopped", "no_actionable_work", "no_work"}
)

#: Documented default for unmapped / empty / residual pass outcomes ([OBS-01]).
DEFAULT_WORKER_REPORT_OUTCOME = "failed"

#: Explicit total mapping over ``PASS_OUTCOMES`` → ``worker_reports.outcome``.
#: Members not listed here fall through to ``DEFAULT_WORKER_REPORT_OUTCOME``;
#: the unit test requires every ``PASS_OUTCOMES`` member to appear as a key.
PASS_OUTCOME_TO_WORKER_REPORT: dict[str, str] = {
    # Clean terminal success (no ``completed`` member in PASS_OUTCOMES).
    "handoff_ready": "finished",
    # Bounded stop awaiting operator — verify-at-gate, not a hard failure.
    "needs_guidance": "stopped",
    # Resource / time exhaustion.
    "timeout": "exhausted",
    "token_budget_exceeded": "exhausted",
    # Explicit failures.
    "error": "failed",
    "self_verify_failed": "failed",
    "composer_violation_quarantined": "failed",
    # Typed empty inbox (canonical name; ``no_work`` is a legacy report alias).
    "no_actionable_work": "no_actionable_work",
    # Remaining / transient members → documented DEFAULT of failed.
    "uncommitted_work": "failed",
    "checkpoint": "failed",
    "still_running": "failed",
    "lane_not_found": "failed",
    "server_stale_restart_required": "failed",
    "admission_deferred": "failed",
    "admission_refused": "failed",
}


def map_pass_outcome_to_worker_report(outcome: str | None) -> str | None:
    """Map a pass-engine (or already-report) outcome to a ``worker_reports.outcome``.

    Returns **None when no outcome was supplied**: an absent outcome is not a
    failure verdict. Legacy callers may record a report without an outcome and
    keep a NULL outcome (a pre-existing, tested contract —
    ``test_worker_reports_validate_and_expose_terminal_outcome``). Fabricating
    ``failed`` here would be strictly worse than NULL: it invents a verdict the
    caller never made, and it would newly trip the slice auto-close gate
    (``offload_pass.py:710`` refuses auto-close on ``failed``) for every
    outcome-less report. implementation note S1 makes every *pass-written* report carry an
    outcome by threading one through the pass-engine callers — not by inventing
    one at the writer.

    A **supplied** value is always mapped: report-vocabulary values pass through
    unchanged (daemon sites write those directly, so no double-mapping); pass
    outcomes map via the table; an unmapped-but-supplied member falls back to
    ``DEFAULT_WORKER_REPORT_OUTCOME`` so a future ``PASS_OUTCOMES`` addition is
    conservatively not a success.
    """
    if outcome is None:
        return None
    key = str(outcome).strip().lower()
    if not key:
        return None
    if key in WORKER_REPORT_OUTCOMES:
        return key
    return PASS_OUTCOME_TO_WORKER_REPORT.get(key, DEFAULT_WORKER_REPORT_OUTCOME)


def assert_mapping_total_over_pass_outcomes() -> None:
    """Raise AssertionError if the mapping is not total over ``PASS_OUTCOMES``.

    Used by unit tests; safe to call from other self-checks.
    """
    missing = sorted(PASS_OUTCOMES - set(PASS_OUTCOME_TO_WORKER_REPORT))
    if missing:
        raise AssertionError(f"PASS_OUTCOME_TO_WORKER_REPORT missing members: {missing}")
    extra = sorted(set(PASS_OUTCOME_TO_WORKER_REPORT) - PASS_OUTCOMES)
    if extra:
        raise AssertionError(f"PASS_OUTCOME_TO_WORKER_REPORT has unknown members: {extra}")
    for pass_outcome, report_outcome in PASS_OUTCOME_TO_WORKER_REPORT.items():
        if report_outcome not in WORKER_REPORT_OUTCOMES:
            raise AssertionError(
                f"mapped outcome {report_outcome!r} for {pass_outcome!r} not in WORKER_REPORT_OUTCOMES"
            )
