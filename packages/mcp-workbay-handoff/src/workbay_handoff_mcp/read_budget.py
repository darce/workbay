"""Server-side response-budget planner for handoff reads.

internal Layer 2. The planner runs before heavy section fetches so it
can choose section limits, detail levels, and omitted sections without
first materialising large rationale, finding, or test rows.

Policy semantics:

* ``warn`` (effective default when ``response_budget_bytes`` is absent):
  pass the requested shape through unchanged; ``_envelope()`` may still
  emit the late ``oversize_response`` advisory.
* ``auto_summary`` (effective default when ``response_budget_bytes`` is
  supplied): force ``detail="summary"`` when needed, lower row limits in
  priority order, and finally omit optional sections — but never omit a
  section that is required for the active profile (e.g. ``open_items``
  always keeps ``blockers_open``, ``actions_pending``, ``findings_open``).
* ``fail``: if the *requested* shape would exceed the budget, do not
  materialise the broad payload. Return a structured retry hint.

Estimates are deliberately cheap and conservative. The planner caps the
common path; ``_envelope()`` remains the last-resort smoke alarm for
unbudgeted reads or pathologically large rationale rows.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace

from .read_profiles import (
    REQUIRED_SECTIONS_BY_PROFILE,
    ResolvedSessionAddOnShape,
    ResolvedStateShape,
)

VALID_POLICIES: tuple[str, ...] = ("warn", "auto_summary", "fail")

# Effective defaults — derived from request-presence so unrequested
# budgets do not silently flip behaviour.
DEFAULT_POLICY_NO_BUDGET = "warn"
DEFAULT_POLICY_WITH_BUDGET = "auto_summary"

# Per-section row cost estimates. Calibrated conservatively against
# observed slice-complete rationale payloads (~2.5 kB/row in full mode).
BASE_OVERHEAD_BYTES = 2500
PER_ROW_BYTES_FULL: dict[str, int] = {
    "decisions_recent": 2500,
    "findings_open": 800,
    "tests_recent": 600,
    "blockers_open": 350,
    "actions_pending": 350,
    "slices_completed": 1500,
}
PER_ROW_BYTES_SUMMARY: dict[str, int] = {
    "decisions_recent": 350,
    "findings_open": 250,
    "tests_recent": 220,
    "blockers_open": 220,
    "actions_pending": 220,
    "slices_completed": 300,
}

# Maps section name -> ResolvedStateShape attribute that controls its
# row count.
SECTION_LIMIT_ATTR: dict[str, str] = {
    "blockers_open": "top_n_blockers",
    "actions_pending": "top_n_actions",
    "decisions_recent": "top_n_decisions",
    "slices_completed": "top_n_slices",
    "tests_recent": "top_n_tests",
    "findings_open": "top_n_findings",
}

# Reduction order — heaviest first. ``auto_summary`` halves these limits
# in priority order, then omits optional sections in the same order.
REDUCTION_PRIORITY: tuple[str, ...] = (
    "decisions_recent",
    "slices_completed",
    "findings_open",
    "tests_recent",
    "actions_pending",
    "blockers_open",
)

# Compound add-on cost — applied on top of the state estimate for
# ``load_session`` compound budgets.
PER_FINDING_BYTES_FULL = 800
PER_FINDING_BYTES_SUMMARY = 250
PER_TOUCHED_FILE_BYTES = 120


class UnknownBudgetPolicyError(ValueError):
    """Raised when a caller supplies a ``budget_policy`` outside ``VALID_POLICIES``."""

    def __init__(self, policy: str) -> None:
        super().__init__(policy)
        self.policy = policy


def resolve_policy(*, response_budget_bytes: int | None, budget_policy: str | None) -> str:
    """Resolve the effective policy from caller inputs.

    Defaults follow the contract: ``warn`` when no budget is supplied,
    ``auto_summary`` when a budget is supplied without an explicit policy.
    """
    if budget_policy is None:
        return DEFAULT_POLICY_WITH_BUDGET if response_budget_bytes is not None else DEFAULT_POLICY_NO_BUDGET
    if budget_policy not in VALID_POLICIES:
        raise UnknownBudgetPolicyError(budget_policy)
    return budget_policy


def _active_sections(shape: ResolvedStateShape) -> set[str]:
    """Sections that the requested state shape will populate."""
    if shape.sections is None:
        return set(SECTION_LIMIT_ATTR.keys())
    tokens = {t.strip() for t in shape.sections.split(",") if t.strip()}
    if "identity" in tokens:
        return set()
    return {t for t in tokens if t in SECTION_LIMIT_ATTR}


def _required_sections_for(applied_profile: str | None) -> frozenset[str]:
    if applied_profile is None:
        return frozenset()
    return REQUIRED_SECTIONS_BY_PROFILE.get(applied_profile, frozenset())


def estimate_state_bytes(
    shape: ResolvedStateShape,
    *,
    omitted: Iterable[str] = (),
) -> int:
    """Cheap per-section estimate of the state payload size in bytes."""
    omitted_set = set(omitted)
    active = _active_sections(shape) - omitted_set
    per_row = PER_ROW_BYTES_SUMMARY if shape.detail == "summary" else PER_ROW_BYTES_FULL
    total = BASE_OVERHEAD_BYTES
    for section in active:
        limit_attr = SECTION_LIMIT_ATTR[section]
        rows = getattr(shape, limit_attr)
        total += per_row.get(section, 400) * max(1, rows)
    return total


def estimate_session_add_on_bytes(
    add_on: ResolvedSessionAddOnShape,
    *,
    omit_open_findings: bool = False,
    omit_touched_files: bool = False,
) -> int:
    """Cheap estimate of ``load_session`` add-on bytes (open findings + touched files)."""
    total = 0
    if not omit_open_findings and add_on.open_findings_limit > 0:
        per_row = PER_FINDING_BYTES_SUMMARY if add_on.open_findings_detail == "summary" else PER_FINDING_BYTES_FULL
        total += per_row * add_on.open_findings_limit
    if not omit_touched_files and add_on.top_n_touched_files > 0:
        total += PER_TOUCHED_FILE_BYTES * add_on.top_n_touched_files
    return total


@dataclass(frozen=True)
class BudgetPlan:
    """Server-side budget plan + metadata for ``data.read_budget``.

    ``fail_now`` is the caller signal for ``budget_policy="fail"``: when
    True, the read handler returns ``ok=false`` with ``retry_with`` and
    never fetches the broad payload.

    ``omitted_sections`` is the union of:

    * Sections the planner dropped to fit the budget (``auto_summary``).
    * Add-on sections omitted by zero-limit sentinels on
      ``load_session`` (``open_findings`` / ``touched_files``).

    ``applied_reductions`` is a structured trace of every reduction the
    planner applied, suitable for the slim-handoff-response hook to turn
    into a retry suggestion.
    """

    requested_bytes: int | None
    policy: str
    estimated_initial_bytes: int
    estimated_after_bytes: int
    applied_reductions: list[str]
    omitted_sections: list[str]
    over_budget_after: bool
    retry_with: dict[str, object] | None
    fail_now: bool


def _no_op_plan(
    *,
    requested_bytes: int | None,
    policy: str,
    initial_bytes: int,
) -> BudgetPlan:
    return BudgetPlan(
        requested_bytes=requested_bytes,
        policy=policy,
        estimated_initial_bytes=initial_bytes,
        estimated_after_bytes=initial_bytes,
        applied_reductions=[],
        omitted_sections=[],
        over_budget_after=False,
        retry_with=None,
        fail_now=False,
    )


def _suggest_retry_after_fail(shape: ResolvedStateShape, budget: int) -> dict[str, object]:
    """Build a retry hint for ``budget_policy="fail"`` rejections."""
    retry: dict[str, object] = {
        "budget_policy": "auto_summary",
        "response_budget_bytes": budget,
    }
    # Suggest a narrower profile when the caller is not already on one.
    if shape.applied_profile in (None, "full_debug"):
        retry["read_profile"] = "hot_summary"
    elif shape.applied_profile == "review_packet":
        retry["read_profile"] = "hot_summary"
    return retry


def plan_state_read(
    *,
    shape: ResolvedStateShape,
    response_budget_bytes: int | None,
    budget_policy: str,
) -> tuple[ResolvedStateShape, BudgetPlan]:
    """Plan a (possibly reduced) state shape and emit budget metadata.

    Caller has already resolved the effective policy via
    :func:`resolve_policy`. Returns the (possibly-reduced) shape plus a
    :class:`BudgetPlan` describing what changed.
    """
    initial_bytes = estimate_state_bytes(shape)

    if response_budget_bytes is None:
        return shape, _no_op_plan(
            requested_bytes=None,
            policy=budget_policy,
            initial_bytes=initial_bytes,
        )

    if initial_bytes <= response_budget_bytes:
        return shape, _no_op_plan(
            requested_bytes=response_budget_bytes,
            policy=budget_policy,
            initial_bytes=initial_bytes,
        )

    # Over budget paths --------------------------------------------------
    if budget_policy == "warn":
        return shape, BudgetPlan(
            requested_bytes=response_budget_bytes,
            policy=budget_policy,
            estimated_initial_bytes=initial_bytes,
            estimated_after_bytes=initial_bytes,
            applied_reductions=[],
            omitted_sections=[],
            over_budget_after=True,
            retry_with={
                "budget_policy": "auto_summary",
                "response_budget_bytes": response_budget_bytes,
            },
            fail_now=False,
        )

    if budget_policy == "fail":
        return shape, BudgetPlan(
            requested_bytes=response_budget_bytes,
            policy=budget_policy,
            estimated_initial_bytes=initial_bytes,
            estimated_after_bytes=initial_bytes,
            applied_reductions=[],
            omitted_sections=[],
            over_budget_after=True,
            retry_with=_suggest_retry_after_fail(shape, response_budget_bytes),
            fail_now=True,
        )

    # auto_summary
    return _plan_auto_summary(shape=shape, response_budget_bytes=response_budget_bytes, initial_bytes=initial_bytes)


def _plan_auto_summary(
    *,
    shape: ResolvedStateShape,
    response_budget_bytes: int,
    initial_bytes: int,
) -> tuple[ResolvedStateShape, BudgetPlan]:
    """``auto_summary`` reduction pass — pure function, no side effects."""
    applied: list[str] = []
    omitted: list[str] = []
    final = shape

    # Step 1: force detail="summary" if currently full.
    if final.detail != "summary":
        final = replace(final, detail="summary")
        applied.append("detail_to_summary")

    required = _required_sections_for(shape.applied_profile)
    active = _active_sections(final)

    # Step 2: halve row limits in priority order until under budget.
    while estimate_state_bytes(final, omitted=omitted) > response_budget_bytes:
        progress = False
        for section in REDUCTION_PRIORITY:
            if section not in active or section in omitted:
                continue
            limit_attr = SECTION_LIMIT_ATTR[section]
            current = int(getattr(final, limit_attr))
            if current <= 1:
                continue
            new_value = max(1, current // 2)
            final = replace(final, **{limit_attr: new_value})  # type: ignore[arg-type]
            applied.append(f"lowered_{limit_attr}_{current}_to_{new_value}")
            progress = True
            if estimate_state_bytes(final, omitted=omitted) <= response_budget_bytes:
                break
        if not progress:
            break

    # Step 3: omit optional sections (never required-by-profile).
    if estimate_state_bytes(final, omitted=omitted) > response_budget_bytes:
        for section in REDUCTION_PRIORITY:
            if section in required:
                continue
            if section not in active or section in omitted:
                continue
            omitted.append(section)
            applied.append(f"omitted_section_{section}")
            if estimate_state_bytes(final, omitted=omitted) <= response_budget_bytes:
                break

    after_bytes = estimate_state_bytes(final, omitted=omitted)
    over = after_bytes > response_budget_bytes
    return final, BudgetPlan(
        requested_bytes=response_budget_bytes,
        policy="auto_summary",
        estimated_initial_bytes=initial_bytes,
        estimated_after_bytes=after_bytes,
        applied_reductions=applied,
        omitted_sections=list(omitted),
        over_budget_after=over,
        # When auto_summary cannot fit even after reductions, the hook can
        # still suggest ``fail`` next time so the caller learns sooner.
        retry_with=({"budget_policy": "fail"} if over else None),
        fail_now=False,
    )


def plan_session_read(
    *,
    state_shape: ResolvedStateShape,
    add_on: ResolvedSessionAddOnShape,
    response_budget_bytes: int | None,
    budget_policy: str,
) -> tuple[ResolvedStateShape, ResolvedSessionAddOnShape, BudgetPlan]:
    """Compound budget for ``load_session``.

    The compound budget covers the nested state payload plus the session
    add-ons (open findings + touched files). When over budget under
    ``auto_summary``, the state shape is reduced first; the add-ons are
    halved only if the state reductions did not fit on their own. Zero
    limits on add-ons (``top_n_touched_files=0`` from the ``identity``
    profile, for instance) are already-omitted sentinels — they are
    reflected in ``omitted_sections`` but never re-fetched.
    """
    add_on_bytes_initial = estimate_session_add_on_bytes(add_on)
    state_bytes_initial = estimate_state_bytes(state_shape)
    compound_initial = state_bytes_initial + add_on_bytes_initial

    # Bake in pre-existing add-on omissions from zero-limit sentinels.
    pre_omitted: list[str] = []
    if add_on.open_findings_limit <= 0:
        pre_omitted.append("open_findings")
    if add_on.top_n_touched_files <= 0:
        pre_omitted.append("touched_files")

    if response_budget_bytes is None or compound_initial <= response_budget_bytes:
        plan = BudgetPlan(
            requested_bytes=response_budget_bytes,
            policy=budget_policy,
            estimated_initial_bytes=compound_initial,
            estimated_after_bytes=compound_initial,
            applied_reductions=[],
            omitted_sections=list(pre_omitted),
            over_budget_after=False,
            retry_with=None,
            fail_now=False,
        )
        return state_shape, add_on, plan

    if budget_policy == "warn":
        return (
            state_shape,
            add_on,
            BudgetPlan(
                requested_bytes=response_budget_bytes,
                policy=budget_policy,
                estimated_initial_bytes=compound_initial,
                estimated_after_bytes=compound_initial,
                applied_reductions=[],
                omitted_sections=list(pre_omitted),
                over_budget_after=True,
                retry_with={
                    "budget_policy": "auto_summary",
                    "response_budget_bytes": response_budget_bytes,
                },
                fail_now=False,
            ),
        )

    if budget_policy == "fail":
        return (
            state_shape,
            add_on,
            BudgetPlan(
                requested_bytes=response_budget_bytes,
                policy=budget_policy,
                estimated_initial_bytes=compound_initial,
                estimated_after_bytes=compound_initial,
                applied_reductions=[],
                omitted_sections=list(pre_omitted),
                over_budget_after=True,
                retry_with=_suggest_retry_after_fail(state_shape, response_budget_bytes),
                fail_now=True,
            ),
        )

    # auto_summary -------------------------------------------------------
    # Step 1: reduce the state shape against the compound budget.
    final_state, state_plan = _plan_auto_summary(
        shape=state_shape,
        response_budget_bytes=response_budget_bytes,
        initial_bytes=compound_initial,
    )

    applied: list[str] = list(state_plan.applied_reductions)
    omitted: list[str] = list(pre_omitted) + list(state_plan.omitted_sections)
    final_add_on = add_on

    # Step 2: trim add-ons if still over budget.
    def _compound_bytes() -> int:
        return estimate_state_bytes(final_state, omitted=state_plan.omitted_sections) + estimate_session_add_on_bytes(
            final_add_on,
            omit_open_findings=("open_findings" in omitted),
            omit_touched_files=("touched_files" in omitted),
        )

    # Force summary detail on findings.
    if (
        _compound_bytes() > response_budget_bytes
        and "open_findings" not in omitted
        and final_add_on.open_findings_detail != "summary"
    ):
        final_add_on = replace(final_add_on, open_findings_detail="summary")
        applied.append("session_open_findings_detail_to_summary")

    # Halve open_findings_limit until fit or floor.
    while _compound_bytes() > response_budget_bytes and final_add_on.open_findings_limit > 1:
        prev = final_add_on.open_findings_limit
        new_val = max(1, prev // 2)
        final_add_on = replace(final_add_on, open_findings_limit=new_val)
        applied.append(f"lowered_session_open_findings_limit_{prev}_to_{new_val}")

    # Halve top_n_touched_files until fit or floor.
    while _compound_bytes() > response_budget_bytes and final_add_on.top_n_touched_files > 1:
        prev = final_add_on.top_n_touched_files
        new_val = max(1, prev // 2)
        final_add_on = replace(final_add_on, top_n_touched_files=new_val)
        applied.append(f"lowered_session_top_n_touched_files_{prev}_to_{new_val}")

    # Omit touched_files entirely as a last resort (open_findings stays
    # available because identity workflows rely on the slot).
    if _compound_bytes() > response_budget_bytes and "touched_files" not in omitted:
        omitted.append("touched_files")
        applied.append("omitted_section_touched_files")

    after_bytes = _compound_bytes()
    over = after_bytes > response_budget_bytes
    return (
        final_state,
        final_add_on,
        BudgetPlan(
            requested_bytes=response_budget_bytes,
            policy="auto_summary",
            estimated_initial_bytes=compound_initial,
            estimated_after_bytes=after_bytes,
            applied_reductions=applied,
            omitted_sections=omitted,
            over_budget_after=over,
            retry_with=({"budget_policy": "fail"} if over else None),
            fail_now=False,
        ),
    )


def budget_payload(plan: BudgetPlan) -> dict[str, object]:
    """Render a ``data.read_budget`` payload."""
    payload: dict[str, object] = {
        "requested_bytes": plan.requested_bytes,
        "policy": plan.policy,
        "estimated_initial_bytes": plan.estimated_initial_bytes,
        "estimated_after_bytes": plan.estimated_after_bytes,
        "applied_reductions": list(plan.applied_reductions),
        "omitted_sections": list(plan.omitted_sections),
        "over_budget_after": plan.over_budget_after,
    }
    if plan.retry_with is not None:
        payload["retry_with"] = dict(plan.retry_with)
    return payload
