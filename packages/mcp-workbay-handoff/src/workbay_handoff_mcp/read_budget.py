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

import json
from collections.abc import Iterable, Mapping
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

# HARM-A-01 [DATA-14]: canonical cross-surface response budget. The client-side
# slim-handoff-response hook (workbay-system payload) hard-truncates handoff tool
# responses at this many chars; when the two values disagree the server plans
# payloads the client then destructively chops mid-structure.
# This exported constant is the single source of truth — the hook's
# CHAR_THRESHOLD / DEFAULT_SUGGESTED_BUDGET_BYTES must mirror it (the hook runs
# as a bare subprocess without this package on sys.path, so it mirrors the
# literal and guards it with a drift-check test).
CANONICAL_RESPONSE_BUDGET_BYTES = 16_000

# [DATA-14] single-sourced bare-call default: when a chatty read is called
# with neither read_profile nor response_budget_bytes nor an explicit
# sections= shape, apply this budget with budget_policy=auto_summary so the
# planner trims before materialising. Explicit caller args always win (see
# apply_bare_call_budget_defaults). Aligned to the canonical cross-surface
# budget so server planning and the client hook hard-truncate agree.
DEFAULT_BARE_CALL_RESPONSE_BUDGET_BYTES = CANONICAL_RESPONSE_BUDGET_BYTES

# Bound for committed write receipts (the op-echo fix). The bare-read default
# moves to this in a follow-up that also moves the hook mirror.
DEFAULT_WRITE_RECEIPT_BUDGET_BYTES = 8_192

# Per-section row cost estimates. Calibrated conservatively against
# observed slice-complete rationale payloads (~2.5 kB/row in full mode).
BASE_OVERHEAD_BYTES = 1_700
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
# Omit optional sections before shrinking default compact row caps so a
# explicitly budgeted 8192-byte auto_summary read can keep DEFAULT_HANDOFF_LIMITS
# (blockers=5, decisions=3, …) by dropping slices_completed first.
_OMIT_PRIORITY: tuple[str, ...] = ("slices_completed",)

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


def apply_bare_call_budget_defaults(
    *,
    read_profile: str | None,
    response_budget_bytes: int | None,
    budget_policy: str | None,
    sections: str | None = None,
    detail: str | None = None,
) -> tuple[int | None, str | None, bool]:
    """Apply the server bare-call default budget when all shape levers are absent.

    Returns ``(effective_budget_bytes, effective_budget_policy, bare_default_applied)``.
    Explicit ``response_budget_bytes`` / ``read_profile`` / ``budget_policy`` always win.
    S10-A-02: an explicit ``sections=`` (or ``detail=``) selection is already a
    shaped read — the caller narrowed the payload deliberately, so the bare-call
    default budget (and its auto_summary trimming) must not be injected on top
    of it (auto_summary would silently override an explicit ``detail='full'``).
    [DATA-14] constant: ``DEFAULT_BARE_CALL_RESPONSE_BUDGET_BYTES``.
    """
    if read_profile is not None or response_budget_bytes is not None or sections is not None or detail is not None:
        return response_budget_bytes, budget_policy, False
    # Bare call: inject the single-sourced default budget. Policy defaults to
    # auto_summary via resolve_policy once the budget is present; an explicit
    # budget_policy still wins.
    return DEFAULT_BARE_CALL_RESPONSE_BUDGET_BYTES, budget_policy, True


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
    omit_continuation: bool = False,
) -> int:
    """Cheap estimate of ``load_session`` add-on bytes (open findings + touched files).

    ``omit_continuation`` is accepted for call-site symmetry with the session
    planner but is not estimated here: continuation is a small, capped
    cold-start add-on attached after budget planning (internal).
    """
    del omit_continuation  # reserved; not part of the pre-fetch estimate
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
      ``load_session`` (``open_findings`` / ``touched_files`` /
      ``continuation``).

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

    # Step 2: omit optional sections (never required-by-profile) before
    # shrinking default compact row caps.
    if estimate_state_bytes(final, omitted=omitted) > response_budget_bytes:
        for section in _OMIT_PRIORITY:
            if section in required:
                continue
            if section not in active or section in omitted:
                continue
            omitted.append(section)
            applied.append(f"omitted_section_{section}")
            if estimate_state_bytes(final, omitted=omitted) <= response_budget_bytes:
                break

    # Step 3: halve remaining row limits in priority order until under budget.
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

    # Step 4: omit remaining optional sections if still over.
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
            omit_continuation=("continuation" in omitted),
        )

    if _compound_bytes() > response_budget_bytes and "continuation" not in omitted:
        omitted.append("continuation")
        applied.append("omitted_section_continuation")

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

    # Halve top_n_touched_files until fit or floor of 2 so an explicit
    # two-file request is not collapsed to a single row.
    while _compound_bytes() > response_budget_bytes and final_add_on.top_n_touched_files > 2:
        prev = final_add_on.top_n_touched_files
        new_val = max(2, prev // 2)
        final_add_on = replace(final_add_on, top_n_touched_files=new_val)
        applied.append(f"lowered_session_top_n_touched_files_{prev}_to_{new_val}")

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


# ---------------------------------------------------------------------------
# op-echo-ho: final UTF-8 envelope accounting + write-receipt shaping
# ---------------------------------------------------------------------------

VALID_WRITE_DETAILS: tuple[str, ...] = ("identity", "summary", "full")
VALID_WRITE_PROJECTION_FIELDS: frozenset[str] = frozenset(
    {
        "ok",
        "schema_version",
        "tool",
        "scope",
        "data",
        "mutation",
        "warnings",
        "task_ref",
        "id",
        "event_id",
        "revision",
        "idempotent",
        "affected_ids",
        "operation",
        "entity",
        "inserted",
        "updated",
        "written",
        "active",
        "decision",
        "findings",
        "results",
        "finding_id",
        "action",
        "truncation",
        "scoped_fetch",
        "read_budget",
        "detail",
        "fields",
        "status",
        "branch",
        "commit_sha",
        "objective",
        "focus",
        "error",
        "committed_effect_id",
        "committed_event_id",
        "partial",
        "side_effect_failures",
        "findings_recorded_count",
        "reopened",
    }
)

_IDENTITY_DECISION_KEYS: tuple[str, ...] = (
    "id",
    "decision",
    "task_ref",
    "session",
    "branch",
    "commit_sha",
    "agent",
)
_IDENTITY_ACTIVE_KEYS: tuple[str, ...] = (
    "task_ref",
    "status",
    "revision",
    "target_branch",
    "updated_at",
    "updated_by",
)
_SESSION_ADDON_DROP_ORDER: tuple[str, ...] = (
    "continuation",
    "slices_completed",
)
_SESSION_ADDON_LAST_RESORT_DROP_ORDER: tuple[str, ...] = (
    "touched_files",
    "open_findings",
    "read_shape",
    "compaction_advisory",
)
_NESTED_STATE_DROP_ORDER: tuple[str, ...] = (
    "decisions_recent",
    "slices_completed",
    "tests_recent",
    "findings_open",
    "findings_all",
    "blockers_open",
    "actions_pending",
    "worker_reports_recent",
    "lane_messages_open",
    "worktree_lanes",
    "current_lane",
    "read_shape",
    "read_budget",
    "compaction_advisory",
)


class InvalidWriteProjectionError(ValueError):
    """Raised when a write projection selector is outside the allowlist."""

    def __init__(self, selector: str, *, allowed: Iterable[str] | None = None) -> None:
        allowed_list = list(allowed) if allowed is not None else sorted(VALID_WRITE_PROJECTION_FIELDS)
        super().__init__(f"Invalid projection selector {selector!r}. Allowed fields: {allowed_list}.")
        self.selector = selector
        self.allowed = allowed_list


class WriteBudgetTooSmallError(ValueError):
    """Raised when a requested write budget cannot fit the mandatory receipt."""

    def __init__(self, requested: int, minimum: int) -> None:
        super().__init__(
            f"response_budget_bytes={requested} cannot fit the minimum write receipt "
            f"({minimum} UTF-8 bytes). Retry with a larger budget or omit response_budget_bytes."
        )
        self.requested = requested
        self.minimum = minimum


class WriteReceiptExceedsBudgetError(ValueError):
    """Raised before a write when its requested receipt exceeds a fail budget."""

    def __init__(self, budget: int, projected: int) -> None:
        super().__init__(f"Requested write receipt is {projected} bytes; budget is {budget} bytes.")
        self.budget = budget
        self.projected = projected


def utf8_payload_bytes(payload: Mapping[str, object] | dict, *, dual_carrier: bool = False) -> int:
    """UTF-8 size of one JSON serialization, optionally counting the duplicate carrier."""
    blob = json.dumps(payload, ensure_ascii=False, default=str)
    n = len(blob.encode("utf-8"))
    return n * 2 if dual_carrier else n


def mcp_transport_envelope_bytes(
    payload: Mapping[str, object] | dict,
    *,
    is_error: bool = False,
) -> int:
    """UTF-8 size of the actual MCP CallToolResult transport envelope.

    Counts escaped text content, the structured duplicate, wrappers, and
    metadata together. Independent of the test serializer (which uses
    ``Tool.from_function`` on the public entrypoint).
    """
    from mcp.types import CallToolResult, TextContent

    as_dict = dict(payload)
    text = json.dumps(as_dict, ensure_ascii=False, separators=(",", ":"), default=str)
    result = CallToolResult(
        content=[TextContent(type="text", text=text)],
        structuredContent=as_dict,
        isError=is_error,
    )
    return len(result.model_dump_json(by_alias=True, exclude_none=True).encode("utf-8"))


MCP_CONTENT_CARRIER_BOTH = "both"
MCP_CONTENT_CARRIER_STRUCTURED = "structured"
_MCP_CONTENT_CARRIERS = frozenset({MCP_CONTENT_CARRIER_BOTH, MCP_CONTENT_CARRIER_STRUCTURED})


def mcp_structured_envelope_bytes(
    payload: Mapping[str, object] | dict,
    *,
    is_error: bool = False,
) -> int:
    """UTF-8 size of the structured-only CallToolResult (no text carrier)."""
    from mcp.types import CallToolResult

    result = CallToolResult(content=[], structuredContent=dict(payload), isError=is_error)
    return len(result.model_dump_json(by_alias=True, exclude_none=True).encode("utf-8"))


def registered_carrier_envelope_bytes(
    payload: Mapping[str, object] | dict, *, content_carrier: str = MCP_CONTENT_CARRIER_BOTH
) -> int:
    """UTF-8 size for an explicit carrier; direct calls conservatively use both."""
    if content_carrier not in _MCP_CONTENT_CARRIERS:
        raise ValueError(f"MCP content carrier must be 'both' or 'structured'; got {content_carrier!r}")
    if content_carrier == MCP_CONTENT_CARRIER_STRUCTURED:
        return mcp_structured_envelope_bytes(payload)
    return mcp_transport_envelope_bytes(payload)


def requested_write_receipt_bytes(
    envelope: Mapping[str, object],
    *,
    detail: str | None = None,
    fields: str | None = None,
    event_id: str | None = None,
) -> int:
    """Measure the requested receipt before budget-driven identity truncation.

    ``budget_policy='fail'`` uses this projection before the database write.
    The post-commit shaper remains responsible for warn/auto-summary behavior.
    """
    shaped = dict(envelope)
    data = dict(shaped.get("data") if isinstance(shaped.get("data"), Mapping) else {})
    if event_id and "event_id" not in data:
        data["event_id"] = event_id
    shaped["data"] = data

    if detail == "identity":
        shaped["data"] = _identity_write_data(data)
    elif detail == "summary":
        shaped["data"] = _summary_write_data(data)
    if fields:
        shaped = _apply_field_projection(shaped, _parse_field_list(fields))
    return mcp_transport_envelope_bytes(shaped)


_SHAPING_ERROR_DETAIL_MAX = 240
TRANSPORT_TRUNCATION_RESERVE_BYTES = 1600


def continuation_scoped_fetch(*, task_ref: object, packet_id: object = None) -> dict[str, object]:
    """Public continuation signature is ``continuation(payload={...})``."""
    payload: dict[str, object] = {"operation": "load", "task_ref": task_ref}
    if packet_id is not None:
        payload["packet_id"] = packet_id
    return {"tool": "continuation", "arguments": {"payload": payload}}


def _parse_field_list(fields: str | None) -> list[str]:
    if fields is None:
        return []
    return [part.strip() for part in fields.split(",") if part.strip()]


def minimum_write_receipt_bytes() -> int:
    """Lower bound for a mandatory identity write receipt (single carrier)."""
    sample = {
        "ok": True,
        "schema_version": 2,
        "tool": "record_event",
        "scope": {"task_ref": "T"},
        "data": {
            "idempotent": False,
            "event_id": "e",
            "truncation": {
                "truncated": True,
                "reason": "response_budget_bytes",
                "scoped_fetch": {"tool": "get_handoff_state", "arguments": {"task_ref": "T"}},
            },
        },
        "mutation": {
            "entity": "decision",
            "operation": "insert",
            "affected_ids": [1],
            "task_revision": 0,
        },
        "task_ref": "T",
    }
    return mcp_transport_envelope_bytes(sample)


def validate_write_shape_inputs(
    *,
    detail: str | None = None,
    fields: str | None = None,
    budget_policy: str | None = None,
    response_budget_bytes: int | None = None,
) -> None:
    """Raise before mutation when projection/budget inputs are illegal."""
    if detail is not None and detail not in VALID_WRITE_DETAILS:
        raise InvalidWriteProjectionError(detail, allowed=VALID_WRITE_DETAILS)
    for field in _parse_field_list(fields):
        if field not in VALID_WRITE_PROJECTION_FIELDS:
            raise InvalidWriteProjectionError(field)
    if budget_policy is not None:
        resolve_policy(response_budget_bytes=response_budget_bytes, budget_policy=budget_policy)
    if response_budget_bytes is not None:
        if response_budget_bytes < 0:
            raise WriteBudgetTooSmallError(response_budget_bytes, minimum_write_receipt_bytes())
        minimum = minimum_write_receipt_bytes()
        if response_budget_bytes < minimum:
            raise WriteBudgetTooSmallError(response_budget_bytes, minimum)


def default_write_budget_bytes(response_budget_bytes: int | None) -> int:
    if response_budget_bytes is not None:
        return response_budget_bytes
    return DEFAULT_WRITE_RECEIPT_BUDGET_BYTES


def _identity_subset(row: Mapping[str, object], keys: tuple[str, ...]) -> dict[str, object]:
    return {key: row[key] for key in keys if key in row}


_WRITE_SUMMARY_TRUNCATE = 200
_WRITE_SUMMARY_TEXT_KEYS: frozenset[str] = frozenset(
    {
        "rationale",
        "objective",
        "focus",
        "description",
        "detail",
        "error",
        "verification_evidence",
        "resolution_notes",
        "fix",
        "summary",
        "command",
        "result",
    }
)


def _truncate_summary_value(value: object, *, depth: int = 0, field: str | None = None) -> object:
    if depth > 4:
        return value
    if isinstance(value, str):
        if (field is None or field in _WRITE_SUMMARY_TEXT_KEYS) and len(value) > _WRITE_SUMMARY_TRUNCATE:
            return value[:_WRITE_SUMMARY_TRUNCATE] + "..."
        return value
    if isinstance(value, Mapping):
        return {str(key): _truncate_summary_value(item, depth=depth + 1, field=str(key)) for key, item in value.items()}
    if isinstance(value, list):
        return [_truncate_summary_value(item, depth=depth + 1, field=field) for item in value]
    return value


def _summary_write_data(data: Mapping[str, object]) -> dict[str, object]:
    """Bounded summary projection. Stored bodies are not mutated."""
    summarized = _truncate_summary_value(dict(data))
    return dict(summarized) if isinstance(summarized, dict) else dict(data)


def _identity_write_data(data: Mapping[str, object]) -> dict[str, object]:
    out: dict[str, object] = {}
    for key in (
        "idempotent",
        "inserted",
        "updated",
        "written",
        "findings_recorded_count",
        "event_id",
        "task_ref",
        "partial",
        "side_effect_failures",
        "error",
        "detail",
        "committed_effect_id",
        "committed_event_id",
        "committed_task_revision",
        "branch",
        "decision",
    ):
        if key in data:
            out[key] = data[key]
    decision = data.get("decision")
    if isinstance(decision, Mapping):
        out["decision"] = _identity_subset(decision, _IDENTITY_DECISION_KEYS)
    active = data.get("active")
    if isinstance(active, Mapping):
        out["active"] = _identity_subset(active, _IDENTITY_ACTIVE_KEYS)
    results = data.get("results")
    if isinstance(results, list):
        projected: list[dict[str, object]] = []
        for item in results:
            if isinstance(item, Mapping):
                projected.append({k: item[k] for k in ("finding_id", "action", "reopened") if k in item})
        out["results"] = projected
    return out


def _default_scoped_fetch(envelope: Mapping[str, object]) -> dict[str, object]:
    scope = envelope.get("scope") if isinstance(envelope.get("scope"), Mapping) else {}
    data = envelope.get("data") if isinstance(envelope.get("data"), Mapping) else {}
    mutation = envelope.get("mutation") if isinstance(envelope.get("mutation"), Mapping) else {}
    task_ref = (
        (scope.get("task_ref") if isinstance(scope, Mapping) else None)
        or envelope.get("task_ref")
        or (data.get("task_ref") if isinstance(data, Mapping) else None)
    )
    entity = mutation.get("entity") if isinstance(mutation, Mapping) else None
    if entity == "finding":
        review: dict[str, object] = {
            "operation": "list",
            "task_ref": task_ref,
            "detail": "full",
        }
        token = data.get("event_id") if isinstance(data, Mapping) else None
        if isinstance(token, str) and token.strip():
            review["event_id"] = token.strip()
            return {"tool": "review_findings", "arguments": {"review": review, "response_budget_bytes": 1048576}}
        finding_ids = mutation.get("affected_ids") if isinstance(mutation, Mapping) else None
        if isinstance(finding_ids, list) and finding_ids:
            ids = [str(fid) for fid in finding_ids]
            if len(ids) == 1:
                review["finding_id"] = ids[0]
            else:
                review["finding_ids"] = ids
        return {"tool": "review_findings", "arguments": {"review": review, "response_budget_bytes": 1048576}}
    if entity == "handoff_state":
        return {
            "tool": "get_handoff_state",
            "arguments": {"task_ref": task_ref, "sections": "identity", "detail": "full"},
        }
    decision = data.get("decision") if isinstance(data, Mapping) else None
    decision_id = decision.get("decision") if isinstance(decision, Mapping) else None
    return {
        "tool": "get_handoff_state",
        "arguments": {
            "task_ref": task_ref,
            "sections": "decisions_recent",
            "detail": "full",
            "decision_id_prefix": decision_id,
        },
    }


def _apply_field_projection(envelope: dict[str, object], fields: list[str]) -> dict[str, object]:
    if not fields:
        return envelope
    keep = set(fields) | {"ok", "schema_version", "tool", "scope", "mutation", "task_ref", "data"}
    projected = {key: value for key, value in envelope.items() if key in keep}
    data = envelope.get("data")
    if isinstance(data, dict):
        data_keep = {key: value for key, value in data.items() if key in fields or key in {"truncation"}}
        if "data" in keep:
            projected["data"] = data_keep
    return projected


def _attach_write_truncation(
    shaped: dict[str, object],
    *,
    envelope: Mapping[str, object],
    budget: int,
    actual_bytes: int,
    scoped_fetch: dict[str, object] | None,
    detail_label: str,
) -> dict[str, object]:
    data = dict(shaped.get("data") if isinstance(shaped.get("data"), Mapping) else {})
    data["truncation"] = {
        "truncated": True,
        "reason": "final_envelope_exceeds_budget",
        "requested_bytes": budget,
        "actual_bytes": actual_bytes,
        "scoped_fetch": scoped_fetch or _default_scoped_fetch(envelope),
        "marker": "echo_ho_write_receipt",
    }
    data["detail"] = detail_label
    shaped["data"] = data
    return shaped


def _compact_finding_batch_receipt(
    shaped: dict[str, object],
    *,
    fetch: dict[str, object] | None,
) -> dict[str, object]:
    """Drop unbounded ID lists from a finding-batch receipt; keep the durable token."""
    mutation = shaped.get("mutation")
    if not isinstance(mutation, Mapping) or mutation.get("entity") != "finding":
        return shaped
    affected = mutation.get("affected_ids")
    results = None
    data_in = shaped.get("data") if isinstance(shaped.get("data"), Mapping) else {}
    if isinstance(data_in, Mapping):
        results = data_in.get("results")
    id_count = len(affected) if isinstance(affected, list) else 0
    result_count = len(results) if isinstance(results, list) else 0
    if id_count <= 1 and result_count <= 1:
        return shaped
    compact_mutation = {key: value for key, value in mutation.items() if key != "affected_ids"}
    compact_mutation["affected_count"] = id_count or result_count
    compact = dict(shaped)
    compact["mutation"] = compact_mutation
    compact.pop("warnings", None)
    compact.pop("artifacts", None)
    data = dict(data_in) if isinstance(data_in, Mapping) else {}
    data.pop("results", None)
    data.pop("side_effect_failures", None)
    data.pop("partial", None)
    if fetch is not None:
        trunc = data.get("truncation")
        if isinstance(trunc, Mapping):
            trunc = dict(trunc)
            trunc["scoped_fetch"] = fetch
            data["truncation"] = trunc
    compact["data"] = data
    return compact


def shape_committed_write_envelope(
    envelope: Mapping[str, object],
    *,
    detail: str | None = None,
    fields: str | None = None,
    response_budget_bytes: int | None = None,
    event_id: str | None = None,
    scoped_fetch: dict[str, object] | None = None,
) -> dict[str, object]:
    """Shape a committed write receipt to the default 8192-byte envelope.

    Storage is a separate effect: this function never mutates the DB. Callers
    invoke it *after* the write commits. Tests inject failures here to prove
    committed identity survives a later shaping error. Budget is the actual
    MCP transport envelope (text + structured + wrappers). Truncation
    metadata is attached, then the envelope is re-checked.
    """
    shaped: dict[str, object] = dict(envelope)
    data = dict(shaped["data"]) if isinstance(shaped.get("data"), Mapping) else {}
    if event_id and "event_id" not in data:
        data["event_id"] = event_id
    shaped["data"] = data
    budget = default_write_budget_bytes(response_budget_bytes)
    field_list = _parse_field_list(fields)
    current = mcp_transport_envelope_bytes(shaped)
    requested_detail = detail
    applied_detail = requested_detail or "full"
    budget_loss = False
    if requested_detail == "identity":
        data = _identity_write_data(data)
        shaped["data"] = data
        applied_detail = "identity"
    elif requested_detail == "summary":
        data = _summary_write_data(data)
        shaped["data"] = data
        applied_detail = "summary"
    if field_list:
        shaped = _apply_field_projection(shaped, field_list)
        data = dict(shaped["data"]) if isinstance(shaped.get("data"), Mapping) else data
    if mcp_transport_envelope_bytes(shaped) > budget:
        data = _identity_write_data(data)
        shaped["data"] = data
        applied_detail = "identity"
        budget_loss = True
    fetch = scoped_fetch or _default_scoped_fetch(shaped)
    if budget_loss:
        shaped = _attach_write_truncation(
            shaped,
            envelope=envelope,
            budget=budget,
            actual_bytes=current,
            scoped_fetch=fetch,
            detail_label=applied_detail,
        )
    elif requested_detail in {"identity", "summary"}:
        data = dict(shaped.get("data") if isinstance(shaped.get("data"), Mapping) else {})
        data["detail"] = applied_detail
        data["truncation"] = {
            "truncated": False,
            "reason": "requested_projection",
            "requested_detail": requested_detail,
            "scoped_fetch": fetch,
        }
        shaped["data"] = data
    elif requested_detail:
        data = dict(shaped.get("data") if isinstance(shaped.get("data"), Mapping) else {})
        data["detail"] = applied_detail
        shaped["data"] = data
    # Check AFTER adding truncation / projection metadata.
    if mcp_transport_envelope_bytes(shaped) > budget:
        data = _identity_write_data(dict(shaped.get("data") if isinstance(shaped.get("data"), Mapping) else {}))
        shaped["data"] = data
        shaped = _attach_write_truncation(
            shaped,
            envelope=envelope,
            budget=budget,
            actual_bytes=current,
            scoped_fetch=fetch,
            detail_label="identity",
        )
        shaped = _compact_finding_batch_receipt(shaped, fetch=fetch)
        if mcp_transport_envelope_bytes(shaped) > budget:
            data = dict(shaped.get("data") if isinstance(shaped.get("data"), Mapping) else {})
            existing = data.get("truncation")
            existing_fetch = None
            if isinstance(existing, Mapping):
                existing_fetch = (
                    existing.get("scoped_fetch") if isinstance(existing.get("scoped_fetch"), dict) else None
                )
            data["truncation"] = {
                "truncated": True,
                "reason": "final_envelope_exceeds_budget",
                "scoped_fetch": existing_fetch or fetch,
            }
            data["detail"] = "identity"
            shaped["data"] = data
            shaped.pop("warnings", None)
            shaped.pop("artifacts", None)
            shaped = _compact_finding_batch_receipt(shaped, fetch=existing_fetch or fetch)
    return shaped


def shaping_failure_envelope(
    committed: Mapping[str, object],
    exc: BaseException,
    *,
    event_id: str | None = None,
) -> dict[str, object]:
    """Error envelope that preserves committed write identity after a shaping fault."""
    data_in = committed.get("data") if isinstance(committed.get("data"), Mapping) else {}
    mutation = committed.get("mutation") if isinstance(committed.get("mutation"), Mapping) else {}
    scope = committed.get("scope") if isinstance(committed.get("scope"), Mapping) else {}
    decision = data_in.get("decision") if isinstance(data_in, Mapping) else None
    active = data_in.get("active") if isinstance(data_in, Mapping) else None
    affected = mutation.get("affected_ids") if isinstance(mutation, Mapping) else None
    committed_id = None
    if isinstance(affected, list) and affected:
        committed_id = affected[0]
    elif isinstance(decision, Mapping) and decision.get("id") is not None:
        committed_id = decision.get("id")
    task_ref = (
        (scope.get("task_ref") if isinstance(scope, Mapping) else None)
        or committed.get("task_ref")
        or (data_in.get("task_ref") if isinstance(data_in, Mapping) else None)
    )
    branch = None
    if isinstance(decision, Mapping):
        branch = decision.get("branch")
    if branch is None and isinstance(active, Mapping):
        branch = active.get("target_branch") or active.get("branch")
    if branch is None and isinstance(scope, Mapping):
        branch = scope.get("branch")
    detail = str(exc)
    if len(detail) > _SHAPING_ERROR_DETAIL_MAX:
        detail = detail[:_SHAPING_ERROR_DETAIL_MAX] + "…"
    resolved_event_id = event_id or (data_in.get("event_id") if isinstance(data_in, Mapping) else None)
    data: dict[str, object] = {
        "error": "response_shaping_failed",
        "detail": detail,
        "committed_effect_id": committed_id,
        "committed_event_id": resolved_event_id,
        "event_id": resolved_event_id,
        "task_ref": task_ref,
        "idempotent": data_in.get("idempotent") if isinstance(data_in, Mapping) else False,
    }
    if isinstance(mutation, Mapping) and mutation.get("task_revision") is not None:
        data["committed_task_revision"] = mutation.get("task_revision")
    if branch is not None:
        data["branch"] = branch
    if isinstance(decision, Mapping) and decision.get("decision"):
        data["decision"] = decision.get("decision")
    payload: dict[str, object] = {
        "ok": False,
        "schema_version": committed.get("schema_version", 2),
        "tool": committed.get("tool"),
        "scope": dict(scope) if isinstance(scope, Mapping) else {"task_ref": task_ref},
        "data": data,
        "task_ref": task_ref,
    }
    if isinstance(mutation, Mapping):
        copied = dict(mutation)
        affected_ids = copied.get("affected_ids")
        if isinstance(affected_ids, list) and len(affected_ids) > 1:
            copied.pop("affected_ids", None)
            copied["affected_count"] = len(affected_ids)
        payload["mutation"] = copied
    if mcp_transport_envelope_bytes(payload) > DEFAULT_WRITE_RECEIPT_BUDGET_BYTES:
        try:
            payload = shape_committed_write_envelope(
                payload,
                detail="identity",
                event_id=resolved_event_id if isinstance(resolved_event_id, str) else None,
            )
        except Exception:
            payload = _compact_finding_batch_receipt(
                payload,
                fetch=_default_scoped_fetch(payload),
            )
        payload["ok"] = False
        data = dict(payload.get("data") if isinstance(payload.get("data"), Mapping) else {})
        data["error"] = "response_shaping_failed"
        data["detail"] = detail
        data["committed_effect_id"] = committed_id
        data["committed_event_id"] = resolved_event_id
        data["event_id"] = resolved_event_id
        payload["data"] = data
    return payload


def _drop_key(container: dict[str, object], key: str) -> bool:
    if key in container:
        del container[key]
        return True
    return False


def enforce_final_serialized_budget(
    envelope: dict[str, object],
    *,
    budget_bytes: int,
    scoped_fetch: dict[str, object] | None = None,
) -> dict[str, object]:
    """Last-byte check after continuation/add-ons are attached.

    Drops optional add-ons (never identity keys) and records explicit
    truncation plus a scoped fetch path. Never labels a shortened payload full.
    Sizes the actual MCP transport envelope. Truncation metadata is attached,
    then the envelope is re-checked.
    """
    original = mcp_transport_envelope_bytes(envelope)
    data = envelope.get("data")
    if not isinstance(data, dict):
        return envelope
    dropped: list[str] = []
    packet_id = None
    continuation = data.get("continuation")
    if isinstance(continuation, Mapping):
        packet_id = continuation.get("packet_id")
    target_before_truncation = max(512, budget_bytes - TRANSPORT_TRUNCATION_RESERVE_BYTES)

    def _over(limit: int) -> bool:
        return mcp_transport_envelope_bytes(envelope) > limit

    if not _over(budget_bytes):
        return envelope
    for key in _SESSION_ADDON_DROP_ORDER:
        if not _over(target_before_truncation):
            break
        if _drop_key(data, key):
            dropped.append(key)
    for key in _SESSION_ADDON_LAST_RESORT_DROP_ORDER:
        if not _over(target_before_truncation):
            break
        if key in {"open_findings", "touched_files", "context_refresh", "read_budget"}:
            continue
        if _drop_key(data, key):
            dropped.append(key)
    state = data.get("state")
    if isinstance(state, dict) and _over(target_before_truncation):
        for key in _NESTED_STATE_DROP_ORDER:
            if not _over(target_before_truncation):
                break
            if _drop_key(state, key):
                dropped.append(f"state.{key}")
        active = state.get("active")
        if isinstance(active, dict) and _over(target_before_truncation):
            for bulky in ("objective", "focus"):
                value = active.get(bulky)
                if isinstance(value, str) and len(value) > 120:
                    active[bulky] = value[:120] + "…"
                    dropped.append(f"state.active.{bulky}")
                    if not _over(target_before_truncation):
                        break
        limits = state.get("limits")
        if isinstance(limits, dict) and "write" in limits and _over(target_before_truncation):
            del limits["write"]
            dropped.append("state.limits.write")
    task_ref = envelope.get("task_ref")
    scope = envelope.get("scope")
    if isinstance(scope, Mapping) and not task_ref:
        task_ref = scope.get("task_ref")
    fetch = scoped_fetch or continuation_scoped_fetch(task_ref=task_ref, packet_id=packet_id)

    def _apply_truncation(*, compact: bool = False) -> None:
        if compact:
            data["truncation"] = {
                "truncated": True,
                "reason": "final_envelope_exceeds_budget",
                "dropped_sections": dropped,
                "scoped_fetch": fetch,
                "marker": "echo_ho_final_budget",
            }
        else:
            data["truncation"] = {
                "truncated": True,
                "reason": "final_envelope_exceeds_budget",
                "requested_bytes": budget_bytes,
                "actual_bytes": original,
                "final_bytes": mcp_transport_envelope_bytes(envelope),
                "dropped_sections": dropped,
                "scoped_fetch": fetch,
                "marker": "echo_ho_final_budget",
            }
        read_shape = data.get("read_shape")
        if isinstance(read_shape, dict):
            state_shape = read_shape.get("state")
            if isinstance(state_shape, dict) and state_shape.get("detail") == "full":
                state_shape = dict(state_shape)
                state_shape["detail"] = "truncated"
                read_shape = dict(read_shape)
                read_shape["state"] = state_shape
                data["read_shape"] = read_shape
            session_shape = read_shape.get("session") if isinstance(read_shape, dict) else None
            if isinstance(session_shape, dict):
                omitted = list(session_shape.get("omitted_sections") or [])
                for key in dropped:
                    name = key.split(".", 1)[-1]
                    if name not in omitted:
                        omitted.append(name)
                session_shape = dict(session_shape)
                session_shape["omitted_sections"] = omitted
                read_shape = dict(read_shape) if isinstance(read_shape, dict) else {}
                read_shape["session"] = session_shape
                data["read_shape"] = read_shape
        envelope["data"] = data

    _apply_truncation()
    # Check AFTER adding truncation metadata.
    if _over(budget_bytes):
        for key in (*_SESSION_ADDON_DROP_ORDER, *_SESSION_ADDON_LAST_RESORT_DROP_ORDER):
            if not _over(budget_bytes):
                break
            if _drop_key(data, key) and key not in dropped:
                dropped.append(key)
        state = data.get("state")
        if isinstance(state, dict) and _over(budget_bytes):
            for key in _NESTED_STATE_DROP_ORDER:
                if not _over(budget_bytes):
                    break
                if _drop_key(state, key) and f"state.{key}" not in dropped:
                    dropped.append(f"state.{key}")
            active = state.get("active")
            if isinstance(active, dict) and _over(budget_bytes):
                for bulky in ("objective", "focus"):
                    value = active.get(bulky)
                    if isinstance(value, str) and len(value) > 40:
                        active[bulky] = value[:40] + "…"
            limits = state.get("limits")
            if isinstance(limits, dict) and "write" in limits and _over(budget_bytes):
                del limits["write"]
                if "state.limits.write" not in dropped:
                    dropped.append("state.limits.write")
        if _over(budget_bytes) and "warnings" in envelope:
            envelope.pop("warnings", None)
            dropped.append("warnings")
        if _over(budget_bytes):
            rb = data.get("read_budget")
            if isinstance(rb, dict):
                data["read_budget"] = {
                    key: rb[key]
                    for key in (
                        "requested_bytes",
                        "policy",
                        "omitted_sections",
                        "estimated_initial_bytes",
                        "estimated_after_bytes",
                    )
                    if key in rb
                }
        _apply_truncation(compact=_over(budget_bytes))
    return envelope


def _stamp_final_bytes(
    envelope: dict[str, object], *, content_carrier: str = MCP_CONTENT_CARRIER_BOTH
) -> dict[str, object]:
    """Set self-inclusive transport counters after all other envelope mutations.

    Planner estimates and requested budgets retain their original meanings;
    ``final_bytes`` measures the complete CallToolResult for the explicit
    ``content_carrier`` supplied by MCP registration: structured-only
    (the default registration) or text plus structured (``both``). A direct,
    non-MCP call has no registration and measures the dual ``both`` carrier.
    Callers still cap the dual carrier, the larger of the two. Inserting or
    changing the counters can change their own digit counts, so remeasure to
    a bounded fixed point rather than reporting a pre-insertion size.
    """
    data = envelope.get("data")
    if not isinstance(data, dict):
        return envelope
    metadata = [data[key] for key in ("truncation", "read_budget") if isinstance(data.get(key), dict)]
    if not metadata:
        return envelope
    for _attempt in range(3):
        final_bytes = registered_carrier_envelope_bytes(envelope, content_carrier=content_carrier)
        for counters in metadata:
            counters["final_bytes"] = final_bytes
        if registered_carrier_envelope_bytes(envelope, content_carrier=content_carrier) == final_bytes:
            return envelope
    raise ValueError("Final response byte metadata did not converge after 3 passes.")
