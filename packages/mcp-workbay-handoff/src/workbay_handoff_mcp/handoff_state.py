"""Handoff state domain module.

Contains set_handoff_state and get_handoff_state.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .concept_embed_hook import embed_concept_on_write
from .orientation_reads import orientation_read_boundary_active, record_orientation_read
from .projection_event_dedupe import (
    claim_projection_event,
    clear_projection_event_claim,
    complete_projection_event,
)
from .read_budget import (
    VALID_POLICIES,
    UnknownBudgetPolicyError,
    budget_payload,
    plan_state_read,
    resolve_policy,
)
from .read_profiles import (
    VALID_PROFILE_NAMES,
    UnknownProfileError,
    resolve_state_shape,
    state_shape_payload,
)
from .shared_db_utils import _fetch_handoff_rows
from .shared_primitives import (
    HANDOFF_ACTIVE_STATUSES,
    RATIONALE_HARD_LIMIT_CHARS,
    RATIONALE_SOFT_LIMIT_CHARS,
    SLICE_COMPLETE_HARD_LIMIT_CHARS,
    SLICE_COMPLETE_REQUIRED_SECTIONS,
    _enrich_handoff_active,
    _envelope,
    _get_handoff_row_for_task,
    _normalize_optional_text,
    _resolve_current_lane_row,
    _resolve_workspace_handoff_row,
    _row_to_dict,
)
from .shared_schema import _get_db_connection
from .shared_write_context import WriteActor, _resolve_write_actor, collect_target_context_warnings
from .slice_decision import PREFIXED_SLICE_COMPLETE_RE, extract_slice_label, is_slice_complete_decision
from .write_contracts import registry_export as _write_contracts_registry_export


def set_handoff_state(
    task_ref: str,
    objective: str | None = None,
    focus: str | None = None,
    status: str = "in_progress",
    expected_revision: int | None = None,
    actor: WriteActor | None = None,
    target_branch: str | None = None,
    target_worktree_path: str | None = None,
    task_plan_path: str | None = None,
    event_id: str | None = None,
) -> dict:
    with _get_db_connection() as conn:
        result = _set_handoff_state_with_conn(
            conn,
            task_ref=task_ref,
            objective=objective,
            focus=focus,
            status=status,
            expected_revision=expected_revision,
            actor=actor,
            target_branch=target_branch,
            target_worktree_path=target_worktree_path,
            task_plan_path=task_plan_path,
            event_id=event_id,
        )
    if result.get("ok"):
        from .current_task_rendering import _write_per_task_projection  # noqa: PLC0415

        scope = result.get("scope") if isinstance(result, dict) else None
        projected_task_ref = scope.get("task_ref") if isinstance(scope, dict) else task_ref
        _write_per_task_projection(str(projected_task_ref or task_ref))
        # Embed objective/focus/plan-path after the row committed (best-effort).
        # None params (e.g. a status-only update) are a no-op, so unchanged text
        # never re-embeds. Plan-path seeding keeps tasks with a declared plan
        # non-empty in the embedding store (PR-0105-03 cold-start).
        embed_concept_on_write("handoff_state.objective", task_ref, task_ref, objective)
        embed_concept_on_write("handoff_state.focus", task_ref, task_ref, focus)
        embed_concept_on_write("handoff_state.task_plan_path", task_ref, task_ref, task_plan_path)
    return result


def _set_handoff_state_with_conn(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    objective: str | None = None,
    focus: str | None = None,
    status: str = "in_progress",
    expected_revision: int | None = None,
    actor: WriteActor | None = None,
    target_branch: str | None = None,
    target_worktree_path: str | None = None,
    task_plan_path: str | None = None,
    event_id: str | None = None,
) -> dict:
    _tool = "set_handoff_state"
    if status not in HANDOFF_ACTIVE_STATUSES:
        return _envelope(
            ok=False,
            tool=_tool,
            task_ref=task_ref,
            data={"error": f"Invalid status. Valid: {', '.join(sorted(HANDOFF_ACTIVE_STATUSES))}"},
        )
    task_row = _get_handoff_row_for_task(conn, task_ref)
    ctx = _resolve_write_actor(
        conn,
        actor,
        task_ref=task_ref,
        derive_worktree_for_branch=(target_branch if task_row is not None and target_branch is not None else None),
    )
    if task_row is not None:
        task_ref = str(task_row["task_ref"])
    if task_row is None:
        if objective is None:
            return _envelope(
                ok=False,
                tool=_tool,
                task_ref=task_ref,
                data={"error": "objective is required when creating a new handoff state."},
            )
        existing_event = claim_projection_event(
            conn,
            event_id=event_id,
            tool_name=_tool,
            target_table="handoff_state",
            task_ref=task_ref,
        )
        if existing_event is not None:
            event_task_ref = str(existing_event["task_ref"]) if existing_event["task_ref"] else task_ref
            active = _enrich_handoff_active(_row_to_dict(_get_handoff_row_for_task(conn, event_task_ref)))
            task_revision = active.get("revision") if active is not None else None
            return _envelope(
                ok=True,
                tool=_tool,
                task_ref=event_task_ref,
                data={
                    "inserted": False,
                    "updated": False,
                    "idempotent": True,
                    "active": active,
                },
                mutation={
                    "entity": "handoff_state",
                    "operation": "noop",
                    "task_revision": task_revision,
                },
            )
        effective_target_worktree_path = target_worktree_path
        conn.execute(
            """
            INSERT INTO handoff_state (
                id, task_ref, objective, focus, status, target_branch, target_worktree_path,
                task_plan_path,
                revision, updated_at, updated_by, updated_branch, updated_commit_sha
            ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, 0, datetime('now'), ?, ?, ?)
            """,
            (
                task_ref,
                objective,
                focus,
                status,
                target_branch,
                effective_target_worktree_path,
                _normalize_optional_text(task_plan_path) if task_plan_path else None,
                ctx.agent,
                ctx.branch,
                ctx.commit_sha,
            ),
        )
        active = _enrich_handoff_active(_row_to_dict(_get_handoff_row_for_task(conn, task_ref)))
        complete_projection_event(
            conn,
            event_id=event_id,
            target_table="handoff_state",
            target_id=None,
            task_ref=task_ref,
        )
        return _envelope(
            ok=True,
            tool=_tool,
            task_ref=task_ref,
            data={"inserted": True, "active": active, "idempotent": False},
            mutation={"entity": "handoff_state", "operation": "insert", "task_revision": 0},
        )
    if expected_revision is None:
        return _envelope(
            ok=False,
            tool=_tool,
            task_ref=task_ref,
            data={
                "error": (
                    "expected_revision is required for updates. "
                    "Fetch the active row first via get_handoff_state(sections='identity') "
                    "and pass its revision field as expected_revision."
                ),
                "current_revision": int(task_row["revision"]),
            },
        )
    existing_event = claim_projection_event(
        conn,
        event_id=event_id,
        tool_name=_tool,
        target_table="handoff_state",
        task_ref=task_ref,
    )
    if existing_event is not None:
        event_task_ref = str(existing_event["task_ref"]) if existing_event["task_ref"] else task_ref
        active = _enrich_handoff_active(_row_to_dict(_get_handoff_row_for_task(conn, event_task_ref)))
        task_revision = active.get("revision") if active is not None else None
        return _envelope(
            ok=True,
            tool=_tool,
            task_ref=event_task_ref,
            data={
                "inserted": False,
                "updated": False,
                "idempotent": True,
                "active": active,
            },
            mutation={
                "entity": "handoff_state",
                "operation": "noop",
                "task_revision": task_revision,
            },
        )
    resolved_objective = objective if objective is not None else str(task_row["objective"])
    resolved_focus = (
        focus if focus is not None else (_normalize_optional_text(task_row["focus"]) if task_row["focus"] else None)
    )
    resolved_target_branch = (
        target_branch
        if target_branch is not None
        else (_normalize_optional_text(task_row["target_branch"]) if task_row["target_branch"] else None)
    )
    resolved_target_worktree_path = (
        target_worktree_path
        if target_worktree_path is not None
        else (_normalize_optional_text(task_row["target_worktree_path"]) if task_row["target_worktree_path"] else None)
    )
    existing_task_plan_path = (
        _normalize_optional_text(task_row["task_plan_path"])
        if "task_plan_path" in task_row.keys() and task_row["task_plan_path"]
        else None
    )
    resolved_task_plan_path = (
        (_normalize_optional_text(task_plan_path) if task_plan_path else None)
        if task_plan_path is not None
        else existing_task_plan_path
    )
    warnings = collect_target_context_warnings(
        conn,
        ctx,
        target_branch=resolved_target_branch,
        target_worktree_path=resolved_target_worktree_path,
        task_ref=task_ref,
    )
    updated = conn.execute(
        """
        UPDATE handoff_state
        SET objective = ?, focus = ?, status = ?,
            target_branch = ?, target_worktree_path = ?,
            task_plan_path = ?,
            revision = revision + 1, updated_at = datetime('now'),
            updated_by = ?, updated_branch = ?, updated_commit_sha = ?
        WHERE task_ref = ? AND revision = ?
        """,
        (
            resolved_objective,
            resolved_focus,
            status,
            resolved_target_branch,
            resolved_target_worktree_path,
            resolved_task_plan_path,
            ctx.agent,
            ctx.branch,
            ctx.commit_sha,
            task_ref,
            expected_revision,
        ),
    )
    if updated.rowcount == 0:
        clear_projection_event_claim(conn, event_id)
        latest = conn.execute("SELECT revision FROM handoff_state WHERE task_ref = ?", (task_ref,)).fetchone()
        return _envelope(
            ok=False,
            tool=_tool,
            task_ref=task_ref,
            data={
                "error": "Revision conflict.",
                "expected_revision": expected_revision,
                "current_revision": int(latest["revision"]) if latest else None,
            },
        )
    active = _enrich_handoff_active(_row_to_dict(_get_handoff_row_for_task(conn, task_ref)))
    if active is None:
        return _envelope(
            ok=False,
            tool=_tool,
            task_ref=task_ref,
            data={"error": "Active handoff state missing after update."},
        )
    complete_projection_event(
        conn,
        event_id=event_id,
        target_table="handoff_state",
        target_id=None,
        task_ref=task_ref,
    )
    return _envelope(
        ok=True,
        tool=_tool,
        task_ref=task_ref,
        data={"updated": True, "active": active, "idempotent": False},
        mutation={"entity": "handoff_state", "operation": "update", "task_revision": active.get("revision")},
        warnings=warnings or None,
    )


_VALID_SECTIONS = frozenset(
    {
        # 'active' and 'limits' are always included as cheap identity data
        # and are not selectable/excludable via the sections parameter.
        "current_lane",
        "blockers_open",
        "actions_pending",
        "decisions_recent",
        "slices_completed",
        "tests_recent",
        "findings_open",
        "worktree_lanes",
        "worker_reports_recent",
        "lane_messages_open",
    }
)

_VALID_DETAIL_LEVELS = frozenset({"full", "summary"})

_SUMMARY_TRUNCATE_LENGTH = 200


def _truncate_for_summary(row: dict, fields: tuple[str, ...]) -> dict:
    """Return a shallow copy with long text fields truncated."""
    out = dict(row)
    for field in fields:
        value = out.get(field)
        if isinstance(value, str) and len(value) > _SUMMARY_TRUNCATE_LENGTH:
            out[field] = value[:_SUMMARY_TRUNCATE_LENGTH] + "..."
    return out


def _escape_sql_like_fragment(value: str) -> str:
    """Escape literal LIKE wildcard characters for SQLite prefix matching."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


_IDENTITY_TOKEN = "identity"
_VERBOSE_HANDOFF_LIMIT = 10_000


@dataclass(frozen=True)
class HandoffReadLimits:
    """Normalized row limits for the bounded get_handoff_state read surface."""

    blockers: int
    actions: int
    decisions: int
    slices: int
    tests: int
    findings: int

    @classmethod
    def from_requested(
        cls,
        *,
        blockers: int,
        actions: int,
        decisions: int,
        slices: int,
        tests: int,
        findings: int,
    ) -> "HandoffReadLimits":
        return cls(
            blockers=max(1, blockers),
            actions=max(1, actions),
            decisions=max(1, decisions),
            slices=max(1, slices),
            tests=max(1, tests),
            findings=max(1, findings),
        )

    def query_limit(self, size: int, *, verbose: bool) -> int:
        return _VERBOSE_HANDOFF_LIMIT if verbose else size

    def as_payload(self) -> dict[str, int]:
        return {
            "blockers": self.blockers,
            "actions": self.actions,
            "decisions": self.decisions,
            "slices": self.slices,
            "tests": self.tests,
            "findings": self.findings,
        }


def _parse_sections(sections: str | None) -> frozenset[str] | None:
    """Parse a comma-separated sections string. Returns None for 'all'.

    The reserved token ``identity`` explicitly requests an identity-only
    response (active + limits only, no data sections). When ``identity``
    is present, all other tokens are ignored.

    Invalid section names are silently dropped. If no valid names remain
    after filtering, returns an empty frozenset (same identity-only shape).
    Callers that want all sections should pass sections=None (the default).
    """
    if sections is None:
        return None
    parts = frozenset(s.strip().lower() for s in sections.split(",") if s.strip())
    if _IDENTITY_TOKEN in parts:
        return frozenset()
    return parts & _VALID_SECTIONS


def get_handoff_state(
    task_ref: str | None = None,
    branch: str | None = None,
    top_n_blockers: int | None = None,
    top_n_actions: int | None = None,
    top_n_decisions: int | None = None,
    top_n_slices: int | None = None,
    top_n_tests: int | None = None,
    top_n_findings: int | None = None,
    verbose: bool = False,
    include_archived: bool = True,
    sections: str | None = None,
    detail: str | None = None,
    decision_fields: list[str] | None = None,
    decision_branch: str | None = None,
    decision_commit_sha: str | None = None,
    decision_lane_id: str | None = None,
    decision_id_prefix: str | None = None,
    read_profile: str | None = None,
    source: str | None = None,
    session: str | None = None,
    response_budget_bytes: int | None = None,
    budget_policy: str | None = None,
) -> dict:
    # internal: ``None`` is the sentinel for "argument not supplied".
    # The profile (or full_debug baseline) provides the default; explicit
    # caller overrides win. This keeps existing default-call behavior
    # unchanged because the full_debug baseline matches DEFAULT_HANDOFF_LIMITS.
    try:
        shape = resolve_state_shape(
            read_profile=read_profile,
            sections=sections,
            detail=detail,
            top_n_blockers=top_n_blockers,
            top_n_actions=top_n_actions,
            top_n_decisions=top_n_decisions,
            top_n_slices=top_n_slices,
            top_n_tests=top_n_tests,
            top_n_findings=top_n_findings,
        )
    except UnknownProfileError as exc:
        return _envelope(
            ok=False,
            tool="get_handoff_state",
            task_ref=task_ref,
            data={
                "error": (f"Unknown read_profile {exc.name!r}. Valid profiles: {list(VALID_PROFILE_NAMES)}."),
                "valid_profiles": list(VALID_PROFILE_NAMES),
            },
        )

    # internal: budget planner runs before heavy section fetches.
    try:
        effective_policy = resolve_policy(response_budget_bytes=response_budget_bytes, budget_policy=budget_policy)
    except UnknownBudgetPolicyError as exc:
        return _envelope(
            ok=False,
            tool="get_handoff_state",
            task_ref=task_ref,
            data={
                "error": (f"Unknown budget_policy {exc.policy!r}. Valid policies: {list(VALID_POLICIES)}."),
                "valid_policies": list(VALID_POLICIES),
            },
        )
    planned_shape, budget_plan = plan_state_read(
        shape=shape,
        response_budget_bytes=response_budget_bytes,
        budget_policy=effective_policy,
    )
    if budget_plan.fail_now:
        return _envelope(
            ok=False,
            tool="get_handoff_state",
            task_ref=task_ref,
            data={
                "error": (
                    f"response_budget_bytes={response_budget_bytes} cannot fit requested shape "
                    f"(estimated {budget_plan.estimated_initial_bytes} bytes). "
                    "Retry with the suggested narrower profile or budget_policy='auto_summary'."
                ),
                "read_budget": budget_payload(budget_plan),
                "read_shape": state_shape_payload(shape),
            },
        )
    shape = planned_shape
    budget_omitted = set(budget_plan.omitted_sections)

    detail_resolved = shape.detail if shape.detail in _VALID_DETAIL_LEVELS else "full"
    requested_sections = _parse_sections(shape.sections)
    limits = HandoffReadLimits.from_requested(
        blockers=shape.top_n_blockers,
        actions=shape.top_n_actions,
        decisions=shape.top_n_decisions,
        slices=shape.top_n_slices,
        tests=shape.top_n_tests,
        findings=shape.top_n_findings,
    )
    # Preserve the old internal name so the rest of the body reads naturally.
    detail = detail_resolved

    def _want(section: str) -> bool:
        if section in budget_omitted:
            return False
        return requested_sections is None or section in requested_sections

    decision_param_present = any(
        p is not None
        for p in (decision_fields, decision_branch, decision_commit_sha, decision_lane_id, decision_id_prefix)
    )
    if decision_param_present and not _want("decisions_recent"):
        return _envelope(
            ok=False,
            tool="get_handoff_state",
            data={
                "error": (
                    "decision_fields/decision_branch/decision_commit_sha/decision_lane_id/decision_id_prefix "
                    "are scoped to the decisions_recent section. Include 'decisions_recent' in sections "
                    "(or omit sections) to use them."
                )
            },
        )

    decision_projection_fields: list[str] | None = None
    if decision_fields is not None:
        from .core import _VALID_DECISION_PROJECTION_FIELDS  # noqa: PLC0415

        invalid = sorted({f for f in decision_fields if f not in _VALID_DECISION_PROJECTION_FIELDS})
        if invalid:
            return _envelope(
                ok=False,
                tool="get_handoff_state",
                data={
                    "error": (
                        f"Invalid decision_fields: {invalid}. Valid: {sorted(_VALID_DECISION_PROJECTION_FIELDS)}."
                    )
                },
            )
        seen: set[str] = set()
        decision_projection_fields = []
        for f in decision_fields:
            if f not in seen:
                seen.add(f)
                decision_projection_fields.append(f)

    with _get_db_connection() as conn:
        resolution_outcome = "explicit" if task_ref is not None else "resolved"
        if task_ref is None:
            any_row = conn.execute("SELECT 1 FROM handoff_state LIMIT 1").fetchone()
            if any_row is None:
                return _envelope(
                    ok=True, tool="get_handoff_state", data={"active": None, "message": "No active handoff state."}
                )
            try:
                resolved_row = _resolve_workspace_handoff_row(conn, branch=branch, honor_active_task_pin=True)
            except ValueError as exc:
                from .shared_write_context import AmbiguousWorkspaceContextError  # noqa: PLC0415

                if orientation_read_boundary_active():
                    record_orientation_read(
                        conn,
                        tool="get_handoff_state",
                        task_ref=task_ref or "__unresolved__",
                        resolution_outcome="ambiguous",
                        source=source,
                        session=session,
                        read_profile=read_profile,
                    )
                    conn.commit()
                error_payload: dict = {"error": str(exc)}
                if isinstance(exc, AmbiguousWorkspaceContextError):
                    error_payload["candidates"] = exc.candidates
                    error_payload["resolution"] = (
                        "Pass task_ref=<one of the listed task_refs>, set WORKBAY_LANE_ID or "
                        "WORKBAY_HANDOFF_ACTIVE_TASK, or pass branch=<target_branch> to "
                        "disambiguate; or run `make task-reap` (dry-run) then "
                        "`make task-reap REAP_ARGS=--apply` to close closeable rows only."
                    )
                return _envelope(ok=False, tool="get_handoff_state", data=error_payload)
            if resolved_row is None:
                return _envelope(
                    ok=True, tool="get_handoff_state", data={"active": None, "message": "No active handoff state."}
                )
            resolved_task_ref = str(resolved_row["task_ref"])
            active_row: sqlite3.Row | None = resolved_row
            resolution_outcome = "branch" if branch is not None else "resolved"
        else:
            active_row = _get_handoff_row_for_task(conn, task_ref)
            resolved_task_ref = str(active_row["task_ref"]) if active_row is not None else task_ref
        active = _enrich_handoff_active(_row_to_dict(active_row)) if active_row is not None else None
        if orientation_read_boundary_active():
            record_orientation_read(
                conn,
                tool="get_handoff_state",
                task_ref=resolved_task_ref,
                resolution_outcome=resolution_outcome,
                source=source,
                session=session,
                read_profile=read_profile,
            )
            conn.commit()

        def _apply_detail(rows: list[dict], fields: tuple[str, ...]) -> list[dict]:
            if detail == "summary":
                return [_truncate_for_summary(r, fields) for r in rows]
            return rows

        result: dict = {
            "ok": True,
            "task_ref": resolved_task_ref,
        }

        # Always include active and limits (cheap identity data)
        result["active"] = active
        result["limits"] = {
            **limits.as_payload(),
            "write": {
                "rationale_soft_chars": RATIONALE_SOFT_LIMIT_CHARS,
                "rationale_hard_chars": RATIONALE_HARD_LIMIT_CHARS,
                "slice_complete_hard_chars": SLICE_COMPLETE_HARD_LIMIT_CHARS,
                "slice_complete_required_sections": list(SLICE_COMPLETE_REQUIRED_SECTIONS),
                "slice_complete_decision_id": {
                    "canonical_form": "<author_tag>_slice_complete_<work_ref>_<slug>",
                    "regex": PREFIXED_SLICE_COMPLETE_RE.pattern,
                    "segment_rules": {
                        "author_tag": "[a-z]{2,12} lowercase letters only.",
                        "work_ref": "[A-Za-z0-9_-]+; task or work reference with letters, digits, underscores, or hyphens.",
                        "slug": r"\w+; letters, digits, and underscores only in the final segment (hyphens rejected).",
                    },
                    "valid_examples": [
                        "codex_slice_complete_plan0005_render_budget_benchmark",
                        "copilot_slice_complete_internal_hook_removed_followup",
                    ],
                    "legacy_write_note": (
                        "Legacy slice_complete_<short_label> ids remain readable for historical rows, "
                        "but new slice-complete writes must use the prefixed canonical form."
                    ),
                },
                # internal / BR-04: publish the write-contract
                # registry through limits.write so callers can discover
                # required fields and field grammars without importing
                # internals. Mirrors the typed Pydantic schemas in api.py
                # and review_findings_api.py.
                "tools": _write_contracts_registry_export(),
            },
        }

        if _want("current_lane"):
            current_lane_row = _resolve_current_lane_row(conn, resolved_task_ref)
            result["current_lane"] = _row_to_dict(current_lane_row)
        else:
            current_lane_row = None

        if _want("blockers_open"):
            result["blockers_open"] = _fetch_handoff_rows(
                conn,
                table="blockers",
                where_sql="task_ref = ? AND status = 'open'",
                order_sql="created_at DESC",
                limit=limits.query_limit(limits.blockers, verbose=verbose),
                params=(resolved_task_ref,),
            )

        if _want("actions_pending"):
            result["actions_pending"] = _fetch_handoff_rows(
                conn,
                table="next_actions",
                where_sql="task_ref = ? AND status = 'pending'",
                order_sql="priority ASC, created_at ASC",
                limit=limits.query_limit(limits.actions, verbose=verbose),
                params=(resolved_task_ref,),
            )

        if _want("decisions_recent"):
            decisions_where_sql = "task_ref = ?"
            decisions_params: list[object] = [resolved_task_ref]
            if decision_branch is not None:
                decisions_where_sql += " AND branch = ?"
                decisions_params.append(decision_branch)
            if decision_commit_sha is not None:
                decisions_where_sql += " AND commit_sha = ?"
                decisions_params.append(decision_commit_sha)
            if decision_lane_id is not None:
                decisions_where_sql += " AND lane_id = ?"
                decisions_params.append(decision_lane_id)
            if decision_id_prefix is not None:
                decisions_where_sql += " AND decision LIKE ? ESCAPE '\\'"
                decisions_params.append(_escape_sql_like_fragment(decision_id_prefix) + "%")
            rows = _fetch_handoff_rows(
                conn,
                table="decisions",
                where_sql=decisions_where_sql,
                order_sql="created_at DESC",
                limit=limits.query_limit(limits.decisions, verbose=verbose),
                params=tuple(decisions_params),
            )
            if decision_projection_fields is not None:
                rows = [{f: row.get(f) for f in decision_projection_fields} for row in rows]
                result["decisions_recent"] = _apply_detail(rows, ("rationale",))
            else:
                result["decisions_recent"] = _apply_detail(rows, ("rationale",))

        if _want("slices_completed"):
            # Escape the literal underscores in the slice-complete tokens: '_' is a
            # single-char SQL LIKE wildcard, so an unescaped pattern false-matches
            # unrelated decisions and consumes the LIMIT window before the
            # is_slice_complete_decision post-filter below.
            escaped_slice_prefix = _escape_sql_like_fragment("slice_complete_")
            rows = _fetch_handoff_rows(
                conn,
                table="decisions",
                where_sql="task_ref = ? AND (decision LIKE ? ESCAPE '\\' OR decision LIKE ? ESCAPE '\\')",
                order_sql="created_at DESC",
                limit=limits.query_limit(limits.slices, verbose=verbose),
                params=(
                    resolved_task_ref,
                    f"{escaped_slice_prefix}%",
                    f"%\\_{escaped_slice_prefix}%",
                ),
            )
            slice_rows = []
            for row in rows:
                decision_id = str(row.get("decision") or "")
                if not is_slice_complete_decision(decision_id):
                    continue
                shaped = dict(row)
                shaped["slice_label"] = extract_slice_label(decision_id)
                slice_rows.append(shaped)
            result["slices_completed"] = _apply_detail(slice_rows, ("rationale",))

        if _want("tests_recent"):
            rows = _fetch_handoff_rows(
                conn,
                table="verified_tests",
                where_sql="task_ref = ?",
                order_sql="verified_at DESC",
                limit=limits.query_limit(limits.tests, verbose=verbose),
                params=(resolved_task_ref,),
            )
            result["tests_recent"] = _apply_detail(rows, ("command", "result"))

        if _want("findings_open"):
            rows = _fetch_handoff_rows(
                conn,
                table="review_findings",
                where_sql="task_ref = ? AND status = 'open'",
                order_sql="CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1 WHEN 'low' THEN 2 END, created_at DESC",
                limit=limits.query_limit(limits.findings, verbose=verbose),
                params=(resolved_task_ref,),
            )
            result["findings_open"] = _apply_detail(
                rows, ("description", "fix", "resolution_notes", "verification_evidence")
            )

        if _want("worktree_lanes"):
            result["worktree_lanes"] = _fetch_handoff_rows(
                conn,
                table="worktree_lanes",
                where_sql="task_ref = ?",
                order_sql="updated_at DESC, id DESC",
                limit=50,
                params=(resolved_task_ref,),
            )

        if _want("worker_reports_recent"):
            result["worker_reports_recent"] = _fetch_handoff_rows(
                conn,
                table="worker_reports",
                where_sql="task_ref = ?",
                order_sql="created_at DESC, id DESC",
                limit=limits.query_limit(limits.tests, verbose=verbose),
                params=(resolved_task_ref,),
            )

        if _want("lane_messages_open"):
            # Always resolve lane for message scoping, even if current_lane
            # section was not requested.
            if current_lane_row is None:
                current_lane_row = _resolve_current_lane_row(conn, resolved_task_ref)
            lane_messages_where_sql = "task_ref = ? AND status = 'open'"
            lane_messages_params: tuple[object, ...] = (resolved_task_ref,)
            if current_lane_row is not None:
                lane_messages_where_sql += " AND lane_id = ?"
                lane_messages_params = (resolved_task_ref, str(current_lane_row["lane_id"]))
            result["lane_messages_open"] = _fetch_handoff_rows(
                conn,
                table="lane_messages",
                where_sql=lane_messages_where_sql,
                order_sql="updated_at DESC, id DESC",
                limit=50,
                params=lane_messages_params,
            )

        from .compaction import compute_compaction_advisory  # noqa: PLC0415
        from .runtime import get_runtime_config  # noqa: PLC0415

        try:
            runtime = get_runtime_config()
            workspace_root = runtime.compaction_config_root
        except Exception:  # noqa: BLE001
            workspace_root = None
        if workspace_root is not None:
            advisory = compute_compaction_advisory(
                workspace_root=workspace_root,
                task_ref=resolved_task_ref,
            )
            advisory_field_name = "compaction_recommended"
            try:
                from .compaction_contract import load_compaction_contract  # noqa: PLC0415

                advisory_field_name = load_compaction_contract(workspace_root).advisory_field
            except Exception:  # noqa: BLE001
                pass
            result[advisory_field_name] = bool(advisory["recommended"])
            result["latest_compaction_id"] = advisory.get("latest_compaction_id")
            result["compaction_advisory"] = advisory

        if shape.requested_profile is not None:
            result["read_shape"] = state_shape_payload(shape, omitted_sections=list(budget_plan.omitted_sections))
        # internal: attach read_budget whenever a budget was supplied
        # or the planner applied any reductions.
        if response_budget_bytes is not None or budget_plan.applied_reductions:
            result["read_budget"] = budget_payload(budget_plan)

        warnings = result.pop("warnings", []) or []
        task_ref_val = result.pop("task_ref", resolved_task_ref)
        result.pop("ok", None)
        return _envelope(
            ok=True,
            tool="get_handoff_state",
            data=result,
            task_ref=task_ref_val,
            warnings=warnings,
        )
