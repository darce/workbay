"""DASHBOARD.txt rendering for workbay_handoff_mcp.

Contains:
  - DashboardContext, DashboardSection, DashboardExtension types
  - Extension registry (register_dashboard_extension, clear_dashboard_extensions)
  - Needs-attention aggregation (_collect_needs_attention)
  - Core dashboard section renderers
  - generate_dashboard_md() — public entry point

Core sections always render before extension sections, regardless of order values.
Extension order field controls relative placement among extensions only.
"""

from __future__ import annotations

import json
import sqlite3
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypedDict, cast

from .runtime import get_runtime_config
from .shared_schema import _get_db_connection

_DASHBOARD_RENDER_BUDGET_MS = 50.0

# ---------------------------------------------------------------------------
# Public protocol types
# ---------------------------------------------------------------------------


class DashboardContext(TypedDict):
    """Pre-queried data passed to extensions so they never touch the DB directly."""

    worktree_lanes: list[dict]
    worker_reports: list[dict]
    turn_metrics: list[dict]


class DashboardSection(TypedDict):
    """A named content block contributed by a DashboardExtension.

    ``order`` controls relative placement among extension sections.
    Core sections always render before any extension section.
    """

    heading: str
    content: str
    order: int


DashboardExtension = Callable[[DashboardContext], list[DashboardSection]]

# ---------------------------------------------------------------------------
# Extension registry
# ---------------------------------------------------------------------------

_extensions: list[DashboardExtension] = []


def register_dashboard_extension(ext: DashboardExtension) -> None:
    """Register a dashboard extension callback.

    The callback receives a DashboardContext with pre-queried data and returns
    a list of DashboardSection dicts that are appended after core sections.
    Called at import time by extension providers (e.g. mcp-workbay-orchestrator).
    """
    _extensions.append(ext)


def clear_dashboard_extensions() -> None:
    """Reset the extension registry.  Use in test fixtures to prevent leakage."""
    _extensions.clear()


# ---------------------------------------------------------------------------
# Needs-attention computation
# ---------------------------------------------------------------------------

_STALE_THRESHOLD_HOURS = 24


class _NeedsAttentionItem(TypedDict):
    task_ref: str
    kind: str  # "findings", "blocked", "stale", "compaction"
    detail: str


def _collect_needs_attention(
    conn: sqlite3.Connection,
    dashboard_rows: list[dict],
    open_findings: dict[str, list[dict]],
) -> list[_NeedsAttentionItem]:
    """Aggregate items that warrant human attention.

    - Tasks with open high/medium findings
    - Tasks with open blockers (open_blockers > 0)
    - Non-archived tasks with no activity in >24 h
    """
    items: list[_NeedsAttentionItem] = []
    seen_tasks: set[str] = set()

    # --- Findings: high or medium severity ---
    for task_ref, findings in open_findings.items():
        high = sum(1 for f in findings if f.get("severity") == "high")
        medium = sum(1 for f in findings if f.get("severity") == "medium")
        if high or medium:
            parts = []
            if high:
                parts.append(f"{high} high")
            if medium:
                parts.append(f"{medium} medium")
            items.append(
                {
                    "task_ref": task_ref,
                    "kind": "findings",
                    "detail": f"{high + medium} open ({', '.join(parts)})",
                }
            )
            seen_tasks.add(task_ref)

    # --- Blocked tasks ---
    for row in dashboard_rows:
        if int(row.get("open_blockers", 0)) > 0:
            task_ref = str(row["task_ref"])
            n = int(row["open_blockers"])
            items.append(
                {
                    "task_ref": task_ref,
                    "kind": "blocked",
                    "detail": f"blocked: {n} open blocker{'s' if n > 1 else ''}",
                }
            )

    # --- Stale tasks (non-archived, no activity in >24 h) ---
    now = datetime.now(UTC)
    stale_cutoff = now - timedelta(hours=_STALE_THRESHOLD_HOURS)
    for row in dashboard_rows:
        if row.get("archived_at"):
            continue
        last_activity = row.get("last_activity")
        if not last_activity:
            continue
        try:
            ts = datetime.fromisoformat(last_activity.replace(" ", "T"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
        except ValueError:
            continue
        if ts < stale_cutoff:
            items.append(
                {
                    "task_ref": str(row["task_ref"]),
                    "kind": "stale",
                    "detail": f"stale: no activity since {ts.strftime('%Y-%m-%d %H:%M')} UTC",
                }
            )

    # --- Compaction advisory: recommended for any live task ---
    try:
        from .compaction import compute_compaction_advisory  # noqa: PLC0415 – late import to break circular
        from .runtime import get_runtime_config  # noqa: PLC0415

        workspace_root = get_runtime_config().compaction_config_root
        for compaction_row in dashboard_rows:
            if compaction_row.get("archived_at"):
                continue
            compaction_task_ref = compaction_row.get("task_ref")
            if not isinstance(compaction_task_ref, str) or not compaction_task_ref:
                continue
            try:
                advisory = compute_compaction_advisory(workspace_root=workspace_root, task_ref=compaction_task_ref)
            except Exception:
                continue
            # internal: disabled state takes precedence over the threshold path.
            if advisory.get("disabled"):
                disabled_source = advisory.get("disabled_source") or "unknown"
                items.append(
                    {
                        "task_ref": compaction_task_ref,
                        "kind": "compaction",
                        "detail": f"compaction: disabled via {disabled_source}",
                    }
                )
                continue
            if not advisory.get("recommended"):
                continue
            observed = advisory.get("observed") or {}
            thresholds = advisory.get("thresholds") or {}
            compaction_parts: list[str] = []
            obs_tokens = observed.get("tokens")
            thr_tokens = thresholds.get("tokens")
            if obs_tokens is not None and thr_tokens is not None and obs_tokens >= thr_tokens:
                compaction_parts.append(f"tokens {obs_tokens}>={thr_tokens}")
            obs_chars = observed.get("chars")
            thr_chars = thresholds.get("chars")
            if obs_chars is not None and thr_chars is not None and obs_chars >= thr_chars:
                compaction_parts.append(f"chars {obs_chars}>={thr_chars}")
            detail = "compaction recommended"
            if compaction_parts:
                detail = f"compaction recommended ({'; '.join(compaction_parts)})"
            recommended_action = advisory.get("recommended_action") or "compaction(operation=record)"
            detail = f"{detail}; record via {recommended_action}"
            contract_source = advisory.get("contract_source") or {}
            drift = contract_source.get("drift") if isinstance(contract_source, dict) else None
            if isinstance(drift, dict) and drift.get("detected"):
                detail = f"{detail}; contract drift detected"
            items.append({"task_ref": compaction_task_ref, "kind": "compaction", "detail": detail})
    except Exception:
        # Advisory is additive; never block dashboard render on its failure.
        pass

    return items


# ---------------------------------------------------------------------------
# Core section renderers
# ---------------------------------------------------------------------------


def _render_needs_attention_section(items: list[_NeedsAttentionItem]) -> list[str]:
    lines: list[str] = ["", "NEEDS ATTENTION", "-" * 15]
    if not items:
        lines.append("  (all clear)")
        return lines
    for item in items:
        icon = "!" if item["kind"] != "stale" else "~"
        task_ref = item["task_ref"]
        detail = item["detail"]
        lines.append(f"  {icon} {task_ref:<20}  {detail}")
    return lines


def _render_all_tasks_section(dashboard_rows: list[dict], active_task_ref: str | None) -> list[str]:
    # Re-use the ASCII table renderer from current_task_rendering.
    from .current_task_rendering import (  # noqa: PLC0415
        DashboardTaskRow,
        _render_dashboard_section,
    )

    rows: list[DashboardTaskRow] = []
    for r in dashboard_rows:
        rows.append(
            {
                "task_ref": str(r.get("task_ref", "")),
                "status": str(r.get("status", "")),
                "last_activity": r.get("last_activity"),
                "open_blockers": int(r.get("open_blockers", 0)),
                "pending_actions": int(r.get("pending_actions", 0)),
                "open_findings": int(r.get("open_findings", 0)),
                "archived_at": r.get("archived_at"),
            }
        )
    return _render_dashboard_section(rows, active_task_ref)


def _resolve_plan_baseline_for_dashboard(*, task_ref: str, plan_path: str | None) -> tuple[str, str | None]:
    """Wrapper that proxies to :func:`resolve_plan_baseline_status`.

    internal: lets dashboard renderers ask for the baseline
    state without importing the lifecycle evaluator. Any unexpected
    error collapses to ``("unknown", "baseline_probe_failed")`` so a
    broken probe never blocks dashboard rendering.
    """
    try:
        from .plan_resolve import resolve_plan_baseline_status

        return resolve_plan_baseline_status(task_ref=task_ref, plan_path=plan_path)
    except Exception:
        return ("unknown", "baseline_probe_failed")


def _resolve_plan_read_branch(*, task_ref: str) -> str | None:
    """Return the branch the dashboard ``plan:`` line should anchor on.

    Calls ``resolve_plan_location(prefer="auto")`` so the read receipt
    shows ``main`` when the plan has been accepted and the task's
    ``target_branch`` otherwise. Missing branches/paths return None so
    the dashboard does not advertise a ``make plan-show`` command that
    would fail.
    """
    try:
        from .plan_resolve import resolve_plan_location

        location = resolve_plan_location(task_ref=task_ref, prefer="auto")
    except Exception:
        return None
    if not location.exists_on_branch:
        return None
    return location.branch


def _render_active_task_plans_section(
    active_rows: list[dict],
    checklist_sync_entries: dict[str, dict] | None = None,
) -> list[str]:
    """Render every active task's plan path so the operator can discover and
    open task plans from the root workspace without switching the root
    worktree to a feature branch.

    Each row is enriched (via _enrich_handoff_active) with task_plan_abs_path
    and task_plan_exists, so the table reflects the live filesystem state of
    the plan file in its sibling worktree.

    internal: when the per-task ``.task-state/checklist_sync.json``
    sidecar (written by the sync handler in ``workbay-system``) reports
    ``ok: false`` for a task, surface a ``checklist_sync_warning`` line
    under that task's block so the operator notices a malformed or
    missing task plan without re-querying.
    """
    sync_entries = checklist_sync_entries or {}
    lines: list[str] = ["", "ACTIVE TASK PLANS", "-" * 17]
    if not active_rows:
        lines.append("  (no active tasks)")
        return lines

    plans = sorted((r for r in active_rows if r.get("task_plan_path")), key=lambda row: str(row.get("task_ref", "")))
    if not plans:
        lines.append("  (no active tasks have task_plan_path set)")
        lines.append("  Set via set_handoff_state(task_plan_path='docs/tasks/...').")
        return lines

    for r in plans:
        task_ref = str(r.get("task_ref", ""))
        branch = str(r.get("target_branch") or "-")
        plan_path = str(r.get("task_plan_path") or "")
        abs_path = r.get("task_plan_abs_path") or ""
        exists = r.get("task_plan_exists")
        marker = "✓" if exists else ("✗" if exists is False else "?")
        # internal: advertise `make plan-show TASK=<ref>` only
        # when the auto-resolved Git branch actually contains the plan.
        resolved_branch = _resolve_plan_read_branch(task_ref=task_ref)
        baseline_status, _baseline_reason = _resolve_plan_baseline_for_dashboard(task_ref=task_ref, plan_path=plan_path)
        lines.append(f"  [{task_ref}] branch={branch}")
        if resolved_branch:
            lines.append(f"      plan: {plan_path} (read: make plan-show TASK={task_ref} on {resolved_branch})")
        else:
            lines.append(f"      plan: {plan_path} (read: unavailable on {branch})")
        # internal: surface baseline state separately from the
        # read branch so the operator sees `baseline=missing` even when
        # the read line falls back to the feature branch.
        lines.append(f"      baseline: {baseline_status}")
        lines.append(f"      abs:  {marker} {abs_path}")
        sync_entry = sync_entries.get(task_ref)
        if isinstance(sync_entry, dict) and sync_entry.get("ok") is False:
            warning = str(sync_entry.get("warning") or "unknown")
            lines.append(f"      checklist_sync_warning: {warning}")

    rows_without = sorted(
        (r for r in active_rows if not r.get("task_plan_path")),
        key=lambda row: str(row.get("task_ref", "")),
    )
    if rows_without:
        refs = ", ".join(str(r.get("task_ref", "")) for r in rows_without)
        lines.append("")
        lines.append(f"  (no task_plan_path set for: {refs})")

    return lines


def _render_open_findings_section(open_findings: dict[str, list[dict]]) -> list[str]:
    lines: list[str] = ["", "OPEN FINDINGS", "-" * 13]
    if not open_findings:
        lines.append("  (none)")
        return lines
    for task_ref, findings in sorted(open_findings.items()):
        lines.extend(["", f"  [{task_ref}]"])
        for f in findings:
            location = f"{f.get('file_path')}:{f.get('line_start')}" if f.get("line_start") else f.get("file_path", "")
            lines.append(
                f"  [{f.get('severity', '').upper()}] {f.get('finding_id')}: {location} -- {f.get('description', '')}"
            )
    return lines


def _resolved_finding_dashboard_line(finding: dict) -> str:
    """Render one resolved-finding line for the dashboard breakdown.

    internal: mirrors the CURRENT_TASK two-state sentence templates
    so the dashboard breakdown is one-to-one with the receipt:

      * ``integrated``         -> ``integrated to <ref>@<sha7>``
      * ``resolved_on_branch`` -> ``fixed on <branch>@<sha7>, pending integration to main``
      * legacy ``fixed``       -> ``fixed on <branch>@<sha7> (legacy)`` (or
                                  ``fixed (legacy)`` when no anchor exists).
    """
    finding_id = finding.get("finding_id")
    severity = str(finding.get("severity") or "").upper()
    status = finding.get("status")
    if status == "integrated" and finding.get("integrated_at_commit"):
        sha7 = str(finding["integrated_at_commit"])[:7]
        ref = finding.get("integrated_at_ref") or "main"
        receipt = f"integrated to {ref}@{sha7}"
    elif status == "resolved_on_branch" and finding.get("resolved_on_branch_at_commit"):
        sha7 = str(finding["resolved_on_branch_at_commit"])[:7]
        branch = finding.get("resolved_on_branch_ref") or finding.get("branch") or "unknown"
        receipt = f"fixed on {branch}@{sha7}, pending integration to main"
    else:
        legacy_branch = finding.get("branch") or "unknown"
        legacy_sha = finding.get("commit_sha")
        if legacy_sha:
            receipt = f"fixed on {legacy_branch}@{str(legacy_sha)[:7]} (legacy)"
        else:
            receipt = "fixed (legacy)"
    return f"  [{severity}] {finding_id}: {receipt}"


def _render_resolved_findings_section(resolved_findings: dict[str, list[dict]]) -> list[str]:
    """Render the RESOLVED FINDINGS section grouped by ``task_ref``.

    Each task header carries a per-status count breakdown so reviewers can
    distinguish on-branch fixes (pending integration) from already-integrated
    rows without scanning every line. Empty mapping renders nothing.
    """
    if not resolved_findings:
        return []
    lines: list[str] = ["", "RESOLVED FINDINGS", "-" * 17]
    for task_ref, findings in sorted(resolved_findings.items()):
        integrated = sum(1 for f in findings if f.get("status") == "integrated")
        resolved_on_branch = sum(1 for f in findings if f.get("status") == "resolved_on_branch")
        legacy_fixed = sum(1 for f in findings if f.get("status") == "fixed")
        breakdown_parts = [
            f"integrated={integrated}",
            f"resolved_on_branch={resolved_on_branch}",
        ]
        if legacy_fixed:
            breakdown_parts.append(f"fixed_legacy={legacy_fixed}")
        breakdown = ", ".join(breakdown_parts)
        lines.extend(["", f"  [{task_ref}] {breakdown}"])
        for f in findings:
            lines.append(_resolved_finding_dashboard_line(f))
    return lines


def _load_eval_results() -> list[dict]:
    try:
        cfg = get_runtime_config()
        results_path = cfg.state_dir / "evals" / "results.jsonl"
    except Exception:
        return []
    if not results_path.is_file():
        return []
    rows: list[dict] = []
    try:
        for line in results_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                rows.append(parsed)
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    return rows


def _render_eval_summary_section(eval_results: list[dict]) -> list[str]:
    lines: list[str] = ["", "EVAL SUMMARY", "-" * 12]
    if not eval_results:
        lines.append("  (no eval results)")
        return lines

    # Group rows by (suite, recorded_at) so multi-case suites aggregate across
    # every case row from the same run rather than reporting only the last row.
    runs_by_suite: dict[str, dict[str, list[dict]]] = {}
    for row in eval_results:
        suite = row.get("suite")
        if not isinstance(suite, str) or not suite:
            continue
        recorded_at = str(row.get("recorded_at") or "")
        runs_by_suite.setdefault(suite, {}).setdefault(recorded_at, []).append(row)

    latest_runs: dict[str, tuple[str, list[dict]]] = {}
    for suite, runs in runs_by_suite.items():
        latest_recorded = max(runs.keys(), default="")
        latest_runs[suite] = (latest_recorded, runs[latest_recorded])

    latest_recorded_at = max(
        (recorded_at for recorded_at, _ in latest_runs.values()),
        default="",
    )
    if latest_recorded_at:
        lines.append(f"  latest: {latest_recorded_at}")

    failing: list[tuple[str, list[dict]]] = []
    for suite, (_, rows) in latest_runs.items():
        if any(row.get("status") != "pass" for row in rows):
            failing.append((suite, rows))

    lines.append(f"  suites tracked: {len(latest_runs)}")
    lines.append(f"  failing suites: {len(failing)}")
    if not failing:
        lines.append("  (all suites passing)")
        return lines
    for suite, rows in sorted(failing, key=lambda item: item[0]):
        failing_rows = [row for row in rows if row.get("status") != "pass"]
        summary_source = next(
            (str(row.get("failure_summary")) for row in failing_rows if row.get("failure_summary")),
            "suite reported a failing case",
        )
        lines.append(f"  ! {suite}: {summary_source}")
        lines.append(f"      next: make evals-run SUITE={suite} LIFECYCLE_ARGS=--json")
    return lines


def _render_deferred_findings_section(deferred_findings: dict[str, list[dict]]) -> list[str]:
    lines: list[str] = ["", "DEFERRED / WONTFIX", "-" * 18]
    if not deferred_findings:
        return []
    for task_ref, findings in sorted(deferred_findings.items()):
        lines.extend(["", f"  [{task_ref}]"])
        for f in findings:
            location = f"{f.get('file_path')}:{f.get('line_start')}" if f.get("line_start") else f.get("file_path", "")
            status_label = f.get("status", "deferred").upper()
            lines.append(
                f"  [{status_label}] [{f.get('severity', '').upper()}] {f.get('finding_id')}: {location} -- {f.get('description', '')}"
            )
    return lines


# ---------------------------------------------------------------------------
# New section: epic recent decisions
# ---------------------------------------------------------------------------


def _collect_epic_decisions(
    conn: sqlite3.Connection,
    active_task_refs: list[str],
    limit: int = 8,
) -> list[tuple[str, list[dict]]]:
    """Return recent decisions for each active scope represented by live rows.

    Each distinct epic inferred from the live task refs contributes one section.
    Non-epic task refs contribute their own task-scoped section so dashboard
    readers can navigate by handoff row id even for maintenance or ad-hoc refs.
    """
    from .current_task_rendering import _infer_epic_ref  # noqa: PLC0415

    sections: list[tuple[str, list[dict]]] = []
    seen_scopes: set[str] = set()
    for task_ref in active_task_refs:
        epic_ref = _infer_epic_ref(task_ref)
        scope_ref = epic_ref or task_ref
        if not scope_ref or scope_ref in seen_scopes:
            continue
        seen_scopes.add(scope_ref)
        if epic_ref:
            rows = conn.execute(
                "SELECT id, task_ref, decision, agent, model_label, reasoning_level, created_at FROM decisions "
                "WHERE task_ref = ? OR task_ref LIKE ? "
                "ORDER BY created_at DESC LIMIT ?",
                (epic_ref, f"{epic_ref}-%", limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, task_ref, decision, agent, model_label, reasoning_level, created_at FROM decisions "
                "WHERE task_ref = ? "
                "ORDER BY created_at DESC LIMIT ?",
                (scope_ref, limit),
            ).fetchall()
        sections.append((scope_ref, [dict(r) for r in rows]))
    return sections


def _render_epic_decisions_section(epic_ref: str, decisions: list[dict]) -> list[str]:
    heading = f"RECENT DECISIONS ({epic_ref})"
    lines: list[str] = ["", heading, "-" * len(heading)]
    if not decisions:
        lines.append("  (none)")
        return lines
    col_ref = 8
    for d in decisions:
        task_ref = str(d.get("task_ref", ""))
        slug = str(d.get("decision", ""))
        created = str(d.get("created_at", ""))[:16]
        model_label = d.get("model_label") or ""
        reasoning_level = d.get("reasoning_level") or ""
        agent = d.get("agent") or ""
        if model_label and reasoning_level:
            suffix = f" ({model_label} {reasoning_level})"
        elif model_label:
            suffix = f" ({model_label})"
        elif agent:
            suffix = f" ({agent})"
        else:
            suffix = ""
        lines.append(f"  {task_ref:<{col_ref}}  [#{d.get('id')}] {slug}{suffix}  {created}")
    return lines


def _render_epic_decisions_sections(epic_decision_groups: list[tuple[str, list[dict]]]) -> list[str]:
    lines: list[str] = []
    for epic_ref, decisions in epic_decision_groups:
        lines.extend(_render_epic_decisions_section(epic_ref, decisions))
    return lines


# ---------------------------------------------------------------------------
# New section: test status per task
# ---------------------------------------------------------------------------


def _collect_task_test_status(
    conn: sqlite3.Connection,
    active_task_refs: list[str] | None = None,
) -> dict[str, dict]:
    """Return per-task test summary across all live dashboard scopes.

    For epic-style task refs, include the full epic scope once. For non-epic
    refs, include that task directly. Never returns the full unbounded table.
    """
    from .current_task_rendering import _infer_epic_ref  # noqa: PLC0415

    summary: dict[str, dict] = {}
    seen_scopes: set[tuple[str, str]] = set()
    for task_ref in active_task_refs or []:
        epic_ref = _infer_epic_ref(task_ref)
        if epic_ref is not None:
            scope = ("epic", epic_ref)
            if scope in seen_scopes:
                continue
            seen_scopes.add(scope)
            rows = conn.execute(
                "SELECT task_ref, passed, verified_at FROM verified_tests"
                " WHERE task_ref = ? OR task_ref LIKE ?"
                " ORDER BY verified_at DESC",
                (epic_ref, f"{epic_ref}-%"),
            ).fetchall()
        else:
            scope = ("task", task_ref)
            if scope in seen_scopes:
                continue
            seen_scopes.add(scope)
            rows = conn.execute(
                "SELECT task_ref, passed, verified_at FROM verified_tests WHERE task_ref = ? ORDER BY verified_at DESC",
                (task_ref,),
            ).fetchall()
        for row in rows:
            ref = str(row["task_ref"])
            passed = bool(row["passed"])
            ts = str(row["verified_at"] or "")
            if ref not in summary:
                summary[ref] = {
                    "latest_passed": passed,
                    "latest_at": ts[:16],
                    "pass_count": 0,
                    "fail_count": 0,
                }
            if passed:
                summary[ref]["pass_count"] += 1
            else:
                summary[ref]["fail_count"] += 1
    return summary


def _render_test_status_section(status_by_task: dict[str, dict]) -> list[str]:
    lines: list[str] = ["", "TEST STATUS", "-" * 11]
    if not status_by_task:
        lines.append("  (no verified tests recorded)")
        return lines
    col_ref = 12
    for task_ref, s in sorted(status_by_task.items()):
        icon = "✓" if s["latest_passed"] else "✗"
        totals = f"pass={s['pass_count']} fail={s['fail_count']}"
        lines.append(f"  {task_ref:<{col_ref}}  {icon}  last: {s['latest_at']}  {totals}")
    return lines


# ---------------------------------------------------------------------------
# Context collection
# ---------------------------------------------------------------------------


def _collect_dashboard_context(conn: sqlite3.Connection, task_ref: str | None) -> DashboardContext:
    """Query pre-aggregated data to pass to extensions."""
    lanes = conn.execute("SELECT * FROM worktree_lanes ORDER BY updated_at DESC, id DESC LIMIT 20").fetchall()
    reports = conn.execute("SELECT * FROM worker_reports ORDER BY created_at DESC, id DESC LIMIT 20").fetchall()
    metrics = conn.execute("SELECT * FROM turn_metrics ORDER BY created_at DESC, id DESC LIMIT 20").fetchall()
    return {
        "worktree_lanes": [dict(r) for r in lanes],
        "worker_reports": [dict(r) for r in reports],
        "turn_metrics": [dict(r) for r in metrics],
    }


# ---------------------------------------------------------------------------
# Workflow integrity checks
# ---------------------------------------------------------------------------

_GIT_TIMEOUT = 5


def _collect_workflow_integrity(
    active_rows: list[dict],
) -> list[str]:
    """Derive workflow-integrity anomalies from all live dashboard rows.

    Returns a list of human-readable anomaly strings.  Empty list = clean.
    All git subprocess calls use a bounded timeout; on TimeoutExpired the
    caller receives a sentinel string instead of an exception.
    """
    anomalies: list[str] = []
    for active_row in active_rows:
        target_branch = active_row.get("target_branch")
        target_worktree_path = active_row.get("target_worktree_path")
        task_ref = str(active_row.get("task_ref") or "")
        if not target_branch:
            continue
        prefix = f"[{task_ref}] " if task_ref else ""

        try:
            result = subprocess.run(
                ["git", "rev-parse", "--verify", str(target_branch)],
                capture_output=True,
                text=True,
                timeout=_GIT_TIMEOUT,
            )
            if result.returncode != 0:
                anomalies.append(f"{prefix}missing branch: {target_branch} does not exist")
                continue

            merged = subprocess.run(
                ["git", "branch", "--merged", "main"],
                capture_output=True,
                text=True,
                timeout=_GIT_TIMEOUT,
            )
            merged_branches = [b.strip().lstrip("* ") for b in merged.stdout.splitlines()]
            if target_branch != "main" and target_branch in merged_branches:
                anomalies.append(f"{prefix}undeleted merged branch: {target_branch} is fully merged to main")

            if target_worktree_path:
                wt_list = subprocess.run(
                    ["git", "worktree", "list", "--porcelain"],
                    capture_output=True,
                    text=True,
                    timeout=_GIT_TIMEOUT,
                )
                worktree_found = False
                for line in wt_list.stdout.splitlines():
                    if line.startswith("worktree ") and line[9:] == target_worktree_path:
                        worktree_found = True
                        break
                if not worktree_found:
                    anomalies.append(f"{prefix}missing worktree: {target_worktree_path} not found")

        except subprocess.TimeoutExpired:
            anomalies.append(f"{prefix}git check timed out (5s)")

    return anomalies


def _render_workflow_integrity_section(anomalies: list[str]) -> list[str]:
    """Render WORKFLOW INTEGRITY section.  Returns empty list when no anomalies."""
    if not anomalies:
        return []
    lines: list[str] = ["", "WORKFLOW INTEGRITY", "-" * 18]
    for a in anomalies:
        lines.append(f"  ! {a}")
    return lines


# ---------------------------------------------------------------------------
# Primary render function
# ---------------------------------------------------------------------------


def _render_dashboard_md(
    generated_at: str,
    dashboard_rows: list[dict],
    open_findings: dict[str, list[dict]],
    deferred_findings: dict[str, list[dict]],
    needs_attention: list[_NeedsAttentionItem],
    active_task_ref: str | None,
    extension_sections: list[DashboardSection],
    active_task_plan_rows: list[dict] | None = None,
    epic_decision_groups: list[tuple[str, list[dict]]] | None = None,
    task_test_status: dict[str, dict] | None = None,
    integrity_anomalies: list[str] | None = None,
    checklist_sync_entries: dict[str, dict] | None = None,
    resolved_findings: dict[str, list[dict]] | None = None,
    eval_results: list[dict] | None = None,
) -> str:
    sep = "=" * 80
    lines: list[str] = [
        "DASHBOARD",
        sep,
        f"DO NOT EDIT: generated from .task-state/handoff.db at {generated_at}",
        sep,
    ]

    lines.extend(_render_needs_attention_section(needs_attention))
    lines.extend(_render_all_tasks_section(dashboard_rows, active_task_ref))
    lines.extend(_render_active_task_plans_section(active_task_plan_rows or [], checklist_sync_entries))

    if epic_decision_groups:
        lines.extend(_render_epic_decisions_sections(epic_decision_groups))

    # Active-task findings and integrity alerts render before TEST STATUS.
    lines.extend(_render_open_findings_section(open_findings))
    lines.extend(_render_workflow_integrity_section(integrity_anomalies or []))
    lines.extend(_render_eval_summary_section(eval_results or []))

    if task_test_status is not None:
        lines.extend(_render_test_status_section(task_test_status))

    resolved_lines = _render_resolved_findings_section(resolved_findings or {})
    if resolved_lines:
        lines.extend(resolved_lines)

    deferred_lines = _render_deferred_findings_section(deferred_findings)
    if deferred_lines:
        lines.extend(deferred_lines)

    for section in sorted(extension_sections, key=lambda s: s["order"]):
        heading = section["heading"].upper()
        lines.extend(["", heading, "-" * len(heading)])
        lines.append(section["content"])

    lines.append("")
    # Strip trailing whitespace per line so right-edge column padding
    # (e.g. short ``HH:MM`` last-activity cells padded to col_last=16)
    # does not break ``git diff --check`` on committed DASHBOARD.txt.
    # BR-internal.
    return "\n".join(line.rstrip() for line in lines)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def _load_checklist_sync_sidecar() -> dict[str, dict]:
    """Read ``.task-state/checklist_sync.json`` if present.

    internal cross-package contract: the ``sync-task-plan-checklist``
    handler in ``workbay-system`` writes per-task entries keyed by
    ``task_ref``; the dashboard renderer reads them to surface a warning
    line when the most recent sync was non-ok. Best-effort: any read or
    parse failure collapses to an empty dict so a malformed sidecar
    never blocks dashboard rendering.
    """
    try:
        cfg = get_runtime_config()
        sidecar_path = cfg.state_dir / "checklist_sync.json"
    except Exception:
        return {}
    if not sidecar_path.is_file():
        return {}
    try:
        parsed = json.loads(sidecar_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {k: v for k, v in parsed.items() if isinstance(v, dict)}


def generate_dashboard_md(write_file: bool = True) -> dict:
    """Generate DASHBOARD.txt from the live handoff DB.

    Core sections (Needs Attention, All Tasks, Open Findings, Deferred/Won't Fix)
    are always rendered.  Extension sections (registered via
    register_dashboard_extension) are appended after core sections, ordered by
    DashboardSection.order.

    Args:
        write_file: Write the markdown to the configured runtime dashboard path.

    Returns:
        A result dict with ``ok``, ``path``, ``written``, and ``markdown``.
    """
    from .current_task_rendering import (  # noqa: PLC0415
        _collect_all_deferred_findings,
        _collect_all_open_findings,
        _collect_all_resolved_findings,
        _collect_dashboard_rows,
    )

    with _get_db_connection() as conn:
        from .shared_primitives import _enrich_handoff_active  # noqa: PLC0415

        active_rows = [
            dict(row)
            for row in conn.execute(
                "SELECT task_ref, objective, target_branch, target_worktree_path, task_plan_path "
                "FROM handoff_state ORDER BY updated_at DESC, task_ref ASC"
            ).fetchall()
        ]
        for row in active_rows:
            _enrich_handoff_active(row)
        active_task_refs = [str(row["task_ref"]) for row in active_rows if row.get("task_ref")]

        from .shared_primitives import _resolve_workspace_handoff_row  # noqa: PLC0415

        try:
            workspace_row = _resolve_workspace_handoff_row(conn)
        except ValueError:
            workspace_row = None
        workspace_task_ref = (
            str(workspace_row["task_ref"]) if workspace_row is not None and workspace_row["task_ref"] else None
        )

        dashboard_rows = cast(list[dict], _collect_dashboard_rows(conn))
        open_findings = _collect_all_open_findings(conn, max_per_task=100)
        deferred_findings = _collect_all_deferred_findings(conn, max_per_task=100)
        resolved_findings = _collect_all_resolved_findings(conn, max_per_task=100)
        needs_attention = _collect_needs_attention(conn, dashboard_rows, open_findings)
        ctx = _collect_dashboard_context(conn, None)
        epic_decision_groups = _collect_epic_decisions(conn, active_task_refs)
        task_test_status = _collect_task_test_status(conn, active_task_refs=active_task_refs)

    integrity_anomalies = _collect_workflow_integrity(active_rows)

    checklist_sync_entries = _load_checklist_sync_sidecar()
    eval_results = _load_eval_results()

    extension_sections: list[DashboardSection] = []
    for ext in _extensions:
        try:
            extension_sections.extend(ext(ctx))
        except Exception:
            pass

    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    markdown = _render_dashboard_md(
        generated_at=generated_at,
        dashboard_rows=dashboard_rows,
        open_findings=open_findings,
        deferred_findings=deferred_findings,
        needs_attention=needs_attention,
        active_task_ref=workspace_task_ref,
        extension_sections=extension_sections,
        active_task_plan_rows=active_rows,
        epic_decision_groups=epic_decision_groups,
        task_test_status=task_test_status,
        integrity_anomalies=integrity_anomalies,
        checklist_sync_entries=checklist_sync_entries,
        resolved_findings=resolved_findings,
        eval_results=eval_results,
    )

    written = False
    dashboard_path: Path | None = None
    fragments_report: dict | None = None
    if write_file:
        from .dashboard_fragments import (  # noqa: PLC0415 — local import keeps import graph thin
            collect_dashboard_fragments,
            maybe_write_dashboard_fragments,
        )

        cfg = get_runtime_config()
        dashboard_path = cfg.dashboard_path
        dashboard_path.write_text(markdown)
        written = True

        # internal / BR-03: emit per-section fragment files
        # under .task-state/DASHBOARD.d/ alongside the concatenated
        # DASHBOARD.txt. Scoped prompt-cache invalidation only works
        # if the fragments and manifest actually land on disk.
        fragments = collect_dashboard_fragments(markdown)
        fragments_report = maybe_write_dashboard_fragments(cfg.state_dir, fragments)

    return {
        "ok": True,
        "path": str(dashboard_path) if dashboard_path else None,
        "written": written,
        "markdown": markdown if not write_file else None,
        "fragments": fragments_report,
    }
