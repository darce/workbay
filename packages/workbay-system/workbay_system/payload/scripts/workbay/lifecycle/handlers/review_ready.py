"""``review-ready`` subcommand (internal).

Local pre-review gate. The handler inspects the current git context
plus handoff state and emits a stable JSON receipt indicating whether
the branch is ready for review.

Sub-implementation note.1 lands the foundation: protected-base, dirty-worktree,
and no-changes-against-base checks. Sub-implementation note.2 factors
:func:`evaluate` out for re-use from ``close-check``. Later sub-slices
extend ``reasons`` with HEAD-tied test-evidence and finding-count
checks.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Iterable

import resolver

from . import _common
from .plan_baseline import evaluate_plan_baseline, is_planning_path
from . import sync_task_plan_checklist as sync_handler

try:
    from workbay_protocol.branch_naming import derive_task_ref_candidates
except ImportError:  # pragma: no cover — degraded checkout without workbay-protocol
    derive_task_ref_candidates = None  # type: ignore[assignment,misc]

PROTECTED_BASES: tuple[str, ...] = ("main", "master")

ZERO_FINDINGS: dict[str, int] = {"high": 0, "medium": 0, "low": 0}

# internal: reason -> owner bucket so close-check / review-ready
# can route failures without callers parsing free text. ``unknown`` is
# the catch-all for any future reason that has not been classified yet.
_REASON_OWNER: dict[str, str] = {
    "on_protected_base": "feature_branch",
    "no_changes_against_base": "feature_branch",
    "dirty_worktree": "feature_branch",
    "open_high_finding": "handoff_evidence",
    "open_medium_finding": "handoff_evidence",
    "stale_test_evidence": "handoff_evidence",
    "unmergeable": "mergeability",
    "dirty_main_protected_paths": "root_main_hygiene",
    "checklist_sync_pending": "checklist_sync",
    "plan_baseline_missing": "planning_baseline",
    "plan_baseline_unknown": "planning_baseline",
    "plan_id_collision_requires_reconcile": "planning_baseline",
    "ambiguous_task_ref": "task_identity",
}
_OWNER_BUCKETS: tuple[str, ...] = (
    "feature_branch",
    "handoff_evidence",
    "mergeability",
    "root_main_hygiene",
    "checklist_sync",
    "planning_baseline",
    "task_identity",
)


def reasons_by_owner(reasons: list[str]) -> dict[str, list[str]]:
    """Group ``reasons`` into the canonical owner buckets (internal).

    Buckets are always present so consumers can index without
    ``KeyError``. Unknown reasons fall through to ``feature_branch`` —
    the safest default since most uncategorized rejects originate on
    the branch under review.
    """
    grouped: dict[str, list[str]] = {bucket: [] for bucket in _OWNER_BUCKETS}
    for reason in reasons:
        bucket = _REASON_OWNER.get(reason, "feature_branch")
        grouped.setdefault(bucket, []).append(reason)
    return grouped


def next_command_for(
    *,
    command: str,
    reasons: list[str],
    grouped: dict[str, list[str]],
    derived_task_ref: str | None = None,
) -> dict[str, str]:
    """Return the canonical next-command hint for the given gate state.

    Mirrors the internal / internal ``status`` next-command grammar so
    callers can route on a single field across the whole workflow loop.

    internal (finding internal): when ``derived_task_ref``
    is known we substitute it for the literal ``<task-ref>`` placeholder
    in the emitted command so the operator can copy-paste the line. We
    keep the placeholder when the ref is unknown (e.g., on a protected
    base) so a half-substituted command never misleads.
    """
    hint = _next_command_template(command=command, reasons=reasons, grouped=grouped)
    if derived_task_ref:
        hint = {
            **hint,
            "command": hint["command"].replace("<task-ref>", derived_task_ref),
        }
    return hint


def _next_command_template(
    *,
    command: str,
    reasons: list[str],
    grouped: dict[str, list[str]],
) -> dict[str, str]:
    if not reasons:
        if command == "review-ready":
            return {
                "command": "make close-check LIFECYCLE_ARGS=--json",
                "reason": "branch_ready_for_close_check",
            }
        return {
            "command": "make task-finish TASK=<task-ref>",
            "reason": "branch_ready_to_merge",
        }

    planning = grouped.get("planning_baseline", [])
    if planning:
        # implementation note D1: a duplicate plan id on another ref is a reconcile, not an
        # acceptance — the later-dated branch must renumber (handed to D2's
        # plan-accept relink). Surface this before the baseline-accept hints so a
        # collision never reads as "go run plan-accept for a missing baseline".
        if "plan_id_collision_requires_reconcile" in planning:
            return {
                "command": (
                    "# duplicate plan id on another ref: renumber the later "
                    "branch's docs/plans/<NNNN>-*.md (rename + update the "
                    "`# Plan <NNNN> —` heading and the branch -plan<NNNN> suffix), "
                    "then re-run make review-ready"
                ),
                "reason": "plan_id_collision_requires_reconcile",
            }
        # ``plan_baseline_unknown`` reflects a degraded MCP query (the
        # evaluator could not determine whether the plan is accepted),
        # so the operator action is "retry the gate" — not "go run
        # plan-accept". Distinguishing the two avoids sending operators
        # on an acceptance loop for what is actually transient MCP state.
        if "plan_baseline_unknown" in planning and "plan_baseline_missing" not in planning:
            return {
                "command": "make review-ready LIFECYCLE_ARGS=--json",
                "reason": "plan_baseline_unknown_retry",
            }
        return {
            "command": "make plan-accept TASK=<task-ref> LIFECYCLE_ARGS=--json",
            "reason": "plan_baseline_missing_on_main",
        }
    if grouped.get("mergeability"):
        return {
            "command": "git rebase main  # resolve conflicts before retry",
            "reason": "branch_unmergeable_against_base",
        }
    if grouped.get("handoff_evidence"):
        return {
            "command": "make slice-start TASK=<task-ref>  # capture fresh test evidence",
            "reason": "handoff_evidence_stale_or_open_findings",
        }
    if grouped.get("checklist_sync"):
        return {
            "command": "make sync-task-plan-checklist TASK=<task-ref> APPLY=1",
            "reason": "task_plan_checklist_evidence_backed_unchecked_items",
        }
    if grouped.get("root_main_hygiene"):
        return {
            "command": "make doctor LIFECYCLE_ARGS=--json",
            "reason": "root_main_dirty_protected_paths",
        }
    if grouped.get("task_identity"):
        return {
            "command": "make close-check LIFECYCLE_ARGS='--json --task-ref <task-ref>'",
            "reason": "ambiguous_task_ref_requires_explicit_task",
        }
    feature = grouped.get("feature_branch", [])
    if "on_protected_base" in feature:
        return {
            "command": "make task-start TASK=<task-ref>",
            "reason": "checkout_is_on_protected_base",
        }
    if "dirty_worktree" in feature:
        return {
            "command": "make slice-commit TASK=<task-ref> MSG='...'",
            "reason": "feature_branch_dirty_worktree",
        }
    if "no_changes_against_base" in feature:
        return {
            "command": "make slice-start TASK=<task-ref>",
            "reason": "feature_branch_has_no_commits",
        }
    return {
        "command": "make status LIFECYCLE_ARGS=--json",
        "reason": "needs_orientation",
    }

def _orphan_planning_warnings(repo: Path) -> list[str]:
    """Return warn-only strings for untracked files under canonical
    planning homes. Empty list means "no orphans" (or the git status
    probe failed — failures degrade silently rather than block).
    """
    # ``--untracked-files=all`` is required so a fresh untracked
    # ``docs/scopes/foo.md`` is reported as the file path rather than
    # the collapsed parent directory ``docs/`` (the porcelain default
    # for untracked dirs is ``normal`` which only emits the directory
    # name when no other files in it are tracked).
    proc = resolver._run_git(repo, "status", "--porcelain", "--untracked-files=all")
    if proc is None or proc.returncode != 0:
        return []
    warnings: list[str] = []
    for raw in proc.stdout.splitlines():
        # Porcelain v1: "XY path"; untracked is "?? path". We only care
        # about untracked rows — staged/tracked-uncommitted artifacts on
        # any branch are normal flight per the rule's three-state model.
        if not raw.startswith("?? "):
            continue
        path = raw[3:].strip()
        # Quoted paths (whitespace / specials) are wrapped in double
        # quotes by porcelain — skip them; conservative for the warn
        # surface, and our canonical homes don't contain such paths.
        if path.startswith('"'):
            continue
        if is_planning_path(path):
            warnings.append(
                f"orphan planning artifact (untracked): {path} — "
                "see docs/workbay/rules/planning-artifact-home.md"
            )
    return warnings


def _query_open_findings(
    repo: Path,
    task_ref: str | None,
) -> tuple[dict[str, int], bool]:
    """Return ``(counts, ok)`` from the handoff CLI's open-findings list.

    Shells out to ``mcp-workbay-handoff review-findings --operation list
    --status open --workspace-root <canonical>`` and projects
    ``data.counts.severity`` into the ``{high, medium, low}`` shape the
    receipt exposes. ``ok`` is False when the CLI is missing, exits
    non-zero, or returns unparseable JSON — callers translate that to
    ``handoff_projection: "pending"``.

    ``--workspace-root`` is mandatory in the real adapter (it raises
    ``AGENT_HANDOFF_WORKSPACE_ROOT must be set`` otherwise), so we always
    thread the canonical workspace root from a linked worktree back to
    the primary one — same pattern as :mod:`projection`.
    """
    workspace = resolver.canonical_workspace_root(repo) or repo
    # ``--workspace-root`` is registered on the parent parser before
    # ``add_subparsers`` in ``mcp-workbay-handoff`` (see cli.py L491 vs
    # L502), so it MUST precede the subcommand. Placing it after
    # ``review-findings`` makes the real adapter exit 2 with
    # ``unrecognized arguments`` and silently degrades the gate to
    # ``handoff_projection="pending"`` with zero counts (regression
    # guarded by ``test_review_ready_workspace_root_precedes_subcommand``;
    # finding BR-internal).
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(workspace),
        "review-findings",
        "--operation", "list",
        "--status", "open",
    ]
    if task_ref:
        argv.extend(["--task-ref", task_ref])
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return dict(ZERO_FINDINGS), False
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return dict(ZERO_FINDINGS), False
    severity = (
        payload.get("data", {}).get("counts", {}).get("severity", {})
        if isinstance(payload, dict)
        else {}
    )
    counts = {
        "high": int(severity.get("high", 0) or 0),
        "medium": int(severity.get("medium", 0) or 0),
        "low": int(severity.get("low", 0) or 0),
    }
    return counts, True


def _query_latest_passing_test_sha(
    repo: Path,
    task_ref: str | None,
) -> tuple[str | None, bool]:
    """Return ``(commit_sha, ok)`` for the most recent passing
    verified_test row.

    Shells out to ``mcp-workbay-handoff --workspace-root <root>
    get-verified-tests --passed true --exclude-never-passed --limit 1``.
    The CLI orders rows by ``verified_at DESC``, so ``data.tests[0]`` is
    the latest. ``commit_sha`` is ``None`` when the response has no rows
    (CLI worked but no passing evidence exists). ``ok`` is False on
    missing CLI / non-zero exit / unparseable JSON — callers translate
    that to ``handoff_projection="pending"`` and skip the freshness
    check.
    """
    workspace = resolver.canonical_workspace_root(repo) or repo
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(workspace),
        "get-verified-tests",
        "--passed", "true",
        "--exclude-never-passed",
        "--limit", "1",
    ]
    if task_ref:
        argv.extend(["--task-ref", task_ref])
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return None, False
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return None, False
    tests = (
        payload.get("data", {}).get("tests", [])
        if isinstance(payload, dict) else []
    )
    if not tests:
        return None, True
    first = tests[0] if isinstance(tests[0], dict) else {}
    sha = first.get("commit_sha")
    return (str(sha) if sha else None), True


def _has_diff_against_base(repo: Path, base: str) -> bool:
    """Return True iff the branch contributes any commits beyond ``base``.

    Uses the ahead-count alone (``git rev-list --count base..HEAD``).
    A two-dot ``git diff base HEAD`` fallback would conflate a base
    that has *advanced* with a branch that has *contributed*: when
    main moves forward after the branch was cut, the diff against the
    moving tip is non-empty even though the feature branch authored
    nothing of its own (regression guarded by
    ``test_review_ready_zero_commits_with_advanced_base_blocks``;
    finding BR-internal).
    """
    proc = resolver._run_git(repo, "rev-list", "--count", f"{base}..HEAD")
    if proc is None or proc.returncode != 0:
        return False
    try:
        commits_ahead = int(proc.stdout.strip() or "0")
    except ValueError:
        return False
    return commits_ahead > 0


def _probe_checklist_sync_pending(
    repo: Path,
    task_ref: str | None,
) -> tuple[int | None, str | None]:
    """Return ``(pending_count, plan_path)`` for evidence-backed unchecked
    checklist items on the active task's plan (internal).

    Reuses the internal ``sync_task_plan_checklist`` parse/resolve pipeline
    against the *canonical* workspace root (so nested-package worktrees
    resolve through the repo root the same way the audit handler does).
    ``pending_count`` is the number of items whose resolver verdict is
    ``RESOLUTION_TICK`` — items whose `- [ ]` boxes the recorded handoff
    evidence would flip if ``make sync-task-plan-checklist APPLY=1`` ran.

    Returns ``(None, None)`` when the lookup fails (no task_ref, no
    stored plan path, plan file missing, parse error, …). The fallback
    is intentional: ``review-ready`` should only *warn* and ``close-check``
    should only *block* when the lookup succeeded — a degraded MCP CLI
    or a plan-less task must not block the gate.
    """
    if not task_ref:
        return None, None
    workspace = sync_handler.resolve_workspace_root()
    stored = sync_handler._lookup_stored_plan_path(workspace, task_ref)
    if not stored:
        return None, None
    plan_path = Path(stored)
    if not plan_path.is_absolute():
        plan_path = workspace / plan_path
    if not plan_path.is_file():
        return None, None
    try:
        text = plan_path.read_text(encoding="utf-8")
    except OSError:
        return None, None
    try:
        parsed = sync_handler.parse(text)
        evidence, _projection, _warning = sync_handler._query_handoff_evidence(
            workspace, task_ref
        )
        resolutions = sync_handler.resolve(parsed, evidence)
    except Exception:
        return None, None
    pending = sum(
        1 for r in resolutions.values()
        if r.action == sync_handler.RESOLUTION_TICK
    )
    return pending, str(plan_path)


def _task_refs_on_branch(repo: Path, branch: str) -> list[str]:
    """Return live task refs whose stored ``target_branch`` matches ``branch``."""
    refs: list[str] = []
    for row in _common._live_handoff_rows(repo):
        target = row.get("target_branch")
        ref = row.get("task_ref")
        if (
            isinstance(target, str)
            and target == branch
            and isinstance(ref, str)
            and ref
        ):
            refs.append(ref)
    return refs


def _matching_registered_candidates(
    branch: str,
    known_task_refs: Iterable[str],
) -> list[str]:
    """Return branch-grammar candidates that intersect the live registry."""
    if derive_task_ref_candidates is None:
        return []
    known_upper = {ref.upper() for ref in known_task_refs if isinstance(ref, str) and ref}
    if not known_upper:
        return []
    return sorted(
        {
            candidate
            for candidate in derive_task_ref_candidates(branch)
            if candidate.upper() in known_upper
        }
    )


def _material_gates_need_task_ref(repo: Path, task_ref: str) -> bool:
    """Return True when close-check would block if ``task_ref`` were explicit."""
    findings_open, findings_ok = _query_open_findings(repo, task_ref)
    if findings_ok and (
        findings_open["high"] > 0 or findings_open["medium"] > 0
    ):
        return True

    pending_count, _ = _probe_checklist_sync_pending(repo, task_ref)
    if pending_count is not None and pending_count > 0:
        return True

    identity, identity_ok = _query_active_task_identity(repo, task_ref)
    _, evidence_ok = _query_latest_passing_test_sha(repo, task_ref)
    mcp_reachable = findings_ok or evidence_ok
    if not identity_ok and mcp_reachable:
        return True

    if identity is not None:
        plan_path = identity.get("task_plan_path")
        if plan_path:
            status = evaluate_plan_baseline(
                repo,
                task_ref=task_ref,
                task_plan_path=plan_path,
                target_branch=identity.get("target_branch"),
            )
            if status.baseline_status in ("missing", "unknown"):
                return True

    return False


def probe_ambiguous_task_ref(
    repo: Path,
    branch: str,
    derived_task_ref: str | None,
    known_task_refs: Iterable[str],
) -> bool:
    """Return True when close-check must block for unresolved task identity.

  Close-check only appends ``ambiguous_task_ref`` when a material gate
  would otherwise be skipped (checklist sync, open findings, plan
  baseline) or when multiple live rows share the same
  ``target_branch``.
    """
    branch_refs = _task_refs_on_branch(repo, branch)
    matching = _matching_registered_candidates(branch, known_task_refs)

    if len(branch_refs) > 1 or len(matching) > 1:
        return True

    if derived_task_ref is not None:
        return False

    single = (
        branch_refs[0]
        if len(branch_refs) == 1
        else matching[0] if len(matching) == 1 else None
    )
    if single is None:
        return False

    return _material_gates_need_task_ref(repo, single)


def _query_active_task_identity(
    repo: Path,
    task_ref: str | None,
) -> tuple[dict[str, str | None] | None, bool]:
    """Return ``(identity, ok)`` for the active task's identity envelope.

    Shells out to ``mcp-workbay-handoff --workspace-root <root> state
    --sections identity <task_ref>`` and projects ``data.active`` into a
    small dict with the fields ``review-ready`` / ``close-check`` need
    (``task_plan_path``, ``target_branch``).

    ``ok`` is False on missing CLI, non-zero exit, or unparseable JSON —
    callers translate that to ``handoff_projection="pending"`` and apply
    fail-closed semantics for the plan-baseline gate (internal PR-04).
    A successful query with no ``active`` block returns ``(None, True)``
    so an unregistered task surfaces as "planless" rather than blocked.
    """
    if not task_ref:
        return None, True
    workspace = resolver.canonical_workspace_root(repo) or repo
    argv = [
        _common.mcp_handoff_bin(),
        "--workspace-root", str(workspace),
        "state",
        "--sections", "identity",
        task_ref,
    ]
    proc = _common.run_subprocess(argv)
    if proc.returncode != 0:
        return None, False
    try:
        payload = json.loads(proc.stdout)
    except (ValueError, json.JSONDecodeError):
        return None, False
    active = (
        (payload.get("data") or {}).get("active")
        if isinstance(payload, dict)
        else None
    )
    if not isinstance(active, dict):
        return None, True
    return (
        {
            "task_plan_path": active.get("task_plan_path"),
            "target_branch": active.get("target_branch"),
        },
        True,
    )


def augment_with_handoff_state(
    repo: Path,
    task_ref: str | None,
    head: str,
    reasons: list[str],
) -> tuple[list[str], dict[str, int], str]:
    """Append handoff-state reasons + return findings_open and projection.

    Composes three CLI queries used by both ``review-ready`` and
    ``close-check``:

    * :func:`_query_open_findings` — appends ``open_high_finding`` /
      ``open_medium_finding`` when the live counts are non-zero.
    * :func:`_query_latest_passing_test_sha` — appends
      ``stale_test_evidence`` when the most recent passing
      verified_test's ``commit_sha`` is missing or differs from the
      current HEAD.
    * :func:`_query_active_task_identity` + :func:`evaluate_plan_baseline` —
      appends ``plan_baseline_missing`` when the active task's stored
      ``task_plan_path`` exists on the feature branch but not on
      ``main``, or ``plan_baseline_unknown`` when the evaluator could
      not determine acceptance (degraded MCP query, missing identity
      row). Both reasons map to the ``planning_baseline`` owner bucket
      so workflow clients keep a single routing key, but the operator
      hint differs: ``plan_baseline_missing`` points at
      ``make plan-accept`` while ``plan_baseline_unknown`` points at a
      retry of the gate (internal).

      The gate is fail-closed (internal PR-04) when MCP is reachable
      but the identity lookup fails. When MCP is entirely unreachable
      (no CLI calls succeeded), the existing degraded-CLI contract is
      preserved so offline operators still get useful gate output
      rather than a blanket block.

    Returns ``(reasons, findings_open, handoff_projection)``.
    ``handoff_projection`` is ``"synced"`` only when *all* queries
    succeed; if any fails, callers see ``"pending"`` to flag
    unverified state. The freshness check is skipped on a CLI failure
    so we don't penalize branches simply because the handoff DB is
    unreachable.
    """
    findings_open, findings_ok = _query_open_findings(repo, task_ref)
    if findings_ok:
        if findings_open["high"] > 0:
            reasons.append("open_high_finding")
        if findings_open["medium"] > 0:
            reasons.append("open_medium_finding")

    latest_sha, evidence_ok = _query_latest_passing_test_sha(repo, task_ref)
    if evidence_ok and latest_sha != head:
        reasons.append("stale_test_evidence")

    identity, identity_ok = _query_active_task_identity(repo, task_ref)
    baseline_ok = True
    mcp_reachable = findings_ok or evidence_ok
    if not identity_ok and mcp_reachable:
        # Identity lookup itself failed against a reachable MCP — we
        # cannot determine the baseline status at all. Fail-closed but
        # report ``plan_baseline_unknown`` so operators retry the gate
        # instead of running ``make plan-accept`` for a plan that may
        # already be accepted (internal).
        reasons.append("plan_baseline_unknown")
        baseline_ok = False
    elif identity is not None:
        plan_path = identity.get("task_plan_path")
        target_branch = identity.get("target_branch")
        if plan_path:
            status = evaluate_plan_baseline(
                repo,
                task_ref=task_ref or "",
                task_plan_path=plan_path,
                target_branch=target_branch,
            )
            if status.baseline_status == "missing":
                reasons.append("plan_baseline_missing")
            elif status.baseline_status == "unknown":
                reasons.append("plan_baseline_unknown")
            if not status.mcp_available:
                baseline_ok = False

    handoff_projection = (
        "synced"
        if (findings_ok and evidence_ok and identity_ok and baseline_ok)
        else "pending"
    )
    return reasons, findings_open, handoff_projection


_BRANCH_PLAN_ID_RE = re.compile(r"-plan(\d{4})$")
_PLAN_REV_SUFFIX_RE = re.compile(r"-r\d+$")


def _plan_id_from_branch(branch: str) -> str | None:
    """Return the ``<NNNN>`` plan id encoded in a ``…-plan<NNNN>`` branch, else None."""
    match = _BRANCH_PLAN_ID_RE.search(branch)
    return match.group(1) if match is not None else None


def _detect_late_plan_id_collision(repo: Path, plan_id: str) -> str | None:
    """Return a conflicting sibling plan-doc path when ``plan_id`` is backed by
    more than one distinct ``docs/plans/<plan_id>-<stem>.md`` across **all refs**,
    else None.

    implementation note D1: two concurrent ``--plan-intent`` starters can compute the same
    ``max+1`` before either claim stub reaches shared history. Once both stubs are
    refs, the same id backs two distinct doc paths — the local claim was never
    globally atomic. review-ready/close-check surface this as reconcile-required
    so D2's ``plan-accept`` relink (not a silent local rename) owns the fix.
    Revision variants (``-r2``) of one plan share a slug stem and never collide.
    """
    id_re = re.compile(r"(?:^|/)" + re.escape(plan_id) + r"-([^/]+)\.md$")
    stems: dict[str, str] = {}
    proc = _common.run_subprocess(
        [
            "git", "-C", str(repo), "log", "--all", "--pretty=format:",
            "--name-only", "--", "docs/plans",
        ]
    )
    if proc.returncode != 0:
        return None
    for raw in (proc.stdout or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        match = id_re.search(line)
        if match is None:
            continue
        stem = _PLAN_REV_SUFFIX_RE.sub("", match.group(1))
        stems.setdefault(stem, line)
    if len(stems) > 1:
        return sorted(stems.values())[0]
    return None


def evaluate(repo: Path, base: str) -> list[str]:
    """Run the review-ready check loop and return failed-reason tokens.

    Public entrypoint so ``close-check`` can compose the same checks
    without re-emitting the receipt. Empty list = ready.
    """
    branch = resolver.current_branch(repo) or ""
    reasons: list[str] = []
    if branch in PROTECTED_BASES:
        reasons.append("on_protected_base")
    elif not _has_diff_against_base(repo, base):
        reasons.append("no_changes_against_base")
    if resolver.dirty_summary(repo)["total"] > 0:
        reasons.append("dirty_worktree")
    plan_id = _plan_id_from_branch(branch)
    if plan_id is not None and _detect_late_plan_id_collision(repo, plan_id):
        reasons.append("plan_id_collision_requires_reconcile")
    return reasons


def _emit(
    *,
    command: str,
    repo: Path,
    branch: str,
    head: str,
    derived_task_ref: str | None,
    reasons: list[str],
    findings_open: dict[str, int] | None = None,
    handoff_projection: str = "synced",
    warnings: list[str] | None = None,
    extras: dict[str, Any] | None = None,
    emit_json: bool,
) -> None:
    warnings = list(warnings) if warnings else []
    grouped = reasons_by_owner(reasons)
    receipt: dict[str, Any] = {
        "ok": True,
        "command": command,
        "task_ref": derived_task_ref,
        "branch": branch,
        "worktree_path": str(repo),
        "head": head,
        "handoff_projection": handoff_projection,
        "events": [f"{command.replace('-', '_')}_evaluated"],
        "ready": not reasons,
        "reasons": reasons,
        "reasons_by_owner": grouped,
        "findings_open": findings_open or dict(ZERO_FINDINGS),
        "warnings": warnings,
        "next_command": next_command_for(
            command=command,
            reasons=reasons,
            grouped=grouped,
            derived_task_ref=derived_task_ref,
        ),
    }
    if extras:
        receipt.update(extras)

    if not emit_json:
        if not reasons:
            sys.stderr.write(f"{command}: READY\n")
        else:
            sys.stderr.write(
                f"{command}: NOT READY: " + ", ".join(reasons) + "\n"
            )
        for w in warnings:
            sys.stderr.write(f"WARNING: {w}\n")

    _common.emit(receipt)


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle review-ready", add_help=True)
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    parser.add_argument(
        "--base",
        dest="base",
        default="main",
        help="Base branch the feature branch should diverge from.",
    )
    args = parser.parse_args(argv)

    repo = resolver.repo_root() or Path.cwd()
    branch = resolver.current_branch(repo) or ""
    head = resolver.head_sha(repo) or ""
    derived_task_ref = resolver.derive_task_ref(
        branch, known_task_refs=_common._live_task_refs(repo)
    )

    reasons = evaluate(repo, args.base)
    reasons, findings_open, handoff_projection = augment_with_handoff_state(
        repo, derived_task_ref, head, reasons
    )
    warnings = _orphan_planning_warnings(repo)
    # internal: warn-only checklist guardrail. review-ready is
    # the mid-loop gate; we surface evidence-backed unchecked items as
    # a warning so the operator can sync, but we do NOT block — the
    # next slice may still be in flight.
    pending_count, plan_path = _probe_checklist_sync_pending(repo, derived_task_ref)
    if pending_count is not None and pending_count > 0:
        warnings.append(
            f"checklist_sync_pending: {pending_count} evidence-backed unchecked "
            f"items in {plan_path} — run `make sync-task-plan-checklist "
            f"TASK={derived_task_ref or '<task-ref>'} APPLY=1`"
        )
    _emit(
        command="review-ready",
        repo=repo,
        branch=branch,
        head=head,
        derived_task_ref=derived_task_ref,
        reasons=reasons,
        findings_open=findings_open,
        handoff_projection=handoff_projection,
        warnings=warnings,
        emit_json=args.emit_json,
    )
    return 0
