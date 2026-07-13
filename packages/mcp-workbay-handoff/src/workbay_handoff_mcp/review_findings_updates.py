"""Update, resolve, and provenance-repair operations for review findings."""

from __future__ import annotations

import logging
import os
import sqlite3
import subprocess
from dataclasses import dataclass
from typing import cast

from . import shared_write_context as _shared_write_context
from .concept_embed_hook import embed_finding_from_envelope
from .enums import FindingStatus
from .git_merge import is_ancestor_of_ref as _is_ancestor_of_ref
from .review_finding_resolution import ResolutionOutcomeKind, classify_resolution_outcome
from .review_findings_support import (
    _canonical_repair_provenance_decision_id,
    _classify_commit_relation,
    _current_task_revision,
    _current_task_revision_for,
    _write_current_task_md_for_active_context,
)
from .runtime import get_runtime_config
from .shared_primitives import (
    BATCH_CLOSE_THRESHOLD,
    BATCH_CLOSE_WINDOW_SECONDS,
    MAX_REOPEN_REASON_LENGTH,
    MAX_RESOLUTION_NOTES_LENGTH,
    MAX_VERIFICATION_EVIDENCE_LENGTH,
    REOPEN_ESCALATION_THRESHOLD,
    REVIEW_FINDING_STATUSES,
    _envelope,
    _normalize_optional_text,
    _resolve_task_ref,
    _row_to_dict,
)
from .shared_schema import _get_db_connection
from .shared_write_context import (
    BranchMismatchError,
    InvalidCommitShaError,
    ResolvedWriteContext,
    WriteActor,
    _resolve_write_actor,
    collect_target_context_warnings,
)
from .structured_rejections import (
    rejection_batch_close_evidence,
    rejection_commit_ancestry,
    rejection_resolution_notes_max_length,
    rejection_superseded_merge_managed,
)
from .verified_tests import get_verified_tests

_LOG = logging.getLogger(__name__)
# Provenance tag for auto-attached verification_evidence (internal).
# Commit-scoped, not finding-scoped: a matching verified_tests row proves the
# commit was green, not that this finding was verified. Kept legible so
# reviewers can distinguish auto-derived from agent-attested evidence.
EVIDENCE_SOURCE_VERIFIED_TESTS_AUTO = "verified_tests_auto"
# T22: planning-mode resolve on an uncommitted/untracked plan draft. Distinct
# from commit-backed verified_tests auto-evidence; closes the plan-accept
# deadlock where resolve demanded a commit while plan-accept demanded resolved findings.
EVIDENCE_SOURCE_DRAFT_EVIDENCE = "draft_evidence"
# Bound multi-command auto-evidence selection (latest passing per command).
MAX_AUTO_EVIDENCE_COMMANDS = 20


@dataclass(frozen=True)
class WorkspaceCleanliness:
    has_uncommitted_changes: bool
    error: str | None = None


def _workspace_git_cwd(worktree_path: str | None = None) -> str:
    if worktree_path is not None:
        return worktree_path
    try:
        return str(get_runtime_config().git_workspace_root)
    except RuntimeError:
        return os.getcwd()


def _workspace_has_uncommitted_changes(worktree_path: str | None = None) -> WorkspaceCleanliness:
    # internal: when a task worktree is derived (resolve path), inspect that
    # worktree; otherwise fall back to the process checkout
    # (``git_workspace_root``), preserving today's behavior for every caller
    # that does not pass an explicit path.
    cwd = _workspace_git_cwd(worktree_path)
    try:
        proc = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except FileNotFoundError:
        return WorkspaceCleanliness(False, "git is not available in PATH for `git status --porcelain`.")
    except OSError as exc:
        return WorkspaceCleanliness(False, f"git status could not run: {exc}")
    except subprocess.TimeoutExpired:
        return WorkspaceCleanliness(False, "`git status --porcelain` timed out while checking workspace cleanliness.")
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip() or "git status exited non-zero"
        return WorkspaceCleanliness(False, stderr)
    return WorkspaceCleanliness(bool(proc.stdout.strip()))


def _path_is_uncommitted_or_untracked(rel_path: str | None, worktree_path: str | None = None) -> bool:
    """True when ``rel_path`` is dirty or untracked in the resolve workspace (T22)."""
    if not rel_path or not isinstance(rel_path, str):
        return False
    normalized = rel_path.strip().replace("\\", "/").lstrip("./")
    if not normalized:
        return False
    cwd = _workspace_git_cwd(worktree_path)
    try:
        proc = subprocess.run(
            ["git", "status", "--porcelain", "--", normalized],
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return False
    if proc.returncode != 0:
        return False
    return bool(proc.stdout.strip())


def _finding_resolution_paths(row: dict) -> list[str]:
    """Normalized finding file path(s) for resolve dirt-scoping (0128 / DBG-10).

    Findings store a single ``file_path`` today; empty/whitespace means "no
    paths" and callers fall back to whole-tree cleanliness.
    """
    raw = row.get("file_path")
    if raw is None or not isinstance(raw, str):
        return []
    normalized = raw.strip().replace("\\", "/").lstrip("./")
    return [normalized] if normalized else []


def _finding_relevant_uncommitted(
    row: dict,
    *,
    worktree_path: str | None,
    whole_tree_dirty: bool,
) -> tuple[bool, list[str]]:
    """Return (relevant_paths_dirty, dirty_paths) for resolve classification.

    When the finding has file path(s), dirt is path-scoped via the existing
    T22 helper (REF-19). Whole-tree dirt only applies when there are no paths.
    """
    paths = _finding_resolution_paths(row)
    if not paths:
        return whole_tree_dirty, []
    dirty = [p for p in paths if _path_is_uncommitted_or_untracked(p, worktree_path)]
    return bool(dirty), dirty


def _is_planning_draft_evidence_eligible(
    row: dict,
    *,
    worktree_path: str | None,
    has_uncommitted_changes: bool,
    verification_evidence: str | None,
    resolution_notes: str | None,
) -> bool:
    """Planning finding on an uncommitted draft doc with explicit draft evidence (T22)."""
    if not has_uncommitted_changes:
        return False
    if not verification_evidence or not resolution_notes:
        return False
    review_mode = str(row.get("review_mode") or "").strip()
    if review_mode != "planning":
        return False
    file_path = row.get("file_path")
    return _path_is_uncommitted_or_untracked(
        str(file_path) if file_path is not None else None,
        worktree_path,
    )


def _derive_resolve_worktree_path(conn: sqlite3.Connection, task_ref: str | None) -> str | None:
    """Derive the task's linked worktree for a resolve, or ``None`` to fall
    back to the process checkout.

    internal: resolve must evaluate cleanliness and commit context against the
    task's own worktree, not the long-lived server's process checkout. The
    worktree is derived from the row's canonical ``target_branch`` via
    :func:`_canonical_worktree_for_task` (internal — the stored
    ``target_worktree_path`` column is never read). Returns ``None`` — meaning
    "use today's process-checkout behavior" — when worktree derivation is
    bypassed (``WORKBAY_HANDOFF_SKIP_WORKTREE_DERIVATION``), the row has no
    branch identity yet, or no matching worktree exists (a ``main``/MAINT row
    or an archived/torn-down task raising ``WorktreeNotFoundError``). The
    derivation is scoped to resolve only; ``_resolve_write_actor`` is left
    untouched so internal cwd-wins precedence holds for other writers.
    """
    if task_ref is None:
        return None
    if not _shared_write_context._worktree_derivation_enabled():
        return None
    row = conn.execute(
        "SELECT target_branch FROM handoff_state WHERE task_ref = ?",
        (task_ref,),
    ).fetchone()
    if row is None:
        return None
    target_branch = _normalize_optional_text(row["target_branch"])
    if target_branch is None:
        return None
    canonical_fn = _shared_write_context._resolve_core_override(
        "_canonical_worktree_for_task",
        _shared_write_context._canonical_worktree_for_task,
    )
    try:
        return cast("str | None", canonical_fn(target_branch))
    except _shared_write_context.WorktreeNotFoundError:
        return None


def _coerce_workspace_cleanliness(value: WorkspaceCleanliness | bool) -> WorkspaceCleanliness:
    if isinstance(value, WorkspaceCleanliness):
        return value
    return WorkspaceCleanliness(bool(value))


def _build_resolve_actor(
    ctx: ResolvedWriteContext,
    *,
    branch: str | None = None,
    commit_sha: str | None = None,
) -> WriteActor:
    actor: WriteActor = {}
    for key in ("agent", "branch", "commit_sha", "lane_id", "model", "model_label", "reasoning_level"):
        value = getattr(ctx, key)
        if value is not None:
            actor[key] = value
    # internal: override branch/commit with the resolve-scoped worktree
    # anchor so the resolution write's provenance is the task branch/commit,
    # not the long-lived server's process checkout.
    if branch is not None:
        actor["branch"] = branch
    if commit_sha is not None:
        actor["commit_sha"] = commit_sha
    return actor


def _normalize_resolution_targets(finding_ids: list[str] | None) -> list[str]:
    targets: list[str] = []
    seen: set[str] = set()
    for raw in finding_ids or []:
        normalized = raw.strip() if isinstance(raw, str) else ""
        if normalized and normalized not in seen:
            seen.add(normalized)
            targets.append(normalized)
    return targets


def _load_resolution_rows(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    finding_ids: list[str],
    all_open: bool,
) -> tuple[list[dict], dict | None]:
    if all_open and finding_ids:
        return [], {"ok": False, "error": "Pass either finding_ids or all_open, not both."}
    if not all_open and not finding_ids:
        return [], {"ok": False, "error": "Pass finding_ids or set all_open=True."}

    if all_open:
        rows = conn.execute(
            "SELECT * FROM review_findings WHERE task_ref = ? AND status = 'open' ORDER BY id ASC",
            (task_ref,),
        ).fetchall()
        return [dict(row) for row in rows], None

    placeholders = ",".join("?" for _ in finding_ids)
    rows = conn.execute(
        f"SELECT * FROM review_findings WHERE task_ref = ? AND finding_id IN ({placeholders}) ORDER BY id ASC",
        (task_ref, *finding_ids),
    ).fetchall()
    found_by_id = {str(row["finding_id"]): row for row in rows}
    missing = [finding_id for finding_id in finding_ids if finding_id not in found_by_id]
    if missing:
        return [], {
            "ok": False,
            "error": f"Findings not found for task {task_ref}: {missing}.",
        }
    non_open = [finding_id for finding_id, row in found_by_id.items() if str(row["status"]) != FindingStatus.OPEN.value]
    if non_open:
        return [], {
            "ok": False,
            "error": f"Only open findings can be resolved. Not open: {non_open}.",
        }
    return [dict(found_by_id[finding_id]) for finding_id in finding_ids], None


@dataclass(frozen=True)
class AutoVerificationEvidence:
    """Bounded, deterministic auto-evidence derived from verified_tests."""

    text: str
    evidence_source: str
    commit_sha: str
    verified_test_ids: tuple[int, ...]
    commands: tuple[str, ...]


def select_auto_verification_evidence(
    *,
    task_ref: str,
    commit_sha: str | None,
) -> AutoVerificationEvidence | None:
    """Build verification_evidence from matching verified_tests rows.

    Match rule (exact): ``verified_tests.commit_sha == commit_sha`` and
    ``passed=True``. Stale rows (other commits) and missing rows leave
    evidence empty — the agent must still supply text.

    Selection is deterministic and bounded: latest passing row per distinct
    ``command`` (source order is ``verified_at DESC, id DESC``), then stable
    command-name order, capped at ``MAX_AUTO_EVIDENCE_COMMANDS``. Attached
    text is truncated to ``MAX_VERIFICATION_EVIDENCE_LENGTH``.
    """
    normalized_commit = _normalize_optional_text(commit_sha)
    if normalized_commit is None:
        return None

    listed = get_verified_tests(
        task_ref=task_ref,
        commit_sha=normalized_commit,
        passed=True,
        limit=200,
    )
    if not listed.get("ok"):
        return None
    tests = listed.get("data", {}).get("tests") or []
    if not isinstance(tests, list) or not tests:
        return None

    # Latest-passing-per-command: get_verified_tests orders verified_at DESC,
    # id DESC, so the first sighting of each command is the newest.
    by_command: dict[str, dict[str, object]] = {}
    for raw in tests:
        if not isinstance(raw, dict):
            continue
        row_commit = _normalize_optional_text(raw.get("commit_sha"))
        if row_commit != normalized_commit:
            continue
        if not bool(raw.get("passed")):
            continue
        command = str(raw.get("command") or "").strip()
        if not command or command in by_command:
            continue
        by_command[command] = raw

    if not by_command:
        return None

    selected = sorted(
        by_command.values(),
        key=lambda row: (str(row.get("command") or ""), int(cast(int, row.get("id") or 0))),
    )[:MAX_AUTO_EVIDENCE_COMMANDS]

    lines: list[str] = []
    ids: list[int] = []
    commands: list[str] = []
    for row in selected:
        command = str(row.get("command") or "")
        result_text = row.get("result")
        result_str = str(result_text).strip() if result_text is not None else ""
        ids.append(int(cast(int, row["id"])))
        commands.append(command)
        if result_str:
            lines.append(f"{command}\n  result: {result_str}")
        else:
            lines.append(command)

    body = "\n".join(lines)
    if not body.strip():
        return None

    # Persist provenance on the finding row (verification_evidence column) so a
    # later list/read can distinguish auto-derived from agent-attested text
    # without a schema migration. Receipt also carries evidence_source.
    id_csv = ",".join(str(i) for i in ids)
    header = (
        f"evidence_source={EVIDENCE_SOURCE_VERIFIED_TESTS_AUTO}; "
        f"commit_sha={normalized_commit}; "
        f"verified_test_ids={id_csv}"
    )
    text = f"{header}\n{body}"
    if len(text) > MAX_VERIFICATION_EVIDENCE_LENGTH:
        text = text[:MAX_VERIFICATION_EVIDENCE_LENGTH]

    return AutoVerificationEvidence(
        text=text,
        evidence_source=EVIDENCE_SOURCE_VERIFIED_TESTS_AUTO,
        commit_sha=normalized_commit,
        verified_test_ids=tuple(ids),
        commands=tuple(commands),
    )


def resolve_review_findings(
    *,
    task_ref: str | None = None,
    session: str | None = None,
    finding_ids: list[str] | None = None,
    all_open: bool = False,
    resolution_notes: str | None = None,
    verification_evidence: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    normalized_finding_ids = _normalize_resolution_targets(finding_ids)
    normalized_resolution_notes = _normalize_optional_text(resolution_notes)
    normalized_verification_evidence = _normalize_optional_text(verification_evidence)
    if normalized_resolution_notes is not None and len(normalized_resolution_notes) > MAX_RESOLUTION_NOTES_LENGTH:
        return _envelope(
            ok=False,
            tool="resolve_review_findings",
            data=rejection_resolution_notes_max_length(
                actual_length=len(normalized_resolution_notes),
            ),
            task_ref=task_ref,
            entity="finding",
        )
    auto_evidence: AutoVerificationEvidence | None = None
    try:
        with _get_db_connection() as conn:
            resolved_task_ref = _resolve_task_ref(conn, task_ref)
            # internal: anchor the resolve to the task's own worktree (derived
            # from target_branch), not the process checkout. None => fall back
            # to today's process-checkout behavior.
            resolve_worktree_path = _derive_resolve_worktree_path(conn, resolved_task_ref)
            ctx = _resolve_write_actor(
                conn,
                actor,
                task_ref=resolved_task_ref,
                allow_missing_worktree_fallback=resolve_worktree_path is None,
            )
            warnings = list(collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref) or [])
            rows, load_error = _load_resolution_rows(
                conn,
                task_ref=resolved_task_ref,
                finding_ids=normalized_finding_ids,
                all_open=all_open,
            )
            recent_fixes = conn.execute(
                """
                SELECT COUNT(*) AS cnt FROM review_findings
                WHERE task_ref = ? AND status IN ('fixed', 'resolved_on_branch')
                  AND resolved_at >= datetime('now', ?)
                """,
                (resolved_task_ref, f"-{BATCH_CLOSE_WINDOW_SECONDS} seconds"),
            ).fetchone()
            recent_fixed_count = int(recent_fixes["cnt"]) if recent_fixes else 0
    except ValueError as exc:
        return _envelope(
            ok=False,
            tool="resolve_review_findings",
            data={"error": str(exc)},
            task_ref=task_ref,
            entity="finding",
        )

    if load_error is not None:
        return _envelope(
            ok=False,
            tool="resolve_review_findings",
            data={"error": load_error["error"]},
            task_ref=resolved_task_ref,
            entity="finding",
            warnings=warnings,
        )

    cleanliness = _coerce_workspace_cleanliness(_workspace_has_uncommitted_changes(resolve_worktree_path))
    whole_tree_dirty = cleanliness.has_uncommitted_changes
    # internal: compute one resolve-scoped (branch, commit) anchor and
    # feed it to every downstream consumer so cleanliness, classification, and
    # provenance can never diverge. Precedence: explicit actor (already baked
    # into ``ctx`` by ``_resolve_write_actor``) wins; otherwise, when a task
    # worktree was derived, its HEAD wins over the caller-cwd ``ctx`` for any
    # field the caller did not pin. ``_resolve_write_actor`` is left untouched,
    # so internal cwd-wins precedence holds for every other writer.
    explicit_branch = _normalize_optional_text(actor.get("branch")) if actor else None
    explicit_commit = _normalize_optional_text(actor.get("commit_sha")) if actor else None
    resolve_branch = ctx.branch
    resolve_commit = ctx.commit_sha
    if resolve_worktree_path is not None:
        worktree_branch, worktree_commit = _shared_write_context._detect_git_write_context_at(resolve_worktree_path)
        if explicit_branch is None and worktree_branch is not None:
            resolve_branch = worktree_branch
        if explicit_commit is None and worktree_commit is not None:
            resolve_commit = worktree_commit
    resolved_actor = _build_resolve_actor(ctx, branch=resolve_branch, commit_sha=resolve_commit)
    planned_results: list[tuple[dict, dict[str, object]]] = []
    results: list[dict[str, object]] = []
    fixed_ids: list[str] = []
    # Receipt-level dirt: OR of per-finding *relevant* dirt (path-scoped when
    # the finding has file_path(s); whole-tree only when paths are absent).
    any_relevant_uncommitted = False

    for row in rows:
        finding_commit_sha = _normalize_optional_text(row.get("commit_sha"))
        commit_relation = _classify_commit_relation(finding_commit_sha, resolve_commit)
        has_uncommitted_changes, dirty_paths = _finding_relevant_uncommitted(
            row,
            worktree_path=resolve_worktree_path,
            whole_tree_dirty=whole_tree_dirty,
        )
        if has_uncommitted_changes:
            any_relevant_uncommitted = True
        # Escape hatch unchanged: verified_commit_sha only matters when relevant
        # paths are clean (or caller would not get FIXED via this gate).
        verified_commit_sha = None if has_uncommitted_changes else _normalize_optional_text(resolve_commit)
        if cleanliness.error is not None:
            outcome = classify_resolution_outcome(
                finding_commit_sha=finding_commit_sha,
                workspace_commit_sha=resolve_commit,
                verified_commit_sha=None,
                commit_relation=commit_relation,
                has_uncommitted_changes=False,
            )
            outcome = outcome.__class__(
                kind=ResolutionOutcomeKind.BLOCKED_BY_CONTEXT,
                reason=(
                    "Could not determine whether the workspace is clean because `git status --porcelain` failed: "
                    f"{cleanliness.error}"
                ),
                verified_commit_sha=None,
                finding_commit_sha=finding_commit_sha,
                workspace_commit_sha=resolve_commit,
                commit_relation=commit_relation,
            )
        else:
            outcome = classify_resolution_outcome(
                finding_commit_sha=finding_commit_sha,
                workspace_commit_sha=resolve_commit,
                verified_commit_sha=verified_commit_sha,
                commit_relation=commit_relation,
                has_uncommitted_changes=has_uncommitted_changes,
                dirty_paths=dirty_paths or None,
            )
            # T22: planning-mode draft-content evidence escapes pending_uncommitted
            # when the finding's plan file is itself the uncommitted draft. Audit
            # trail still requires verification_evidence + resolution_notes.
            if outcome.kind is ResolutionOutcomeKind.PENDING_UNCOMMITTED and _is_planning_draft_evidence_eligible(
                row,
                worktree_path=resolve_worktree_path,
                has_uncommitted_changes=has_uncommitted_changes,
                verification_evidence=normalized_verification_evidence,
                resolution_notes=normalized_resolution_notes,
            ):
                outcome = outcome.__class__(
                    kind=ResolutionOutcomeKind.FIXED,
                    reason=(
                        "Planning draft-content evidence accepted for uncommitted plan "
                        "document; recorded as draft_evidence (commit-guard unchanged for "
                        "non-planning branch findings)."
                    ),
                    verified_commit_sha=None,
                    finding_commit_sha=finding_commit_sha,
                    workspace_commit_sha=resolve_commit,
                    commit_relation=commit_relation,
                )
            if (
                outcome.kind is ResolutionOutcomeKind.FIXED
                and commit_relation == "descendant"
                and normalized_resolution_notes is None
            ):
                outcome = outcome.__class__(
                    kind=ResolutionOutcomeKind.BLOCKED_BY_CONTEXT,
                    reason=(
                        "resolution_notes is required when resolving a finding from a newer descendant commit. "
                        "Pass human-authored notes explaining how the later commit closes the finding."
                    ),
                    verified_commit_sha=outcome.verified_commit_sha,
                    finding_commit_sha=outcome.finding_commit_sha,
                    workspace_commit_sha=outcome.workspace_commit_sha,
                    commit_relation=outcome.commit_relation,
                )
        entry: dict[str, object] = {
            "finding_id": str(row["finding_id"]),
            "finding_db_id": int(row["id"]),
            "outcome": outcome.kind.value,
            "reason": outcome.reason,
            "finding_commit_sha": outcome.finding_commit_sha,
            "workspace_commit_sha": outcome.workspace_commit_sha,
            "verified_commit_sha": outcome.verified_commit_sha,
            "commit_relation": outcome.commit_relation,
        }
        if (
            outcome.kind is ResolutionOutcomeKind.FIXED
            and outcome.reason
            and "draft_evidence" in (outcome.reason or "")
        ):
            entry["evidence_source"] = EVIDENCE_SOURCE_DRAFT_EVIDENCE
            entry["draft_evidence"] = True
        # Ancestry blocks: attach the structured rejection envelope (internal).
        if outcome.kind is ResolutionOutcomeKind.BLOCKED_BY_CONTEXT and commit_relation in {
            "ancestor",
            "diverged",
        }:
            ancestry = rejection_commit_ancestry(
                relation=commit_relation,
                finding_commit_sha=finding_commit_sha,
                current_commit_sha=resolve_commit,
                current_branch=resolve_branch,
                verified_commit_sha=verified_commit_sha,
                finding_id=str(row["finding_id"]),
            )
            entry["rule_id"] = ancestry["rule_id"]
            entry["violated"] = ancestry["violated"]
            entry["expected"] = ancestry["expected"]
            entry["example"] = ancestry["example"]
        planned_results.append((row, entry))

    # implementation note: when the agent omitted verification_evidence, auto-attach from
    # verified_tests rows whose commit_sha exactly matches the resolve's
    # verified/workspace commit. No match / stale-only rows → leave empty so
    # the existing batch-close and reopen guards still require agent text.
    # Use workspace HEAD (not per-finding dirt) so path-clean findings can still
    # auto-attach evidence when other findings are path-dirty.
    if normalized_verification_evidence is None:
        auto_evidence = select_auto_verification_evidence(
            task_ref=resolved_task_ref,
            commit_sha=_normalize_optional_text(resolve_commit),
        )
        if auto_evidence is not None:
            normalized_verification_evidence = auto_evidence.text

    fixed_candidates = [entry for _, entry in planned_results if entry["outcome"] == ResolutionOutcomeKind.FIXED.value]
    if normalized_verification_evidence is None and recent_fixed_count + len(fixed_candidates) > BATCH_CLOSE_THRESHOLD:
        batch_rejection = rejection_batch_close_evidence(
            recent_fixes_in_window=recent_fixed_count,
            additional_closing=len(fixed_candidates),
        )
        batch_guard_reason = str(batch_rejection["error"])
        for _, entry in planned_results:
            if entry["outcome"] == ResolutionOutcomeKind.FIXED.value:
                entry["outcome"] = ResolutionOutcomeKind.BLOCKED_BY_CONTEXT.value
                entry["reason"] = batch_guard_reason
                entry["batch_close_guard"] = {
                    "recent_fixes_in_window": recent_fixed_count,
                    "window_seconds": BATCH_CLOSE_WINDOW_SECONDS,
                    "threshold": BATCH_CLOSE_THRESHOLD,
                }
                # Structured rejection envelope on each blocked entry (internal).
                entry["rule_id"] = batch_rejection["rule_id"]
                entry["violated"] = batch_rejection["violated"]
                entry["expected"] = batch_rejection["expected"]
                entry["example"] = batch_rejection["example"]

    for row, entry in planned_results:
        if entry["outcome"] == ResolutionOutcomeKind.FIXED.value:
            try:
                update_result = update_review_finding(
                    status=FindingStatus.FIXED.value,
                    finding_id=str(row["finding_id"]),
                    task_ref=resolved_task_ref,
                    session=session,
                    actor=resolved_actor,
                    verified_commit_sha=cast("str | None", entry["verified_commit_sha"]),
                    resolution_notes=normalized_resolution_notes,
                    verification_evidence=normalized_verification_evidence,
                    allow_missing_worktree_fallback=resolve_worktree_path is None,
                )
            except BranchMismatchError as exc:
                update_result = {
                    "ok": False,
                    "data": {
                        "error": str(exc),
                        "expected_branch": exc.expected_branch,
                        "actual_branch": exc.actual_branch,
                        "task_ref": exc.task_ref,
                    },
                }
            if not update_result.get("ok"):
                entry["outcome"] = ResolutionOutcomeKind.ERROR.value
                entry["reason"] = update_result.get("data", {}).get("error") or "failed to update finding"
                if update_result.get("data", {}).get("commit_guard") is not None:
                    entry["commit_guard"] = update_result["data"]["commit_guard"]
                if update_result.get("data", {}).get("false_fix_guard") is not None:
                    entry["false_fix_guard"] = update_result["data"]["false_fix_guard"]
            else:
                entry["finding"] = update_result.get("data", {}).get("finding")
                entry["commit_guard"] = update_result.get("data", {}).get("commit_guard")
                if auto_evidence is not None:
                    entry["evidence_source"] = auto_evidence.evidence_source
                    entry["auto_evidence"] = {
                        "commit_sha": auto_evidence.commit_sha,
                        "verified_test_ids": list(auto_evidence.verified_test_ids),
                        "commands": list(auto_evidence.commands),
                    }
                fixed_ids.append(str(row["finding_id"]))
        results.append(entry)

    counts = {kind.value: 0 for kind in ResolutionOutcomeKind}
    for entry in results:
        counts[str(entry["outcome"])] += 1

    dashboard = None
    if fixed_ids:
        from .dashboard_rendering import generate_dashboard_md  # noqa: PLC0415

        dashboard = generate_dashboard_md(write_file=True)

    receipt = {
        "task_ref": resolved_task_ref,
        "workspace_branch": resolve_branch,
        "workspace_commit_sha": resolve_commit,
        "has_uncommitted_changes": any_relevant_uncommitted,
        "counts": counts,
        "results": results,
    }
    if session is not None:
        receipt["session"] = session
    if dashboard is not None:
        receipt["dashboard"] = dashboard.get("data", {})
    return _envelope(
        ok=True,
        tool="resolve_review_findings",
        data={"receipt": receipt},
        task_ref=resolved_task_ref,
        entity="finding",
        mutation={
            "entity": "finding",
            "operation": "resolve",
            "affected_ids": fixed_ids,
            "task_revision": _current_task_revision_for(resolved_task_ref) if fixed_ids else None,
        },
        warnings=warnings,
    )


@dataclass(frozen=True)
class FindingUpdateInput:
    status: FindingStatus
    resolution_notes: str | None
    reopen_reason: str | None
    verified_commit_sha: str | None
    verification_evidence: str | None
    is_reopen_transition: bool


@dataclass(frozen=True)
class FindingUpdateContext:
    conn: sqlite3.Connection
    existing: sqlite3.Row
    ctx: ResolvedWriteContext
    session: str | None
    task_ref: str
    warnings: list[str] | None = None


def _check_reopen_escalation_guard(
    existing: sqlite3.Row,
    verification_evidence: str | None,
) -> dict | None:
    existing_reopen_count = int(existing["reopen_count"] or 0)
    if existing_reopen_count >= REOPEN_ESCALATION_THRESHOLD and verification_evidence is None:
        return {
            "ok": False,
            "error": (
                f"verification_evidence is required when fixing a finding that has been reopened "
                f"{existing_reopen_count} times (threshold: {REOPEN_ESCALATION_THRESHOLD}). "
                f"Provide code snippets, grep output, or diff output proving the fix exists."
            ),
            "false_fix_guard": {
                "finding_id": str(existing["finding_id"]),
                "reopen_count": existing_reopen_count,
                "threshold": REOPEN_ESCALATION_THRESHOLD,
                "guard": "reopen_escalation",
            },
        }
    return None


def _check_batch_close_guard(
    conn: sqlite3.Connection,
    task_ref: str,
    existing: sqlite3.Row,
) -> dict | None:
    recent_fixes = conn.execute(
        """
        SELECT COUNT(*) AS cnt FROM review_findings
        WHERE task_ref = ? AND status IN ('fixed', 'resolved_on_branch')
          AND resolved_at >= datetime('now', ?)
          AND id != ?
        """,
        (task_ref, f"-{BATCH_CLOSE_WINDOW_SECONDS} seconds", int(existing["id"])),
    ).fetchone()
    recent_count = int(recent_fixes["cnt"]) if recent_fixes else 0
    if recent_count >= BATCH_CLOSE_THRESHOLD:
        payload = rejection_batch_close_evidence(
            finding_id=str(existing["finding_id"]),
            recent_fixes_in_window=recent_count,
        )
        return {"ok": False, **payload}
    return None


def _check_commit_relation_guard(
    existing: sqlite3.Row,
    commit_sha: str | None,
    verified_commit_sha: str | None,
    branch: str | None,
    resolution_notes: str | None,
) -> dict | None:
    finding_commit_sha = _normalize_optional_text(existing["commit_sha"])
    current_commit_sha = _normalize_optional_text(commit_sha)
    commit_relation = _classify_commit_relation(finding_commit_sha, current_commit_sha)
    if commit_relation in {"ancestor", "diverged"}:
        payload = rejection_commit_ancestry(
            relation=commit_relation,
            finding_commit_sha=finding_commit_sha,
            current_commit_sha=current_commit_sha,
            current_branch=branch,
            verified_commit_sha=verified_commit_sha,
            finding_id=str(existing["finding_id"]) if "finding_id" in existing.keys() else None,
        )
        return {"ok": False, **payload}
    if commit_relation == "descendant":
        if resolution_notes is None:
            return {
                "ok": False,
                "error": "resolution_notes is required when fixing a finding from a newer descendant commit.",
                "commit_guard": {
                    "finding_commit_sha": finding_commit_sha,
                    "current_commit_sha": current_commit_sha,
                    "current_branch": branch,
                    "relation": commit_relation,
                    "requires_verified_commit_sha": True,
                },
            }
        if verified_commit_sha is None:
            return {
                "ok": False,
                "error": "verified_commit_sha is required when fixing a finding from a newer descendant commit.",
                "commit_guard": {
                    "finding_commit_sha": finding_commit_sha,
                    "current_commit_sha": current_commit_sha,
                    "current_branch": branch,
                    "relation": commit_relation,
                    "requires_verified_commit_sha": True,
                },
            }
        if current_commit_sha is not None and verified_commit_sha != current_commit_sha:
            return {
                "ok": False,
                "error": "verified_commit_sha must match the current workspace/actor commit when resolving from a newer descendant commit.",
                "commit_guard": {
                    "finding_commit_sha": finding_commit_sha,
                    "current_commit_sha": current_commit_sha,
                    "current_branch": branch,
                    "verified_commit_sha": verified_commit_sha,
                    "relation": commit_relation,
                },
            }
        verified_relation = _classify_commit_relation(finding_commit_sha, verified_commit_sha)
        if verified_relation not in {"same", "descendant"}:
            return {
                "ok": False,
                "error": "verified_commit_sha must be the finding commit or a descendant of it.",
                "commit_guard": {
                    "finding_commit_sha": finding_commit_sha,
                    "current_commit_sha": current_commit_sha,
                    "current_branch": branch,
                    "verified_commit_sha": verified_commit_sha,
                    "relation": commit_relation,
                    "verified_relation": verified_relation,
                },
            }
    return None


def _apply_finding_update(update_ctx: FindingUpdateContext, update_input: FindingUpdateInput) -> dict:
    finding_commit_sha = _normalize_optional_text(update_ctx.existing["commit_sha"])
    current_commit_sha = _normalize_optional_text(update_ctx.ctx.commit_sha)
    commit_relation = _classify_commit_relation(finding_commit_sha, current_commit_sha)
    needs_descendant_ack = update_input.status == FindingStatus.FIXED and commit_relation == "descendant"
    target_db_id = int(update_ctx.existing["id"])
    reopen_transition_int = 1 if update_input.is_reopen_transition else 0
    # internal: persist the resolution anchor on every successful fixed-close.
    # verified_commit_sha takes precedence over the actor commit so the descendant-close
    # path records the operator-attested commit; the actor commit is the fallback when
    # the close is from `same`. The columns are written whether the feature flag is on
    # or off — the flag governs the status string flip in a later slice, not whether
    # we have evidence to anchor a future integrate reconciliation against.
    resolution_anchor_sha = (
        update_input.verified_commit_sha if update_input.verified_commit_sha else update_ctx.ctx.commit_sha
    )
    resolution_anchor_ref = update_ctx.ctx.branch
    # internal: when the lifecycle flag is on, a successful ``fixed``
    # close persists the new ``resolved_on_branch`` status value instead. All
    # CASE-WHEN guards keep matching against the user-input value (``'fixed'``)
    # so the resolution-anchor columns, ``resolved_at``, and resolution-notes
    # clearing all behave identically; only the column itself flips.
    lifecycle_flag_on = bool(get_runtime_config().finding_lifecycle_states_enabled)
    effective_status_value = (
        FindingStatus.RESOLVED_ON_BRANCH.value
        if lifecycle_flag_on and update_input.status is FindingStatus.FIXED
        else update_input.status.value
    )
    expected_updated_at = update_ctx.existing["updated_at"]
    expected_reopen_count = int(update_ctx.existing["reopen_count"] or 0)
    expected_status = str(update_ctx.existing["status"])
    updated = update_ctx.conn.execute(
        """
        UPDATE review_findings
        SET status = ?, resolved_at = CASE WHEN ? IN ('fixed', 'wontfix') THEN datetime('now') ELSE NULL END,
            agent = COALESCE(agent, ?), branch = COALESCE(branch, ?), commit_sha = COALESCE(commit_sha, ?),
            lane_id = COALESCE(lane_id, ?),
            session = COALESCE(?, session),
            resolution_notes = CASE WHEN ? = 'open' THEN NULL WHEN ? IS NOT NULL THEN ? WHEN ? = 'fixed' THEN NULL ELSE resolution_notes END,
            reopen_count = CASE WHEN ? = 1 THEN COALESCE(reopen_count, 0) + 1 ELSE COALESCE(reopen_count, 0) END,
            last_reopen_reason = CASE WHEN ? = 1 THEN ? ELSE last_reopen_reason END,
            last_reopened_at = CASE WHEN ? = 1 THEN datetime('now') ELSE last_reopened_at END,
            verification_evidence = CASE WHEN ? = 'open' THEN NULL WHEN ? IS NOT NULL THEN ? ELSE verification_evidence END,
            resolved_on_branch_at_commit = CASE
                WHEN ? = 'fixed' AND ? IS NOT NULL THEN ?
                WHEN ? = 'open' THEN NULL
                ELSE resolved_on_branch_at_commit
            END,
            resolved_on_branch_ref = CASE
                WHEN ? = 'fixed' AND ? IS NOT NULL THEN ?
                WHEN ? = 'open' THEN NULL
                ELSE resolved_on_branch_ref
            END,
            resolved_on_branch_at_ts = CASE
                WHEN ? = 'fixed' AND ? IS NOT NULL THEN datetime('now')
                WHEN ? = 'open' THEN NULL
                ELSE resolved_on_branch_at_ts
            END,
            updated_at = datetime('now')
        WHERE id = ? AND task_ref = ? AND updated_at = ? AND COALESCE(reopen_count, 0) = ? AND status = ?
        """,
        (
            effective_status_value,
            update_input.status.value,
            update_ctx.ctx.agent,
            update_ctx.ctx.branch,
            update_ctx.ctx.commit_sha,
            update_ctx.ctx.lane_id,
            update_ctx.session,
            update_input.status.value,
            update_input.resolution_notes,
            update_input.resolution_notes,
            update_input.status.value,
            reopen_transition_int,
            reopen_transition_int,
            update_input.reopen_reason,
            reopen_transition_int,
            update_input.status.value,
            update_input.verification_evidence,
            update_input.verification_evidence,
            update_input.status.value,
            resolution_anchor_sha,
            resolution_anchor_sha,
            update_input.status.value,
            update_input.status.value,
            resolution_anchor_ref,
            resolution_anchor_ref,
            update_input.status.value,
            update_input.status.value,
            resolution_anchor_sha,
            update_input.status.value,
            target_db_id,
            update_ctx.task_ref,
            expected_updated_at,
            expected_reopen_count,
            expected_status,
        ),
    )
    if updated.rowcount == 0:
        latest = update_ctx.conn.execute(
            "SELECT updated_at, status, reopen_count FROM review_findings WHERE id = ?",
            (target_db_id,),
        ).fetchone()
        return _envelope(
            ok=False,
            tool="update_review_finding",
            data={
                "error": "Finding state conflict.",
                "expected_updated_at": expected_updated_at,
                "expected_reopen_count": expected_reopen_count,
                "expected_status": expected_status,
                "current_updated_at": latest["updated_at"] if latest else None,
                "current_status": latest["status"] if latest else None,
                "current_reopen_count": int(latest["reopen_count"] or 0) if latest else None,
            },
            task_ref=update_ctx.task_ref,
            entity="finding",
        )
    row = update_ctx.conn.execute("SELECT * FROM review_findings WHERE id = ?", (target_db_id,)).fetchone()
    _write_current_task_md_for_active_context(update_ctx.conn, update_ctx.task_ref)
    # internal: surface the resolution-anchor commit on the
    # commit-guard envelope so callers can render it pre-implementation note (e.g. for
    # operator receipts) without re-querying the row. Only populated on a
    # successful close transition; reopens and pure metadata writes return
    # None so downstream consumers can branch on presence.
    persisted_anchor = resolution_anchor_sha if effective_status_value in {"fixed", "resolved_on_branch"} else None
    data: dict[str, object] = {
        "finding": _row_to_dict(row),
        "commit_guard": {
            "finding_commit_sha": finding_commit_sha,
            "current_commit_sha": current_commit_sha,
            "current_branch": update_ctx.ctx.branch,
            "relation": commit_relation,
            "verified_commit_sha": update_input.verified_commit_sha,
            "required": needs_descendant_ack,
            "resolution_anchor_commit": persisted_anchor,
        },
    }
    if update_input.is_reopen_transition:
        data["reopened"] = True
        data["reopen_reason"] = update_input.reopen_reason
    if update_input.verification_evidence is not None:
        data["verification_evidence"] = update_input.verification_evidence
    finding_id_str = str(update_ctx.existing["finding_id"])
    task_revision = _current_task_revision(update_ctx.conn, update_ctx.task_ref)
    return _envelope(
        ok=True,
        tool="update_review_finding",
        data=data,
        task_ref=update_ctx.task_ref,
        entity="finding",
        mutation={
            "entity": "finding",
            "operation": "update",
            "affected_ids": [finding_id_str],
            "task_revision": task_revision,
        },
        warnings=update_ctx.warnings or None,
    )


def _validate_update_finding_input(
    status: str,
    finding_id: str | None,
    finding_db_id: int | None,
    normalized_finding_id: str | None,
    normalized_resolution_notes: str | None,
    normalized_reopen_reason: str | None,
    normalized_verified_commit_sha: str | None,
    normalized_verification_evidence: str | None,
) -> tuple[FindingStatus | None, dict | None]:
    if (finding_id is None and finding_db_id is None) or (finding_id is not None and finding_db_id is not None):
        return None, {"ok": False, "error": "Pass exactly one of finding_id (preferred) or finding_db_id."}
    try:
        normalized_status = FindingStatus(status)
    except ValueError:
        return None, {
            "ok": False,
            "error": f"Invalid status. Valid: {', '.join(sorted(REVIEW_FINDING_STATUSES))}",
        }
    # internal: the new lifecycle values are integrate-managed or
    # write-derived. Direct ``update`` callers must close as ``fixed`` and let
    # the runtime flag flip the persisted value to ``resolved_on_branch``.
    if normalized_status is FindingStatus.INTEGRATED:
        return None, {
            "ok": False,
            "error": "status='integrated' is integrate-managed; use operation=integrate.",
        }
    if normalized_status is FindingStatus.SUPERSEDED:
        return None, {"ok": False, **rejection_superseded_merge_managed()}
    if normalized_status is FindingStatus.RESOLVED_ON_BRANCH:
        # internal (BR-002): when the lifecycle flag is on, the task
        # plan's Update Path × Flag matrix permits explicit
        # ``status='resolved_on_branch'`` updates. Normalize to ``FIXED`` here
        # so the downstream guards (reopen escalation, batch close, commit
        # relation) and the SQL CASE-WHEN guards in ``_apply_finding_update``
        # — keyed on the user-input ``'fixed'`` string — run unchanged; the
        # flag-aware ``effective_status_value`` mapping then persists
        # ``status='resolved_on_branch'`` on the row.
        if bool(get_runtime_config().finding_lifecycle_states_enabled):
            normalized_status = FindingStatus.FIXED
        else:
            return None, {
                "ok": False,
                "error": (
                    "status='resolved_on_branch' is write-derived from status='fixed'; "
                    "close the finding as 'fixed' and enable finding_lifecycle_states_enabled."
                ),
            }
    if normalized_finding_id == "":
        return None, {"ok": False, "error": "finding_id must not be empty."}
    if (
        normalized_verification_evidence is not None
        and len(normalized_verification_evidence) > MAX_VERIFICATION_EVIDENCE_LENGTH
    ):
        return None, {
            "ok": False,
            "error": f"verification_evidence must be <= {MAX_VERIFICATION_EVIDENCE_LENGTH} characters.",
        }
    if normalized_status is not FindingStatus.FIXED and normalized_verification_evidence is not None:
        return None, {"ok": False, "error": "verification_evidence is only supported when status='fixed'."}
    if normalized_status in {FindingStatus.WONTFIX, FindingStatus.DEFERRED} and normalized_resolution_notes is None:
        return None, {
            "ok": False,
            "error": f"resolution_notes is required when status is '{normalized_status.value}'.",
        }
    if normalized_status is FindingStatus.OPEN and normalized_resolution_notes is not None:
        return None, {
            "ok": False,
            "error": "resolution_notes is not supported for status='open'. Use reopen_reason when reopening.",
        }
    if normalized_resolution_notes is not None and len(normalized_resolution_notes) > MAX_RESOLUTION_NOTES_LENGTH:
        return None, {
            "ok": False,
            **rejection_resolution_notes_max_length(
                actual_length=len(normalized_resolution_notes),
                finding_id=normalized_finding_id,
            ),
        }
    if normalized_reopen_reason is not None and len(normalized_reopen_reason) > MAX_REOPEN_REASON_LENGTH:
        return None, {"ok": False, "error": f"reopen_reason must be <= {MAX_REOPEN_REASON_LENGTH} characters."}
    if normalized_status is not FindingStatus.FIXED and normalized_verified_commit_sha is not None:
        return None, {"ok": False, "error": "verified_commit_sha is only supported when status='fixed'."}
    return normalized_status, None


def update_review_finding(
    status: str,
    finding_id: str | None = None,
    finding_db_id: int | None = None,
    resolution_notes: str | None = None,
    reopen_reason: str | None = None,
    task_ref: str | None = None,
    session: str | None = None,
    actor: WriteActor | None = None,
    verified_commit_sha: str | None = None,
    verification_evidence: str | None = None,
    allow_missing_worktree_fallback: bool = False,
) -> dict:
    """Public entry: delegate, then re-embed the finding's text fields after they commit."""
    result = _update_review_finding_impl(
        status,
        finding_id=finding_id,
        finding_db_id=finding_db_id,
        resolution_notes=resolution_notes,
        reopen_reason=reopen_reason,
        task_ref=task_ref,
        session=session,
        actor=actor,
        verified_commit_sha=verified_commit_sha,
        verification_evidence=verification_evidence,
        allow_missing_worktree_fallback=allow_missing_worktree_fallback,
    )
    embed_finding_from_envelope(result)
    return result


def reanchor_review_finding(
    finding_id: str,
    file_path: str,
    task_ref: str,
    *,
    expected_file_path: str | None = None,
    resolution_notes: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    """Open-preserving file_path rewrite (implementation note re-anchor). Status stays open."""
    result = _reanchor_review_finding_impl(
        finding_id=finding_id,
        file_path=file_path,
        task_ref=task_ref,
        expected_file_path=expected_file_path,
        resolution_notes=resolution_notes,
        actor=actor,
    )
    embed_finding_from_envelope(result)
    return result


def _reanchor_review_finding_impl(
    *,
    finding_id: str,
    file_path: str,
    task_ref: str,
    expected_file_path: str | None = None,
    resolution_notes: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    normalized_finding_id = finding_id.strip() if isinstance(finding_id, str) else ""
    normalized_file_path = (file_path or "").strip().replace("\\", "/")
    normalized_expected = expected_file_path.strip().replace("\\", "/") if isinstance(expected_file_path, str) else None
    if normalized_expected == "":
        normalized_expected = None
    normalized_notes = _normalize_optional_text(resolution_notes)

    if not normalized_finding_id:
        return _envelope(
            ok=False,
            tool="reanchor_review_finding",
            data={"error": "finding_id must not be empty."},
            entity="finding",
        )
    if not normalized_file_path:
        return _envelope(
            ok=False,
            tool="reanchor_review_finding",
            data={"error": "file_path must not be empty."},
            entity="finding",
        )
    if normalized_file_path.startswith("/") or ".." in normalized_file_path.split("/"):
        return _envelope(
            ok=False,
            tool="reanchor_review_finding",
            data={"error": "file_path must be monorepo-relative (no absolute paths or '..' segments)."},
            entity="finding",
        )
    if normalized_notes is not None and len(normalized_notes) > MAX_RESOLUTION_NOTES_LENGTH:
        return _envelope(
            ok=False,
            tool="reanchor_review_finding",
            data={"error": f"resolution_notes must be <= {MAX_RESOLUTION_NOTES_LENGTH} characters."},
            entity="finding",
        )

    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        existing = conn.execute(
            "SELECT * FROM review_findings WHERE finding_id = ? AND task_ref = ?",
            (normalized_finding_id, resolved_task_ref),
        ).fetchone()
        if existing is None:
            return _envelope(
                ok=False,
                tool="reanchor_review_finding",
                data={"error": "Finding not found for task."},
                task_ref=resolved_task_ref,
                entity="finding",
            )

        live_status = str(existing["status"])
        live_path = str(existing["file_path"] or "").replace("\\", "/")
        if live_status != FindingStatus.OPEN.value:
            return _envelope(
                ok=False,
                tool="reanchor_review_finding",
                data={
                    "error": "reanchor requires status=open (open-preserving path rewrite only).",
                    "current_status": live_status,
                },
                task_ref=resolved_task_ref,
                entity="finding",
            )
        if normalized_expected is not None and live_path != normalized_expected:
            return _envelope(
                ok=False,
                tool="reanchor_review_finding",
                data={
                    "error": "live file_path does not match expected_file_path (concurrency skip).",
                    "expected_file_path": normalized_expected,
                    "current_file_path": live_path,
                },
                task_ref=resolved_task_ref,
                entity="finding",
            )
        # Idempotent no-op: already at target path.
        if live_path == normalized_file_path:
            return _envelope(
                ok=True,
                tool="reanchor_review_finding",
                data={
                    "finding": _row_to_dict(existing),
                    "already_applied": True,
                    "file_path": live_path,
                },
                task_ref=resolved_task_ref,
                entity="finding",
            )

        ctx = _resolve_write_actor(
            conn,
            actor,
            task_ref=resolved_task_ref,
            allow_missing_worktree_fallback=True,
        )
        warnings = list(collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref) or [])
        target_db_id = int(existing["id"])
        expected_updated_at = existing["updated_at"]
        expected_status = live_status

        # Keep status open; rewrite path only. Optional notes land in resolution_notes
        # for rename-map provenance (does not close the finding).
        #
        # SANCTIONED EXCEPTION (implementation note): this writes resolution_notes while
        # status stays 'open', which the update_review_finding() input guard
        # (see the `status is OPEN and resolution_notes is not None` reject near
        # the top of this module) explicitly forbids. That guard governs the
        # human close/reopen path, where notes on an open row are a caller
        # mistake. Re-anchor notes are *provenance* (rename-map source →
        # target), not closure rationale, so the guard deliberately does not
        # apply here. Benign today (no consumer asserts open ⇒ notes-NULL); a
        # future guard author touching this invariant must special-case backlog
        # provenance rows. Mirrored at import_export._migrate_open_findings_to_backlog.
        updated = conn.execute(
            """
            UPDATE review_findings
            SET file_path = ?,
                resolution_notes = CASE WHEN ? IS NOT NULL THEN ? ELSE resolution_notes END,
                agent = COALESCE(agent, ?),
                branch = COALESCE(branch, ?),
                commit_sha = COALESCE(commit_sha, ?),
                lane_id = COALESCE(lane_id, ?),
                updated_at = datetime('now')
            WHERE id = ? AND task_ref = ? AND updated_at = ? AND status = ?
            """,
            (
                normalized_file_path,
                normalized_notes,
                normalized_notes,
                ctx.agent,
                ctx.branch,
                ctx.commit_sha,
                ctx.lane_id,
                target_db_id,
                resolved_task_ref,
                expected_updated_at,
                expected_status,
            ),
        )
        if updated.rowcount == 0:
            latest = conn.execute(
                "SELECT updated_at, status, file_path FROM review_findings WHERE id = ?",
                (target_db_id,),
            ).fetchone()
            return _envelope(
                ok=False,
                tool="reanchor_review_finding",
                data={
                    "error": "Finding state conflict.",
                    "expected_updated_at": expected_updated_at,
                    "expected_status": expected_status,
                    "current_updated_at": latest["updated_at"] if latest else None,
                    "current_status": latest["status"] if latest else None,
                    "current_file_path": latest["file_path"] if latest else None,
                },
                task_ref=resolved_task_ref,
                entity="finding",
            )

        row = conn.execute("SELECT * FROM review_findings WHERE id = ?", (target_db_id,)).fetchone()
        _write_current_task_md_for_active_context(conn, resolved_task_ref)
        task_revision = _current_task_revision(conn, resolved_task_ref)
        return _envelope(
            ok=True,
            tool="reanchor_review_finding",
            data={
                "finding": _row_to_dict(row),
                "already_applied": False,
                "before_file_path": live_path,
                "file_path": normalized_file_path,
            },
            task_ref=resolved_task_ref,
            entity="finding",
            mutation={
                "entity": "finding",
                "operation": "reanchor",
                "affected_ids": [normalized_finding_id],
                "task_revision": task_revision,
            },
            warnings=warnings or None,
        )


def _update_review_finding_impl(
    status: str,
    finding_id: str | None = None,
    finding_db_id: int | None = None,
    resolution_notes: str | None = None,
    reopen_reason: str | None = None,
    task_ref: str | None = None,
    session: str | None = None,
    actor: WriteActor | None = None,
    verified_commit_sha: str | None = None,
    verification_evidence: str | None = None,
    allow_missing_worktree_fallback: bool = False,
) -> dict:
    normalized_finding_id = finding_id.strip() if isinstance(finding_id, str) else None
    normalized_resolution_notes = _normalize_optional_text(resolution_notes)
    normalized_reopen_reason = _normalize_optional_text(reopen_reason)
    normalized_verified_commit_sha = _normalize_optional_text(verified_commit_sha)
    try:
        normalized_verified_commit_sha = _shared_write_context._validate_and_expand_commit_sha(
            normalized_verified_commit_sha
        )
    except InvalidCommitShaError as exc:
        return _envelope(
            ok=False,
            tool="update_review_finding",
            data={"error": str(exc)},
            entity="finding",
        )
    normalized_verification_evidence = _normalize_optional_text(verification_evidence)
    normalized_status, input_error = _validate_update_finding_input(
        status,
        finding_id,
        finding_db_id,
        normalized_finding_id,
        normalized_resolution_notes,
        normalized_reopen_reason,
        normalized_verified_commit_sha,
        normalized_verification_evidence,
    )
    if input_error is not None or normalized_status is None:
        # Pass the full structured rejection (internal) when present;
        # fall back to a bare error only for the invalid-status edge case.
        if input_error is not None:
            data = {key: value for key, value in input_error.items() if key != "ok"}
        else:
            data = {"error": "invalid status"}
        return _envelope(
            ok=False,
            tool="update_review_finding",
            data=data,
            entity="finding",
        )

    with _get_db_connection() as conn:
        if task_ref is None:
            if normalized_finding_id is not None:
                rows = conn.execute(
                    "SELECT * FROM review_findings WHERE finding_id = ?", (normalized_finding_id,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM review_findings WHERE id = ?", (finding_db_id,)).fetchall()
            if not rows:
                return _envelope(
                    ok=False,
                    tool="update_review_finding",
                    data={"error": "Finding not found."},
                    entity="finding",
                )
            if len(rows) > 1:
                candidate_scopes = sorted({str(row["task_ref"]) for row in rows})
                return _envelope(
                    ok=False,
                    tool="update_review_finding",
                    data={
                        "error": f"Ambiguous finding_id: {len(rows)} rows across task_refs {candidate_scopes}. Pass task_ref explicitly to disambiguate.",
                    },
                    entity="finding",
                )
            existing = rows[0]
            resolved_task_ref = str(existing["task_ref"])
        else:
            resolved_task_ref = _resolve_task_ref(conn, task_ref)
            existing = conn.execute(
                "SELECT * FROM review_findings WHERE finding_id = ? AND task_ref = ?"
                if normalized_finding_id is not None
                else "SELECT * FROM review_findings WHERE id = ? AND task_ref = ?",
                (normalized_finding_id, resolved_task_ref)
                if normalized_finding_id is not None
                else (finding_db_id, resolved_task_ref),
            ).fetchone()
            if existing is None:
                return _envelope(
                    ok=False,
                    tool="update_review_finding",
                    data={"error": "Finding not found for task."},
                    task_ref=resolved_task_ref,
                    entity="finding",
                )

        ctx = _resolve_write_actor(
            conn,
            actor,
            task_ref=resolved_task_ref,
            allow_missing_worktree_fallback=allow_missing_worktree_fallback,
        )
        warnings = list(collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref) or [])
        existing_status = FindingStatus(str(existing["status"]))
        is_reopen_transition = existing_status is not FindingStatus.OPEN and normalized_status is FindingStatus.OPEN
        if is_reopen_transition and normalized_reopen_reason is None:
            return _envelope(
                ok=False,
                tool="update_review_finding",
                data={"error": "reopen_reason is required when reopening a finding."},
                task_ref=resolved_task_ref,
                entity="finding",
            )
        if not is_reopen_transition and normalized_reopen_reason is not None:
            return _envelope(
                ok=False,
                tool="update_review_finding",
                data={"error": "reopen_reason is only valid when transitioning a finding back to open."},
                task_ref=resolved_task_ref,
                entity="finding",
            )

        if normalized_status is FindingStatus.FIXED:
            guard_error = _check_reopen_escalation_guard(existing, normalized_verification_evidence)
            if guard_error is not None:
                return _envelope(
                    ok=False,
                    tool="update_review_finding",
                    data={key: value for key, value in guard_error.items() if key != "ok"},
                    task_ref=resolved_task_ref,
                    entity="finding",
                )
            if normalized_verification_evidence is None:
                guard_error = _check_batch_close_guard(conn, resolved_task_ref, existing)
                if guard_error is not None:
                    return _envelope(
                        ok=False,
                        tool="update_review_finding",
                        data={key: value for key, value in guard_error.items() if key != "ok"},
                        task_ref=resolved_task_ref,
                        entity="finding",
                    )
            guard_error = _check_commit_relation_guard(
                existing,
                ctx.commit_sha,
                normalized_verified_commit_sha,
                ctx.branch,
                normalized_resolution_notes,
            )
            if guard_error is not None:
                return _envelope(
                    ok=False,
                    tool="update_review_finding",
                    data={key: value for key, value in guard_error.items() if key != "ok"},
                    task_ref=resolved_task_ref,
                    entity="finding",
                )

        return _apply_finding_update(
            FindingUpdateContext(
                conn=conn,
                existing=existing,
                ctx=ctx,
                session=session,
                task_ref=resolved_task_ref,
                warnings=warnings,
            ),
            FindingUpdateInput(
                status=normalized_status,
                resolution_notes=normalized_resolution_notes,
                reopen_reason=normalized_reopen_reason,
                verified_commit_sha=normalized_verified_commit_sha,
                verification_evidence=normalized_verification_evidence,
                is_reopen_transition=is_reopen_transition,
            ),
        )


@dataclass
class ProvenanceRepairRequest:
    """Validated, normalized inputs for a provenance repair operation."""

    finding_id: str
    expected_branch: str
    expected_commit_sha: str
    literal_expected_commit_sha: str
    new_branch: str
    new_commit_sha: str
    reason: str
    session: str
    task_ref: str | None
    actor: WriteActor | None


def _parse_provenance_repair_request(
    finding_id: str,
    expected_branch: str,
    expected_commit_sha: str,
    new_branch: str,
    new_commit_sha: str,
    reason: str,
    session: str,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> ProvenanceRepairRequest | dict:
    """Validate and normalize provenance repair inputs.

    Returns a ProvenanceRepairRequest on success or an error envelope dict on failure.
    """
    normalized_finding_id = finding_id.strip() if isinstance(finding_id, str) else None
    if not normalized_finding_id:
        return _envelope(
            ok=False,
            tool="repair_review_finding_provenance",
            data={"error": "finding_id must not be empty."},
            entity="finding",
        )
    normalized_expected_branch = expected_branch.strip() if isinstance(expected_branch, str) else None
    normalized_expected_commit_sha = expected_commit_sha.strip() if isinstance(expected_commit_sha, str) else None
    normalized_new_branch = new_branch.strip() if isinstance(new_branch, str) else None
    normalized_new_commit_sha = new_commit_sha.strip() if isinstance(new_commit_sha, str) else None
    normalized_reason = reason.strip() if isinstance(reason, str) else None
    if not normalized_expected_branch:
        return _envelope(
            ok=False,
            tool="repair_review_finding_provenance",
            data={"error": "expected_branch must not be empty."},
            entity="finding",
        )
    if not normalized_expected_commit_sha:
        return _envelope(
            ok=False,
            tool="repair_review_finding_provenance",
            data={"error": "expected_commit_sha must not be empty."},
            entity="finding",
        )
    if not normalized_new_branch:
        return _envelope(
            ok=False,
            tool="repair_review_finding_provenance",
            data={"error": "new_branch must not be empty."},
            entity="finding",
        )
    if not normalized_new_commit_sha:
        return _envelope(
            ok=False,
            tool="repair_review_finding_provenance",
            data={"error": "new_commit_sha must not be empty."},
            entity="finding",
        )
    if not normalized_reason or len(normalized_reason) < 20:
        return _envelope(
            ok=False,
            tool="repair_review_finding_provenance",
            data={"error": "reason must be at least 20 characters; describe why the original attribution was wrong."},
            entity="finding",
        )
    try:
        expanded_new = _shared_write_context._validate_and_expand_commit_sha(normalized_new_commit_sha)
    except InvalidCommitShaError as exc:
        return _envelope(
            ok=False,
            tool="repair_review_finding_provenance",
            data={"error": str(exc)},
            entity="finding",
        )
    if expanded_new is None:
        return _envelope(
            ok=False,
            tool="repair_review_finding_provenance",
            data={"error": "new_commit_sha could not be resolved."},
            entity="finding",
        )
    normalized_new_commit_sha = expanded_new
    literal_expected_commit_sha = normalized_expected_commit_sha
    try:
        expanded_expected = _shared_write_context._validate_and_expand_commit_sha(normalized_expected_commit_sha)
        if expanded_expected is not None:
            normalized_expected_commit_sha = expanded_expected
    except InvalidCommitShaError as exc:
        _LOG.warning(
            "repair_review_finding_provenance could not expand expected_commit_sha %s: %s",
            normalized_expected_commit_sha,
            exc,
        )
    if (
        normalized_expected_branch == normalized_new_branch
        and normalized_expected_commit_sha == normalized_new_commit_sha
    ):
        return _envelope(
            ok=False,
            tool="repair_review_finding_provenance",
            data={"error": "expected and new branch+commit_sha are identical; nothing to repair."},
            entity="finding",
        )
    return ProvenanceRepairRequest(
        finding_id=normalized_finding_id,
        expected_branch=normalized_expected_branch,
        expected_commit_sha=normalized_expected_commit_sha,
        literal_expected_commit_sha=literal_expected_commit_sha,
        new_branch=normalized_new_branch,
        new_commit_sha=normalized_new_commit_sha,
        reason=normalized_reason,
        session=session,
        task_ref=task_ref,
        actor=actor,
    )


def repair_review_finding_provenance(
    finding_id: str,
    expected_branch: str,
    expected_commit_sha: str,
    new_branch: str,
    new_commit_sha: str,
    reason: str,
    session: str,
    task_ref: str | None = None,
    actor: WriteActor | None = None,
) -> dict:
    req_or_error = _parse_provenance_repair_request(
        finding_id=finding_id,
        expected_branch=expected_branch,
        expected_commit_sha=expected_commit_sha,
        new_branch=new_branch,
        new_commit_sha=new_commit_sha,
        reason=reason,
        session=session,
        task_ref=task_ref,
        actor=actor,
    )
    if isinstance(req_or_error, dict):
        return req_or_error
    req = req_or_error

    with _get_db_connection() as conn:
        if req.task_ref is None:
            rows = conn.execute("SELECT * FROM review_findings WHERE finding_id = ?", (req.finding_id,)).fetchall()
            if not rows:
                return _envelope(
                    ok=False,
                    tool="repair_review_finding_provenance",
                    data={"error": "Finding not found."},
                    entity="finding",
                )
            if len(rows) > 1:
                candidate_scopes = sorted({str(row["task_ref"]) for row in rows})
                return _envelope(
                    ok=False,
                    tool="repair_review_finding_provenance",
                    data={
                        "error": f"Ambiguous finding_id: {len(rows)} rows across task_refs {candidate_scopes}. Pass task_ref explicitly to disambiguate.",
                    },
                    entity="finding",
                )
            existing = rows[0]
            resolved_task_ref = str(existing["task_ref"])
        else:
            resolved_task_ref = _resolve_task_ref(conn, req.task_ref)
            existing = conn.execute(
                "SELECT * FROM review_findings WHERE finding_id = ? AND task_ref = ?",
                (req.finding_id, resolved_task_ref),
            ).fetchone()
            if existing is None:
                return _envelope(
                    ok=False,
                    tool="repair_review_finding_provenance",
                    data={"error": "Finding not found for task."},
                    task_ref=resolved_task_ref,
                    entity="finding",
                )

        ctx = _resolve_write_actor(conn, req.actor, task_ref=resolved_task_ref)
        warnings = list(collect_target_context_warnings(conn, ctx, task_ref=resolved_task_ref) or [])

        existing_branch = _normalize_optional_text(existing["branch"])
        existing_commit_sha = _normalize_optional_text(existing["commit_sha"])
        if existing_branch != req.expected_branch:
            return _envelope(
                ok=False,
                tool="repair_review_finding_provenance",
                data={
                    "error": "expected_branch does not match the stored row.",
                    "expected_branch": req.expected_branch,
                    "actual_branch": existing_branch,
                },
                task_ref=resolved_task_ref,
                entity="finding",
            )

        existing_commit_sha_expanded = existing_commit_sha
        if existing_commit_sha:
            try:
                expanded_existing = _shared_write_context._validate_and_expand_commit_sha(existing_commit_sha)
                if expanded_existing is not None:
                    existing_commit_sha_expanded = expanded_existing
            except InvalidCommitShaError as exc:
                warnings.append(
                    f"stored commit_sha {existing_commit_sha!r} for finding {req.finding_id} could not be expanded during provenance repair: {exc}"
                )
                _LOG.warning(
                    "repair_review_finding_provenance could not expand stored commit_sha %s for %s: %s",
                    existing_commit_sha,
                    req.finding_id,
                    exc,
                )

        acceptable_existing = {existing_commit_sha, existing_commit_sha_expanded}
        acceptable_expected = {req.literal_expected_commit_sha, req.expected_commit_sha}
        if not (acceptable_expected & acceptable_existing):
            return _envelope(
                ok=False,
                tool="repair_review_finding_provenance",
                data={
                    "error": "expected_commit_sha does not match the stored row.",
                    "expected_commit_sha": req.expected_commit_sha,
                    "actual_commit_sha": existing_commit_sha,
                },
                task_ref=resolved_task_ref,
                entity="finding",
            )

        target_db_id = int(existing["id"])
        before = {
            "branch": existing_branch,
            "commit_sha": existing_commit_sha,
        }
        after = {
            "branch": req.new_branch,
            "commit_sha": req.new_commit_sha,
        }

        conn.execute(
            """
            UPDATE review_findings
            SET branch = ?,
                commit_sha = ?,
                updated_at = datetime('now')
            WHERE id = ? AND task_ref = ?
            """,
            (
                req.new_branch,
                req.new_commit_sha,
                target_db_id,
                resolved_task_ref,
            ),
        )

        audit_decision_id = _canonical_repair_provenance_decision_id(
            task_ref=resolved_task_ref,
            finding_id=req.finding_id,
            agent=ctx.agent,
        )
        audit_rationale = (
            f"Repaired source provenance on review finding `{req.finding_id}` "
            f"(row id={target_db_id}, task_ref={resolved_task_ref}).\n\n"
            f"**Before:** branch=`{before['branch']}`, commit_sha=`{before['commit_sha']}`\n"
            f"**After:**  branch=`{after['branch']}`,  commit_sha=`{after['commit_sha']}`\n\n"
            f"**Reason:** {req.reason}"
        )
        conn.execute(
            """
            INSERT INTO decisions (
                task_ref, session, decision, rationale, agent, harness, branch, commit_sha, lane_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                resolved_task_ref,
                req.session,
                audit_decision_id,
                audit_rationale,
                ctx.agent,
                ctx.harness,
                ctx.branch,
                ctx.commit_sha,
                ctx.lane_id,
            ),
        )
        audit_row_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

        row = conn.execute("SELECT * FROM review_findings WHERE id = ?", (target_db_id,)).fetchone()
        _write_current_task_md_for_active_context(conn, resolved_task_ref)
        task_revision = _current_task_revision(conn, resolved_task_ref)

        return _envelope(
            ok=True,
            tool="repair_review_finding_provenance",
            data={
                "finding": _row_to_dict(row),
                "before": before,
                "after": after,
                "audit_decision_id": audit_decision_id,
                "audit_decision_db_id": audit_row_id,
            },
            task_ref=resolved_task_ref,
            entity="finding",
            mutation={
                "entity": "finding",
                "operation": "repair_provenance",
                "affected_ids": [req.finding_id],
                "task_revision": task_revision,
            },
            warnings=warnings or None,
        )


# ---------------------------------------------------------------------------
# internal: integrate operation + opportunistic trigger
# ---------------------------------------------------------------------------

# Hard cap on promotions per integrate pass. The opportunistic trigger fires
# from host write paths, so the bound must keep a single sweep cheap even on
# noisy long-running branches. Excess rows are simply left at
# ``resolved_on_branch`` for the next pass once the next commit advances the
# integration ref.
INTEGRATE_REVIEW_FINDINGS_MAX_PER_PASS = 200


def _resolve_integration_ref_head_sha(integration_ref: str) -> str | None:
    """Return the 40-char HEAD SHA of ``integration_ref``, or None if the ref
    cannot be resolved (e.g. detached worktree, ref does not exist, or git is
    unavailable). Errors are swallowed — the opportunistic trigger treats
    "unknown" as "skip this pass" rather than blocking the host write."""
    config = get_runtime_config()
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--verify", f"{integration_ref}^{{commit}}"],
            cwd=str(config.git_workspace_root),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    sha = (proc.stdout or "").strip()
    return sha or None


def integrate_review_findings(
    *,
    task_ref: str | None = None,
    integration_ref: str = "main",
    actor: WriteActor | None = None,
) -> dict:
    """Promote every ``resolved_on_branch`` finding for ``task_ref`` whose
    anchor commit is reachable from ``integration_ref`` HEAD to
    ``status='integrated'``. Each promotion writes the three
    ``integrated_at_*`` columns and a decision row that anchors the
    promotion to the integration SHA. Capped at
    :data:`INTEGRATE_REVIEW_FINDINGS_MAX_PER_PASS` rows per call so the
    opportunistic trigger stays bounded; excess rows roll into the next pass.

    This entry point is **distinct** from internal's
    :func:`reconcile_review_findings`, which performs integrity / dedup
    checks. The two operations are not aliases.
    """
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)

        head_sha = _resolve_integration_ref_head_sha(integration_ref)
        if head_sha is None:
            return _envelope(
                ok=True,
                tool="integrate_review_findings",
                data={
                    "task_ref": resolved_task_ref,
                    "integration_ref": integration_ref,
                    "integration_sha": None,
                    "promoted": [],
                    "skipped_unreachable": [],
                    "cap_applied": False,
                    "errors": ["integration_ref could not be resolved to a commit"],
                },
                task_ref=resolved_task_ref,
                entity="finding",
            )

        rows = conn.execute(
            """
            SELECT id, finding_id, resolved_on_branch_at_commit
            FROM review_findings
            WHERE task_ref = ? AND status = 'resolved_on_branch'
            ORDER BY id ASC
            LIMIT ?
            """,
            (resolved_task_ref, INTEGRATE_REVIEW_FINDINGS_MAX_PER_PASS + 1),
        ).fetchall()

        cap_applied = len(rows) > INTEGRATE_REVIEW_FINDINGS_MAX_PER_PASS
        rows = rows[:INTEGRATE_REVIEW_FINDINGS_MAX_PER_PASS]

        promoted: list[dict[str, str]] = []
        skipped_unreachable: list[dict[str, str | None]] = []
        errors: list[str] = []

        for row in rows:
            finding_id = str(row["finding_id"])
            anchor_commit = _normalize_optional_text(row["resolved_on_branch_at_commit"])
            if not anchor_commit:
                skipped_unreachable.append(
                    {"finding_id": finding_id, "anchor_commit": None, "reason": "missing_anchor"}
                )
                continue
            try:
                reachable = _is_ancestor_of_ref(anchor_commit, integration_ref)
            except Exception as exc:  # noqa: BLE001 — git wrapper hardening
                _LOG.warning(
                    "integrate_review_findings: ancestry check failed for %s (task=%s): %s",
                    finding_id,
                    resolved_task_ref,
                    exc,
                )
                errors.append(f"{finding_id}: {exc}")
                continue
            if not reachable:
                skipped_unreachable.append(
                    {"finding_id": finding_id, "anchor_commit": anchor_commit, "reason": "not_ancestor"}
                )
                continue

            conn.execute(
                """
                UPDATE review_findings
                SET status = 'integrated',
                    integrated_at_commit = ?,
                    integrated_at_ref = ?,
                    integrated_at_ts = datetime('now'),
                    updated_at = datetime('now')
                WHERE id = ? AND task_ref = ?
                """,
                (head_sha, integration_ref, int(row["id"]), resolved_task_ref),
            )

            decision_id = f"integrate_finding_{finding_id}_{head_sha[:12]}"
            decision_rationale = (
                f"internal integrate promotion: finding `{finding_id}` "
                f"(anchor=`{anchor_commit}`) is reachable from "
                f"`{integration_ref}` HEAD `{head_sha}`."
            )
            conn.execute(
                """
                INSERT INTO decisions (
                    task_ref, session, decision, rationale, agent, harness, branch, commit_sha, lane_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    resolved_task_ref,
                    f"integrate-{integration_ref}",
                    decision_id,
                    decision_rationale,
                    ctx.agent,
                    ctx.harness,
                    ctx.branch,
                    ctx.commit_sha,
                    ctx.lane_id,
                ),
            )
            promoted.append({"finding_id": finding_id, "anchor_commit": anchor_commit})

        conn.execute(
            "UPDATE handoff_state SET last_observed_integration_sha = ? WHERE task_ref = ?",
            (head_sha, resolved_task_ref),
        )

        if promoted:
            _write_current_task_md_for_active_context(conn, resolved_task_ref)
        task_revision = _current_task_revision(conn, resolved_task_ref)

        return _envelope(
            ok=True,
            tool="integrate_review_findings",
            data={
                "task_ref": resolved_task_ref,
                "integration_ref": integration_ref,
                "integration_sha": head_sha,
                "promoted": promoted,
                "skipped_unreachable": skipped_unreachable,
                "cap_applied": cap_applied,
                "errors": errors,
            },
            task_ref=resolved_task_ref,
            entity="finding",
            mutation={
                "entity": "finding",
                "operation": "integrate",
                "affected_ids": [item["finding_id"] for item in promoted],
                "task_revision": task_revision,
            },
        )


def _run_opportunistic_integrate_for_task(
    task_ref: str | None,
    integration_ref: str = "main",
) -> None:
    """Best-effort opportunistic integrate trigger for host write paths.

    Reads ``handoff_state.last_observed_integration_sha`` for the resolved
    task; if the current integration-ref HEAD differs, runs
    :func:`integrate_review_findings` for that task. Every failure mode —
    git unavailable, missing task row, integrate raising — is logged and
    swallowed so the host write never blocks on this side effect.
    """
    try:
        with _get_db_connection() as conn:
            try:
                resolved_task_ref = _resolve_task_ref(conn, task_ref)
            except Exception:  # noqa: BLE001 — no active task is fine
                return
            row = conn.execute(
                "SELECT last_observed_integration_sha FROM handoff_state WHERE task_ref = ?",
                (resolved_task_ref,),
            ).fetchone()
            last_observed = _normalize_optional_text(row["last_observed_integration_sha"]) if row is not None else None
        head_sha = _resolve_integration_ref_head_sha(integration_ref)
        if head_sha is None or head_sha == last_observed:
            return
        integrate_review_findings(task_ref=resolved_task_ref, integration_ref=integration_ref)
    except Exception as exc:  # noqa: BLE001 — opportunistic best-effort
        _LOG.warning("opportunistic integrate trigger failed (task=%s): %s", task_ref, exc)
