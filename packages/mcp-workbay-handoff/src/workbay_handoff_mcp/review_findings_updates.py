"""Update, resolve, and provenance-repair operations for review findings."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from . import shared_write_context as _shared_write_context
from .concept_embed_hook import embed_finding_from_envelope
from .enums import FindingStatus
from .git_merge import AncestryFatal
from .git_merge import is_ancestor_of_ref as _is_ancestor_of_ref
from .review_finding_resolution import ResolutionOutcomeKind, classify_resolution_outcome
from .review_findings_support import (
    _canonical_repair_provenance_decision_id,
    _classify_commit_relation,
    _current_task_revision,
    _current_task_revision_for,
    _write_current_task_md_for_active_context,
)
from .runtime import RuntimeNotConfiguredError, get_runtime_config
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
    rejection_status_decision_requires_rationale,
    rejection_superseded_merge_managed,
    rejection_verification_evidence_max_length,
)
from .verified_tests import get_verified_tests

# Terminal statuses that are operator status decisions (not fix-claims).
# Ancestry classifier does not own these; resolve stores them verbatim with rationale.
STATUS_DECISION_STATUSES: frozenset[str] = frozenset(
    {
        FindingStatus.DEFERRED.value,
        FindingStatus.WONTFIX.value,
    }
)

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


# Minimum stripped rationale length for deferred/wontfix status decisions (r0153-3).
STATUS_DECISION_MIN_RATIONALE_LENGTH = 20


def _status_decision_rationale(
    *,
    resolution_notes: str | None,
    notes: str | None,
    verification_evidence: str | None,
) -> str | None:
    """Return rationale text for a deferred/wontfix status decision, if long enough.

    Requires at least ``STATUS_DECISION_MIN_RATIONALE_LENGTH`` characters after
    strip. Absent or too-short candidates are treated as missing so the typed
    ``status_decision_requires_rationale`` refusal covers both cases.
    """
    for candidate in (resolution_notes, notes, verification_evidence):
        normalized = _normalize_optional_text(candidate)
        if normalized is not None and len(normalized) >= STATUS_DECISION_MIN_RATIONALE_LENGTH:
            return normalized
    return None


def resolve_review_findings(
    *,
    task_ref: str | None = None,
    session: str | None = None,
    finding_ids: list[str] | None = None,
    all_open: bool = False,
    resolution_notes: str | None = None,
    notes: str | None = None,
    verification_evidence: str | None = None,
    actor: WriteActor | None = None,
    status: str | None = None,
    # Optional per-finding status overrides (finding_id -> deferred|wontfix|fixed).
    # Used when a single resolve batch mixes fix-claims and status decisions.
    finding_statuses: dict[str, str] | None = None,
) -> dict:
    normalized_finding_ids = _normalize_resolution_targets(finding_ids)
    normalized_resolution_notes = _normalize_optional_text(resolution_notes)
    normalized_notes_alias = _normalize_optional_text(notes)
    normalized_verification_evidence = _normalize_optional_text(verification_evidence)
    normalized_global_status = _normalize_optional_text(status)
    if normalized_global_status is not None:
        normalized_global_status = normalized_global_status.lower()
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
    if normalized_notes_alias is not None and len(normalized_notes_alias) > MAX_RESOLUTION_NOTES_LENGTH:
        return _envelope(
            ok=False,
            tool="resolve_review_findings",
            data=rejection_resolution_notes_max_length(
                actual_length=len(normalized_notes_alias),
            ),
            task_ref=task_ref,
            entity="finding",
        )
    if (
        normalized_verification_evidence is not None
        and len(normalized_verification_evidence) > MAX_VERIFICATION_EVIDENCE_LENGTH
    ):
        return _envelope(
            ok=False,
            tool="resolve_review_findings",
            data=rejection_verification_evidence_max_length(
                actual_length=len(normalized_verification_evidence),
            ),
            task_ref=task_ref,
            entity="finding",
        )
    if normalized_global_status is not None and normalized_global_status not in {
        FindingStatus.FIXED.value,
        *STATUS_DECISION_STATUSES,
    }:
        return _envelope(
            ok=False,
            tool="resolve_review_findings",
            data={
                "error": (f"Invalid resolve status {normalized_global_status!r}. Valid: fixed, deferred, wontfix."),
            },
            task_ref=task_ref,
            entity="finding",
        )
    status_decision_rationale = _status_decision_rationale(
        resolution_notes=normalized_resolution_notes,
        notes=normalized_notes_alias,
        verification_evidence=normalized_verification_evidence,
    )
    # Prefer explicit resolution_notes, then notes alias, for persistence.
    decision_notes_for_write = normalized_resolution_notes or normalized_notes_alias or status_decision_rationale
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
            # Count fix-claims and status-decision closes (deferred/wontfix)
            # toward the in-window batch-close total (r0153-3).
            recent_fixes = conn.execute(
                """
                SELECT COUNT(*) AS cnt FROM review_findings
                WHERE task_ref = ?
                  AND status IN ('fixed', 'resolved_on_branch', 'deferred', 'wontfix')
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

    normalized_finding_statuses: dict[str, str] = {}
    if finding_statuses:
        for raw_id, raw_status in finding_statuses.items():
            fid = str(raw_id).strip()
            st = _normalize_optional_text(raw_status)
            if not fid or st is None:
                continue
            st = st.lower()
            if st not in {FindingStatus.FIXED.value, *STATUS_DECISION_STATUSES}:
                return _envelope(
                    ok=False,
                    tool="resolve_review_findings",
                    data={
                        "error": (
                            f"Invalid per-finding resolve status {st!r} for {fid}. Valid: fixed, deferred, wontfix."
                        ),
                    },
                    task_ref=task_ref,
                    entity="finding",
                )
            normalized_finding_statuses[fid] = st

    for row in rows:
        finding_id_str = str(row["finding_id"])
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

        # implementation note S2: status decisions (deferred/wontfix) bypass the fix-claim
        # classifier and commit-ancestry guard entirely. Store requested status
        # verbatim with required rationale; record commit fields for audit only.
        requested_status = normalized_finding_statuses.get(finding_id_str, normalized_global_status)
        if requested_status is None:
            requested_status = FindingStatus.FIXED.value
        if requested_status in STATUS_DECISION_STATUSES:
            if status_decision_rationale is None:
                decision_rejection = rejection_status_decision_requires_rationale(
                    status=requested_status,
                    finding_id=finding_id_str,
                )
                entry = {
                    "finding_id": finding_id_str,
                    "finding_db_id": int(row["id"]),
                    "outcome": ResolutionOutcomeKind.ERROR.value,
                    "reason": decision_rejection["error"],
                    "rule_id": decision_rejection["rule_id"],
                    "violated": decision_rejection["violated"],
                    "expected": decision_rejection["expected"],
                    "example": decision_rejection["example"],
                    "finding_commit_sha": finding_commit_sha,
                    "workspace_commit_sha": resolve_commit,
                    "verified_commit_sha": None,
                    "commit_relation": commit_relation,
                    "requested_status": requested_status,
                }
            else:
                entry = {
                    "finding_id": finding_id_str,
                    "finding_db_id": int(row["id"]),
                    "outcome": requested_status,
                    "reason": None,
                    "finding_commit_sha": finding_commit_sha,
                    "workspace_commit_sha": resolve_commit,
                    "verified_commit_sha": None,
                    "commit_relation": commit_relation,
                    "requested_status": requested_status,
                    "status_decision": True,
                }
            planned_results.append((row, entry))
            continue

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
        entry = {
            "finding_id": finding_id_str,
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
                finding_id=finding_id_str,
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

    # Closing candidates: fix-claims and status-decision closes both count
    # toward the batch-close threshold; when it trips, require verification_evidence.
    closing_candidates = [
        entry
        for _, entry in planned_results
        if entry["outcome"] == ResolutionOutcomeKind.FIXED.value or str(entry["outcome"]) in STATUS_DECISION_STATUSES
    ]
    if (
        normalized_verification_evidence is None
        and recent_fixed_count + len(closing_candidates) > BATCH_CLOSE_THRESHOLD
    ):
        batch_rejection = rejection_batch_close_evidence(
            recent_fixes_in_window=recent_fixed_count,
            additional_closing=len(closing_candidates),
        )
        batch_guard_reason = str(batch_rejection["error"])
        for _, entry in planned_results:
            outcome_value = str(entry["outcome"])
            if outcome_value == ResolutionOutcomeKind.FIXED.value or outcome_value in STATUS_DECISION_STATUSES:
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
        outcome_value = str(entry["outcome"])
        if outcome_value == ResolutionOutcomeKind.FIXED.value or outcome_value in STATUS_DECISION_STATUSES:
            close_status = outcome_value if outcome_value in STATUS_DECISION_STATUSES else FindingStatus.FIXED.value
            try:
                update_result = update_review_finding(
                    status=close_status,
                    finding_id=str(row["finding_id"]),
                    task_ref=resolved_task_ref,
                    session=session,
                    actor=resolved_actor,
                    verified_commit_sha=(
                        None
                        if close_status in STATUS_DECISION_STATUSES
                        else cast("str | None", entry["verified_commit_sha"])
                    ),
                    # Status decisions persist rationale as resolution_notes; verification_evidence
                    # may satisfy the rationale gate but is only stored on fix-claims.
                    resolution_notes=(
                        decision_notes_for_write
                        if close_status in STATUS_DECISION_STATUSES
                        else normalized_resolution_notes
                    ),
                    verification_evidence=(
                        None if close_status in STATUS_DECISION_STATUSES else normalized_verification_evidence
                    ),
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
                if close_status == FindingStatus.FIXED.value and auto_evidence is not None:
                    entry["evidence_source"] = auto_evidence.evidence_source
                    entry["auto_evidence"] = {
                        "commit_sha": auto_evidence.commit_sha,
                        "verified_test_ids": list(auto_evidence.verified_test_ids),
                        "commands": list(auto_evidence.commands),
                    }
                fixed_ids.append(str(row["finding_id"]))
        results.append(entry)

    counts: dict[str, int] = {kind.value: 0 for kind in ResolutionOutcomeKind}
    for status_value in STATUS_DECISION_STATUSES:
        counts.setdefault(status_value, 0)
    for entry in results:
        outcome_key = str(entry["outcome"])
        counts[outcome_key] = counts.get(outcome_key, 0) + 1

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
        WHERE task_ref = ? AND status IN ('fixed', 'resolved_on_branch', 'deferred', 'wontfix')
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
        SET status = ?, resolved_at = CASE WHEN ? IN ('fixed', 'wontfix', 'deferred') THEN datetime('now') ELSE NULL END,
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
    from .review_findings_support import _canonical_task_ref, _canonicalize_finding_path  # noqa: PLC0415

    normalized_file_path = _canonicalize_finding_path(file_path)
    normalized_expected = (
        _canonicalize_finding_path(expected_file_path) if isinstance(expected_file_path, str) else None
    )
    normalized_notes = _normalize_optional_text(resolution_notes)

    if not normalized_finding_id:
        return _envelope(
            ok=False,
            tool="reanchor_review_finding",
            data={"error": "finding_id must not be empty."},
            entity="finding",
        )
    if normalized_file_path is None:
        return _envelope(
            ok=False,
            tool="reanchor_review_finding",
            data={"error": "file_path must be monorepo-relative (no absolute or escaping paths)."},
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
        resolved_task_ref = _canonical_task_ref(conn, resolved_task_ref)
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
# noisy long-running branches. The cap counts promotions, not rows considered:
# ineligible (not-ancestor / determinate fatal / missing-anchor) rows are
# skipped without charging the cap, and the scan continues by id keyset until
# MAX promotions or end of table. Eligible overflow stays
# ``resolved_on_branch``. The latch does not advance while any eligible
# (non-fatal, still-anchored, not-already-classified-unreachable)
# ``resolved_on_branch`` leftover remains, so the next opportunistic pass at
# this HEAD drains that overflow. An ineligible prefix does not starve unread
# eligible rows behind it.
INTEGRATE_REVIEW_FINDINGS_MAX_PER_PASS = 200
# Consecutive transient (None) ancestry failures at one HEAD after which
# opportunistic integrate quarantines the HEAD: the circuit opens so host
# writes fail fast. The latch is not advanced — last_observed means this
# HEAD was processed, not that we gave up. Explicit integrate_review_findings
# still runs, and the open circuit half-opens after a bounded skip budget.
INTEGRATE_TRANSIENT_ATTEMPT_LIMIT = 3
# Consecutive integration-ref HEAD resolution failures after which the
# opportunistic path circuits and skips git entirely.
INTEGRATE_HEAD_RESOLVE_FAILURE_LIMIT = 3
# Release It! ch.5 half-open: after this many fail-fast skips, or this
# many seconds, admit exactly one trial call. Recovery must fire without
# a human — opportunistic is the only live caller.
INTEGRATE_CIRCUIT_HALF_OPEN_AFTER_SKIPS = 8
INTEGRATE_CIRCUIT_HALF_OPEN_AFTER_SECONDS = 30.0


@dataclass
class _OpportunisticGitCircuit:
    """Per-workspace bulkhead for opportunistic integrate git calls."""

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    git_missing: bool = False
    git_missing_skips: int = 0
    git_missing_opened_at: float | None = None
    git_missing_half_open: bool = False
    head_failures: int = 0
    head_open: bool = False
    head_open_skips: int = 0
    head_open_opened_at: float | None = None
    head_open_half_open: bool = False
    resolved_heads: dict[str, str] = field(default_factory=dict)
    transient_attempts: dict[tuple[str, str], int] = field(default_factory=dict)
    quarantined: dict[tuple[str, str], int] = field(default_factory=dict)
    quarantine_opened_at: dict[tuple[str, str], float] = field(default_factory=dict)
    quarantine_half_open: set[tuple[str, str]] = field(default_factory=set)


_OPPORTUNISTIC_GIT_CIRCUIT = _OpportunisticGitCircuit()
_CIRCUIT_REGISTRY: dict[tuple[str, str], _OpportunisticGitCircuit] = {}
_CIRCUIT_REGISTRY_LOCK = threading.Lock()
_CIRCUIT_LOCAL = threading.local()


def _circuit_workspace_key(config: object) -> tuple[str, str]:
    """Stable bulkhead identity: same DB + git root share one circuit.

    Object-identity (`id(config)`) recycles after GC and splits equal-valued
    reconstructions of a frozen RuntimeConfig. ``db_path`` names the handoff
    workspace; ``git_workspace_root`` keeps linked worktrees from sharing
    HEAD-resolution counters with the primary.
    """
    db_path = Path(getattr(config, "db_path"))
    git_root = getattr(config, "git_workspace_root", None) or getattr(config, "workspace_root")
    return (str(db_path.expanduser().resolve()), str(Path(git_root).expanduser().resolve()))


def _reset_opportunistic_integrate_circuit() -> None:
    """Test helper: clear the process-local git circuit and attempt counters."""
    global _OPPORTUNISTIC_GIT_CIRCUIT
    with _CIRCUIT_REGISTRY_LOCK:
        _CIRCUIT_REGISTRY.clear()
        _OPPORTUNISTIC_GIT_CIRCUIT = _OpportunisticGitCircuit()
    _CIRCUIT_LOCAL.circuit = _OPPORTUNISTIC_GIT_CIRCUIT


def _bind_circuit_to_runtime() -> _OpportunisticGitCircuit:
    """Select the per-workspace circuit for the current RuntimeConfig.

    The circuit is a bulkhead for one workspace, not one process. Two
    workspaces served by one MCP process must not share counters or open
    flags: one workspace's missing git must not fail-fast the other, and
    switching config identity must not wipe the first workspace's state.
    The registry is keyed by ``(db_path, git_workspace_root)`` so a
    reconstructed equal-valued RuntimeConfig reuses the same bulkhead.
    """
    global _OPPORTUNISTIC_GIT_CIRCUIT
    try:
        config = get_runtime_config()
    except RuntimeNotConfiguredError:
        circuit = _OPPORTUNISTIC_GIT_CIRCUIT
        _CIRCUIT_LOCAL.circuit = circuit
        return circuit
    key = _circuit_workspace_key(config)
    with _CIRCUIT_REGISTRY_LOCK:
        circuit = _CIRCUIT_REGISTRY.get(key)
        if circuit is None:
            circuit = _OpportunisticGitCircuit()
            _CIRCUIT_REGISTRY[key] = circuit
        _OPPORTUNISTIC_GIT_CIRCUIT = circuit
    _CIRCUIT_LOCAL.circuit = circuit
    return circuit


def _active_circuit() -> _OpportunisticGitCircuit:
    circuit = getattr(_CIRCUIT_LOCAL, "circuit", None)
    if isinstance(circuit, _OpportunisticGitCircuit):
        return circuit
    return _OPPORTUNISTIC_GIT_CIRCUIT


def _half_open_due(skips: int, opened_at: float | None, now: float) -> bool:
    if skips >= INTEGRATE_CIRCUIT_HALF_OPEN_AFTER_SKIPS:
        return True
    if opened_at is not None and (now - opened_at) >= INTEGRATE_CIRCUIT_HALF_OPEN_AFTER_SECONDS:
        return True
    return False


def _fail_fast_git_missing(circuit: _OpportunisticGitCircuit) -> bool:
    """Return True when opportunistic should skip (open, not probing).

    Admitting a half-open trial consumes the permit: ``git_missing_half_open``
    is set False so a later caller cannot keep forking git. Exactly one
    trial per skip/time window; an unsuccessful probe re-opens via
    ``_circuit_note_git_probe_failed``.
    """
    now = time.monotonic()
    with circuit._lock:
        if not circuit.git_missing:
            return False
        if circuit.git_missing_half_open:
            circuit.git_missing_half_open = False
            return False
        if _half_open_due(circuit.git_missing_skips, circuit.git_missing_opened_at, now):
            circuit.git_missing_half_open = False
            circuit.git_missing_skips = 0
            return False
        circuit.git_missing_skips += 1
        return True


def _fail_fast_head_open(circuit: _OpportunisticGitCircuit) -> bool:
    """Return True when opportunistic should skip (open, not probing)."""
    now = time.monotonic()
    with circuit._lock:
        if not circuit.head_open:
            return False
        if circuit.head_open_half_open:
            return False
        if _half_open_due(circuit.head_open_skips, circuit.head_open_opened_at, now):
            circuit.head_open_half_open = True
            circuit.head_open_skips = 0
            return False
        circuit.head_open_skips += 1
        return True


def _quarantine_skip_or_probe(
    circuit: _OpportunisticGitCircuit,
    task_ref: str,
    head_sha: str,
) -> dict[str, object] | None:
    """Fail-fast a quarantined HEAD, or admit one half-open trial."""
    key = (task_ref, head_sha)
    now = time.monotonic()
    with circuit._lock:
        if key in circuit.quarantine_half_open:
            return None
        if key not in circuit.quarantined:
            return None
        skips = circuit.quarantined[key]
        opened_at = circuit.quarantine_opened_at.get(key)
        if _half_open_due(skips, opened_at, now):
            circuit.quarantine_half_open.add(key)
            circuit.quarantined[key] = 0
            return None
        circuit.quarantined[key] = skips + 1
    _LOG.warning(
        "opportunistic integrate skipped: transient_quarantined (task=%s head=%s)",
        task_ref,
        head_sha,
    )
    return {"integrate_skipped": "transient_quarantined"}


def _mark_quarantined(
    circuit: _OpportunisticGitCircuit,
    task_ref: str,
    head_sha: str,
) -> None:
    key = (task_ref, head_sha)
    now = time.monotonic()
    with circuit._lock:
        if key not in circuit.quarantined:
            circuit.quarantined[key] = 0
            circuit.quarantine_opened_at[key] = now
        circuit.quarantine_half_open.discard(key)


def _clear_quarantine(
    circuit: _OpportunisticGitCircuit,
    task_ref: str,
    head_sha: str,
) -> None:
    key = (task_ref, head_sha)
    with circuit._lock:
        circuit.quarantined.pop(key, None)
        circuit.quarantine_opened_at.pop(key, None)
        circuit.quarantine_half_open.discard(key)


def _circuit_note_head_unresolved(circuit: _OpportunisticGitCircuit) -> None:
    now = time.monotonic()
    with circuit._lock:
        circuit.head_failures += 1
        circuit.head_open_half_open = False
        if circuit.head_failures >= INTEGRATE_HEAD_RESOLVE_FAILURE_LIMIT:
            circuit.head_open = True
            if circuit.head_open_opened_at is None:
                circuit.head_open_opened_at = now


def _circuit_note_git_probe_failed(circuit: _OpportunisticGitCircuit) -> None:
    """Re-open git_missing after an unsuccessful resolve that was not ENOENT.

    Timeout, other OSError, and nonzero rev-parse must consume the half-open
    permit so opportunistic does not keep forking git on every host write.
    A healthy circuit (git_missing False) stays on the head_unresolved path.
    """
    now = time.monotonic()
    with circuit._lock:
        if not circuit.git_missing:
            return
        circuit.git_missing_half_open = False
        if circuit.git_missing_opened_at is None:
            circuit.git_missing_opened_at = now


class _LooseRefPresentButUnusable(Exception):
    """Loose ref exists but is not a followable SHA; do not scan packed-refs."""


def _looks_like_sha(value: str) -> bool:
    return len(value) == 40 and all(c in "0123456789abcdefABCDEF" for c in value)


def _git_dir(workspace_root: str | Path) -> Path | None:
    git_path = Path(workspace_root) / ".git"
    try:
        if git_path.is_file():
            text = git_path.read_text(encoding="utf-8")
            for line in text.splitlines():
                if line.startswith("gitdir:"):
                    gitdir = Path(line.split(":", 1)[1].strip())
                    if not gitdir.is_absolute():
                        gitdir = (git_path.parent / gitdir).resolve()
                    return gitdir
            return None
        if git_path.is_dir():
            return git_path
    except OSError:
        return None
    return None


def _git_common_dir(git_dir: Path) -> Path:
    """Return the shared git directory that holds refs and packed-refs.

    Linked worktrees name that directory in ``commondir``, relative to the
    per-worktree git directory. HEAD stays in ``git_dir``; only a missing
    ``commondir`` file means this *is* the common directory. Any other
    ``OSError`` reading ``commondir`` propagates so the cheap peek can
    degrade to unknown instead of inventing a path.
    """
    commondir_file = git_dir / "commondir"
    try:
        raw = commondir_file.read_text(encoding="utf-8")
    except FileNotFoundError:
        return git_dir
    line = raw.splitlines()[0].strip() if raw else ""
    if not line:
        return git_dir
    common = Path(line)
    if not common.is_absolute():
        common = (git_dir / common).resolve()
    return common


def _ref_lookup_base(name: str, git_dir: Path, common_dir: Path) -> Path:
    """HEAD is per-worktree; branch/tag refs live in the common directory."""
    return git_dir if name == "HEAD" else common_dir


def _cheap_read_git_ref_sha(integration_ref: str) -> str | None:
    """Read a 40-char SHA from the git ref file without a subprocess.

    Used so a latched HEAD does not occupy the 5s ``rev-parse`` budget on
    every host write. Falls back to None when the ref is packed, symbolic
    in a way we cannot follow, or the workspace is not a git repo; callers
    then use ``_resolve_integration_ref_head_sha``.

    Loose-ref ``FileNotFoundError`` is genuine absence and is the only
    error that licenses the packed-refs fallback. A present loose file
    that is not a followable SHA (garbage, empty, dangling ``ref:``,
    non-UTF8, or any other ``OSError``) is authoritative unknown: return
    None without scanning a stale packed copy.
    """
    try:
        config = get_runtime_config()
        git_dir = _git_dir(config.git_workspace_root)
    except Exception:  # noqa: BLE001 — cheap peek must never raise
        return None
    if git_dir is None:
        return None
    ref_name = integration_ref.strip()
    if not ref_name:
        return None
    if ref_name.startswith("refs/"):
        candidates = [ref_name]
    else:
        candidates = [f"refs/heads/{ref_name}", f"refs/tags/{ref_name}", ref_name]

    try:
        common_dir = _git_common_dir(git_dir)
    except OSError:
        return None

    def _read_ref_file(name: str, depth: int = 0) -> str | None:
        if depth > 4:
            raise _LooseRefPresentButUnusable
        path = _ref_lookup_base(name, git_dir, common_dir) / name
        try:
            text = path.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            if depth > 0:
                raise _LooseRefPresentButUnusable from None
            return None
        except (OSError, UnicodeDecodeError) as exc:
            raise _LooseRefPresentButUnusable from exc
        if text.startswith("ref:"):
            target = text.split(":", 1)[1].strip()
            if not target:
                raise _LooseRefPresentButUnusable
            sha = _read_ref_file(target, depth + 1)
            if sha is None:
                raise _LooseRefPresentButUnusable
            return sha
        sha = text.split()[0] if text else ""
        if _looks_like_sha(sha):
            return sha.lower()
        raise _LooseRefPresentButUnusable

    try:
        for name in candidates:
            try:
                sha = _read_ref_file(name)
            except _LooseRefPresentButUnusable:
                return None
            if sha is not None:
                return sha
        packed = common_dir / "packed-refs"
        wanted = set(candidates)
        for line in packed.read_text(encoding="utf-8").splitlines():
            if not line or line.startswith("#") or line.startswith("^"):
                continue
            sha, _, ref = line.partition(" ")
            if ref.strip() in wanted and _looks_like_sha(sha.strip()):
                return sha.strip().lower()
    except (OSError, UnicodeDecodeError):
        return None
    return None


def _ineligible_anchor_json(
    ancestry_fatals: dict[str, str],
    reachable_by_sha: dict[str, bool],
) -> str:
    """JSON array of anchors that must not pin the latch at this HEAD.

    Determinate fatals and not-ancestor SHAs cannot become reachable until
    HEAD moves (fatals: never). Leaving them in the NOT EXISTS predicate
    would recreate the round-1 opportunistic spin from the other direction.
    """
    ineligible = set(ancestry_fatals)
    ineligible.update(sha for sha, is_reach in reachable_by_sha.items() if is_reach is not True)
    return json.dumps(sorted(ineligible))


def _read_last_observed_integration_sha(task_ref: str) -> str | None:
    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT last_observed_integration_sha FROM handoff_state WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()
    if row is None:
        return None
    return _normalize_optional_text(row["last_observed_integration_sha"])


def _integrate_receipt_fields(
    *,
    resolved_task_ref: str,
    integration_ref: str,
    head_sha: str | None,
    promoted: list[dict[str, str]],
    skipped_unreachable: list[dict[str, str | None]],
    skipped_conflict: list[dict[str, str | None]],
    cap_applied: bool,
    errors: list[str],
    debounce_held: bool,
    last_observed_integration_sha: str | None,
    transient_attempt: int = 0,
    integrate_quarantined: bool = False,
) -> dict[str, object]:
    data: dict[str, object] = {
        "task_ref": resolved_task_ref,
        "integration_ref": integration_ref,
        "integration_sha": head_sha,
        "promoted": promoted,
        "skipped_unreachable": skipped_unreachable,
        "skipped_conflict": skipped_conflict,
        "cap_applied": cap_applied,
        "errors": errors,
        "promotion_failed_count": len(errors),
        "debounce_held": debounce_held,
        "last_observed_integration_sha": last_observed_integration_sha,
    }
    if transient_attempt:
        data["transient_attempt"] = transient_attempt
        data["transient_attempt_limit"] = INTEGRATE_TRANSIENT_ATTEMPT_LIMIT
    if integrate_quarantined:
        data["integrate_quarantined"] = True
    return data


def _precompute_integration_ancestry(
    anchors: list[str],
    integration_ref: str,
) -> tuple[dict[str, bool], dict[str, str], dict[str, str]]:
    """Map unique anchor SHAs to reachability with no DB connection held.

    ``integration_ref`` must be the already-snapshotted commit SHA, not a
    moving symbolic ref, so the proof and stored attribution name the same
    immutable object.

    Deduplicates so N rows that share an anchor pay one git fork, not N.
    The ancestry primitive is four-state (``True`` / ``False`` /
    ``AncestryFatal`` / ``None``). Transient ``None`` and raised exceptions
    are captured separately from determinate fatals (missing object) so the
    write phase never shells out, unknown is not collapsed into
    not-ancestor, and a corrupt anchor does not pin the debounce latch.
    """
    reachable: dict[str, bool] = {}
    transient_failures: dict[str, str] = {}
    fatal_failures: dict[str, str] = {}
    for sha in dict.fromkeys(anchors):
        try:
            result = _is_ancestor_of_ref(sha, integration_ref)
        except Exception as exc:  # noqa: BLE001 — git wrapper hardening
            transient_failures[sha] = str(exc)
            continue
        if result is True:
            reachable[sha] = True
        elif result is False:
            reachable[sha] = False
        elif isinstance(result, AncestryFatal):
            detail = result.stderr.strip() or f"git exit {result.returncode}"
            fatal_failures[sha] = f"determinate ancestry fatal for {sha} relative to {integration_ref}: {detail}"
        else:
            transient_failures[sha] = f"could not determine ancestry of {sha} relative to {integration_ref}"
    return reachable, transient_failures, fatal_failures


def _resolve_integration_ref_head_sha(integration_ref: str) -> str | None:
    """Return the 40-char HEAD SHA of ``integration_ref``, or None if the ref
    cannot be resolved (e.g. detached worktree, ref does not exist, or git is
    unavailable). Errors are swallowed — the opportunistic trigger treats
    "unknown" as "skip this pass" rather than blocking the host write.

    ``FileNotFoundError`` (missing git binary) trips the workspace circuit
    so *opportunistic* later calls fail fast. This function always tries
    git: explicit ``integrate_review_findings`` is the documented escape
    hatch and must not inherit the opportunistic fail-fast.
    """
    circuit = _active_circuit()
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
    except FileNotFoundError:
        now = time.monotonic()
        with circuit._lock:
            circuit.git_missing = True
            circuit.git_missing_half_open = False
            if circuit.git_missing_opened_at is None:
                circuit.git_missing_opened_at = now
        return None
    except (OSError, subprocess.TimeoutExpired):
        _circuit_note_git_probe_failed(circuit)
        return None
    if proc.returncode != 0:
        _circuit_note_git_probe_failed(circuit)
        return None
    sha = (proc.stdout or "").strip()
    if sha:
        with circuit._lock:
            circuit.git_missing = False
            circuit.git_missing_skips = 0
            circuit.git_missing_opened_at = None
            circuit.git_missing_half_open = False
        return sha
    _circuit_note_git_probe_failed(circuit)
    return None


@dataclass
class _IntegrateScanResult:
    to_promote: list[tuple[int, str, str]]
    skipped_unreachable: list[dict[str, str | None]]
    errors: list[str]
    hold_debounce: bool
    cap_applied: bool
    reachable_by_sha: dict[str, bool]
    ancestry_failures: dict[str, str]
    ancestry_fatals: dict[str, str]


def _load_resolved_on_branch_page(
    task_ref: str,
    *,
    after_id: int,
    limit: int,
) -> list[dict[str, object]]:
    """Keyset page of ``resolved_on_branch`` rows, ordered by id."""
    with _get_db_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, finding_id, resolved_on_branch_at_commit
            FROM review_findings
            WHERE task_ref = ? AND status = 'resolved_on_branch' AND id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (task_ref, after_id, limit),
        ).fetchall()
    return [
        {
            "id": int(row["id"]),
            "finding_id": str(row["finding_id"]),
            "resolved_on_branch_at_commit": row["resolved_on_branch_at_commit"],
        }
        for row in rows
    ]


def _collect_integrate_scan(
    resolved_task_ref: str,
    head_sha: str,
    *,
    seed_page: list[dict[str, object]] | None = None,
) -> _IntegrateScanResult:
    """Load and classify candidates until MAX promotions or end of table.

    Ineligible rows (missing anchor, not-ancestor, determinate fatal) do not
    charge the promotion cap. Git ancestry runs with no write transaction
    open, one keyset page at a time. ``seed_page`` is the snapshot taken
    before HEAD resolve so a row inserted during that git window is not
    classified here and still blocks the latch ``NOT EXISTS``.
    """
    max_per_pass = INTEGRATE_REVIEW_FINDINGS_MAX_PER_PASS
    to_promote: list[tuple[int, str, str]] = []
    skipped_unreachable: list[dict[str, str | None]] = []
    errors: list[str] = []
    hold_debounce = False
    reachable_by_sha: dict[str, bool] = {}
    ancestry_failures: dict[str, str] = {}
    ancestry_fatals: dict[str, str] = {}
    cap_applied = False
    after_id = 0
    use_seed = seed_page is not None

    while len(to_promote) < max_per_pass:
        room = max_per_pass - len(to_promote)
        if use_seed:
            page = list(seed_page or [])
            use_seed = False
        else:
            page = _load_resolved_on_branch_page(
                resolved_task_ref,
                after_id=after_id,
                limit=room + 1,
            )
        if not page:
            break
        after_id = int(page[-1]["id"])
        page_full = len(page) >= room + 1

        anchored: list[tuple[int, str, str]] = []
        for row in page:
            finding_id = str(row["finding_id"])
            anchor_commit = _normalize_optional_text(row["resolved_on_branch_at_commit"])
            if not anchor_commit:
                skipped_unreachable.append(
                    {"finding_id": finding_id, "anchor_commit": None, "reason": "missing_anchor"}
                )
                continue
            anchored.append((int(row["id"]), finding_id, anchor_commit))

        new_anchors = [
            anchor
            for _, _, anchor in anchored
            if anchor not in reachable_by_sha
            and anchor not in ancestry_failures
            and anchor not in ancestry_fatals
        ]
        new_reach, new_fail, new_fatal = _precompute_integration_ancestry(new_anchors, head_sha)
        reachable_by_sha.update(new_reach)
        ancestry_failures.update(new_fail)
        ancestry_fatals.update(new_fatal)

        discarded_eligible = False
        for row_id, finding_id, anchor_commit in anchored:
            fatal = ancestry_fatals.get(anchor_commit)
            if fatal is not None:
                _LOG.warning(
                    "integrate_review_findings: determinate ancestry fatal for %s (task=%s): %s",
                    finding_id,
                    resolved_task_ref,
                    fatal,
                )
                errors.append(f"{finding_id}: {fatal}")
                continue
            failure = ancestry_failures.get(anchor_commit)
            if failure is not None:
                _LOG.warning(
                    "integrate_review_findings: ancestry check failed for %s (task=%s): %s",
                    finding_id,
                    resolved_task_ref,
                    failure,
                )
                errors.append(f"{finding_id}: {failure}")
                hold_debounce = True
                continue
            if not reachable_by_sha.get(anchor_commit):
                skipped_unreachable.append(
                    {"finding_id": finding_id, "anchor_commit": anchor_commit, "reason": "not_ancestor"}
                )
                continue
            if len(to_promote) >= max_per_pass:
                discarded_eligible = True
                continue
            to_promote.append((row_id, finding_id, anchor_commit))

        if discarded_eligible or (len(to_promote) >= max_per_pass and page_full):
            cap_applied = True
            break
        if not page_full:
            break

    return _IntegrateScanResult(
        to_promote=to_promote,
        skipped_unreachable=skipped_unreachable,
        errors=errors,
        hold_debounce=hold_debounce,
        cap_applied=cap_applied,
        reachable_by_sha=reachable_by_sha,
        ancestry_failures=ancestry_failures,
        ancestry_fatals=ancestry_fatals,
    )


def _write_integrate_promotions(
    conn: sqlite3.Connection,
    *,
    to_promote: list[tuple[int, str, str]],
    head_sha: str,
    integration_ref: str,
    resolved_task_ref: str,
    ctx: ResolvedWriteContext,
    promoted: list[dict[str, str]],
    skipped_conflict: list[dict[str, str | None]],
    errors: list[str],
    hold_debounce: bool,
) -> bool:
    """Per-row SAVEPOINT promotions. RELEASE commits immediately.

    Appends landed rows to ``promoted`` and write failures to ``errors``.
    Raises when ``ROLLBACK TO`` itself fails so the caller can emit the
    abort envelope next to RELEASE semantics, with ``promoted`` already
    naming landed rows.
    """
    for row_id, finding_id, anchor_commit in to_promote:
        # Outermost savepoint: isolation_level='' with no BEGIN, so
        # RELEASE commits this finding immediately. Sibling writes
        # do not share a batch transaction.
        conn.execute("SAVEPOINT integrate_row")
        try:
            updated = conn.execute(
                """
                UPDATE review_findings
                SET status = 'integrated',
                    integrated_at_commit = ?,
                    integrated_at_ref = ?,
                    integrated_at_ts = datetime('now'),
                    updated_at = datetime('now')
                WHERE id = ?
                  AND task_ref = ?
                  AND status = 'resolved_on_branch'
                  AND resolved_on_branch_at_commit = ?
                """,
                (head_sha, integration_ref, row_id, resolved_task_ref, anchor_commit),
            )
            if updated.rowcount == 0:
                _LOG.warning(
                    "integrate_review_findings: CAS miss for %s (task=%s); "
                    "row was not still resolved_on_branch at classified anchor",
                    finding_id,
                    resolved_task_ref,
                )
                skipped_conflict.append(
                    {
                        "finding_id": finding_id,
                        "anchor_commit": anchor_commit,
                        "reason": "cas_miss",
                    }
                )
                hold_debounce = True
                conn.execute("RELEASE SAVEPOINT integrate_row")
                continue

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
                ON CONFLICT(task_ref, decision, session) DO NOTHING
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
            conn.execute("RELEASE SAVEPOINT integrate_row")
            promoted.append({"finding_id": finding_id, "anchor_commit": anchor_commit})
        except Exception as exc:  # noqa: BLE001 — bound blast radius to this finding
            write_error = f"{finding_id}: promotion write failed: {exc}"
            try:
                conn.execute("ROLLBACK TO SAVEPOINT integrate_row")
                conn.execute("RELEASE SAVEPOINT integrate_row")
            except Exception as rollback_exc:  # noqa: BLE001 — preserve the write error
                errors.append(write_error)
                errors.append(f"{finding_id}: savepoint rollback failed: {rollback_exc}")
                hold_debounce = True
                raise
            _LOG.warning(
                "integrate_review_findings: write failed for %s (task=%s): %s",
                finding_id,
                resolved_task_ref,
                exc,
            )
            errors.append(write_error)
            hold_debounce = True
    return hold_debounce


def _advance_integrate_latch(
    conn: sqlite3.Connection,
    *,
    head_sha: str,
    resolved_task_ref: str,
    ineligible_json: str,
) -> None:
    """Advance last_observed only when nothing eligible remains at this HEAD."""
    # Latch write: in-statement NOT EXISTS (not a Python SELECT then
    # UPDATE) so a concurrent resolved_on_branch row is serialized
    # by SQLite. Quarantine after N transients opens the circuit
    # without force-advancing: last_observed means this HEAD was
    # fully processed, not that we gave up. Fail-fast lives on the
    # circuit, matching head_open.
    conn.execute(
        """
        UPDATE handoff_state
        SET last_observed_integration_sha = ?
        WHERE task_ref = ?
          AND NOT EXISTS (
            SELECT 1 FROM review_findings
            WHERE task_ref = ?
              AND status = 'resolved_on_branch'
              AND resolved_on_branch_at_commit IS NOT NULL
              AND TRIM(resolved_on_branch_at_commit) != ''
              AND resolved_on_branch_at_commit NOT IN (
                SELECT value FROM json_each(?)
              )
          )
        """,
        (head_sha, resolved_task_ref, resolved_task_ref, ineligible_json),
    )


def _integrate_abort_envelope(
    *,
    resolved_task_ref: str,
    integration_ref: str,
    head_sha: str | None,
    promoted: list[dict[str, str]],
    skipped_unreachable: list[dict[str, str | None]],
    skipped_conflict: list[dict[str, str | None]],
    cap_applied: bool,
    errors: list[str],
    exc: BaseException,
    transient_attempt: int,
) -> dict:
    """Honest abort receipt: RELEASE already committed ``promoted``."""
    abort_errors = [*errors, f"promotion batch aborted: {exc}"]
    return _envelope(
        ok=False,
        tool="integrate_review_findings",
        data=_integrate_receipt_fields(
            resolved_task_ref=resolved_task_ref,
            integration_ref=integration_ref,
            head_sha=head_sha,
            promoted=list(promoted),
            skipped_unreachable=skipped_unreachable,
            skipped_conflict=skipped_conflict,
            cap_applied=cap_applied,
            errors=abort_errors,
            debounce_held=True,
            last_observed_integration_sha=_read_last_observed_integration_sha(resolved_task_ref),
            transient_attempt=transient_attempt,
            integrate_quarantined=False,
        ),
        task_ref=resolved_task_ref,
        entity="finding",
        mutation={
            "entity": "finding",
            "operation": "integrate",
            "affected_ids": [item["finding_id"] for item in promoted],
        },
    )


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
    :data:`INTEGRATE_REVIEW_FINDINGS_MAX_PER_PASS` *promotions* per call
    so the opportunistic trigger stays bounded; ineligible rows do not
    charge that cap. Eligible overflow rolls into the next pass.

    This entry point is **distinct** from internal's
    :func:`reconcile_review_findings`, which performs integrity / dedup
    checks. The two operations are not aliases.

    Git ancestry is precomputed against the snapshotted integration SHA
    with no write transaction open ([CON-18], [CON-21]). The write phase
    applies already-decided promotions only, CAS-guarded on
    ``status='resolved_on_branch'`` and the classified anchor: no
    subprocess, so ``assert_no_write_lock_held`` is never reachable from
    this loop. ``last_observed_integration_sha`` is advanced only when
    no eligible ``resolved_on_branch`` row remains at this HEAD. The
    latch ``UPDATE`` uses an in-statement ``NOT EXISTS`` predicate so a
    row that arrived during the git window still blocks the advance
    (SQLite serializes the check with the write; a Python SELECT then
    UPDATE would reintroduce that skew). Determinate ancestry fatals
    and already-classified not-ancestor SHAs are excluded from that
    predicate — retrying them at this HEAD can never succeed and must
    not pin the latch. Transient ancestry ``None`` holds the latch
    until :data:`INTEGRATE_TRANSIENT_ATTEMPT_LIMIT` consecutive
    attempts at this HEAD, after which the HEAD is quarantined (the
    circuit opens; the latch is not advanced; leftovers wait for a
    half-open trial, HEAD to move, or an explicit integrate). A leftover
    decision row is idempotent
    (``ON CONFLICT DO NOTHING``). A raise while promoting one finding
    is isolated via ``SAVEPOINT integrate_row``. That savepoint is the
    **outermost** savepoint on a connection with ``isolation_level=''``
    and no enclosing ``BEGIN``, so ``RELEASE`` commits that finding
    immediately; a later sibling failure does not roll it back. The
    outer abort envelope reports the rows whose ``RELEASE`` already
    committed, not an empty promotion list. A non-empty ``errors``
    list returns ``ok=False``.
    """
    _bind_circuit_to_runtime()
    with _get_db_connection() as conn:
        resolved_task_ref = _resolve_task_ref(conn, task_ref)
        ctx = _resolve_write_actor(conn, actor, task_ref=resolved_task_ref)

    max_per_pass = INTEGRATE_REVIEW_FINDINGS_MAX_PER_PASS
    seed_page = _load_resolved_on_branch_page(
        resolved_task_ref,
        after_id=0,
        limit=max_per_pass + 1,
    )
    head_sha = _resolve_integration_ref_head_sha(integration_ref)
    if head_sha is None:
        unresolved_errors = ["integration_ref could not be resolved to a commit"]
        return _envelope(
            ok=False,
            tool="integrate_review_findings",
            data=_integrate_receipt_fields(
                resolved_task_ref=resolved_task_ref,
                integration_ref=integration_ref,
                head_sha=None,
                promoted=[],
                skipped_unreachable=[],
                skipped_conflict=[],
                cap_applied=len(seed_page) > max_per_pass,
                errors=unresolved_errors,
                debounce_held=True,
                last_observed_integration_sha=_read_last_observed_integration_sha(resolved_task_ref),
            ),
            task_ref=resolved_task_ref,
            entity="finding",
        )

    scan = _collect_integrate_scan(resolved_task_ref, head_sha, seed_page=seed_page)
    skipped_unreachable = scan.skipped_unreachable
    errors = scan.errors
    hold_debounce = scan.hold_debounce
    cap_applied = scan.cap_applied
    # Transient ancestry / write failures and CAS misses hold the debounce
    # so opportunistic integrate retries at this HEAD. Determinate fatals
    # (missing object) are attributed in ``errors`` but do not hold it —
    # retrying them can never succeed and would re-fork git on every host write.
    transient_attempt = 0
    integrate_quarantined = False
    circuit = _active_circuit()
    if hold_debounce and scan.ancestry_failures:
        key = (resolved_task_ref, head_sha)
        with circuit._lock:
            circuit.transient_attempts[key] = circuit.transient_attempts.get(key, 0) + 1
            transient_attempt = circuit.transient_attempts[key]
            if transient_attempt >= INTEGRATE_TRANSIENT_ATTEMPT_LIMIT:
                integrate_quarantined = True
        if integrate_quarantined:
            _mark_quarantined(circuit, resolved_task_ref, head_sha)
    elif head_sha is not None:
        _clear_quarantine(circuit, resolved_task_ref, head_sha)

    ineligible_json = _ineligible_anchor_json(scan.ancestry_fatals, scan.reachable_by_sha)
    with circuit._lock:
        circuit.resolved_heads[integration_ref] = head_sha

    promoted: list[dict[str, str]] = []
    skipped_conflict: list[dict[str, str | None]] = []
    try:
        with _get_db_connection() as conn:
            hold_debounce = _write_integrate_promotions(
                conn,
                to_promote=scan.to_promote,
                head_sha=head_sha,
                integration_ref=integration_ref,
                resolved_task_ref=resolved_task_ref,
                ctx=ctx,
                promoted=promoted,
                skipped_conflict=skipped_conflict,
                errors=errors,
                hold_debounce=hold_debounce,
            )
            _advance_integrate_latch(
                conn,
                head_sha=head_sha,
                resolved_task_ref=resolved_task_ref,
                ineligible_json=ineligible_json,
            )
            if promoted:
                _write_current_task_md_for_active_context(conn, resolved_task_ref)
            task_revision = _current_task_revision(conn, resolved_task_ref)
            observed_row = conn.execute(
                "SELECT last_observed_integration_sha FROM handoff_state WHERE task_ref = ?",
                (resolved_task_ref,),
            ).fetchone()
            last_observed_now = (
                _normalize_optional_text(observed_row["last_observed_integration_sha"])
                if observed_row is not None
                else None
            )
    except Exception as exc:  # noqa: BLE001 — still return an honest envelope
        _LOG.warning(
            "integrate_review_findings: promotion batch aborted (task=%s): %s",
            resolved_task_ref,
            exc,
        )
        # RELEASE of the outermost savepoint already committed `promoted`.
        # Factory rollback cannot undo those rows; report what landed.
        return _integrate_abort_envelope(
            resolved_task_ref=resolved_task_ref,
            integration_ref=integration_ref,
            head_sha=head_sha,
            promoted=promoted,
            skipped_unreachable=skipped_unreachable,
            skipped_conflict=skipped_conflict,
            cap_applied=cap_applied,
            errors=errors,
            exc=exc,
            transient_attempt=transient_attempt,
        )

    debounce_held = bool(hold_debounce) and not integrate_quarantined
    return _envelope(
        ok=not errors,
        tool="integrate_review_findings",
        data=_integrate_receipt_fields(
            resolved_task_ref=resolved_task_ref,
            integration_ref=integration_ref,
            head_sha=head_sha,
            promoted=promoted,
            skipped_unreachable=skipped_unreachable,
            skipped_conflict=skipped_conflict,
            cap_applied=cap_applied,
            errors=errors,
            debounce_held=debounce_held,
            last_observed_integration_sha=last_observed_now,
            transient_attempt=transient_attempt,
            integrate_quarantined=integrate_quarantined,
        ),
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
) -> dict[str, object] | None:
    """Best-effort opportunistic integrate trigger for host write paths.

    Reads ``handoff_state.last_observed_integration_sha`` for the resolved
    task; if the current integration-ref HEAD differs, runs
    :func:`integrate_review_findings` for that task. Every failure mode —
    git unavailable, missing task row, integrate raising — is logged and
    swallowed so the host write never blocks on this side effect.

    Returns a typed skip/hold signal for the host-write envelope, or
    ``None`` when the trigger was a silent no-op success (fully latched
    and drained, or no active task).
    """
    _bind_circuit_to_runtime()
    circuit = _active_circuit()
    try:
        if _fail_fast_git_missing(circuit):
            _LOG.warning("opportunistic integrate skipped: git_missing")
            return {"integrate_skipped": "git_missing"}
        if _fail_fast_head_open(circuit):
            _LOG.warning("opportunistic integrate skipped: head_resolve_circuit_open")
            return {"integrate_skipped": "head_resolve_circuit_open"}
        with _get_db_connection() as conn:
            try:
                resolved_task_ref = _resolve_task_ref(conn, task_ref)
            except Exception:  # noqa: BLE001 — no active task is fine
                return None
            row = conn.execute(
                "SELECT last_observed_integration_sha FROM handoff_state WHERE task_ref = ?",
                (resolved_task_ref,),
            ).fetchone()
            last_observed = _normalize_optional_text(row["last_observed_integration_sha"]) if row is not None else None
        # Only a *live* cheap read may stand in for HEAD. A cheap miss must
        # not fall back to the cached ``resolved_heads`` entry: that entry is
        # this circuit's last successful observation, so treating it as current
        # asserts "HEAD is unchanged" on no evidence. It masks exactly the two
        # cases this stack exists to fix -- a linked-worktree commondir miss,
        # and HEAD advancing after the first latch -- and the latter wedges the
        # trigger permanently, because nothing else ever re-reads HEAD.
        observed_head: str | None = None
        if last_observed is not None:
            cheap = _cheap_read_git_ref_sha(integration_ref)
            if cheap is not None:
                with circuit._lock:
                    circuit.resolved_heads[integration_ref] = cheap
                observed_head = cheap
        if observed_head is not None:
            qskip = _quarantine_skip_or_probe(circuit, resolved_task_ref, observed_head)
            if qskip is not None:
                qskip["last_observed_integration_sha"] = last_observed
                return qskip
            if observed_head == last_observed:
                return {"integrate_skipped": "latched_head"}
        head_sha = _resolve_integration_ref_head_sha(integration_ref)
        if head_sha is None:
            git_missing = False
            with circuit._lock:
                git_missing = circuit.git_missing
            if git_missing:
                return {"integrate_skipped": "git_missing"}
            _circuit_note_head_unresolved(circuit)
            return {"integrate_skipped": "head_unresolved"}
        with circuit._lock:
            circuit.head_failures = 0
            circuit.head_open = False
            circuit.head_open_skips = 0
            circuit.head_open_opened_at = None
            circuit.head_open_half_open = False
            circuit.resolved_heads[integration_ref] = head_sha
        qskip = _quarantine_skip_or_probe(circuit, resolved_task_ref, head_sha)
        if qskip is not None:
            qskip["last_observed_integration_sha"] = last_observed
            return qskip
        if head_sha == last_observed:
            return {"integrate_skipped": "latched_head"}
        result = integrate_review_findings(task_ref=resolved_task_ref, integration_ref=integration_ref)
        data = result.get("data") if isinstance(result, dict) else None
        if not isinstance(data, dict):
            data = result if isinstance(result, dict) else {}
        signal: dict[str, object] = {}
        if data.get("integrate_quarantined"):
            _mark_quarantined(circuit, resolved_task_ref, head_sha)
            signal["integrate_skipped"] = "transient_quarantined"
        else:
            _clear_quarantine(circuit, resolved_task_ref, head_sha)
            if data.get("debounce_held"):
                signal["integrate_held"] = True
                if data.get("transient_attempt"):
                    signal["integrate_hold_attempt"] = data["transient_attempt"]
                    signal["integrate_hold_limit"] = INTEGRATE_TRANSIENT_ATTEMPT_LIMIT
        if "last_observed_integration_sha" in data:
            signal["last_observed_integration_sha"] = data["last_observed_integration_sha"]
        return signal or None
    except Exception as exc:  # noqa: BLE001 — opportunistic best-effort
        _LOG.warning("opportunistic integrate trigger failed (task=%s): %s", task_ref, exc)
        return {"integrate_skipped": "error"}


def _attach_opportunistic_integrate_signal(
    result: dict,
    task_ref: str | None,
    integration_ref: str = "main",
) -> None:
    """Merge the opportunistic skip/hold signal into a host-write envelope."""
    signal = _run_opportunistic_integrate_for_task(task_ref, integration_ref=integration_ref)
    if not signal or not isinstance(result, dict):
        return
    data = result.get("data")
    if isinstance(data, dict):
        data.update(signal)
    else:
        result.update(signal)
