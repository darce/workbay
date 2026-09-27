"""Resolve a task's plan location to ``(branch, path, exists_on_branch)``.

internal surface. The package owns this logic; bootstrap-hoisted
``Makefile.d/plans.mk`` and ``scripts/workbay/git-plan-cat.sh`` are
wrappers that shell out to ``python3 -m workbay_handoff_mcp.plan_cli`` and
ultimately call into here.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from typing import Literal

from .handoff_state import get_handoff_state, set_handoff_state
from .shared_primitives import LIVE_ACTIVE_STATUSES, _workspace_root
from .shared_schema import _get_db_connection

# Plans declare ownership via a `Task ID: <ref>` line in their frontmatter,
# typically wrapped in markdown emphasis and backticks (e.g.
# ``> - **Task ID**: `internal```). The canonical numbered plan layout
# `docs/plans/NNNN-<slug>.md` does not include the task ref in the
# filename, so discovery must also scan content. The character class
# between "Task ID" and the ref tolerates `*`, `_`, `` ` ``, `'`, `"`,
# `:`, `=`, and whitespace so the same pattern handles plain, bold,
# italic, and code-fenced declarations without matching prose mentions
# like "this plan touches internal in passing".
_TASK_ID_DECLARATION = r"task\s*id[*_`'\"\s]*[:=][*_`'\"\s]*"
# Read at most this many bytes per candidate. Plans declare Task ID in
# frontmatter (within the first few hundred bytes); cap at 8KB so a
# stray multi-megabyte markdown file in docs/ does not stall discovery.
_PLAN_FRONTMATTER_SCAN_BYTES = 8 * 1024

# internal: ``prefer="baseline"`` and ``prefer="auto"`` (when the
# plan exists on main) resolve against this branch. Hardcoded to ``main``
# because every repo in the monorepo treats ``main`` as the integration
# trunk; if we ever support a configurable trunk this constant moves to
# RuntimeConfig.
_BASELINE_BRANCH = "main"

PlanPreference = Literal["baseline", "working_copy", "auto"]

# LIVE_ACTIVE_STATUSES (in_progress/review/blocked) is the canonical "live for
# resolver/renderer purposes" subset of HANDOFF_ACTIVE_STATUSES. Excludes
# `done` because completed tasks have nothing left for the operator to act on
# and must not be promoted into CURRENT_TASK.json by the renderer. internal
# implementation note promoted the symbol from this module into shared_primitives.py so
# all consumers (plan_resolve, current_task_rendering, lifecycle handlers,
# workspace resolver) import the same source of truth.


class PlanPathNotRegistered(Exception):
    """Raised when the active task's ``task_plan_path`` is unset.

    Surfaces as a hard error rather than a silent ``None`` so callers
    (``make plan-show``, ``make plan-edit``, ``plans-list``) can render
    a focused "set task_plan_path via set_handoff_state(...)" hint.
    """


@dataclass(frozen=True)
class PlanLocation:
    """Branch-resolved location of a task plan.

    - ``task_ref``: the task identifier the location belongs to.
    - ``branch``: the task's ``target_branch`` (the branch the plan
      lives on, regardless of which branch the operator is currently
      checked out on). May be ``None`` for active rows that have no
      branch set yet.
    - ``path``: the repo-relative ``task_plan_path`` stored on the
      handoff row, or ``None`` for active rows that have not had a
      plan path registered (only surfaces from
      ``list_active_task_locations``; ``resolve_plan_location`` raises
      ``PlanPathNotRegistered`` instead).
    - ``exists_on_branch``: True iff ``git show <branch>:<path>``
      succeeds in the workspace repo. False when the branch is not
      fetched locally, the file is not committed on it, or either of
      ``branch``/``path`` is unset.
    """

    task_ref: str
    branch: str | None
    path: str | None
    exists_on_branch: bool


def plan_show_command(branch: str, path: str) -> list[str]:
    """Return the argv list that prints the plan via ``git show``.

    Locked here so implementation note's ``make plan-show`` recipe and any future
    consumer wrappers share one definition of "how the plan is read".
    """
    return ["git", "show", f"{branch}:{path}"]


def _branch_has_path(branch: str, path: str) -> bool:
    """Probe ``git show <branch>:<path>`` in the workspace repo.

    Returns False on any non-zero exit (branch missing, path missing,
    not a git repo, ``git`` not on PATH). The probe is intentionally
    silent — callers want a boolean, not a stack trace.
    """
    try:
        result = subprocess.run(
            plan_show_command(branch=branch, path=path),
            cwd=str(_workspace_root()),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except (FileNotFoundError, OSError):
        return False
    return result.returncode == 0


def resolve_plan_location(
    task_ref: str | None = None,
    prefer: PlanPreference = "auto",
) -> PlanLocation:
    """Resolve the active task's plan to ``(branch, path, exists_on_branch)``.

    Pure read: calls ``get_handoff_state`` and one ``git show`` probe;
    performs no MCP write.

    ``prefer`` selects which snapshot the location anchors to (internal
    implementation note):

    - ``"baseline"``: always anchor on ``main`` so coordinators read the
      accepted plan locally. ``exists_on_branch`` is False when the plan
      has not been accepted yet (i.e. not committed on ``main``).
    - ``"working_copy"``: always anchor on the task's ``target_branch``
      so implementers can read the in-flight draft.
    - ``"auto"`` (default): probe ``main`` first; return the baseline
      view when the plan is present there, else fall back to the
      working-copy view. This is the read path used by ``status`` and
      the dashboard so the same call returns the steady-state view
      post-acceptance without surprising pre-acceptance callers.

    Raises ``PlanPathNotRegistered`` when the task has no
    ``task_plan_path`` set.
    """
    envelope = get_handoff_state(task_ref=task_ref, sections="identity")
    data = envelope.get("data") if isinstance(envelope, dict) else None
    active = data.get("active") if isinstance(data, dict) else None
    if not isinstance(active, dict):
        raise PlanPathNotRegistered(f"No active handoff state found for task_ref={task_ref!r}.")

    branch = active.get("target_branch")
    path = active.get("task_plan_path")
    if not isinstance(path, str) or not path.strip():
        raise PlanPathNotRegistered(
            f"task_plan_path is unset for task_ref={active.get('task_ref')!r}. "
            "Set it via set_handoff_state(task_plan_path='docs/plans/...')."
        )
    if not isinstance(branch, str) or not branch.strip():
        raise PlanPathNotRegistered(
            f"target_branch is unset for task_ref={active.get('task_ref')!r}; "
            "cannot resolve plan location without a branch."
        )

    if prefer == "baseline":
        resolved_branch = _BASELINE_BRANCH
    elif prefer == "working_copy":
        resolved_branch = branch
    elif prefer == "auto":
        resolved_branch = _BASELINE_BRANCH if _branch_has_path(branch=_BASELINE_BRANCH, path=path) else branch
    else:
        raise ValueError(
            f"resolve_plan_location: prefer must be 'baseline', 'working_copy', or 'auto'; got {prefer!r}."
        )

    return PlanLocation(
        task_ref=str(active.get("task_ref")),
        branch=resolved_branch,
        path=path,
        exists_on_branch=_branch_has_path(branch=resolved_branch, path=path),
    )


def resolve_plan_baseline_status(task_ref: str, plan_path: str | None) -> tuple[str, str | None]:
    """Return ``(status, reason)`` for the plan-baseline view on ``main``.

    internal: a local sibling of the ``workbay-system``
    ``evaluate_plan_baseline`` evaluator. The dashboard renderer cannot
    import lifecycle handlers (cross-package boundary), so this helper
    answers the narrower question the dashboard surface needs: is the
    registered plan committed on ``main`` (``accepted``), known-absent
    (``missing``), or unverifiable (``unknown``)?

    ``status`` values:

    - ``accepted``: ``git cat-file -e main:<plan_path>`` succeeded.
    - ``missing``: probe succeeded with a non-zero exit (path absent).
    - ``unknown``: plan_path unset, or the probe itself failed (no git
      on PATH, workspace not a repo). Callers render this as a neutral
      "cannot verify" rather than a definite gap.
    """
    path = plan_path.strip() if isinstance(plan_path, str) else ""
    if not path:
        return ("unknown", "task_plan_path_unset")
    try:
        if _branch_has_path(branch=_BASELINE_BRANCH, path=path):
            return ("accepted", None)
        return ("missing", "plan_baseline_missing")
    except Exception:
        return ("unknown", "baseline_probe_failed")


class PlanRegistrationError(Exception):
    """Raised when ``register_plan_path`` cannot resolve a unique plan.

    Carries the candidate matches (for the multi-match case) so the
    caller can render an actionable disambiguation hint instead of a
    bare error.
    """

    def __init__(self, message: str, candidates: list[str] | None = None) -> None:
        super().__init__(message)
        self.candidates: list[str] = candidates or []


def discover_plan_path_candidates(task_ref: str) -> list[str]:
    """Find plans under ``docs/`` that belong to ``task_ref``.

    Two complementary signals (BR-internal):

    1. Filename substring — matches the legacy ``docs/**/*<task-id>*.md``
       layout case-insensitively.
    2. Frontmatter ``Task ID:`` declaration — matches the canonical
       numbered plan layout ``docs/plans/NNNN-<slug>.md`` whose filename
       does not embed the task ref. Prose mentions in the body are not
       enough; only the ``Task ID: <ref>`` declaration counts so that
       cross-references in revision history do not generate false-
       positive multi-match collisions.

    Returns repo-relative paths sorted asc for deterministic output;
    duplicates (filename + frontmatter both match) collapse to one row.
    Empty list when no match.
    """
    root = _workspace_root()
    docs = root / "docs"
    if not docs.is_dir():
        return []
    needle = task_ref.lower()
    declaration = re.compile(_TASK_ID_DECLARATION + re.escape(task_ref), re.IGNORECASE)
    matches: set[str] = set()
    for candidate in docs.rglob("*.md"):
        rel = str(candidate.relative_to(root))
        if needle in candidate.name.lower():
            matches.add(rel)
            continue
        try:
            with candidate.open("r", encoding="utf-8", errors="ignore") as fh:
                head = fh.read(_PLAN_FRONTMATTER_SCAN_BYTES)
        except OSError:
            continue
        if declaration.search(head):
            matches.add(rel)
    return sorted(matches)


def register_plan_path(task_ref: str, plan_path: str) -> dict:
    """Persist ``task_plan_path`` on the active row for ``task_ref``.

    Fetches the current revision via ``get_handoff_state(sections=
    'identity')`` and forwards it as ``expected_revision`` so the
    optimistic-concurrency guard is respected. Raises
    ``PlanRegistrationError`` when the active row does not exist; the
    CLI shells these errors back as exit-code 2 with the message text.
    """
    envelope = get_handoff_state(task_ref=task_ref, sections="identity")
    data = envelope.get("data") if isinstance(envelope, dict) else None
    active = data.get("active") if isinstance(data, dict) else None
    if not isinstance(active, dict):
        raise PlanRegistrationError(
            f"No active handoff state found for task_ref={task_ref!r}. "
            "Create one via set_handoff_state(...) before registering a plan path."
        )
    revision = active.get("revision")
    if not isinstance(revision, int):
        raise PlanRegistrationError(
            f"Active row for task_ref={task_ref!r} has no integer revision; "
            "refusing to write without a concurrency guard."
        )
    result = set_handoff_state(
        task_ref=task_ref,
        task_plan_path=plan_path,
        expected_revision=revision,
    )
    if not result.get("ok"):
        err = result.get("data", {}).get("error") if isinstance(result.get("data"), dict) else result
        raise PlanRegistrationError(f"set_handoff_state rejected the update: {err}")
    return result


def list_active_task_locations(include_unset_path: bool = True) -> list[PlanLocation]:
    """Enumerate every active (non-archived) task as a ``PlanLocation``.

    Ordered by ``updated_at`` descending (most recently touched first),
    with ``task_ref`` ascending as the deterministic tiebreaker — same
    ordering as ``list_active_tasks``. Archived tasks are excluded by
    construction; the live ``handoff_state`` table holds active rows
    only. ``done``-status rows are also filtered out: they live in
    ``handoff_state`` until archival runs but have nothing for the
    operator to act on, so ``plans-list`` (implementation note) keeps them off the
    list. The plan-list active set is ``in_progress``, ``review``,
    ``blocked``.

    - ``include_unset_path=True`` (default): every active row is
      returned, with ``path=None`` for rows that have no
      ``task_plan_path`` set. ``exists_on_branch`` is ``False`` for
      those rows. This is the shape ``plans-list`` (implementation note) renders.
    - ``include_unset_path=False``: filter to rows with a
      ``task_plan_path`` set — useful for callers that only care about
      resolvable plans (e.g. a "show me every plan I could open" view).

    Empty active table returns ``[]``, never ``None``.
    """
    locations: list[PlanLocation] = []
    with _get_db_connection() as conn:
        for raw in conn.execute(
            "SELECT task_ref, target_branch, task_plan_path, status "
            "FROM handoff_state "
            "WHERE status IN (" + ",".join(["?"] * len(LIVE_ACTIVE_STATUSES)) + ") "
            "ORDER BY updated_at DESC, task_ref ASC",
            LIVE_ACTIVE_STATUSES,
        ).fetchall():
            path_raw = raw["task_plan_path"]
            path = path_raw.strip() if isinstance(path_raw, str) and path_raw.strip() else None
            if path is None and not include_unset_path:
                continue
            branch_raw = raw["target_branch"]
            branch = branch_raw if isinstance(branch_raw, str) and branch_raw.strip() else None
            exists = bool(branch and path and _branch_has_path(branch=branch, path=path))
            locations.append(
                PlanLocation(
                    task_ref=str(raw["task_ref"]),
                    branch=branch,
                    path=path,
                    exists_on_branch=exists,
                )
            )
    return locations
