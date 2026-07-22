"""Lane operations: dispatch, poll, intake, refresh, and cross-lane verification."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

from workbay_protocol import resolve_env_alias

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _env import pythonpath_env
from orchestrator_helpers import _require_dict_payload

# Full 40-char hex SHA only. Short/partial/garbage stdout must never become
# landing evidence (implementation note review H9).
_FULL_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)

# Machine success terminals for the moment-1 vacuous arm (CS-6). NULL/absent
# outcomes are never coerced into this set.
_SUCCESS_WORKER_REPORT_OUTCOMES = frozenset({"finished", "no_actionable_work", "no_work"})

# Refusal reasons returned by lane_dependency_satisfied / dispatch gates.
REASON_UNRESOLVED_UPSTREAM = "unresolved_upstream_dependencies"
REASON_LANDING_RECORD_MISSING = "landing_record_missing"
REASON_LANE_TIP_UNAVAILABLE = "lane_tip_unavailable"
REASON_LANDING_SHA_NOT_ANCESTOR = "landing_sha_not_ancestor"
REASON_DEPENDENCY_CHECK_FAILED = "dependency_check_failed"

# Read once at process start (plan Objective 5). A running daemon must restart
# to pick up a change; callers may re-read via allow_empty_dependency_graph().
_ALLOW_EMPTY_DEPENDENCY_GRAPH = os.environ.get("WORKBAY_ALLOW_EMPTY_DEPENDENCY_GRAPH", "").strip() == "1"

# Observability: refusal counts keyed by reason (plan: every refusal counted).
# Bare daemon import (`orchestrator_lanes`) and packaged API import
# (`workbay_orchestrator_mcp.orchestration.orchestrator_lanes`) are distinct
# module objects; counters must be the same dict or cycle-end reset and
# worker_start_all increment different surfaces. Alias to the twin's dict
# when that identity is already loaded. reset must clear() in place, never rebind.
_REFUSAL_COUNT_MODULE_NAMES = (
    "orchestrator_lanes",
    "workbay_orchestrator_mcp.orchestration.orchestrator_lanes",
)


def _shared_dependency_refusal_counts() -> dict[str, int]:
    for name in _REFUSAL_COUNT_MODULE_NAMES:
        if name == __name__:
            continue
        twin = sys.modules.get(name)
        if twin is None:
            continue
        counts = getattr(twin, "_DEPENDENCY_REFUSAL_COUNTS", None)
        if isinstance(counts, dict):
            return counts
    return {}


_DEPENDENCY_REFUSAL_COUNTS: dict[str, int] = _shared_dependency_refusal_counts()


def allow_empty_dependency_graph() -> bool:
    """True when WORKBAY_ALLOW_EMPTY_DEPENDENCY_GRAPH=1 was set at process start."""
    return _ALLOW_EMPTY_DEPENDENCY_GRAPH


def dependency_refusal_counts() -> dict[str, int]:
    """Snapshot of in-process refusal counts by reason (test / observability)."""
    return dict(_DEPENDENCY_REFUSAL_COUNTS)


def _count_dependency_refusal(reason: str) -> None:
    _DEPENDENCY_REFUSAL_COUNTS[reason] = _DEPENDENCY_REFUSAL_COUNTS.get(reason, 0) + 1


def reset_dependency_refusal_counts() -> None:
    """Test hook: clear the in-process refusal counter (in-place; never rebind)."""
    _DEPENDENCY_REFUSAL_COUNTS.clear()


def log_dependency_refusal_summary(
    log: Any | None = None,
    *,
    reset: bool = False,
    **extra: Any,
) -> dict[str, int]:
    """Emit a summary of non-zero refusal counts when *log* is callable.

    Returns the snapshot that was (or would be) logged. When *reset* is True,
    clears counters after the snapshot (daemon per-cycle hygiene).
    """
    counts = dependency_refusal_counts()
    if counts and any(int(v) > 0 for v in counts.values()) and callable(log):
        log("INFO", "dependency_refusal_summary", counts=counts, **extra)
    if reset:
        reset_dependency_refusal_counts()
    return counts


def parse_collect_unsatisfied_result(
    collected: Any,
) -> tuple[list[str], str | None] | None:
    """Validate ``collect_unsatisfied_dependencies`` return shape.

    Returns ``(blocked_by, reason)`` on a conforming ``(list, str|None)``
    2-tuple; ``None`` when the shape is invalid (caller must fail closed).
    """
    if not isinstance(collected, tuple) or len(collected) != 2:
        return None
    raw_blocked, reason = collected
    if not isinstance(raw_blocked, list):
        return None
    if reason is not None and not isinstance(reason, str):
        return None
    blocked = [b for b in raw_blocked if isinstance(b, str)]
    parsed_reason: str | None = reason if isinstance(reason, str) and reason else None
    return blocked, parsed_reason


# ---------------------------------------------------------------------------
# Dispatch, poll, intake
# ---------------------------------------------------------------------------


def _run_handoff_dispatch(
    orchestrator_root: Path,
    task_ref: str,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run ``review_dispatch.py`` and return its JSON output."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "review_dispatch.py"),
        "--orchestrator-root",
        str(orchestrator_root),
        "--task-ref",
        task_ref,
    ]
    if dry_run:
        cmd.append("--dry-run")
    env = pythonpath_env(orchestrator_root)
    result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"review_dispatch.py failed (exit {result.returncode}):\n{result.stderr.strip()}")
    data = json.loads(result.stdout)
    if not isinstance(data, dict):
        raise TypeError("review_dispatch.py stdout returned non-object JSON payload.")
    return data


def _lane_has_unmerged_commits(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
) -> bool:
    """Return True if the lane branch has commits not yet on the current branch."""
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from lane_manifest import get_lane_config

    config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    if not config or not config.get("branch"):
        return False
    branch = config["branch"]
    result = subprocess.run(
        ["git", "log", "--oneline", f"HEAD..{branch}"],
        cwd=orchestrator_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return bool(result.returncode == 0 and result.stdout.strip())


def _sort_by_manifest_merge_order(ready: list[str], manifest_order: list[str]) -> list[str]:
    """Sort *ready* lanes by the manifest merge order, unknown lanes last."""
    order_map = {lane: i for i, lane in enumerate(manifest_order)}
    return sorted(ready, key=lambda lane: order_map.get(lane, len(manifest_order)))


def _git_stdout(repo: Path, *args: str) -> str | None:
    """Run a git command in *repo*; return stripped stdout, or None on failure."""
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _is_full_commit_sha(value: str | None) -> bool:
    """True when *value* is a full 40-char hex commit SHA."""
    if not value:
        return False
    return _FULL_COMMIT_SHA_RE.fullmatch(value.strip()) is not None


def _git_is_ancestor(repo: Path, commit: str, tip: str) -> bool:
    """Return True when *commit* is an ancestor of *tip* (or equal) in *repo*.

    ``merge-base --is-ancestor`` reports success via exit code with empty
    stdout, so it cannot reuse ``_git_stdout`` (which maps empty stdout to
    None).
    """
    result = subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, tip],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _resolve_lane_branch(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    branch_hint: str | None = None,
) -> str | None:
    """Resolve the lane branch name from *branch_hint* or the lane manifest."""
    if isinstance(branch_hint, str):
        hint = branch_hint.strip()
        if hint and hint != "HEAD":
            return hint
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from lane_manifest import get_lane_config

    config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    if not config:
        return None
    branch = config.get("branch")
    if isinstance(branch, str):
        cleaned = branch.strip()
        if cleaned and cleaned != "HEAD":
            return cleaned
    return None


def _lane_branch_contained_in(
    orchestrator_root: Path,
    candidate_sha: str,
    lane_branch: str,
) -> bool | None:
    """Return whether *lane_branch* is fully contained in *candidate_sha*.

    Uses ``git log --oneline <candidate_sha>..<lane_branch>``:
    - empty stdout → contained (True)
    - non-empty stdout → not contained (False)
    - non-zero exit / unresolvable refs → None (caller must not record)

    This is the dual of ``_lane_has_unmerged_commits`` scoped to an explicit
    candidate tip rather than ``HEAD``. False landings (recipe exit-0 without
    merge, worker tip captured as task tip, reopened lane with new commits)
    are rejected here so consumers never see false ancestry evidence.
    """
    if not candidate_sha or not lane_branch:
        return None
    result = subprocess.run(
        [
            "git",
            "-C",
            str(orchestrator_root),
            "log",
            "--oneline",
            f"{candidate_sha}..{lane_branch}",
            "--",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return not bool(result.stdout.strip())


def _intake_lane(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    dry_run: bool = False,
) -> bool:
    """Run ``make lane-intake`` for a single lane.  Returns True on success."""
    cmd = [
        "make",
        "lane-intake",
        f"TASK={task_ref}",
        f"LANE={lane_id}",
    ]
    if dry_run:
        cmd.append("DRY_RUN=1")
    result = subprocess.run(
        cmd,
        cwd=orchestrator_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _task_branch_landing(
    orchestrator_root: Path,
    *,
    fallback_branch: str = "main",
) -> tuple[str | None, str]:
    """Return (landing SHA, task branch name) for the orchestrator root's HEAD.

    The task branch is the branch checked out at *orchestrator_root* — the same
    reference ``_lane_has_unmerged_commits`` measures lane branches against — so
    its post-intake tip is exactly the SHA at which the lane's work landed.

    Detached HEAD yields the literal string ``\"HEAD\"`` from
    ``rev-parse --abbrev-ref``; treat that as unresolved and fall back to
    *fallback_branch* so ``actor.branch`` is never stamped as ``\"HEAD\"``.

    Non-full SHA stdout (not 40 hex chars) is rejected as unusable evidence.
    """
    sha = _git_stdout(orchestrator_root, "rev-parse", "HEAD")
    if sha is not None and not _is_full_commit_sha(sha):
        sha = None
    branch = _git_stdout(orchestrator_root, "rev-parse", "--abbrev-ref", "HEAD")
    if not branch or branch == "HEAD":
        branch = fallback_branch
    return sha, branch


def record_lane_landing(
    task_ref: str,
    lane_id: str,
    sha: str,
    task_branch: str,
    *,
    log: Any | None = None,
) -> bool:
    """Record ``sha`` as the landing commit for ``lane_id``.

    Returns True when the ledger holds a landing row for this exact SHA
    (freshly inserted or already present), False when the write could not
    be trusted. Callers SHOULD attempt MERGED only after this returns True
    when a SHA is available; unresolved-SHA paths may still transition to
    avoid permanently wedging a lane (see daemon intake failure policy).
    """
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415

    try:
        raw = record_decision(
            # SHA-scoped session. The insert carries
            # ON CONFLICT(task_ref, decision, session) DO NOTHING
            # (decisions.py:275); task_ref and decision are fixed per lane,
            # so the session is the only leg that can vary — each distinct
            # landing SHA therefore inserts a NEW row instead of silently
            # keeping a stale one.
            session=f"lane-intake-{lane_id}-{sha[:12]}",
            decision=f"lane_landed_{task_ref}_{lane_id}",
            rationale=None,
            # The SHA travels via actor.commit_sha — record_decision has no
            # commit_sha kwarg (api.py:1254-1283) and no decision_origin
            # kwarg (origin is stamped by trg_decisions_origin_default).
            # event_id is deliberately OMITTED: a claimed event id returns
            # at decisions.py:232-254, BEFORE _resolve_write_actor (:255),
            # so the SHA would never reach the row while the envelope still
            # reported ok=True.
            actor={
                "commit_sha": sha,
                "branch": task_branch,
                "agent": "orchestrator-daemon",
                "lane_id": lane_id,
            },
            task_ref=task_ref,
        )
        if isinstance(raw, str):
            raw = json.loads(raw)
        payload = _require_dict_payload(
            raw,
            source=f"record_decision(lane_landed:{lane_id})",
        )
    except Exception as exc:  # noqa: BLE001 — never raise out of intake
        # Catch-all (sqlite OperationalError/IntegrityError, OSError, typed
        # actor-validation faults, JSON/type errors, unexpected RuntimeError):
        # propagating would abort the ordered_ready loop and leave the lane
        # without a ledger row or MERGED. Fail closed → False; recovery heals.
        # Named log so operators can see record faults that are not envelope
        # rejections. Never retry without `actor`: the resolver would fall
        # back to the daemon's own cwd HEAD and persist a WRONG landed_sha.
        if callable(log):
            log(
                "ERROR",
                "lane_landing_record_failed",
                lane=lane_id,
                sha=sha,
                error=str(exc),
                error_type=type(exc).__name__,
            )
        return False

    mutation = m if isinstance((m := payload.get("mutation")), dict) else {}
    operation = mutation.get("operation")
    if not payload.get("ok") or operation not in {"insert", "noop"}:
        if callable(log):
            log("ERROR", "lane_landing_record_rejected", lane=lane_id, sha=sha, payload=payload)
        return False
    if operation == "noop":
        # Same (task_ref, decision, session) triple: this exact SHA is already
        # on the ledger (decisions.py:302-309). Idempotent replay, not failure.
        if callable(log):
            log("INFO", "lane_landing_already_recorded", lane=lane_id, sha=sha)
    return True


# ---------------------------------------------------------------------------
# Downstream refresh and cross-lane verification
# ---------------------------------------------------------------------------


def _refresh_downstream(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    downstream: list[str],
    *,
    dry_run: bool = False,
) -> list[tuple[str, bool]]:
    """Refresh each downstream lane.  Returns list of (lane, success) pairs."""
    results: list[tuple[str, bool]] = []
    for dep in downstream:
        cmd = [
            "make",
            "lane-refresh",
            f"TASK={task_ref}",
            f"LANE={dep}",
        ]
        if dry_run:
            cmd.append("DRY_RUN=1")
        r = subprocess.run(
            cmd,
            cwd=orchestrator_root,
            capture_output=True,
            text=True,
            check=False,
        )
        results.append((dep, r.returncode == 0))
    return results


def _resolve_lane_worktree(orchestrator_root: Path, task_ref: str, lane_id: str) -> Optional[Path]:
    """Resolve the worktree path for a lane from the manifest."""
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from lane_manifest import get_lane_config

    config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    if config and config.get("worktree_path"):
        return Path(config["worktree_path"])
    return None


def _lane_has_capacity(task_ref: str, lane_id: str) -> bool:
    """Backpressure probe: True when the lane has no open dispatch, pending action, or open plan cursor.

    internal re-scopes this to pure backpressure / idleness. Dependency
    readiness is decided by ``lane_dependency_satisfied`` over ``depends_on``
    (or legacy merge-order prefix gating when the declared edge set is empty).
    """
    from workbay_orchestrator_mcp.lanes import get_lane_activity, lane_communication, plan_cursor  # noqa: PLC0415

    messages_payload = _require_dict_payload(
        lane_communication(
            kind="message",
            operation="list",
            task_ref=task_ref,
            lane_id=lane_id,
            status="open",
            limit=200,
            fields="direction",
        ),
        source=f"lane_communication(list capacity messages:{lane_id})",
    )
    if messages_payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list lane messages for {lane_id}.")
    for row in messages_payload.get("messages", []):
        if isinstance(row, dict) and row.get("direction") == "orchestrator_to_worker":
            return False

    activity_payload = _require_dict_payload(
        get_lane_activity(
            task_ref=task_ref,
            lane_id=lane_id,
            sections="actions",
            fields="status",
            limit_actions=50,
        ),
        source=f"get_lane_activity(capacity:{lane_id})",
    )
    if activity_payload.get("ok") is not True:
        raise RuntimeError(f"Failed to fetch lane activity for {lane_id}.")
    for row in activity_payload.get("actions", []):
        if isinstance(row, dict) and row.get("status") == "pending":
            return False

    cursor_payload = _require_dict_payload(
        plan_cursor(
            operation="list",
            task_ref=task_ref,
            state="dispatched",
            lane_id=lane_id,
            limit=20,
            fields="plan_item_id",
        ),
        source=f"plan_cursor(list capacity:{lane_id})",
    )
    if cursor_payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list plan cursors for {lane_id}.")
    return not bool(cursor_payload.get("cursors"))


# ---------------------------------------------------------------------------
# internal — completion predicate + depends_on edge source
# ---------------------------------------------------------------------------


def _latest_worker_report_outcome(task_ref: str, lane_id: str) -> str | None:
    """Return the latest worker_report outcome for *lane_id*, or None.

    Report rows survive consumption (intake ACKs, does not delete), so this reads
    the newest row by created_at/id regardless of status. NULL/absent outcomes
    are returned as None and never coerced to ``failed`` (CS-6 / C8).
    """
    from workbay_orchestrator_mcp.lanes import worker_reports  # noqa: PLC0415

    try:
        payload = worker_reports(
            operation="list",
            task_ref=task_ref,
            lane_id=lane_id,
            limit=1,
            fields="outcome",
        )
    except Exception:  # noqa: BLE001 — predicate fails closed without raising
        return None
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            return None
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return None
    reports = payload.get("reports")
    if not isinstance(reports, list) or not reports:
        return None
    report = reports[0]
    if not isinstance(report, dict):
        return None
    outcome = report.get("outcome")
    if outcome is None:
        return None
    text = str(outcome).strip()
    return text or None


def _depends_on_map(manifest: dict[str, Any] | None) -> dict[str, list[str]]:
    """Extract a clean depends_on adjacency from a manifest (or empty)."""
    if not isinstance(manifest, dict):
        return {}
    raw = manifest.get("depends_on")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, list[str]] = {}
    for lane, prereqs in raw.items():
        if not isinstance(lane, str) or not isinstance(prereqs, list):
            continue
        cleaned = [p for p in prereqs if isinstance(p, str) and p.strip()]
        out[lane] = cleaned
    return out


def depends_on_ancestors(depends_on: dict[str, list[str]], lane_id: str) -> list[str]:
    """Return the transitive ``depends_on`` ancestors of *lane_id* (prerequisites).

    DFS reachability over the lane→prereq adjacency ([GRPH-03]/[GRPH-04]).
    Cycle-safe via a visited set (validation rejects cycles, but dispatch must
    not hang if handed a cyclic map anyway). Order is preorder discovery order
    excluding *lane_id* itself (including under cycles / self-loops).
    """
    if not isinstance(depends_on, dict) or not lane_id:
        return []
    ancestors: list[str] = []
    # Pre-seed with lane_id so self-loops and cycles never re-introduce it.
    seen: set[str] = {lane_id}
    stack: list[str] = list(depends_on.get(lane_id, []) or [])
    while stack:
        node = stack.pop()
        if not isinstance(node, str) or not node or node in seen:
            continue
        seen.add(node)
        ancestors.append(node)
        for prereq in depends_on.get(node, []) or []:
            if isinstance(prereq, str) and prereq and prereq not in seen:
                stack.append(prereq)
    return ancestors


def declared_edge_count(metrics: Any) -> int:
    """Safe density-metric edge count from ``manifest_metrics`` (Mock-safe).

    This is the *density* metric (edges beyond merge-order closure). It is NOT
    the activation gate — use :func:`total_depends_on_edge_count` for that.
    """
    if not isinstance(metrics, dict):
        return 0
    count = metrics.get("depends_on_declared_count")
    if isinstance(count, int) and count >= 0:
        return count
    edges = metrics.get("declared_edges")
    if isinstance(edges, (list, tuple, set)):
        return len(edges)
    return 0


def total_depends_on_edge_count(depends_on: dict[str, list[str]] | Any) -> int:
    """Count every declared ``depends_on`` edge, including merge-order-aligned ones.

    Activation gate for depends_on scheduling uses this total (edge_set non-empty),
    not the density metric ``depends_on_declared_count`` which deliberately drops
    edges that mirror merge-order precedence.
    """
    if not isinstance(depends_on, dict):
        return 0
    total = 0
    for prereqs in depends_on.values():
        if not isinstance(prereqs, list):
            continue
        total += sum(1 for p in prereqs if isinstance(p, str) and p.strip())
    return total


def depends_on_scheduling_active(
    *,
    declared_edges: int,
    allow_empty: bool | None = None,
) -> bool:
    """True when dispatch should use depends_on (vs legacy merge-order / health-only).

    ``declared_edges`` here means the *total* depends_on edge count (edge_set size),
    not the density metric ``depends_on_declared_count``.

    - ``declared_edges > 0`` → always active.
    - ``declared_edges == 0`` + ``WORKBAY_ALLOW_EMPTY_DEPENDENCY_GRAPH=1`` →
      active as unconstrained (empty ancestor sets for every lane).
    - else → legacy surfaces (operator: merge-order prefix; daemon: health only).
    """
    if declared_edges > 0:
        return True
    if allow_empty is None:
        allow_empty = allow_empty_dependency_graph()
    return bool(allow_empty)


def load_manifest_scheduling_state(
    task_ref: str,
    *,
    orchestrator_root: Path | str | None,
    lane_manifest_module: Any | None = None,
) -> tuple[dict[str, list[str]], int, bool]:
    """Return ``(depends_on, total_edge_count, scheduling_active)``.

    Scheduling activates when the total declared ``depends_on`` edge set is
    non-empty (or the empty-graph env override is set). The density metric
    ``depends_on_declared_count`` is intentionally *not* used as the gate —
    merge-order-aligned chains and diamonds still have real edges.

    Manifest missing/unparseable degrades to total=0 (legacy), never raises.
    """
    root_str = str(orchestrator_root) if orchestrator_root is not None else None
    depends: dict[str, list[str]] = {}
    total_edges = 0
    try:
        if lane_manifest_module is None:
            if str(SCRIPT_DIR) not in sys.path:
                sys.path.insert(0, str(SCRIPT_DIR))
            from lane_manifest import load_manifest  # noqa: PLC0415
        else:
            load_manifest = getattr(lane_manifest_module, "load_manifest", None)
            if not callable(load_manifest):
                return {}, 0, depends_on_scheduling_active(declared_edges=0)

        manifest = load_manifest(task_ref, orchestrator_root=root_str)
        if not isinstance(manifest, dict):
            return {}, 0, depends_on_scheduling_active(declared_edges=0)
        depends = _depends_on_map(manifest)
        total_edges = total_depends_on_edge_count(depends)
    except (FileNotFoundError, OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError, KeyError):
        depends = {}
        total_edges = 0
    return depends, total_edges, depends_on_scheduling_active(declared_edges=total_edges)


def lane_dependency_satisfied(
    orchestrator_root: Path,
    task_ref: str,
    upstream_id: str,
    dependent_id: str,
    *,
    log: Any | None = None,
    _memo: dict[str, Any] | None = None,
) -> tuple[bool, str | None]:
    """Return whether *upstream_id* has completed enough for *dependent_id* to start.

    Algorithm (git cwd = *orchestrator_root*; internal):

    1. ``r = latest_lane_landing`` for upstream (keyword-only reader).
       Reader raise or ``ok is not True`` → fail closed
       (``dependency_check_failed``); never treat as absent landing.
    2. If a landing record exists: require ``r.commit_sha`` is an ancestor of the
       task-branch tip (moment 1) and, when the dependent worktree exists, of
       ``merge-base(dependent.branch, task_branch)`` (moment 3). Failure reason:
       ``landing_sha_not_ancestor``. Detached HEAD (abbrev-ref == ``HEAD``) is
       unresolved in both moment arms.
    3. Else read the latest worker_report outcome for upstream.
    4. If outcome ∉ {finished, no_actionable_work, no_work}: refuse
       (``unresolved_upstream_dependencies``). NULL is never coerced.
    5–6. Resolve the upstream lane branch tip; missing ref → ``lane_tip_unavailable``.
    7. Tip ancestor of task tip → vacuous discharge (True); else
       ``landing_record_missing`` (success terminal with unmerged work / no record).

    Uses ``_git_is_ancestor`` (``git merge-base --is-ancestor``) rather than
    ``_lane_branch_contained_in``: the latter answers "is every commit on branch
    B contained in SHA S" (landing writer guard); the predicate needs the dual
    "is SHA S an ancestor of tip T" which ``merge-base --is-ancestor`` answers
    directly with exit-code semantics.

    When *_memo* is provided (one collect invocation), predicate results keyed by
    upstream id and git ancestry queries are reused; never retain across calls.
    """
    # Memo key includes the dependent: moment 3 depends on the dependent's
    # worktree/base, so a shared memo must never leak one dependent's verdict
    # to another (collect uses one dependent per call; this guards other callers).
    _memo_key = (upstream_id, dependent_id)
    if _memo is not None:
        pred_cache = _memo.setdefault("predicate", {})
        cached = pred_cache.get(_memo_key)
        if isinstance(cached, tuple) and len(cached) == 2:
            return cached  # type: ignore[return-value]

    def _finish(ok: bool, reason: str | None) -> tuple[bool, str | None]:
        if _memo is not None:
            _memo.setdefault("predicate", {})[_memo_key] = (ok, reason)
        return ok, reason

    def _is_ancestor(commit: str, tip: str) -> bool:
        if _memo is not None:
            git_cache = _memo.setdefault("git_ancestor", {})
            key = (commit, tip)
            if key in git_cache:
                return bool(git_cache[key])
            val = _git_is_ancestor(root, commit, tip)
            git_cache[key] = val
            return val
        return _git_is_ancestor(root, commit, tip)

    root = Path(orchestrator_root)
    # --- step 1: landing record ---
    # (a) ok=True + landing=None → genuine absence, fall through to outcomes.
    # (b) raise OR ok is not True → fail closed (never vacuous discharge).
    landing_sha: str | None = None
    try:
        from workbay_handoff_mcp import latest_lane_landing  # noqa: PLC0415

        env = latest_lane_landing(lane_id=upstream_id, task_ref=task_ref)
        if isinstance(env, str):
            env = json.loads(env)
        if isinstance(env, dict):
            if env.get("ok") is not True:
                if callable(log):
                    log(
                        "ERROR",
                        "lane_landing_reader_failed",
                        upstream_id=upstream_id,
                        task_ref=task_ref,
                        ok=env.get("ok"),
                    )
                return _finish(False, REASON_DEPENDENCY_CHECK_FAILED)
            # Envelope: data.landing; tolerate a flat shape from mocks/tests.
            data = env.get("data") if isinstance(env.get("data"), dict) else env
            landing = data.get("landing") if isinstance(data, dict) else None
            if isinstance(landing, dict):
                raw_sha = landing.get("commit_sha")
                if isinstance(raw_sha, str) and raw_sha.strip():
                    landing_sha = raw_sha.strip()
    except Exception as exc:  # noqa: BLE001 — reader fault → fail closed
        if callable(log):
            log(
                "ERROR",
                "lane_landing_reader_failed",
                upstream_id=upstream_id,
                task_ref=task_ref,
                error=str(exc),
                error_type=type(exc).__name__,
            )
        return _finish(False, REASON_DEPENDENCY_CHECK_FAILED)

    # Task branch + tip resolved once for both moment arms (detached HEAD =
    # literal "HEAD" from --abbrev-ref is unresolved, not a branch name).
    task_branch = _git_stdout(root, "rev-parse", "--abbrev-ref", "HEAD")
    task_tip = _git_stdout(root, "rev-parse", "HEAD")

    if landing_sha:
        # Detached / unresolved task branch: refuse moment 1 and moment 3 alike.
        if not task_branch or task_branch == "HEAD":
            return _finish(False, REASON_LANDING_SHA_NOT_ANCESTOR)
        # Moment 1: landing SHA must be ancestor of the task-branch tip.
        if not task_tip or not _is_ancestor(landing_sha, task_tip):
            return _finish(False, REASON_LANDING_SHA_NOT_ANCESTOR)
        # Moment 3: when dependent worktree exists, landing must be ancestor of base(B).
        worktree = _resolve_lane_worktree(root, task_ref, dependent_id)
        if worktree is not None and worktree.exists():
            dep_branch = _resolve_lane_branch(root, task_ref, dependent_id)
            if not dep_branch:
                return _finish(False, REASON_LANDING_SHA_NOT_ANCESTOR)
            base = _git_stdout(root, "merge-base", dep_branch, task_branch)
            if not base or not _is_ancestor(landing_sha, base):
                return _finish(False, REASON_LANDING_SHA_NOT_ANCESTOR)
        return _finish(True, None)

    # --- steps 3–4: success-terminal outcome required for vacuous arm ---
    outcome = _latest_worker_report_outcome(task_ref, upstream_id)
    if outcome not in _SUCCESS_WORKER_REPORT_OUTCOMES:
        return _finish(False, REASON_UNRESOLVED_UPSTREAM)

    # --- steps 5–6: lane tip ---
    upstream_branch = _resolve_lane_branch(root, task_ref, upstream_id)
    if not upstream_branch:
        return _finish(False, REASON_LANE_TIP_UNAVAILABLE)
    tip = _git_stdout(root, "rev-parse", f"refs/heads/{upstream_branch}")
    if not tip:
        # Bare name fallback (some fixtures use un-namespaced refs).
        tip = _git_stdout(root, "rev-parse", upstream_branch)
    if not tip:
        return _finish(False, REASON_LANE_TIP_UNAVAILABLE)

    # --- step 7: vacuous discharge vs unmerged work ---
    if not task_tip or not _is_ancestor(tip, task_tip):
        return _finish(False, REASON_LANDING_RECORD_MISSING)
    return _finish(True, None)


def collect_unsatisfied_dependencies(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    depends_on: dict[str, list[str]],
    *,
    log: Any | None = None,
) -> tuple[list[str], str | None]:
    """Return ``(blocked_by, reason)`` for transitive unsatisfied ancestors.

    Empty ``blocked_by`` means the lane may dispatch under depends_on scheduling.
    Stops at the first unsatisfied ancestor (dispatch only needs one blocker).
    Predicate and git-ancestry results are memoized for this call only.
    """
    blocked: list[str] = []
    first_reason: str | None = None
    # Per-invocation only — never retained across calls (staleness).
    memo: dict[str, Any] = {"predicate": {}, "git_ancestor": {}}
    for ancestor in depends_on_ancestors(depends_on, lane_id):
        ok, reason = lane_dependency_satisfied(
            orchestrator_root,
            task_ref,
            ancestor,
            lane_id,
            log=log,
            _memo=memo,
        )
        if ok:
            continue
        blocked.append(ancestor)
        reason = reason or REASON_UNRESOLVED_UPSTREAM
        if first_reason is None:
            first_reason = reason
        _count_dependency_refusal(reason)
        if callable(log):
            outcome = _latest_worker_report_outcome(task_ref, ancestor)
            log(
                "INFO",
                "lane_dependency_refused",
                lane_id=lane_id,
                blocked_by=ancestor,
                reason=reason,
                outcome=outcome,
                ancestry="unsatisfied",
            )
        # First blocker short-circuit: remaining ancestors are not evaluated.
        break
    return blocked, first_reason


def _complete_lane_plan_cursor(
    task_ref: str, lane_id: str, *, worker_message_id: Optional[int] = None
) -> Optional[dict[str, Any]]:
    """Mark the newest dispatched plan cursor for a lane complete."""
    from workbay_orchestrator_mcp.lanes import plan_cursor  # noqa: PLC0415

    payload = _require_dict_payload(
        plan_cursor(
            operation="list",
            task_ref=task_ref,
            state="dispatched",
            lane_id=lane_id,
            limit=20,
            fields="plan_item_id,summary,source_heading",
        ),
        source=f"plan_cursor(list complete:{lane_id})",
    )
    if payload.get("ok") is not True:
        raise RuntimeError(f"Failed to list plan cursors for {lane_id}.")
    rows = payload.get("cursors", [])
    if not isinstance(rows, list) or not rows:
        return None
    row = rows[0]
    if not isinstance(row, dict):
        return None
    update = _require_dict_payload(
        plan_cursor(
            operation="upsert",
            task_ref=task_ref,
            plan_item_id=str(row.get("plan_item_id") or ""),
            state="completed",
            lane_id=lane_id,
            worker_message_id=worker_message_id,
            summary=str(row.get("summary") or ""),
            source_heading=str(row.get("source_heading") or "") or None,
        ),
        source=f"plan_cursor(upsert complete:{lane_id})",
    )
    if update.get("ok") is not True:
        raise RuntimeError(f"Failed to complete plan cursor for {lane_id}.")
    cursor = update.get("cursor")
    return cursor if isinstance(cursor, dict) else None


# ---------------------------------------------------------------------------
# fresh_worktree provisioning (redispatch_mode: fresh_worktree)
# ---------------------------------------------------------------------------


# A fresh worktree created outside ``make task-start`` still wants a
# worktree-root ``.venv`` so a bare ``pytest`` resolves locally. The lifecycle
# ``provision-env`` entry point is located via ``WORKBAY_LIFECYCLE_DIR``, else
# ``scripts/workbay_lifecycle`` under the orchestrator repo root.
WORKBAY_LIFECYCLE_DIR_ENV = "WORKBAY_LIFECYCLE_DIR"


def _lifecycle_dir(orchestrator_root: Path) -> Optional[Path]:
    """Resolve the lifecycle scripts dir via the shared discovery rule."""
    override = resolve_env_alias(WORKBAY_LIFECYCLE_DIR_ENV)
    candidate = Path(override) if override else orchestrator_root / "scripts" / "workbay_lifecycle"
    return candidate if candidate.is_dir() else None


def _provision_root_venv(orchestrator_root: Path, worktree: Path) -> dict[str, Any]:
    """Provision the new worktree's root ``.venv`` via ``provision-env``.

    Returns a status dict (``invoked`` / ``absent`` / ``failed``) so callers
    and tests can distinguish "ran provisioning" from "silently did nothing".
    Never raises and never aborts fresh-lane creation.
    """
    lifecycle_dir = _lifecycle_dir(orchestrator_root)
    if lifecycle_dir is None:
        sys.stderr.write(
            "orchestrator: lifecycle provisioning entry point not found "
            f"(set {WORKBAY_LIFECYCLE_DIR_ENV} or add scripts/workbay_lifecycle "
            f"under {orchestrator_root}); run manually before tests: "
            f"python <lifecycle> provision-env --worktree {worktree}\n"
        )
        return {"status": "absent", "worktree": str(worktree)}
    proc = subprocess.run(
        [
            sys.executable,
            str(lifecycle_dir),
            "provision-env",
            "--worktree",
            str(worktree),
            "--json",
        ],
        cwd=str(orchestrator_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(
            f"orchestrator: provision-env failed (exit {proc.returncode}) for "
            f"{worktree}; run manually: python {lifecycle_dir} provision-env "
            f"--worktree {worktree}\n"
        )
        return {
            "status": "failed",
            "worktree": str(worktree),
            "returncode": proc.returncode,
        }
    return {"status": "invoked", "worktree": str(worktree)}


def _provision_fresh_worktree(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    dry_run: bool = False,
) -> Optional[Path]:
    """Create a clean sibling worktree for a lane branched from the orchestrator HEAD.

    Returns the new worktree path, or ``None`` if provisioning failed or was skipped.
    The new worktree is created as a sibling of *orchestrator_root* with a
    timestamped suffix so concurrent lanes never collide.
    """
    import datetime as _dt

    from lane_manifest import get_lane_config

    config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    if not config:
        return None

    # Resolve the base branch (current HEAD of the orchestrator root)
    head_result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=orchestrator_root,
        capture_output=True,
        text=True,
        check=False,
    )
    base_branch = head_result.stdout.strip() or "main"

    timestamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    fresh_branch = f"codex/{task_ref}-{lane_id}-fresh-{timestamp}"
    fresh_wt = orchestrator_root.parent / f"{orchestrator_root.name}-{lane_id}-fresh-{timestamp}"

    if dry_run:
        return fresh_wt

    result = subprocess.run(
        ["git", "worktree", "add", "-b", fresh_branch, str(fresh_wt), base_branch],
        cwd=orchestrator_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None

    # internal: provision the new worktree's root ``.venv`` so lane
    # workers get worktree-local pytest resolution. Best-effort: an absent or
    # failing entry point warns but does not unwind the created worktree.
    _provision_root_venv(orchestrator_root, fresh_wt)

    return fresh_wt
