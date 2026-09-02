"""Lane operations: dispatch, poll, intake, refresh, and cross-lane verification."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from workbay_protocol import resolve_env_alias

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _env import pythonpath_env, resolve_lane_python
from orchestrator_helpers import _require_dict_payload

# Full 40-char hex SHA only. Short/partial/garbage stdout must never become
# landing evidence (implementation note review H9).
_FULL_COMMIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$", re.IGNORECASE)

logger = logging.getLogger(__name__)

# Prospective landing topology is deliberately opt-in.  This constant and the
# strict ``is True`` loader below make malformed/missing policy fail toward the
# established patch-landing path (*Release It!* safe-direction fallback).
DEFAULT_NO_FF_LANDING = False

# Named outcomes of the optional no-ff topology step. The helper still
# returns bool (and raises on ownership refusal); the caller must name the
# three cases so a declined attempt is not collapsed into a refused landing.
NO_FF_TOPOLOGY_LANDED = "landed"
NO_FF_TOPOLOGY_DECLINED = "declined"
NO_FF_TOPOLOGY_REFUSED = "refused"


class NoFfLandingOwnershipError(Exception):
    """Prospective no-ff merge would import a tree delta intake refused."""

    def __init__(self, lane_id: str, offending_paths: list[str] | tuple[str, ...]):
        self.lane_id = lane_id
        self.offending_paths = tuple(offending_paths)
        paths = ", ".join(self.offending_paths) or "(none)"
        super().__init__(
            f"no-ff landing refused for lane {lane_id}: "
            f"prospective merge tree imports paths intake excluded: {paths}"
        )


def _safe_log(log: Any | None, level: str, event: str, **fields: Any) -> None:
    """Call a reclaim/landing log sink without letting it escape [AGT-10][OBS-06].

    Single owner for the no-throw log contract shared by reclaim scan/nudge
    and the landing/candidate writers (PLAN0181-S2GATE-LOGRAISE-01)
    [REF-19][REF-13]. Swallows only exceptions raised by the sink itself;
    a healthy sink is invoked exactly as written.
    """
    if not callable(log):
        return
    try:
        log(level, event, **fields)
    except Exception:  # noqa: BLE001 — raising log must not escape
        pass


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
    # PLAN0181-S2GATE-LOGRAISE-RESIDUAL-01 [AGT-10][RES-13][OBS-06]:
    # try/finally (not reset-before-log): return must still be the pre-reset
    # snapshot that was/would-have-been logged, while reset remains
    # unconditional w.r.t. sink health so a dead sink cannot leave cycle
    # counters accumulating monotonically.
    try:
        if counts and any(int(v) > 0 for v in counts.values()):
            _safe_log(log, "INFO", "dependency_refusal_summary", counts=counts, **extra)
    finally:
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
        resolve_lane_python(orchestrator_root),
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
) -> bool | None:
    """Tri-state: True = unmerged commits, False = none, None = unknown.

    Missing lane/branch config or a non-zero git exit returns None so each
    consumer can choose its safe direction (REF-37). The noop/cursor-complete
    path must treat None like work remaining (skip completion). The merge-ready
    poll must treat None as not-ready (exclude the lane from a real merge).
    Do not collapse uncertainty into a bool: fail-closed at the predicate is
    fail-open at the merge-ready consumer when both share a single True.
    """
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    from lane_manifest import get_lane_config

    config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    if not config or not config.get("branch"):
        return None
    branch = config["branch"]
    result = subprocess.run(
        ["git", "log", "--oneline", f"HEAD..{branch}"],
        cwd=orchestrator_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return bool(result.stdout.strip())


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


_CONTAINMENT_GIT_TIMEOUT_SECONDS = 30


def _git_is_ancestor(repo: Path, commit: str, tip: str) -> bool | str:
    """Return True when *commit* is an ancestor of *tip* (or equal) in *repo*.

    ``merge-base --is-ancestor`` reports success via exit code with empty
    stdout, so it cannot reuse ``_git_stdout`` (which maps empty stdout to
    None).
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, tip],
            capture_output=True,
            text=True,
            check=False,
            timeout=_CONTAINMENT_GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return "timeout"
    except OSError:
        return False
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


def _rev_list_count(repo: Path, rev_range: str) -> int | str | None:
    """Return ``git rev-list --count`` for *rev_range*, or None on failure.

    ``rev-list --count`` always prints a decimal (including ``0``) on success,
    so empty stdout is failure, not zero. Prefer this over ``git log``: a
    path-limited log is known to lie in both directions in this repo.
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), "rev-list", "--count", rev_range, "--"],
            capture_output=True,
            text=True,
            check=False,
            timeout=_CONTAINMENT_GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return "timeout"
    except OSError:
        return None
    if result.returncode != 0:
        return None
    raw = result.stdout.strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _lane_branch_contained_in(
    orchestrator_root: Path,
    candidate_sha: str,
    lane_branch: str,
    *,
    base_sha: str | None = None,
    task_ref: str | None = None,
    lane_id: str | None = None,
    own_commits_evidence: bool = False,
    allow_candidate_tip: bool = False,
    target_tip_before_landing: str | None = None,
    data_loss_safety: bool = False,
) -> bool | str | None:
    """Return whether *lane_branch* is fully contained in *candidate_sha*.

    Uses ``git rev-list --count <candidate_sha>..<lane_branch>``:
    - count > 0 → not contained (False)
    - count == 0 and the lane branch has own commits vs its recorded base
      → contained (True)
    - count == 0 and the lane branch has no own commits → ``"empty"``
      (terminal non-landing disposition)
    - count == 0 and the base cannot be resolved → None (absence of
      evidence; caller must not record)
    - non-zero exit / unresolvable refs → None

    ``data_loss_safety`` asks the narrower reclaim question: whether any commit
    on the lane branch remains outside the candidate.  For that question an
    empty candidate-to-lane range is conclusive even when the lane never
    authored a commit; reclaim has nothing on the branch to lose.

    An empty range is not evidence of landing. The dominant lane shape here
    is a branch that never received a commit; ``candidate..branch`` is then
    empty for the same reason a never-started cut is empty. True is reserved
    for the case where the branch had commits that could have appeared in
    the range and none of them remain outside *candidate_sha*.

    Own-commit evidence is ``rev-list --count <base>..<lane_branch>`` against
    *base_sha* or the lane's recorded manifest ``base_sha``. Missing base
    fails closed to None. This is scoped to an explicit candidate tip rather
    than ``HEAD``; ``_lane_has_unmerged_commits`` answers the different
    question "are there commits outside HEAD?" and empty is a valid False
    there, not a landing claim.
    """
    if not candidate_sha or not lane_branch:
        return None
    ahead = _rev_list_count(orchestrator_root, f"{candidate_sha}..{lane_branch}")
    if ahead == "timeout":
        return "timeout"
    if ahead is None:
        return None
    if ahead > 0:
        try:
            cherry = subprocess.run(
                ["git", "-C", str(orchestrator_root), "cherry", candidate_sha, lane_branch],
                capture_output=True,
                text=True,
                check=False,
                timeout=_CONTAINMENT_GIT_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired:
            return "timeout"
        except OSError:
            return None
        if cherry.returncode != 0:
            return None
        lines = [line.strip() for line in cherry.stdout.splitlines() if line.strip()]
        return bool(lines) and all(line.startswith("-") for line in lines)

    if data_loss_safety:
        return True

    # A subject can fast-forward its branch to the task tip without having
    # authored anything. In that shape base..branch counts inherited task
    # commits, so it is not valid own-commit evidence. Fail closed; a genuine
    # no-ff landing keeps the lane tip distinct from the candidate merge tip.
    branch_tip = _rev_parse_sha(orchestrator_root, lane_branch)
    if branch_tip == "timeout":
        return "timeout"
    candidate_tip = _rev_parse_sha(orchestrator_root, candidate_sha)
    if candidate_tip == "timeout":
        return "timeout"
    if branch_tip is None or candidate_tip is None:
        return None
    if allow_candidate_tip:
        if not target_tip_before_landing:
            return None
        authored = _rev_list_count(
            orchestrator_root,
            f"{target_tip_before_landing}..{lane_branch}",
        )
        if authored == "timeout":
            return "timeout"
        if authored is None:
            return None
        return True if authored > 0 else "empty"

    resolved_base: str | None = None
    if isinstance(base_sha, str) and _is_full_commit_sha(base_sha):
        resolved_base = base_sha.strip().lower()
    elif task_ref and lane_id:
        resolved_base = _lane_manifest_base_sha(orchestrator_root, task_ref, lane_id)
    if resolved_base is None:
        return None

    own = _rev_list_count(orchestrator_root, f"{resolved_base}..{lane_branch}")
    if own == "timeout":
        return "timeout"
    if own is None:
        return None
    if own == 0:
        return "empty"
    if own_commits_evidence:
        return True if branch_tip != candidate_tip else None
    if branch_tip == candidate_tip:
        return None
    return True


def _rev_parse_sha(orchestrator_root: Path, ref: str) -> str | None:
    """Resolve *ref* to a full commit SHA, or None when git cannot.

    Always peels to a commit (``^{commit}``) so annotated tags yield the
    pointed-to commit rather than the tag object id. Lineage membership and
    committer-epoch lookup must see the same object; an unpeeled tag object
    can never appear in a first-parent listing and would falsely fall through
    to the timestamp arm.
    """
    if not ref:
        return None
    # Peel annotated tags (and any other non-commit tip) to a commit before
    # shape-checking. A bare rev-parse of an annotated tag returns the tag
    # object id — 40 hex that passes _is_full_commit_sha but is not a commit.
    peeled_ref = f"{ref}^{{commit}}"
    try:
        result = subprocess.run(
            ["git", "-C", str(orchestrator_root), "rev-parse", peeled_ref],
            capture_output=True,
            text=True,
            check=False,
            timeout=_CONTAINMENT_GIT_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return "timeout"
    except OSError:
        return None
    if result.returncode != 0:
        return None
    sha = result.stdout.strip()
    return sha if _is_full_commit_sha(sha) else None


def _lane_manifest_base_sha(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
) -> str | None:
    """Return the provisioned per-lane ``base_sha`` when the manifest has one."""
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    try:
        from lane_manifest import get_lane_config
    except ImportError:
        return None
    try:
        config = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    except Exception:  # noqa: BLE001 — missing/corrupt manifest is absence, not a crash
        return None
    if not isinstance(config, dict):
        return None
    raw = config.get("base_sha")
    if isinstance(raw, str) and _is_full_commit_sha(raw):
        return raw.strip().lower()
    return None


def _lane_branch_on_first_parent_lineage(
    orchestrator_root: Path,
    integration_tip: str,
    branch_tip: str,
) -> bool | None:
    """True when *branch_tip* sits on *integration_tip*'s first-parent history.

    A no-ff merge leaves the lane tip as the merge's second parent — off this
    lineage. A never-started cut (and a fast-forward landing) sits on it.

    Containment is already established before this runs, so *branch_tip* is an
    ancestor of *integration_tip*. The exclusive range ``branch_tip..tip`` has
    size N; if the branch tip is on the first-parent chain at all, it lies at
    most N steps back. Two git calls (count + capped first-parent listing)
    answer membership without walking repository history.
    """
    tip = integration_tip.strip().lower()
    commit = branch_tip.strip().lower()
    if not tip or not commit:
        return None
    if tip == commit:
        return True

    count_out = _git_stdout(
        orchestrator_root, "rev-list", "--count", f"{commit}..{tip}"
    )
    if count_out is None:
        return None
    try:
        exclusive = int(count_out)
    except ValueError:
        return None
    if exclusive < 0:
        return None

    listing = _git_stdout(
        orchestrator_root,
        "rev-list",
        "--first-parent",
        f"-n{exclusive + 1}",
        tip,
    )
    if listing is None:
        return None
    lineage = {line.strip().lower() for line in listing.splitlines() if line.strip()}
    return commit in lineage


def _commit_committer_epoch(orchestrator_root: Path, commit_sha: str) -> int | None:
    """Return the committer unix timestamp for *commit_sha*, or None."""
    raw = _git_stdout(orchestrator_root, "log", "-1", "--format=%ct", commit_sha)
    if raw is None:
        return None
    try:
        return int(raw.strip())
    except ValueError:
        return None


def _coerce_full_commit_sha(value: object | None) -> str | None:
    """Return a lowercase 40-hex SHA, or None when *value* is not one."""
    if not isinstance(value, str):
        return None
    stripped = value.strip().lower()
    return stripped if _is_full_commit_sha(stripped) else None


def _lookup_recorded_landing_sha(*, task_ref: str, lane_id: str) -> str | None:
    """Read a recorded landing SHA from the lane row or landing ledger.

    Consults the worktree lane row first (``landing_commit_sha``), then the
    newest ``latest_lane_landing`` record. Lookup faults are absence — this
    helper never writes, and the caller decides what a missing SHA means.
    """
    try:
        from workbay_handoff_mcp.lanes_api import get_lane  # noqa: PLC0415

        env: Any = get_lane(lane_id=lane_id, task_ref=task_ref)
        if isinstance(env, str):
            env = json.loads(env)
        if isinstance(env, dict) and env.get("ok") is True:
            data = env.get("data") if isinstance(env.get("data"), dict) else env
            lane = data.get("lane") if isinstance(data, dict) else None
            row = lane if isinstance(lane, dict) else data
            if isinstance(row, dict):
                recorded = _coerce_full_commit_sha(row.get("landing_commit_sha"))
                if recorded is not None:
                    return recorded
    except Exception:  # noqa: BLE001 — missing runtime/row is absence, not a crash
        pass

    try:
        from workbay_handoff_mcp import latest_lane_landing  # noqa: PLC0415

        env = latest_lane_landing(lane_id=lane_id, task_ref=task_ref)
        if isinstance(env, str):
            env = json.loads(env)
        if not isinstance(env, dict) or env.get("ok") is not True:
            return None
        data = env.get("data") if isinstance(env.get("data"), dict) else env
        landing = data.get("landing") if isinstance(data, dict) else None
        if isinstance(landing, dict):
            return _coerce_full_commit_sha(landing.get("commit_sha"))
    except Exception:  # noqa: BLE001 — missing ledger is absence, not a crash
        return None
    return None


# Distinct tagged results for an absent branch. Callers must branch on these
# tokens; they are never the refuse bit (True) and never the produced-work
# bit (False). Unrecoverable is terminal: a deleted branch does not return.
ABSENT_LANE_BRANCH_WORK_LANDED = "work_landed"
ABSENT_LANE_BRANCH_UNRECOVERABLE = "unrecoverable"
REASON_ABSENT_BRANCH_EVIDENCE_NEVER_CAPTURED = (
    "empty_lane_absent_branch_evidence_never_captured"
)


def _lookup_integration_receipt_landed_revision(
    orchestrator_root: Path,
    *,
    task_ref: str,
    lane_id: str,
) -> str | None:
    """Return the landed revision from a receipt that names this lane.

    The receipt is evidence written at landing. Lookup faults are absence —
    this helper never writes, and a receipt for a different lane is not proof.
    """
    try:
        from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
            list_integration_receipts,
        )
    except Exception:  # noqa: BLE001 — missing reader is absence, not a crash
        return None
    try:
        receipts = list_integration_receipts(orchestrator_root)
    except Exception:  # noqa: BLE001 — missing store is absence, not a crash
        return None
    if not isinstance(receipts, list):
        return None
    for receipt in receipts:
        if not isinstance(receipt, dict):
            continue
        if str(receipt.get("lane_id") or "") != str(lane_id):
            continue
        if str(receipt.get("task_ref") or "") != str(task_ref):
            continue
        landed = _coerce_full_commit_sha(receipt.get("integration_rev"))
        if landed is not None:
            return landed
    return None


def _absent_lane_branch_never_produced_answer(
    orchestrator_root: Path,
    integration_tip: str,
    *,
    task_ref: str,
    lane_id: str,
    landing_sha: object | None = None,
) -> tuple[bool | None | str, str]:
    """Terminal empty-lane answer when the lane branch ref no longer resolves.

    A missing ref is not a transient probe failure: a deleted branch will not
    come back on the next reap. Return ``ABSENT_LANE_BRANCH_WORK_LANDED`` when
    a recorded landing SHA *or* a receipt naming this lane has a landed
    revision contained in the integration tip. Only when neither form of
    evidence exists return the tagged terminal
    ``ABSENT_LANE_BRANCH_UNRECOVERABLE`` with a reason that the evidence was
    never captured. A recorded SHA that is not yet contained stays the
    refuse bit so a later tip can still catch up. Never writes the lane
    row. An unresolvable tip still stays undecidable when recorded
    evidence exists, because containment cannot be scored without the
    tip. A containment timeout stays undecidable. The refuse bit means
    only refuse.
    """
    if landing_sha is not None:
        recorded = _coerce_full_commit_sha(landing_sha)
    else:
        recorded = _lookup_recorded_landing_sha(task_ref=task_ref, lane_id=lane_id)
    receipt_sha = _lookup_integration_receipt_landed_revision(
        orchestrator_root, task_ref=task_ref, lane_id=lane_id
    )
    evidence: list[str] = []
    for sha in (recorded, receipt_sha):
        if sha is not None and sha not in evidence:
            evidence.append(sha)

    tip_sha = integration_tip.strip().lower() if isinstance(integration_tip, str) else ""
    if not _is_full_commit_sha(tip_sha):
        tip_resolved = _rev_parse_sha(orchestrator_root, integration_tip)
        if tip_resolved == "timeout" or tip_resolved is None:
            if not evidence:
                return (
                    ABSENT_LANE_BRANCH_UNRECOVERABLE,
                    REASON_ABSENT_BRANCH_EVIDENCE_NEVER_CAPTURED,
                )
            return None, "empty_lane_integration_tip_unresolvable"
        tip_sha = tip_resolved.lower()

    if not evidence:
        return (
            ABSENT_LANE_BRANCH_UNRECOVERABLE,
            REASON_ABSENT_BRANCH_EVIDENCE_NEVER_CAPTURED,
        )

    saw_timeout = False
    for sha in evidence:
        contained = _git_is_ancestor(orchestrator_root, sha, tip_sha)
        if contained is True:
            return ABSENT_LANE_BRANCH_WORK_LANDED, "empty_lane_absent_branch_work_landed"
        if contained == "timeout":
            saw_timeout = True
    if saw_timeout:
        return None, "empty_lane_integration_tip_unresolvable"
    if recorded is not None:
        # Evidence was captured; the tip may still move to contain it.
        return True, "empty_lane_absent_branch_unrecoverable"
    return (
        ABSENT_LANE_BRANCH_UNRECOVERABLE,
        REASON_ABSENT_BRANCH_EVIDENCE_NEVER_CAPTURED,
    )


def _lane_created_at_epoch(created_at: object) -> int | None:
    """Normalise a lane-row ``created_at`` stamp to a unix epoch seconds value.

    SQLite ``datetime('now')`` strings are treated as UTC when timezone-naive,
    matching git ``%ct`` (UTC-based unix time) so the two can be compared in
    one reference frame. Missing or unparseable values yield None — callers
    must not guess.
    """
    if not isinstance(created_at, str) or not created_at.strip():
        return None
    raw = created_at.strip().replace("T", " ")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw[:19] if len(raw) >= 19 else raw, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp())


def _lane_branch_never_produced_commits(
    orchestrator_root: Path,
    integration_tip: str,
    lane_branch: str,
    *,
    task_ref: str,
    lane_id: str,
    lane_created_at: object | None = None,
    landing_sha: object | None = None,
) -> tuple[bool | None | str, str]:
    """Detect a lane cut that never diverged from the integration line.

    Containment alone cannot separate a real landing from a branch that was
    cut from the tip and never received a commit — both yield an empty
    ``tip..branch`` range. The manifest ``base_sha`` arm is refusal-only:
    tip equals base refuses; tip differs does not authorise a close — lineage
    (and tip-vs-created) still run. First-parent reachability refuses empty
    cuts that sit on the integration line. Off that line, a tip whose
    committer date predates the lane row cannot have been authored by the
    lane, so the close is refused; when either stamp is missing the result
    is undecidable (leave the lane open) rather than "produced work".

    An absent lane-branch ref is terminal, not a retry. When a recorded
    landing SHA or a receipt naming this lane has a landed revision
    contained in the integration tip the answer is the tagged result
    ``(ABSENT_LANE_BRANCH_WORK_LANDED, empty_lane_absent_branch_work_landed)``;
    when neither form of evidence exists it is
    ``(ABSENT_LANE_BRANCH_UNRECOVERABLE, empty_lane_absent_branch_evidence_never_captured)``.
    A resolvable branch whose integration tip or lineage probe fails stays
    undecidable. This function never writes or deletes a lane row.

    Returns ``(True, reason)`` to refuse close, ``(False, "")`` when the
    branch produced work, ``(None, reason)`` when the check cannot decide,
    ``(ABSENT_LANE_BRANCH_WORK_LANDED, reason)`` when an absent branch
    already has landing evidence in the integration tip, and
    ``(ABSENT_LANE_BRANCH_UNRECOVERABLE, reason)`` when the branch is gone
    and that evidence was never captured. Callers must branch on those
    tags; the refuse bit is never True for the landed or unrecoverable
    cases.
    """
    branch_tip = _rev_parse_sha(orchestrator_root, lane_branch)
    if branch_tip == "timeout":
        return None, "empty_lane_branch_tip_unresolvable"
    if branch_tip is None:
        return _absent_lane_branch_never_produced_answer(
            orchestrator_root,
            integration_tip,
            task_ref=task_ref,
            lane_id=lane_id,
            landing_sha=landing_sha,
        )
    tip_sha = integration_tip.strip().lower()
    if not _is_full_commit_sha(tip_sha):
        tip_resolved = _rev_parse_sha(orchestrator_root, integration_tip)
        if tip_resolved is None:
            return None, "empty_lane_integration_tip_unresolvable"
        tip_sha = tip_resolved.lower()
    branch_tip = branch_tip.lower()

    base_sha = _lane_manifest_base_sha(orchestrator_root, task_ref, lane_id)
    if base_sha is not None and branch_tip == base_sha:
        return True, "empty_lane_branch_equals_manifest_base_sha"
    # Manifest inequality is not proof of work — fall through.

    on_lineage = _lane_branch_on_first_parent_lineage(orchestrator_root, tip_sha, branch_tip)
    if on_lineage is None:
        return None, "empty_lane_first_parent_lineage_unresolvable"
    if on_lineage:
        return True, "empty_lane_branch_on_first_parent_lineage"

    # Off the first-parent chain: empty cut from a merged feature tip, or a
    # real no-ff landing. Topology cannot settle this; use the lane row.
    tip_epoch = _commit_committer_epoch(orchestrator_root, branch_tip)
    created_epoch = _lane_created_at_epoch(lane_created_at)
    if tip_epoch is None:
        return None, "empty_lane_branch_tip_committer_unresolvable"
    if created_epoch is None:
        return None, "empty_lane_created_at_unresolvable"
    if tip_epoch < created_epoch:
        return True, "empty_lane_branch_tip_predates_lane_created_at"
    return False, ""


def no_ff_landing_enabled(orchestrator_root: Path, task_ref: str) -> bool:
    """Read the manifest landing policy; absence or any error means OFF."""
    try:
        if str(SCRIPT_DIR) not in sys.path:
            sys.path.insert(0, str(SCRIPT_DIR))
        from lane_manifest import load_manifest  # noqa: PLC0415

        manifest = load_manifest(task_ref, orchestrator_root=str(orchestrator_root))
        policy = manifest.get("landing_policy")
        return isinstance(policy, dict) and policy.get("no_ff_merge") is True
    except Exception as exc:  # noqa: BLE001 -- policy errors preserve current behavior
        logger.warning(
            "lane_no_ff_policy_fallback task=%s error=%s",
            task_ref,
            exc,
        )
        return DEFAULT_NO_FF_LANDING


def _no_ff_prospective_tree(repo: Path, ours: str, theirs: str) -> str | None:
    """Return the clean merge-tree OID, or None if conflicted/unavailable."""
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), "merge-tree", "--write-tree", ours, theirs],
            capture_output=True,
            text=True,
            check=False,
            timeout=_CONTAINMENT_GIT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    lines = result.stdout.strip().splitlines()
    sha = lines[0].strip() if lines else ""
    return sha or None


def _no_ff_tree_delta(repo: Path, intake_tree: str, prospective_tree: str) -> list[str]:
    """Paths that differ between the post-intake tree and a prospective merge tree.

    Compare trees, not owned-path names. An empty list means byte-identical
    trees. Unresolvable operands fail closed with a sentinel path so a
    comparison outage cannot look like identity.
    """
    if not intake_tree or not prospective_tree:
        return ["<unresolved-tree>"]
    if intake_tree == prospective_tree:
        return []
    raw = _git_stdout(repo, "diff", "--name-only", intake_tree, prospective_tree)
    if raw is None:
        return ["<unresolved-tree-delta>"]
    paths = [line for line in raw.splitlines() if line.strip()]
    if not paths:
        return ["<tree-identity-mismatch>"]
    return paths


def _refuse_no_ff_ownership(
    log: Any | None,
    *,
    lane_id: str,
    branch: str,
    offending_paths: list[str],
    intake_tree: str,
    prospective_tree: str,
) -> None:
    _safe_log(
        log,
        "WARNING",
        "lane_no_ff_ownership_refused",
        lane=lane_id,
        branch=branch,
        offending_paths=offending_paths,
        intake_tree=intake_tree,
        prospective_tree=prospective_tree,
    )
    logger.warning(
        "lane_no_ff_ownership_refused lane=%s paths=%s",
        lane_id,
        offending_paths,
    )


def _merge_lane_no_ff(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    log: Any | None = None,
) -> bool:
    """Manufacture lane ancestry, aborting cleanly on every merge failure.

    The caller has already completed the established intake path, so ``False``
    means that product remains landed with the old topology.  Conflict paths
    are captured before abort for an explicit observable fallback event.

    A clean prospective merge whose tree is not byte-identical to the
    post-intake tree is refused: that delta is exactly the set of paths
    intake excluded. Compare trees, not owned-path names.
    """
    lane_branch = _resolve_lane_branch(orchestrator_root, task_ref, lane_id)
    if not lane_branch:
        _safe_log(
            log,
            "WARNING",
            "lane_no_ff_merge_fallback",
            lane=lane_id,
            reason="branch_unresolved",
            conflicting_paths=[],
        )
        logger.warning("lane_no_ff_merge_fallback lane=%s reason=branch_unresolved conflicting_paths=[]", lane_id)
        return False

    tip_before = _git_stdout(orchestrator_root, "rev-parse", "HEAD")
    tree_before = _git_stdout(orchestrator_root, "rev-parse", "HEAD^{tree}")
    if not tip_before or not tree_before:
        _safe_log(
            log,
            "WARNING",
            "lane_no_ff_merge_fallback",
            lane=lane_id,
            reason="integration_tip_unresolved",
            conflicting_paths=[],
        )
        return False

    prospective = _no_ff_prospective_tree(orchestrator_root, "HEAD", lane_branch)
    if prospective is not None:
        offending = _no_ff_tree_delta(orchestrator_root, tree_before, prospective)
        if offending:
            _refuse_no_ff_ownership(
                log,
                lane_id=lane_id,
                branch=lane_branch,
                offending_paths=offending,
                intake_tree=tree_before,
                prospective_tree=prospective,
            )
            raise NoFfLandingOwnershipError(lane_id, offending)

    try:
        merge = subprocess.run(
            ["git", "-C", str(orchestrator_root), "merge", "--no-ff", "--no-edit", lane_branch],
            capture_output=True,
            text=True,
            check=False,
            timeout=_CONTAINMENT_GIT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        merge = None
        merge_error = str(exc)
    else:
        merge_error = merge.stderr.strip()[-1000:]

    if merge is not None and merge.returncode == 0:
        tree_after = _git_stdout(orchestrator_root, "rev-parse", "HEAD^{tree}") or ""
        drifted = _no_ff_tree_delta(orchestrator_root, tree_before, tree_after)
        if drifted:
            reset = subprocess.run(
                ["git", "-C", str(orchestrator_root), "reset", "--hard", tip_before],
                capture_output=True,
                text=True,
                check=False,
                timeout=_CONTAINMENT_GIT_TIMEOUT_SECONDS,
            )
            if reset.returncode != 0:
                raise RuntimeError(
                    f"git reset --hard failed for lane {lane_id}: {reset.stderr.strip() or 'no stderr'}"
                )
            restored_tip = _git_stdout(orchestrator_root, "rev-parse", "HEAD")
            restored_tree = _git_stdout(orchestrator_root, "rev-parse", "HEAD^{tree}")
            if restored_tip != tip_before or restored_tree != tree_before:
                raise RuntimeError(
                    f"git merge fallback did not restore integration tip/tree for lane {lane_id}"
                )
            _refuse_no_ff_ownership(
                log,
                lane_id=lane_id,
                branch=lane_branch,
                offending_paths=drifted,
                intake_tree=tree_before,
                prospective_tree=tree_after,
            )
            raise NoFfLandingOwnershipError(lane_id, drifted)
        _safe_log(log, "INFO", "lane_no_ff_merge_landed", lane=lane_id, branch=lane_branch)
        return True

    conflicts = _git_stdout(orchestrator_root, "diff", "--name-only", "--diff-filter=U") or ""
    conflicting_paths = [line for line in conflicts.splitlines() if line.strip()]
    merge_head = subprocess.run(
        ["git", "-C", str(orchestrator_root), "rev-parse", "-q", "--verify", "MERGE_HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if merge_head.returncode == 0:
        abort = subprocess.run(
            ["git", "-C", str(orchestrator_root), "merge", "--abort"],
            capture_output=True,
            text=True,
            check=False,
        )
        if abort.returncode != 0:
            raise RuntimeError(
                f"git merge --abort failed for lane {lane_id}: {abort.stderr.strip() or 'no stderr'}"
            )
    tip_after = _git_stdout(orchestrator_root, "rev-parse", "HEAD")
    tree_after = _git_stdout(orchestrator_root, "rev-parse", "HEAD^{tree}")
    if tip_after != tip_before or tree_after != tree_before:
        raise RuntimeError(
            f"git merge fallback did not restore integration tip/tree for lane {lane_id}"
        )
    fields = {
        "lane": lane_id,
        "branch": lane_branch,
        "reason": "merge_conflict" if conflicting_paths else "merge_failed",
        "conflicting_paths": conflicting_paths,
        "error": merge_error,
        "tip_before": tip_before,
        "tree_before": tree_before,
        "restored_tip": tip_after,
        "restored_tree": tree_after,
    }
    _safe_log(log, "WARNING", "lane_no_ff_merge_fallback", **fields)
    logger.warning("lane_no_ff_merge_fallback %s", json.dumps(fields, sort_keys=True))
    return False


def _intake_lane(
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    *,
    dry_run: bool = False,
    log: Any | None = None,
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
    if result.returncode != 0 or dry_run:
        return result.returncode == 0
    if no_ff_landing_enabled(orchestrator_root, task_ref):
        try:
            produced = _merge_lane_no_ff(orchestrator_root, task_ref, lane_id, log=log)
        except NoFfLandingOwnershipError:
            topology_outcome = NO_FF_TOPOLOGY_REFUSED
        else:
            topology_outcome = (
                NO_FF_TOPOLOGY_LANDED if produced else NO_FF_TOPOLOGY_DECLINED
            )
        if topology_outcome == NO_FF_TOPOLOGY_REFUSED:
            return False
        # LANDED and DECLINED both leave the product on the integration
        # branch. DECLINED is the helper's documented False: topology
        # failed, product remains landed with the established intake path.
    return True


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
        # Defensive log: a broken log callable must not convert a handled
        # ledger failure into an escaping exception [RLSE-05]
        # (PLAN0181-S2LOGRAISETWIN-01) — via shared _safe_log [REF-19].
        tb_tail = ""
        try:
            import traceback  # noqa: PLC0415

            tb_tail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-2000:]
        except Exception:  # noqa: BLE001 — traceback is diagnostic only
            pass
        _safe_log(
            log,
            "ERROR",
            "lane_landing_record_failed",
            lane=lane_id,
            sha=sha,
            error=str(exc),
            error_type=type(exc).__name__,
            traceback_tail=tb_tail,
        )
        return False

    mutation = m if isinstance((m := payload.get("mutation")), dict) else {}
    operation = mutation.get("operation")
    # Match record_reclaim_candidate / record_decision's three-valued operation
    # [AGT-10] (PLAN0181-S2ALLOWDRIFT-01): insert, update, and true noop.
    if not payload.get("ok") or operation not in {"insert", "update", "noop"}:
        # PLAN0181-S2GATE-LOGRAISE-01: sink faults must not escape [AGT-10].
        _safe_log(
            log,
            "ERROR",
            "lane_landing_record_rejected",
            lane=lane_id,
            sha=sha,
            payload=payload,
        )
        return False
    if operation == "noop":
        # Same (task_ref, decision, session) triple: this exact SHA is already
        # on the ledger (decisions.py:302-309). Idempotent replay, not failure.
        # PLAN0181-S2GATE-LOGRAISE-01: INFO sink must not escape either.
        _safe_log(log, "INFO", "lane_landing_already_recorded", lane=lane_id, sha=sha)
    return True


def _canonical_for_digest(
    obj: Any,
    *,
    _depth: int = 0,
    _stack: list[Any] | None = None,
) -> Any:
    """Type-aware, cycle-safe form for observed_digest only [DATA-03][MODEL-03].

    Tags non-str keys and distinguishes list from tuple so JSON key/value
    normalisation cannot collapse distinct maps
    (PLAN0181-S2GATE2-JSON-KEYNORM-COLLIDE-01). Cycles are path-indexed via
    identity (``is``) without calling ``id()`` or ``hash()``
    (PLAN0181-S2GATE2-DIGEST-UNSERIALISABLE-COLLIDE-01). Bounded depth keeps
    the payload finite.
    """
    if _stack is None:
        _stack = []
    if _depth > 64:
        return {"__t": "maxdepth"}
    for i, seen in enumerate(_stack):
        if obj is seen:
            return {"__t": "cycle", "i": i}

    if obj is None or isinstance(obj, bool):
        return obj
    if isinstance(obj, int) and not isinstance(obj, bool):
        return obj
    if isinstance(obj, float):
        # NaN/Inf are not JSON numbers; tag them so dumps never fails open.
        if obj != obj or obj in (float("inf"), float("-inf")):
            return {"__t": "float", "s": str(obj)}
        return obj
    if isinstance(obj, str):
        return obj
    if isinstance(obj, bytes):
        return {"__t": "bytes", "hex": obj.hex()}
    if isinstance(obj, bytearray):
        return {"__t": "bytearray", "hex": bytes(obj).hex()}
    if isinstance(obj, tuple):
        _stack = _stack + [obj]
        return {
            "__t": "tuple",
            "v": [_canonical_for_digest(x, _depth=_depth + 1, _stack=_stack) for x in obj],
        }
    if isinstance(obj, list):
        _stack = _stack + [obj]
        return {
            "__t": "list",
            "v": [_canonical_for_digest(x, _depth=_depth + 1, _stack=_stack) for x in obj],
        }
    if isinstance(obj, dict):
        _stack = _stack + [obj]

        def _key_sort(k: Any) -> tuple[str, str]:
            return (type(k).__name__, repr(k))

        items: list[Any] = []
        for k in sorted(obj.keys(), key=_key_sort):
            key_tag = {
                "__t": type(k).__name__,
                "k": k if isinstance(k, (str, int, float, bool)) or k is None else repr(k)[:200],
            }
            items.append(
                [
                    key_tag,
                    _canonical_for_digest(obj[k], _depth=_depth + 1, _stack=_stack),
                ]
            )
        return {"__t": "dict", "items": items}
    # Non-JSON types: stable type name + bounded str, no process-local ids.
    try:
        text = str(obj)
    except Exception:  # noqa: BLE001 — str() must not escape digest
        text = "<unprintable>"
    return {"__t": type(obj).__name__, "s": text[:500]}


def _digest_hex(canonical_text: str, *, mode: str = "") -> str:
    """Fixed-width digest; optional mode prefix separates failure families."""
    digest = hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()
    if mode:
        # Distinct failure modes must not share a single sentinel
        # (PLAN0181-S2GATE2-ENCODE-OUTSIDE-GUARD-01 composed with
        # PLAN0181-S2GATE2-DIGEST-UNSERIALISABLE-COLLIDE-01).
        return f"{mode}:{digest[:12]}"
    return digest[:16]


def _observed_map_digest(observed: dict[str, Any]) -> str:
    """Stable fixed-width digest of a full observed map [DATA-03][MODEL-03].

    Built from a type-aware canonical form so list/tuple and non-str keys stay
    distinct (PLAN0181-S2GATE2-JSON-KEYNORM-COLLIDE-01). Serialisation and
    UTF-8 encode stay inside one guard; failures fall back to a structural
    digest that still separates shape and keys rather than one constant
    (PLAN0181-S2GATE2-DIGEST-UNSERIALISABLE-COLLIDE-01 /
    PLAN0181-S2GATE2-ENCODE-OUTSIDE-GUARD-01). Survives the key-dropping loop
    because callers compute it once against the pre-drop map
    (PLAN0181-S2GATE-OBSERVED-COLLIDE-01).
    """
    try:
        tagged = _canonical_for_digest(observed)
        # ensure_ascii=True keeps the dumps result ASCII so .encode("utf-8")
        # cannot raise on lone surrogates from os.fsdecode surrogateescape
        # (PLAN0181-S2GATE2-ENCODE-OUTSIDE-GUARD-01).
        canonical = json.dumps(
            tagged,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        return _digest_hex(canonical)
    except (TypeError, ValueError, UnicodeEncodeError):
        # Structural fallback: sorted key names + type names, bounded.
        try:
            shape_parts: list[str] = []
            for k in sorted(observed.keys(), key=lambda x: (type(x).__name__, repr(x))):
                try:
                    v = observed[k]
                    vty = type(v).__name__
                except Exception:  # noqa: BLE001
                    vty = "?"
                shape_parts.append(f"{type(k).__name__}:{k!r}->{vty}")
            shape = "{" + ",".join(shape_parts) + "}"
            return _digest_hex(shape, mode="s")
        except Exception:  # noqa: BLE001 — absolute floor, still distinct
            return _digest_hex(f"keys={len(observed)}", mode="s")


def _reclaim_candidate_rationale(verdict: Any) -> str:
    """Build a size-bounded reclaim rationale as a total JSON envelope.

    Format (PLAN0181-S2TRUNCSHAPE-01 / S2TRUNCKEY-01 / S2RATNL-01 /
    PLAN0181-S2GATE-RECLAIMABLE-LOST-01 / PLAN0181-S2GATE-TRUNCATED-NAME-01 /
    PLAN0181-S2GATE-OBSERVED-COLLIDE-01 / PLAN0181-S2GATE2-*):
      <reason-prefix>\\n
      {
        "reason": <str>,
        "observed": <dict>,
        "truncated": <int>,           # observed keys dropped only
        "reclaimable": <bool>,
        "reason_derived": <bool>,     # synthetic default vs operator reason
        "observed_digest": <str>,     # digest of full pre-drop observed map
        "prefix_dropped": <bool>,     # reason-prefix omitted to fit budget
        "reason_truncated": <bool>,   # reason string itself was sliced
        "coerced": <bool>,            # default=str fired on a non-JSON value
        "reason_stripped": <bool>,    # operator reason lost edge whitespace
        "evaluated_at": <str>         # UTC evaluation stamp (not in digest)
      }

    Every outcome carries the same structured key set — complete, truncated,
    empty-observed, serialisation-failure, and budget-exhausted alike. The
    reason is a field in that object (not only a positionally-parsed line) so a
    newline inside it cannot be confused with the delimiter, and so decoding is
    total. Observed keys nest under ``observed`` so they cannot collide with
    envelope markers. ``truncated`` is present and zero on complete outcomes
    (absent-means-zero is the fail-open reading this class closes) and means
    **only** the count of observed keys dropped — never other loss modes
    [MODEL-03][API-09].

    Additive fields (``reclaimable``, ``reason_derived``, ``observed_digest``,
    ``prefix_dropped``, ``reason_truncated``, ``coerced``, ``reason_stripped``,
    ``evaluated_at``) keep the encoding injective over the verdict while
    remaining optional for older readers that only know
    {reason, observed, truncated} [DATA-03][API-09].

    A single-token reason prefix is retained ahead of the envelope so existing
    first-line / substring operator reads and suite premises keep working; the
    structured body is authoritative. Serialisation failure degrades to a valid
    envelope with an empty observed map (never raises). The whole string stays
    within the decisions hard limit so the write is not rejected for size.
    """
    reason = getattr(verdict, "reason", None)
    reclaimable = bool(getattr(verdict, "reclaimable", False))
    # Reason must survive the round trip for operator diagnostics [OBS-06].
    # Reclaimable (no reason) still needs a non-empty rationale so the row is
    # readable; the landing twin uses rationale=None because its evidence is
    # the SHA in actor — here the evidence *is* the reason string.
    # reason_derived marks synthetic defaults so an operator-supplied reason of
    # exactly "reclaimable"/"not_reclaimable" is distinguishable [DATA-03]
    # (PLAN0181-S2GATE-RECLAIMABLE-LOST-01).
    # reason_stripped names edge-whitespace loss from strip()
    # (PLAN0181-S2GATE2-REASON-STRIP-UNFLAGGED-01).
    reason_stripped = False
    if isinstance(reason, str) and reason.strip():
        first = reason.strip()
        reason_stripped = first != reason
        reason_derived = False
    elif reclaimable:
        first = "reclaimable"
        reason_derived = True
    else:
        first = "not_reclaimable"
        reason_derived = True

    # Operator-facing first line: single-token reasons stay one line so
    # ``splitlines()[0]`` premises and greps keep working. Multi-line reasons
    # are still recovered verbatim from the envelope's ``reason`` field.
    prefix = first.split("\n", 1)[0].strip() or first

    observed = getattr(verdict, "observed", None)
    if not isinstance(observed, dict):
        observed = {}

    # Digest the FULL observed map once, before any key dropping, so two maps
    # that share a kept prefix remain distinguishable in the ledger
    # [DATA-03] (PLAN0181-S2GATE-OBSERVED-COLLIDE-01). Digest covers observed
    # only — evaluated_at must not enter it.
    observed_digest = _observed_map_digest(observed)

    # Evaluation stamp: frozen created_at cannot express freshness; this is
    # additive and independent of observed_digest.
    evaluated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    try:
        from workbay_handoff_mcp.shared_primitives import (  # noqa: PLC0415
            RATIONALE_HARD_LIMIT_CHARS,
        )
    except Exception:  # noqa: BLE001 — degrade to a known-safe ceiling
        RATIONALE_HARD_LIMIT_CHARS = 3000

    # Track default=str coercion so a lossy dump is never presented as
    # full-fidelity (PLAN0181-S2GATE2-DEFAULTSTR-LOSSY-UNFLAGGED-01).
    coerced = False

    def _default_str(obj: Any) -> str:
        nonlocal coerced
        coerced = True
        return str(obj)

    def _dumps(payload: dict[str, Any]) -> str | None:
        try:
            return json.dumps(
                payload,
                ensure_ascii=False,
                default=_default_str,
                separators=(",", ":"),
            )
        except (TypeError, ValueError, UnicodeEncodeError):
            return None

    def _envelope_payload(
        obs: dict[str, Any],
        dropped: int,
        *,
        prefix_dropped: bool = False,
        reason_truncated: bool = False,
        reason_text: str | None = None,
    ) -> dict[str, Any]:
        return {
            "reason": first if reason_text is None else reason_text,
            "observed": obs,
            "truncated": dropped,
            "reclaimable": reclaimable,
            "reason_derived": reason_derived,
            "observed_digest": observed_digest,
            "prefix_dropped": prefix_dropped,
            "reason_truncated": reason_truncated,
            "coerced": coerced,
            "reason_stripped": reason_stripped,
            "evaluated_at": evaluated_at,
        }

    def _envelope(
        obs: dict[str, Any],
        dropped: int,
        *,
        prefix_dropped: bool = False,
        reason_truncated: bool = False,
        reason_text: str | None = None,
    ) -> str | None:
        # Single total envelope. Nesting makes marker/data collision
        # structurally impossible [CON-05] (PLAN0181-S2TRUNCKEY-01);
        # always-present keys close three-shape asymmetry
        # (PLAN0181-S2TRUNCSHAPE-01) and reclaimable/reason_derived keep the
        # encoding injective [DATA-03][MODEL-03]
        # (PLAN0181-S2GATE-RECLAIMABLE-LOST-01).
        # ``truncated`` = observed keys dropped only
        # (PLAN0181-S2GATE-TRUNCATED-NAME-01); other losses use named flags.
        # First dump may flip coerced; re-dump once so the flag is accurate
        # inside the same payload (PLAN0181-S2GATE2-DEFAULTSTR-LOSSY-UNFLAGGED-01).
        before = coerced
        blob = _dumps(
            _envelope_payload(
                obs,
                dropped,
                prefix_dropped=prefix_dropped,
                reason_truncated=reason_truncated,
                reason_text=reason_text,
            )
        )
        if blob is None:
            return None
        if coerced and not before:
            blob = _dumps(
                _envelope_payload(
                    obs,
                    dropped,
                    prefix_dropped=prefix_dropped,
                    reason_truncated=reason_truncated,
                    reason_text=reason_text,
                )
            )
        return blob

    def _render(obs: dict[str, Any], dropped: int) -> str | None:
        blob = _envelope(obs, dropped, prefix_dropped=False)
        if blob is None:
            return None
        # Prefer reason-prefix + envelope when it fits; fall back to envelope
        # alone if the prefix would blow the hard limit, and name that loss
        # [MODEL-03][DATA-03] (PLAN0181-S2GATE-TRUNCATED-NAME-01).
        with_prefix = f"{prefix}\n{blob}"
        if len(with_prefix) <= RATIONALE_HARD_LIMIT_CHARS:
            return with_prefix
        bare = _envelope(obs, dropped, prefix_dropped=True)
        if bare is not None and len(bare) <= RATIONALE_HARD_LIMIT_CHARS:
            return bare
        return None

    original_count = len(observed)
    items = list(observed.items())

    # Complete path: drop count is present and zero, not absent [OBS-06].
    rendered = _render(dict(items), 0)
    if rendered is not None:
        return rendered

    # Serialisation of the full observed map failed, or the complete envelope
    # exceeded the hard limit. Drop keys from the end until the envelope fits.
    # Silent amputation is forbidden [CON-05][OBS-06][RL-04] (S2RATBUDGET-01):
    # a partial map without a marker is indistinguishable from a full one.
    # observed_digest stays the pre-drop digest (unchanged by this loop).
    while items:
        items.pop()
        dropped = original_count - len(items)
        rendered = _render(dict(items), dropped)
        if rendered is not None:
            return rendered

    # Nothing of the original map fits (or serialisation only succeeds empty).
    # Still report how many keys were dropped; degrade rather than bare-string.
    rendered = _render({}, original_count)
    if rendered is not None:
        return rendered

    # Absolute last resort: reason may itself push past the limit, but the
    # writer still needs a structured, self-describing payload rather than a
    # bare string (PLAN0181-S2TRUNCSHAPE-01). Never return an oversize string
    # (PLAN0181-S2GATE-RATIONALEOVERSIZE-01) [DATA-03][CON-05]: hard-slice the
    # reason until json.dumps fits, and mark reason_truncated so readers know
    # the reason itself was cut (``truncated`` counts only dropped observed keys).
    # reclaimable and the other injective fields stay on this path too
    # (PLAN0181-S2GATE-RECLAIMABLE-LOST-01).
    # Shrink is measured in source-character units that match the slice
    # (PLAN0181-S2GATE2-NONASCII-OVERTRUNCATE-01). Floor is always a valid
    # JSON envelope, never a raw character slice
    # (PLAN0181-S2GATE2-LASTRESORT-NONJSON-01).
    cut_reason = str(first)
    reason_was_cut = False

    def _last_resort_payload(
        reason_text: str,
        *,
        prefix_dropped: bool,
        reason_truncated: bool,
    ) -> dict[str, Any]:
        return {
            "reason": reason_text,
            "observed": {},
            "truncated": original_count,
            "reclaimable": reclaimable,
            "reason_derived": reason_derived,
            "observed_digest": observed_digest,
            "prefix_dropped": prefix_dropped,
            "reason_truncated": reason_truncated,
            "coerced": coerced,
            "reason_stripped": reason_stripped,
            "evaluated_at": evaluated_at,
        }

    def _last_resort_dumps(payload: dict[str, Any]) -> str:
        try:
            return json.dumps(
                payload,
                ensure_ascii=True,
                separators=(",", ":"),
                default=_default_str,
            )
        except (TypeError, ValueError, UnicodeEncodeError):
            # Drop non-JSON values from reason only; keep envelope total.
            return json.dumps(
                _last_resort_payload(
                    "",
                    prefix_dropped=bool(payload.get("prefix_dropped")),
                    reason_truncated=True,
                ),
                ensure_ascii=True,
                separators=(",", ":"),
            )

    def _minimal_envelope() -> str:
        return json.dumps(
            _last_resort_payload(
                "",
                prefix_dropped=True,
                reason_truncated=True,
            ),
            ensure_ascii=True,
            separators=(",", ":"),
        )

    while True:
        payload = _last_resort_payload(
            cut_reason,
            prefix_dropped=False,
            reason_truncated=reason_was_cut,
        )
        fallback = _last_resort_dumps(payload)
        with_prefix = f"{prefix}\n{fallback}"
        if len(with_prefix) <= RATIONALE_HARD_LIMIT_CHARS:
            return with_prefix
        # Prefix does not fit: rebuild with prefix_dropped named
        # (PLAN0181-S2GATE-TRUNCATED-NAME-01).
        payload["prefix_dropped"] = True
        bare_fallback = _last_resort_dumps(payload)
        if len(bare_fallback) <= RATIONALE_HARD_LIMIT_CHARS:
            return bare_fallback
        if len(cut_reason) == 0:
            # Empty-reason envelope still oversize — emit the minimal valid
            # JSON floor or raise (programming error: limit below envelope).
            # Never return a raw character slice that fails json.loads
            # (PLAN0181-S2GATE2-LASTRESORT-NONJSON-01).
            minimal = _minimal_envelope()
            if len(minimal) <= RATIONALE_HARD_LIMIT_CHARS:
                return minimal
            raise RuntimeError(
                "RATIONALE_HARD_LIMIT_CHARS="
                f"{RATIONALE_HARD_LIMIT_CHARS} cannot fit the minimal "
                f"reclaim rationale envelope ({len(minimal)} chars); "
                "raising rather than storing a non-decodable slice "
                "(PLAN0181-S2GATE2-LASTRESORT-NONJSON-01)"
            )
        # Binary-search the longest prefix of cut_reason whose encoded
        # envelope fits. Shrink is measured in the same source-character
        # units it is applied in (PLAN0181-S2GATE2-NONASCII-OVERTRUNCATE-01).
        lo, hi = 0, len(cut_reason)
        best = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            candidate = cut_reason[:mid]
            trial = _last_resort_dumps(
                _last_resort_payload(
                    candidate,
                    prefix_dropped=True,
                    reason_truncated=True,
                )
            )
            if len(trial) <= RATIONALE_HARD_LIMIT_CHARS:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1
        if best >= len(cut_reason):
            # Should not happen (bare_fallback was oversize); force progress.
            cut_reason = cut_reason[: max(0, len(cut_reason) - 1)]
        else:
            cut_reason = cut_reason[:best]
        reason_was_cut = True


def record_reclaim_candidate(
    *,
    task_ref: str,
    lane_id: str,
    verdict: Any,
    log: Any | None = None,
    _branch_verdict: bool = False,
) -> bool:
    """Record a reclaim-candidate verdict for ``lane_id``.

    Writes the decision id from :func:`reclaim_candidate_decision_id` (shared
    with the handoff reader; components are stripped then percent-encoded so
    distinct ``(task_ref, lane_id)`` pairs cannot collide).

    ``task_ref`` and ``lane_id`` are keyword-only so the two opaque strings
    cannot be swapped at a call site (PLAN0181-S2NUDGEARG-01) [CON-05][AGT-10].

    Persistence contract (PLAN0181-S2OBS/S2GROW/S2ORIGIN/S2CREATEDAT):
    - ``verdict.observed`` is folded into ``rationale`` as a total JSON envelope
      with ``reason`` / ``observed`` / ``truncated`` (observed-key drop count)
      plus additive injective fields ``reclaimable``, ``reason_derived``,
      ``observed_digest``, ``prefix_dropped``, ``reason_truncated``,
      ``coerced``, ``reason_stripped``, ``evaluated_at``
      (PLAN0181-S2TRUNCSHAPE-01 / PLAN0181-S2GATE-RECLAIMABLE-LOST-01 /
      PLAN0181-S2GATE2-*).
    - Session is stable per lane so the ledger does not grow unbounded; the
      write refreshes rationale on conflict (``created_at`` is preserved) so
      a later verdict still wins under newest-wins readers.
    - Rows are stamped ``decision_origin='system'`` so ``gc_system_decisions``
      can reap them (the origin trigger has no ``lane_reclaim_candidate_*`` arm).

    Returns True when the ledger holds the row (insert, update, or true noop),
    False when the write cannot be trusted. Never raises — mirrors
    ``record_lane_landing``.
    """
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415

    # Match latest_reclaim_candidate's strip normalisation so the writer and
    # reader compose the same decision id (PLAN0181-S2WSTRIP-01) [CON-05][OBS-12].
    # Refusals must log: callers ignore the False return (PLAN0181-S2RECFALSE-01)
    # so a silent exit leaves no operator signal [OBS-06] (PLAN0181-S2WSTRIPQUIET-01).
    # Guard log: never-raise contract covers ID refusal paths too
    # (PLAN0181-S2LOGRAISETWIN-01) [AGT-10].
    if not isinstance(task_ref, str) or not isinstance(lane_id, str):
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_candidate_id_invalid",
            task_ref=task_ref,
            lane=lane_id,
            reason="non_string",
        )
        return False
    task_ref = task_ref.strip()
    lane_id = lane_id.strip()
    if not task_ref or not lane_id:
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_candidate_id_invalid",
            task_ref=task_ref,
            lane=lane_id,
            reason="empty_after_strip",
        )
        return False

    rationale = _reclaim_candidate_rationale(verdict)
    # Function-local import (PLAN0181-S2GATE2-MODULE-LEVEL-XPKG-IMPORT-01): a
    # skewed handoff install must not fail the whole orchestrator_lanes import
    # surface at load time. Same pattern as the other handoff imports here.
    from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
        branch_reclaim_candidate_decision_id,
        reclaim_candidate_decision_id,
    )

    # Single shared constructor (PLAN0181-S2IDFMT-01); no dual-read fallback —
    # a second format would reintroduce the ambiguity this encoding closes.
    decision_id_builder = (
        branch_reclaim_candidate_decision_id if _branch_verdict else reclaim_candidate_decision_id
    )
    decision_id = decision_id_builder(task_ref=task_ref, lane_id=lane_id)
    # Stable session per lane: ON CONFLICT(task_ref, decision, session) with
    # refresh_rationale_on_conflict collapses re-evaluations to one row
    # (S2GROW-01) while keeping the latest verdict under newest-wins readers
    # (S2CREATEDAT-01: rationale only, never created_at).
    session = f"lane-{'branch-' if _branch_verdict else ''}reclaim-{lane_id}"

    try:
        raw = record_decision(
            session=session,
            decision=decision_id,
            rationale=rationale,
            actor={
                "agent": "orchestrator-daemon",
                "lane_id": lane_id,
            },
            task_ref=task_ref,
            decision_origin="system",
            refresh_rationale_on_conflict=True,
        )
        if isinstance(raw, str):
            raw = json.loads(raw)
        payload = _require_dict_payload(
            raw,
            source=f"record_decision(lane_reclaim_candidate:{lane_id})",
        )
    except Exception as exc:  # noqa: BLE001 — never raise out of the scan cycle
        # Defensive log: a broken log callable must not convert a handled
        # ledger failure into an escaping exception [RLSE-05]
        # (PLAN0181-S2LOGRAISE-01 / S2LOGRAISETWIN-01). Capture a traceback
        # tail so the sink is not limited to str(exc) + type name.
        # Shared _safe_log owns the sink no-throw contract [REF-19].
        tb_tail = ""
        try:
            import traceback  # noqa: PLC0415

            tb_tail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-2000:]
        except Exception:  # noqa: BLE001 — traceback is diagnostic only
            pass
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_candidate_record_failed",
            lane=lane_id,
            task_ref=task_ref,
            error=str(exc),
            error_type=type(exc).__name__,
            traceback_tail=tb_tail,
        )
        return False

    mutation = m if isinstance((m := payload.get("mutation")), dict) else {}
    operation = mutation.get("operation")
    # Accept insert, DO UPDATE ("update"), and "noop". Under
    # refresh_rationale_on_conflict=True the conflict clause is DO UPDATE, so
    # "noop" is not producible from this call site and is accepted only
    # defensively (PLAN0181-S2NOOP-01). Classifying refresh as "update"
    # (PLAN0181-S2REFRESHIDEM-01) must not reject re-evaluations that rewrote
    # the ledger row.
    if not payload.get("ok") or operation not in {"insert", "update", "noop"}:
        # PLAN0181-S2GATE-LOGRAISE-01 [AGT-10].
        _safe_log(
            log,
            "ERROR",
            "lane_reclaim_candidate_record_rejected",
            lane=lane_id,
            task_ref=task_ref,
            payload=payload,
        )
        return False

    # Refresh log keys on the real DO UPDATE outcome, not the old misclassified
    # "noop" (PLAN0181-S2REFRESHIDEM-01). PLAN0181-S2GATE-LOGRAISE-01: INFO
    # sink must not escape either.
    if operation == "update":
        _safe_log(
            log,
            "INFO",
            "lane_reclaim_candidate_refreshed",
            lane=lane_id,
            task_ref=task_ref,
        )
    elif operation == "noop":
        # Under refresh_rationale_on_conflict=True a true noop is not expected
        # from this call site. Still accept it (callers treat False as "not
        # recorded") but name the assumption break so it cannot pass in silence
        # [AGT-10][OBS-08][RLSE-05] (PLAN0181-S2GATE-NOOP-ACCEPT-01).
        _safe_log(
            log,
            "WARNING",
            "lane_reclaim_candidate_noop",
            lane=lane_id,
            task_ref=task_ref,
        )
    return True


def record_branch_reclaim_candidate(
    *,
    task_ref: str,
    lane_id: str,
    verdict: Any,
    log: Any | None = None,
) -> bool:
    """Record the reporting-only branch verdict without replacing the worktree verdict."""
    return record_reclaim_candidate(
        task_ref=task_ref,
        lane_id=lane_id,
        verdict=verdict,
        log=log,
        _branch_verdict=True,
    )


def bundle_lane_branch(
    orchestrator_root: Path,
    branch: str,
    *,
    state_dir: Path | None = None,
) -> dict[str, Any]:
    """Create and verify a durable rollback bundle for a local branch tip.

    This helper enforces the destructive-operation ordering contract:
    **bundle → repoint ledger → delete ref**, never any other order.  It
    grants no deletion authority itself.  All failures are returned as typed
    ``ok=False`` envelopes so a future reaper can safely choose not to act
    [RLSE-05][CON-05].
    """
    root = Path(orchestrator_root)
    cleaned_branch = branch.strip() if isinstance(branch, str) else ""

    def finish(ok: bool, **data: Any) -> dict[str, Any]:
        return {"ok": ok, "tool": "bundle_lane_branch", "data": data}

    if not cleaned_branch:
        return finish(False, error="branch is required.", branch=cleaned_branch)
    ref = f"refs/heads/{cleaned_branch}"
    try:
        resolved = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", f"{ref}^{{commit}}"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        return finish(False, error="git invocation failed", branch=cleaned_branch, detail=str(exc))
    tip_sha = resolved.stdout.strip()
    if resolved.returncode != 0 or not _is_full_commit_sha(tip_sha):
        return finish(False, error="branch tip is unavailable", branch=cleaned_branch)

    bundle_dir = Path(state_dir) if state_dir is not None else root / ".task-state"
    bundle_dir = bundle_dir / "branch-reclaim-bundles"
    branch_key = hashlib.sha256(cleaned_branch.encode("utf-8")).hexdigest()[:16]
    bundle_path = bundle_dir / f"{branch_key}-{tip_sha}.bundle"

    def verify(path: Path) -> bool:
        try:
            listed = subprocess.run(
                ["git", "-C", str(root), "bundle", "list-heads", str(path)],
                capture_output=True,
                text=True,
                check=False,
            )
            checked = subprocess.run(
                ["git", "-C", str(root), "bundle", "verify", str(path)],
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            return False
        listed_shas = {line.split(maxsplit=1)[0] for line in listed.stdout.splitlines() if line.strip()}
        return listed.returncode == 0 and checked.returncode == 0 and tip_sha in listed_shas

    try:
        bundle_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return finish(
            False,
            error="bundle destination is unavailable",
            branch=cleaned_branch,
            tip_sha=tip_sha,
            detail=str(exc),
        )

    if bundle_path.exists() and verify(bundle_path):
        return finish(
            True,
            branch=cleaned_branch,
            bundle_path=str(bundle_path),
            tip_sha=tip_sha,
            verified_sha=tip_sha,
            reused=True,
        )

    temporary_path: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(prefix=".branch-bundle-", suffix=".tmp", dir=bundle_dir)
        os.close(descriptor)
        temporary_path = Path(temporary_name)
        temporary_path.unlink()
        created = subprocess.run(
            ["git", "-C", str(root), "bundle", "create", str(temporary_path), ref],
            capture_output=True,
            text=True,
            check=False,
        )
        if created.returncode != 0:
            return finish(
                False,
                error="git bundle create failed",
                branch=cleaned_branch,
                tip_sha=tip_sha,
                detail=created.stderr.strip(),
            )
        if not verify(temporary_path):
            return finish(False, error="git bundle verification failed", branch=cleaned_branch, tip_sha=tip_sha)
        os.replace(temporary_path, bundle_path)
        temporary_path = None
    except OSError as exc:
        return finish(
            False,
            error="bundle write failed",
            branch=cleaned_branch,
            tip_sha=tip_sha,
            detail=str(exc),
        )
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass

    return finish(
        True,
        branch=cleaned_branch,
        bundle_path=str(bundle_path),
        tip_sha=tip_sha,
        verified_sha=tip_sha,
        reused=False,
    )


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
       Reader raise, ``ok is not True``, non-dict envelope, or landing present
       but unusable (non-dict / missing-empty-nonstring ``commit_sha``) → fail
       closed (``dependency_check_failed``); never treat corrupt evidence as
       absent landing (cs0166-r04-06).
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
    # (a) ok=True + landing absent/null → genuine absence, fall through to outcomes.
    # (b) raise OR ok is not True → fail closed (never vacuous discharge).
    # (c) ok=True with non-dict envelope, or landing present but unusable
    #     (non-dict / missing-empty-nonstring commit_sha) → fail closed
    #     (cs0166-r04-06); never treat corrupt evidence as absence.
    landing_sha: str | None = None
    try:
        from workbay_handoff_mcp import latest_lane_landing  # noqa: PLC0415

        env = latest_lane_landing(lane_id=upstream_id, task_ref=task_ref)
        if isinstance(env, str):
            env = json.loads(env)
        if not isinstance(env, dict):
            # Non-dict envelope cannot be scored as "no landing" — fail closed.
            if callable(log):
                try:
                    log(
                        "ERROR",
                        "lane_landing_reader_failed",
                        upstream_id=upstream_id,
                        task_ref=task_ref,
                        error="non_dict_envelope",
                        error_type=type(env).__name__,
                    )
                except Exception:  # noqa: BLE001 — never raise out of intake
                    pass
            return _finish(False, REASON_DEPENDENCY_CHECK_FAILED)
        if env.get("ok") is not True:
            # No exception object is in scope on this arm (envelope ok-false,
            # not a raised fault); do not invent traceback_tail. Guard the
            # sink so a raising log cannot be re-caught by the reader
            # handler below and escape (PLAN0181-S2LOGRAISEDEP-01).
            if callable(log):
                try:
                    log(
                        "ERROR",
                        "lane_landing_reader_failed",
                        upstream_id=upstream_id,
                        task_ref=task_ref,
                        ok=env.get("ok"),
                    )
                except Exception:  # noqa: BLE001 — never raise out of intake
                    pass
            return _finish(False, REASON_DEPENDENCY_CHECK_FAILED)
        # Envelope: data.landing; tolerate a flat shape from mocks/tests.
        data = env.get("data") if isinstance(env.get("data"), dict) else env
        landing_present = isinstance(data, dict) and "landing" in data
        landing = data.get("landing") if landing_present else None
        if landing_present and landing is not None:
            # Landing key present and non-null: must yield a usable SHA or
            # refuse — never fall through to the vacuous worker-report arm.
            if not isinstance(landing, dict):
                if callable(log):
                    try:
                        log(
                            "ERROR",
                            "lane_landing_reader_failed",
                            upstream_id=upstream_id,
                            task_ref=task_ref,
                            error="landing_not_dict",
                            error_type=type(landing).__name__,
                        )
                    except Exception:  # noqa: BLE001 — never raise out of intake
                        pass
                return _finish(False, REASON_DEPENDENCY_CHECK_FAILED)
            raw_sha = landing.get("commit_sha")
            if not isinstance(raw_sha, str) or not raw_sha.strip():
                if callable(log):
                    try:
                        log(
                            "ERROR",
                            "lane_landing_reader_failed",
                            upstream_id=upstream_id,
                            task_ref=task_ref,
                            error="landing_commit_sha_unusable",
                            commit_sha_type=type(raw_sha).__name__,
                        )
                    except Exception:  # noqa: BLE001 — never raise out of intake
                        pass
                return _finish(False, REASON_DEPENDENCY_CHECK_FAILED)
            landing_sha = raw_sha.strip()
    except Exception as exc:  # noqa: BLE001 — reader fault → fail closed
        # Defensive log: a broken log callable must not convert a handled
        # reader fault into an escaping exception [RLSE-05]
        # (PLAN0181-S2LOGRAISEDEP-01). Capture a traceback tail so the sink is
        # not limited to str(exc) + type name. Always _finish so the caller's
        # memo still records the refusal.
        if callable(log):
            try:
                import traceback  # noqa: PLC0415

                tb_tail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-2000:]
                log(
                    "ERROR",
                    "lane_landing_reader_failed",
                    upstream_id=upstream_id,
                    task_ref=task_ref,
                    error=str(exc),
                    error_type=type(exc).__name__,
                    traceback_tail=tb_tail,
                )
            except Exception:  # noqa: BLE001 — never raise out of intake
                pass
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
        # PLAN0181-S2GATE-LOGRAISE-DEPGATE-01 [AGT-10][RES-13][OBS-08]:
        # blocked/first_reason already assigned — diagnostic enrichment must
        # never abort the gate. Protect the DB read (outcome=None on fault)
        # and route the sink through _safe_log; event/field names unchanged.
        outcome: str | None = None
        try:
            outcome = _latest_worker_report_outcome(task_ref, ancestor)
        except Exception:  # noqa: BLE001 — diagnostic-only; fall back to None
            outcome = None
        _safe_log(
            log,
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


def __getattr__(name: str) -> Any:
    """Lazy re-export for the shared decision-id constructor.

    The import is function-local in :func:`record_reclaim_candidate` so a
    skewed handoff install cannot fail this module at import time
    (PLAN0181-S2GATE2-MODULE-LEVEL-XPKG-IMPORT-01). Attribute access still
    resolves the same object for identity pins and external callers.
    """
    if name in {"reclaim_candidate_decision_id", "branch_reclaim_candidate_decision_id"}:
        from workbay_handoff_mcp.lanes_recording import (  # noqa: PLC0415
            branch_reclaim_candidate_decision_id as _branch_fn,
        )
        from workbay_handoff_mcp.lanes_recording import (
            reclaim_candidate_decision_id as _fn,
        )

        return _branch_fn if name == "branch_reclaim_candidate_decision_id" else _fn
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
