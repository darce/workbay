from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any

from workbay_protocol import CONTRACTS_DIR, RULES_DIR

from .backend_registry import get_backend_spec
from .red_green_gate import verify_red_green

if TYPE_CHECKING:
    from workbay_handoff_mcp.enums import ReviewKind, ReviewScopeSource

_handoff_read_shapes = import_module(f"{__package__}.handoff_read_shapes" if __package__ else "handoff_read_shapes")

DEFAULT_BOUNDARY_PREFIXES = (
    "apps/",
    "packages/mcp-workbay-orchestrator/src/",
    "packages/shared-contracts/schemas/",
)
BOUNDARY_PREFIXES = DEFAULT_BOUNDARY_PREFIXES
DEFAULT_CONTRACT_PREFIXES = (
    f"{CONTRACTS_DIR}/",
    "packages/workbay-system/workbay_system/payload/docs/workbay/contracts/",
    "packages/shared-contracts/",
)
CONTRACT_PREFIXES = DEFAULT_CONTRACT_PREFIXES
DEFAULT_CONTRACT_CHECKLIST_PATH = f"{RULES_DIR}/contract-change-checklist.md"
CONTRACT_CHECKLIST_PATH = DEFAULT_CONTRACT_CHECKLIST_PATH
REVIEW_LENS_OUTCOMES = frozenset({"findings", "clean", "timeout", "disqualified"})
REVIEW_COMPLETE_LENS_OUTCOMES = frozenset({"findings", "clean"})
QUALIFYING_REVIEW_LENS_OUTCOMES = REVIEW_COMPLETE_LENS_OUTCOMES
TURN_PATCH_ARTIFACT_SCAN_LIMIT = 100


class _TierUnset:
    """Sentinel for "this review has no lane row at all"."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<tier unset>"


# ``tier=None`` and an omitted ``tier`` are indistinguishable in Python, but they
# mean opposite things here. A lane row whose tier column is NULL is an unknown
# tier and must fail closed to the junior standard (RLSE-05); a review with no
# lane row at all is simply not a lane review and has no tier to judge. Passing
# the junior gate over the second case would refuse every non-lane review-ready
# run. main() always forwards ``tier`` when a lane exists, so the sentinel only
# survives when ``_load_lane_for_review_ready`` found nothing.
TIER_UNSET = _TierUnset()


@dataclass(frozen=True)
class ReviewReadyResult:
    ready: bool
    task_ref: str
    base_ref: str
    base_sha: str
    open_findings: int
    open_blockers: int
    current_task_in_sync: bool
    current_commit_summary_present: bool
    tests_recent_count: int
    has_test_evidence: bool
    contract_violation: bool
    scope_source: ReviewScopeSource
    review_kind: ReviewKind
    boundary_files: list[str]
    contract_files: list[str]
    tier: str | None
    junior_gate_applied: bool
    red_green_passed: bool
    lens_outcome: str | None
    producer_family: str | None
    reviewer_family: str | None
    independent_review: bool
    reasons: list[str]


def _run_git(*args: str, cwd: Path) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _configure_runtime(orchestrator_root: Path) -> None:
    from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415
    from workbay_handoff_mcp.runtime import configure_runtime  # noqa: PLC0415

    configure_runtime(RuntimeConfig.for_repo(orchestrator_root))


def _load_ok_payload(name: str, payload: dict[str, Any] | str | bytes | bytearray) -> dict[str, Any]:
    if isinstance(payload, dict):
        data = payload
    else:
        try:
            loaded = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RuntimeError(f"MCP query failed: {name}: invalid JSON payload") from exc
        if not isinstance(loaded, dict):
            raise RuntimeError(f"MCP query failed: {name}: expected object payload, got {type(loaded).__name__}")
        data = loaded
    if not data.get("ok"):
        error = data.get("error") or "unknown error"
        raise RuntimeError(f"MCP query failed: {name}: {error}")
    return data


def _has_valid_current_commit_handoff(value: object) -> bool:
    """Accept only explicit successful close-check evidence with a commit link."""
    if not isinstance(value, Mapping) or value.get("is_violation") is not False:
        return False
    current_sha = value.get("current_commit_sha")
    satisfied_sha = value.get("satisfied_by_commit_sha")
    decision_count = value.get("structured_slice_decision_count")
    match_kind = value.get("match_kind")
    if not isinstance(current_sha, str) or not current_sha.strip():
        return False
    if not isinstance(satisfied_sha, str) or not satisfied_sha.strip():
        return False
    if isinstance(decision_count, bool) or not isinstance(decision_count, int) or decision_count < 1:
        return False
    if match_kind == "exact":
        return satisfied_sha == current_sha
    return match_kind == "reachable_ancestor"


def evaluate_review_ready(
    *,
    task_ref: str,
    base_ref: str,
    base_sha: str,
    current_commit_sha: str | None = None,
    changed_files: list[str],
    scope_source: ReviewScopeSource,
    review_kind: ReviewKind,
    review: dict[str, Any],
    state: dict[str, Any],
    close: dict[str, Any],
    tier: str | None | _TierUnset = TIER_UNSET,
    red_green: dict[str, Any] | str | None = None,
    producer_family: str | None = None,
    reviewer_family: str | None = None,
    lens_outcome: str | None = None,
    review_run: dict[str, Any] | None = None,
    boundary_prefixes: tuple[str, ...] = BOUNDARY_PREFIXES,
    contract_prefixes: tuple[str, ...] = CONTRACT_PREFIXES,
    contract_checklist_path: str = CONTRACT_CHECKLIST_PATH,
) -> ReviewReadyResult:
    boundary_files = [path for path in changed_files if path.startswith(boundary_prefixes)]
    contract_files = [
        path for path in changed_files if path.startswith(contract_prefixes) or path == contract_checklist_path
    ]

    open_findings = int(review.get("counts", {}).get("status", {}).get("open", 0))
    # Support both flat dict (test mocks) and nested envelope (production API)
    close_checks = close.get("checks") or close.get("data", {}).get("checks", {})
    open_blockers = int(close_checks.get("open_blockers", {}).get("count", 0))
    current_task_sync = close_checks.get("current_task_sync", {})
    current_task_in_sync = bool(current_task_sync.get("is_in_sync"))
    # Trust the close-check's explicit is_violation. The prior
    # `not current_task_in_sync` fallback silently re-introduced
    # "CURRENT_TASK.json is out of sync with handoff state" as a hard
    # blocking reason whenever close_check responses pre-dated the
    # is_violation key (older installed mcp-workbay-handoff, cached
    # envelopes). The materialized-on-demand contract in
    # handoff_close_check makes sync a guaranteed post-condition, so
    # there is no informational signal left to surface as a failure.
    current_task_is_violation = bool(current_task_sync.get("is_violation", False))
    current_commit_summary_present = _has_valid_current_commit_handoff(close_checks.get("current_commit_handoff"))
    # Support both flat dict (test mocks) and nested envelope (production API)
    tests_recent = state.get("tests_recent") or state.get("data", {}).get("tests_recent", []) or []
    current_test_rows = [
        row
        for row in tests_recent
        if isinstance(row, dict) and current_commit_sha is not None and row.get("commit_sha") == current_commit_sha
    ]
    failed_current_test = any(row.get("passed") is not True or row.get("exit_code") != 0 for row in current_test_rows)
    if current_commit_sha is None:
        # Library callers that do not have a resolved worktree SHA retain the
        # legacy aggregate check. The production CLI always supplies the SHA.
        has_test_evidence = len(tests_recent) > 0
    else:
        has_test_evidence = (
            bool(current_test_rows)
            and not failed_current_test
            and any(row.get("passed") is True and row.get("exit_code") == 0 for row in current_test_rows)
        )
    contract_violation = bool(boundary_files and not contract_files)

    normalized_tier = tier.strip().lower() if isinstance(tier, str) and tier.strip() else None
    red_green_status: Any = red_green
    red_green_conflict = False
    if isinstance(red_green, dict):
        status_value = red_green.get("status")
        named_value = red_green.get("red_green")
        red_green_conflict = status_value is not None and named_value is not None and status_value != named_value
        red_green_status = status_value if status_value is not None else named_value
    red_green_passed = (
        isinstance(red_green_status, str) and red_green_status.strip().lower() == "pass" and not red_green_conflict
    )

    evidence = review_run if isinstance(review_run, dict) else {}
    evidence_lens = evidence.get("lens_outcome", evidence.get("outcome"))
    evidence_reviewer = evidence.get("reviewer_family")
    effective_lens = lens_outcome if lens_outcome is not None else evidence_lens
    effective_reviewer = reviewer_family if reviewer_family is not None else evidence_reviewer
    normalized_lens = (
        effective_lens.strip().lower() if isinstance(effective_lens, str) and effective_lens.strip() else None
    )
    normalized_producer = (
        producer_family.strip().lower() if isinstance(producer_family, str) and producer_family.strip() else None
    )
    normalized_reviewer = (
        effective_reviewer.strip().lower()
        if isinstance(effective_reviewer, str) and effective_reviewer.strip()
        else None
    )
    lens_conflict = lens_outcome is not None and evidence_lens is not None and lens_outcome != evidence_lens
    reviewer_conflict = (
        reviewer_family is not None and evidence_reviewer is not None and reviewer_family != evidence_reviewer
    )
    valid_lens_outcome = normalized_lens in REVIEW_LENS_OUTCOMES and not lens_conflict
    qualifying_lens_outcome = normalized_lens in QUALIFYING_REVIEW_LENS_OUTCOMES and not lens_conflict
    independent_review = bool(
        qualifying_lens_outcome
        and normalized_producer
        and normalized_reviewer
        and normalized_producer != normalized_reviewer
        and not reviewer_conflict
    )

    reasons: list[str] = []
    if open_findings:
        reasons.append(f"{open_findings} open review finding(s)")
    if open_blockers:
        reasons.append(f"{open_blockers} open blocker(s)")
    if current_task_is_violation:
        reasons.append("CURRENT_TASK.json is out of sync with handoff state")
    if not current_commit_summary_present:
        reasons.append("no structured slice-completion summary recorded for the current commit")
    if failed_current_test:
        reasons.append("current commit has failed recorded test evidence")
    elif not has_test_evidence:
        reasons.append(
            "no passing test evidence for current commit"
            if current_commit_sha is not None
            else "no recorded test evidence in handoff state"
        )
    if contract_violation:
        reasons.append("boundary-touching files changed without contract/checklist co-change")
    # A lane row exists but carries no tier: every lane created before the tier
    # column, and every mocked lane. Unknown is not senior.
    lane_tier_unknown = not isinstance(tier, _TierUnset) and normalized_tier is None
    junior_gate_applied = normalized_tier == "junior" or lane_tier_unknown
    if junior_gate_applied:
        if lane_tier_unknown:
            reasons.append("lane tier unknown; junior gate applied")
        if not red_green_passed:
            reasons.append("junior lane lacks passing red/green evidence")
        if normalized_lens is None:
            reasons.append("junior lane has no recorded lens outcome")
        elif not valid_lens_outcome:
            reasons.append("junior lane lens outcome is outside the closed vocabulary")
        elif normalized_lens not in REVIEW_COMPLETE_LENS_OUTCOMES:
            reasons.append(f"junior lane review outcome {normalized_lens!r} is not review-complete")
        if reviewer_conflict or lens_conflict:
            reasons.append("junior lane review evidence conflicts across sources")
        if not normalized_producer or not normalized_reviewer:
            reasons.append("junior lane reviewer independence cannot be verified")
        elif normalized_producer == normalized_reviewer:
            reasons.append("junior lane reviewer family matches producer family")

    return ReviewReadyResult(
        ready=not reasons,
        task_ref=state.get("task_ref") or review.get("task_ref") or task_ref,
        base_ref=base_ref,
        base_sha=base_sha,
        open_findings=open_findings,
        open_blockers=open_blockers,
        current_task_in_sync=current_task_in_sync,
        current_commit_summary_present=current_commit_summary_present,
        tests_recent_count=len(tests_recent),
        has_test_evidence=has_test_evidence,
        contract_violation=contract_violation,
        scope_source=scope_source,
        review_kind=review_kind,
        boundary_files=boundary_files,
        contract_files=contract_files,
        tier=normalized_tier,
        junior_gate_applied=junior_gate_applied,
        red_green_passed=red_green_passed,
        lens_outcome=normalized_lens,
        producer_family=normalized_producer,
        reviewer_family=normalized_reviewer,
        independent_review=independent_review,
        reasons=reasons,
    )


def render_review_ready(result: ReviewReadyResult) -> str:
    lines = [
        f"REVIEW READY: {'READY' if result.ready else 'NOT READY'}",
        f"Task: {result.task_ref}",
        f"Base ref: {result.base_ref} ({result.base_sha[:12]})",
        f"Open findings: {result.open_findings}",
        f"Open blockers: {result.open_blockers}",
        f"CURRENT_TASK export: {'current' if result.current_task_in_sync else 'not current (informational)'}",
        f"Current commit summary: {'present' if result.current_commit_summary_present else 'missing'}",
        f"Review kind: {result.review_kind}",
        f"Scope source: {result.scope_source}",
        "Test evidence: "
        f"{'present' if result.has_test_evidence else 'missing'} "
        f"({result.tests_recent_count} recent record(s))",
        f"Contract co-change: {'ok' if not result.contract_violation else 'missing'}",
    ]
    if result.boundary_files:
        lines.append("Boundary files:")
        lines.extend(f"- {path}" for path in result.boundary_files)
    if result.contract_files:
        lines.append("Contract files:")
        lines.extend(f"- {path}" for path in result.contract_files)
    if result.junior_gate_applied:
        lines.extend(
            [
                f"Junior red/green: {'pass' if result.red_green_passed else 'missing or failed'}",
                f"Junior lens outcome: {result.lens_outcome or 'absent'}",
                f"Junior independent review: {'verified' if result.independent_review else 'unverified'}",
            ]
        )
    if result.reasons:
        lines.append("Reasons:")
        lines.extend(f"- {reason}" for reason in result.reasons)
    return "\n".join(lines)


def _load_latest_slice_packet(task_ref: str, review_kind: str | None) -> dict[str, Any]:
    from workbay_orchestrator_mcp.lanes import get_latest_slice_review_packet  # noqa: PLC0415

    payload = _load_ok_payload(
        "get_latest_slice_review_packet",
        get_latest_slice_review_packet(task_ref=task_ref, review_kind=review_kind),
    )
    packet: dict[str, Any] = payload["packet"]
    return packet


def _same_path(left: object, right: Path) -> bool:
    if not isinstance(left, str) or not left.strip():
        return False
    try:
        return Path(left).expanduser().resolve() == right
    except OSError:
        return False


def _load_lane_for_review_ready(
    *,
    task_ref: str,
    worktree_root: Path,
    current_branch: str,
) -> dict[str, Any] | None:
    """Return the one lane row naming this checkout, or the legacy lane-less state."""
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415

    lanes: list[dict[str, Any]] = []
    offset = 0
    for _ in range(100):
        payload = _load_ok_payload(
            "manage_worktree_lane.list",
            manage_worktree_lane(operation="list", task_ref=task_ref, status="all", limit=100, offset=offset),
        )
        nested = payload.get("data")
        data = nested if isinstance(nested, dict) else payload
        raw_lanes = data.get("lanes", [])
        if not isinstance(raw_lanes, list):
            raise RuntimeError("MCP query failed: manage_worktree_lane.list: expected lanes array")
        page = [lane for lane in raw_lanes if isinstance(lane, dict)]
        if len(page) != len(raw_lanes):
            raise RuntimeError("MCP query failed: manage_worktree_lane.list: invalid lane row")
        lanes.extend(page)
        if not data.get("has_more"):
            break
        if not page:
            raise RuntimeError("MCP query failed: manage_worktree_lane.list: pagination made no progress")
        offset += len(page)
    else:
        raise RuntimeError("MCP query failed: manage_worktree_lane.list: pagination limit exceeded")
    resolved_worktree = worktree_root.resolve()
    path_matches = [lane for lane in lanes if _same_path(lane.get("worktree_path"), resolved_worktree)]
    matches = path_matches or [lane for lane in lanes if lane.get("branch") == current_branch]
    if len(matches) > 1:
        raise RuntimeError(
            f"MCP query failed: manage_worktree_lane.list: multiple lanes name {resolved_worktree} / {current_branch}"
        )
    return matches[0] if matches else None


def _resolve_turn_patch(orchestrator_root: Path, lane_id: str) -> Path | None:
    """Resolve the unique latest producer transport product for a lane."""
    from .remote_exec_staging_reaper import STAGING_DIR_PREFIX  # noqa: PLC0415

    state_dir = orchestrator_root / ".task-state"
    if not state_dir.is_dir():
        return None
    safe_lane = "".join(ch if ch.isalnum() or ch in "-_." else "-" for ch in lane_id) or "lane"
    prefix = f"{STAGING_DIR_PREFIX}{safe_lane}-"
    producer_patches: list[tuple[int, Path]] = []
    try:
        matching_artifacts = 0
        for artifact_dir in state_dir.iterdir():
            if not artifact_dir.name.startswith(prefix) or not artifact_dir.is_dir():
                continue
            matching_artifacts += 1
            if matching_artifacts > TURN_PATCH_ARTIFACT_SCAN_LIMIT:
                return None
            patch_path = artifact_dir / "turn.patch"
            schema_path = artifact_dir / "schema.json"
            if not patch_path.is_file() or not schema_path.is_file():
                continue
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            if not isinstance(schema, dict):
                continue
            required = schema.get("required")
            properties = schema.get("properties")
            if (
                not isinstance(required, list)
                or "handoff_action" not in required
                or "findings" in required
                or not isinstance(properties, dict)
                or not isinstance(properties.get("handoff_action"), dict)
            ):
                continue
            producer_patches.append((artifact_dir.stat().st_mtime_ns, patch_path))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not producer_patches:
        return None
    newest_mtime = max(mtime for mtime, _ in producer_patches)
    newest_patches = [patch for mtime, patch in producer_patches if mtime == newest_mtime]
    return newest_patches[0] if len(newest_patches) == 1 else None


def _load_lens_evidence(
    *,
    task_ref: str,
    current_branch: str,
    current_sha: str,
    producer_family: str | None,
) -> dict[str, Any] | None:
    """Load commit-scoped evidence from the production ``review_runs`` ledger."""
    import sqlite3  # noqa: PLC0415

    from workbay_handoff_mcp.runtime import get_runtime_config  # noqa: PLC0415

    try:
        db_path = Path(get_runtime_config().db_path)
        if not db_path.is_file():
            return None
        with sqlite3.connect(str(db_path)) as conn:
            conn.row_factory = sqlite3.Row
            columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(review_runs)").fetchall()}
            required = {
                "id",
                "task_ref",
                "review_mode",
                "verdict",
                "agent",
                "model",
                "branch",
                "commit_sha",
                "reviewed_at",
            }
            if not required.issubset(columns):
                return None
            rows = conn.execute(
                """
                SELECT verdict, agent, model
                FROM review_runs
                WHERE lower(task_ref) = lower(?)
                  AND review_mode = 'branch'
                  AND branch = ?
                  AND commit_sha = ?
                ORDER BY reviewed_at DESC, id DESC
                """,
                (task_ref, current_branch, current_sha),
            ).fetchall()
    except (OSError, RuntimeError, sqlite3.Error):
        return None

    for row in rows:
        reviewer_family: str | None = None
        for backend in (row["agent"], row["model"]):
            if not isinstance(backend, str) or not backend.strip():
                continue
            try:
                reviewer_family = get_backend_spec(backend).model_family
            except (KeyError, RuntimeError):
                continue
            if reviewer_family:
                break
        if not reviewer_family:
            continue
        normalized_family = reviewer_family.strip().lower()
        if producer_family and normalized_family == producer_family.strip().lower():
            continue
        lens_outcome = {
            "pass": "clean",
            "pass_with_findings": "findings",
            "conditional_pass": "disqualified",
            "fail": "disqualified",
        }.get(str(row["verdict"] or "").strip().lower())
        if lens_outcome is None:
            continue
        return {
            "lens_outcome": lens_outcome,
            "reviewer_family": normalized_family,
        }
    return None


def _junior_evidence_for_lane(
    *,
    orchestrator_root: Path,
    worktree_root: Path,
    task_ref: str,
    lane: dict[str, Any],
    current_branch: str,
    base_sha: str,
    current_sha: str,
) -> dict[str, Any]:
    """Collect deterministic junior evidence, failing closed on every missing input."""
    backend = lane.get("backend")
    producer_family: str | None = None
    if isinstance(backend, str) and backend.strip():
        try:
            producer_family = get_backend_spec(backend).model_family
        except (KeyError, RuntimeError):
            producer_family = None

    review_run = _load_lens_evidence(
        task_ref=task_ref,
        current_branch=current_branch,
        current_sha=current_sha,
        producer_family=producer_family,
    )
    lane_id = lane.get("lane_id")
    test_cmd = lane.get("test_cmd")
    try:
        turn_patch = _resolve_turn_patch(orchestrator_root, lane_id) if isinstance(lane_id, str) else None
    except (OSError, RuntimeError):
        turn_patch = None

    if lane.get("branch") != current_branch:
        red_green: dict[str, Any] = {"status": "fail", "reason": "lane_branch_mismatch"}
    elif not isinstance(test_cmd, str) or not test_cmd.strip():
        red_green = {"status": "fail", "reason": "missing_test_cmd"}
    elif turn_patch is None:
        red_green = {"status": "fail", "reason": "missing_transport_patch"}
    else:
        try:
            red_green = verify_red_green(worktree_root, base_sha, current_sha, test_cmd, turn_patch)
        except Exception as exc:  # noqa: BLE001 - a bad harness is failed evidence, never a gate crash
            red_green = {
                "status": "fail",
                "reason": "red_green_verification_error",
                "error": f"{type(exc).__name__}: {exc}",
            }

    return {
        "tier": "junior",
        "red_green": red_green,
        "producer_family": producer_family,
        "review_run": review_run,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orchestrator-root", required=True)
    parser.add_argument("--worktree-root", required=True)
    parser.add_argument("--task-ref", required=True)
    parser.add_argument("--review-base", required=True)
    parser.add_argument("--latest-slice", action="store_true")
    parser.add_argument("--review-kind", choices=("branch", "planning"))
    parser.add_argument(
        "--boundary-prefix",
        action="append",
        dest="boundary_prefixes",
        help="Optional boundary-file prefix override. Repeat to add multiple prefixes.",
    )
    parser.add_argument(
        "--contract-prefix",
        action="append",
        dest="contract_prefixes",
        help="Optional contract-file prefix override. Repeat to add multiple prefixes.",
    )
    parser.add_argument(
        "--contract-checklist-path",
        help="Optional contract checklist path override.",
    )
    args = parser.parse_args()

    orchestrator_root = Path(args.orchestrator_root).resolve()
    worktree_root = Path(args.worktree_root).resolve()

    _configure_runtime(orchestrator_root)

    try:
        base_sha = _run_git("merge-base", args.review_base, "HEAD", cwd=worktree_root)
        current_sha = _run_git("rev-parse", "HEAD", cwd=worktree_root)
        current_branch = _run_git("rev-parse", "--abbrev-ref", "HEAD", cwd=worktree_root)
    except subprocess.CalledProcessError:
        print(
            f"REVIEW_BASE '{args.review_base}' does not resolve to a merge-base from {worktree_root}.",
            file=sys.stderr,
        )
        return 1

    from workbay_handoff_mcp import handoff_close_check  # noqa: PLC0415
    from workbay_handoff_mcp.enums import ReviewKind, ReviewScopeSource  # noqa: PLC0415
    from workbay_handoff_mcp.review_findings import get_review_findings_summary  # noqa: PLC0415

    scope_source = ReviewScopeSource.BRANCH_DIFF
    review_kind = ReviewKind(args.review_kind or ReviewKind.BRANCH.value)
    changed_files: list[str]

    try:
        if args.latest_slice:
            packet = _load_latest_slice_packet(args.task_ref, args.review_kind)
            changed_files = list(packet.get("changed_files") or [])
            scope_source = ReviewScopeSource(str(packet.get("scope_source") or ReviewScopeSource.SLICE_PACKET.value))
            review_kind = ReviewKind(str(packet.get("review_kind") or review_kind.value))
        else:
            changed = _run_git("diff", "--name-only", f"{base_sha}..HEAD", cwd=worktree_root)
            changed_files = [line for line in changed.splitlines() if line.strip()]
        review = _load_ok_payload(
            "get_review_findings_summary",
            get_review_findings_summary(task_ref=args.task_ref),
        )
        state = _load_ok_payload(
            "get_handoff_state",
            _handoff_read_shapes.read_handoff_state(**_handoff_read_shapes.review_ready_state_kwargs(args.task_ref)),
        )
        close = _load_ok_payload(
            "handoff_close_check",
            handoff_close_check(
                task_ref=args.task_ref,
                current_commit_sha=current_sha,
                require_fresh_tests=True,
            ),
        )
        lane = _load_lane_for_review_ready(
            task_ref=args.task_ref,
            worktree_root=worktree_root,
            current_branch=current_branch,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    lane_evidence: dict[str, Any] = {}
    if lane is not None:
        tier = lane.get("tier")
        lane_evidence["tier"] = tier
        normalized = tier.strip().lower() if isinstance(tier, str) and tier.strip() else None
        if normalized in (None, "junior"):
            lane_evidence = _junior_evidence_for_lane(
                orchestrator_root=orchestrator_root,
                worktree_root=worktree_root,
                task_ref=args.task_ref,
                lane=lane,
                current_branch=current_branch,
                base_sha=base_sha,
                current_sha=current_sha,
            )

    result = evaluate_review_ready(
        task_ref=args.task_ref,
        base_ref=args.review_base,
        base_sha=base_sha,
        current_commit_sha=current_sha,
        changed_files=changed_files,
        scope_source=scope_source,
        review_kind=review_kind,
        review=review,
        state=state,
        close=close,
        **lane_evidence,
        boundary_prefixes=tuple(args.boundary_prefixes or BOUNDARY_PREFIXES),
        contract_prefixes=tuple(args.contract_prefixes or CONTRACT_PREFIXES),
        contract_checklist_path=args.contract_checklist_path or CONTRACT_CHECKLIST_PATH,
    )
    print(render_review_ready(result))
    return 0 if result.ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
