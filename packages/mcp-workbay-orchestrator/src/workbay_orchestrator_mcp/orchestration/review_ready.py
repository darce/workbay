from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING, Any

from workbay_protocol import CONTRACTS_DIR, RULES_DIR

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
    "packages/shared-contracts/",
)
CONTRACT_PREFIXES = DEFAULT_CONTRACT_PREFIXES
DEFAULT_CONTRACT_CHECKLIST_PATH = f"{RULES_DIR}/contract-change-checklist.md"
CONTRACT_CHECKLIST_PATH = DEFAULT_CONTRACT_CHECKLIST_PATH


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
        loaded = json.loads(payload)
        if not isinstance(loaded, dict):
            raise RuntimeError(f"MCP query failed: {name}: expected object payload, got {type(loaded).__name__}")
        data = loaded
    if not data.get("ok"):
        error = data.get("error") or "unknown error"
        raise RuntimeError(f"MCP query failed: {name}: {error}")
    return data


def evaluate_review_ready(
    *,
    task_ref: str,
    base_ref: str,
    base_sha: str,
    changed_files: list[str],
    scope_source: ReviewScopeSource,
    review_kind: ReviewKind,
    review: dict[str, Any],
    state: dict[str, Any],
    close: dict[str, Any],
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
    current_commit_summary_present = not bool(close_checks.get("current_commit_handoff", {}).get("is_violation"))
    # Support both flat dict (test mocks) and nested envelope (production API)
    tests_recent = state.get("tests_recent") or state.get("data", {}).get("tests_recent", []) or []
    has_test_evidence = len(tests_recent) > 0
    contract_violation = bool(boundary_files and not contract_files)

    reasons: list[str] = []
    if open_findings:
        reasons.append(f"{open_findings} open review finding(s)")
    if open_blockers:
        reasons.append(f"{open_blockers} open blocker(s)")
    if current_task_is_violation:
        reasons.append("CURRENT_TASK.json is out of sync with handoff state")
    if not current_commit_summary_present:
        reasons.append("no structured slice-completion summary recorded for the current commit")
    if not has_test_evidence:
        reasons.append("no recorded test evidence in handoff state")
    if contract_violation:
        reasons.append("boundary-touching files changed without contract/checklist co-change")

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
            handoff_close_check(task_ref=args.task_ref, current_commit_sha=current_sha),
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    result = evaluate_review_ready(
        task_ref=args.task_ref,
        base_ref=args.review_base,
        base_sha=base_sha,
        changed_files=changed_files,
        scope_source=scope_source,
        review_kind=review_kind,
        review=review,
        state=state,
        close=close,
        boundary_prefixes=tuple(args.boundary_prefixes or BOUNDARY_PREFIXES),
        contract_prefixes=tuple(args.contract_prefixes or CONTRACT_PREFIXES),
        contract_checklist_path=args.contract_checklist_path or CONTRACT_CHECKLIST_PATH,
    )
    print(render_review_ready(result))
    return 0 if result.ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
