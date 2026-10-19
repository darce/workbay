"""One read-only snapshot and one precedence table for branch disposition."""

from __future__ import annotations

import fnmatch
import re
import subprocess
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import lane_reclaim
from .harness_protocol import LandingPolicy, load_landing_policy
from .landing_log import (
    CarrierResult,
    ReceiptVerdict,
    ScanResult,
    Tombstone,
    TombstoneVerdict,
    find_carrier,
    parse_landing_receipt,
    scan_first_parent,
    tombstone_verification_deadline,
    verify_receipt,
    verify_tombstone,
)
from .lane_terminal_dispose import _TERMINAL_STATUSES, BranchDisposition

_GIT_TIMEOUT_S = 20.0
_MAX_EVIDENCE_PATHS = 50
_MAX_REVIEW_ARTIFACT_COMMITS = 20
_REVIEW_DOC_PREFIX = "docs/reviews/"
_REVIEW_DOC_SUFFIX = ".md"


def _is_review_doc_path(path: str) -> bool:
    """The one review-document rule: a markdown file under ``docs/reviews/``."""
    normalized = path.removeprefix("./")
    return normalized.startswith(_REVIEW_DOC_PREFIX) and normalized.endswith(_REVIEW_DOC_SUFFIX)


SEAMS = (
    "integration_ref",
    "landing_policy",
    "receipt_scan",
    "branch_tip",
    "worktree_list",
    "list_lanes_by_branch",
    "carrier",
    "receipt_verify",
    "lane_receipts",
    "tombstone_verify",
    "survivor_refs",
    "changed_paths",
    "main_diff",
    "review_artifacts",
)


@dataclass(frozen=True)
class SharedReads:
    integration_ref: str
    integration_tip: str | None
    policy: LandingPolicy | None
    scan: ScanResult | None
    branch_tips: Mapping[str, tuple[str, int]]
    worktrees: Mapping[str, tuple[str, ...]] | None
    probes: Mapping[str, str]


@dataclass(frozen=True)
class Snapshot:
    branch: str
    integration_ref: str
    integration_tip: str | None
    tip: str | None
    now: float
    tip_commit_time: int | None
    age_floor_s: float
    lane_rows: tuple[dict[str, Any], ...]
    worktree_paths: tuple[str, ...]
    landing: ReceiptVerdict | None
    unverified_receipts: tuple[ReceiptVerdict, ...]
    lane_receipts: tuple[ReceiptVerdict, ...]
    tombstone_groups: tuple[tuple[str, tuple[TombstoneVerdict, ...]], ...]
    stale_tombstones: tuple[Tombstone, ...]
    carrier: CarrierResult
    survivors: tuple[str, ...]
    changed_paths: tuple[str, ...]
    changed_off_main: tuple[str, ...]
    scratch_globs: tuple[str, ...]
    probes: Mapping[str, str]
    work_complete_empty: bool = False
    review_harvested: bool = False
    work_complete_guarded: bool = False
    review_archive_available: bool = False
    work_complete_evidence: Mapping[str, Any] = field(default_factory=dict)
    review_artifacts_preserved: bool = False
    review_artifacts_evidence: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Disposition:
    kind: BranchDisposition
    reason: str
    evidence: dict[str, Any]
    branch_action: str
    work_item: str
    next_action: str
    rule: int


@dataclass(frozen=True)
class RuleMatch:
    kind: BranchDisposition
    reason: str
    work_item: str
    next_action: str
    evidence: Mapping[str, Any]


@dataclass(frozen=True)
class Rule:
    row: int
    name: str
    branch_action: str
    kinds: tuple[BranchDisposition, ...]
    when: Callable[[Snapshot], RuleMatch | None]


def _git(repo: str | Path, *args: str, timeout_s: float = _GIT_TIMEOUT_S) -> subprocess.CompletedProcess[str]:
    """Run a bounded git probe, converting timeout and OS errors to rc 124/127."""

    command = ["git", "-C", str(repo), *args]
    try:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="surrogateescape",
            check=False,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = exc.stderr.decode("utf-8", "replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        return subprocess.CompletedProcess(command, 124, "", stderr or "git timeout")
    except OSError as exc:
        return subprocess.CompletedProcess(command, 127, "", str(exc))


def _error_text(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stderr or result.stdout or f"git_failed:{result.returncode}").strip()


def _deadline_probe_timeout(timeout_s: float, deadline: float | None) -> float | None:
    if deadline is None:
        return timeout_s
    remaining_s = deadline - time.monotonic()
    return min(timeout_s, remaining_s) if remaining_s > 0 else None


def _is_sha40(value: str) -> bool:
    return len(value) == 40 and all(character in "0123456789abcdef" for character in value)


def collect_superseded_lane_git_facts(
    repo_root: Path,
    branch: str,
    base_sha: str,
    base_source: str | None,
    integration_refs: list[str],
    *,
    timeout_s: float,
    deadline: float | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Prove a superseded lane's own-commit count and, when empty, integration ancestry.

    Unknown counts, timeouts, missing tips, and indeterminate ancestry all fail
    closed so the retirement sweep keeps the lane for a later probe.
    """
    branch = branch.strip().removeprefix("refs/heads/")
    if not branch:
        return None, "superseded_branch_missing"
    try:
        probe_timeout = _deadline_probe_timeout(timeout_s, deadline)
        if probe_timeout is None:
            return None, "deadline_exceeded"
        tip_result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=probe_timeout,
        )
        if deadline is not None and time.monotonic() >= deadline:
            return None, "deadline_exceeded"
        if tip_result.returncode != 0:
            return None, "superseded_branch_tip_unavailable"
        tip = tip_result.stdout.strip().lower()
        if not _is_sha40(tip):
            return None, "superseded_branch_tip_unavailable"
        probe_timeout = _deadline_probe_timeout(timeout_s, deadline)
        if probe_timeout is None:
            return None, "deadline_exceeded"
        count_result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-list", "--count", f"{base_sha}..{tip}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=probe_timeout,
        )
        if deadline is not None and time.monotonic() >= deadline:
            return None, "deadline_exceeded"
        if count_result.returncode != 0:
            return None, "superseded_commit_count_unknown"
        own_commits = int(count_result.stdout.strip())
    except (OSError, subprocess.TimeoutExpired, ValueError):
        if deadline is not None and time.monotonic() >= deadline:
            return None, "deadline_exceeded"
        return None, "superseded_git_probe_failed"

    evidence: dict[str, Any] = {
        "base_sha": base_sha,
        "base_source": base_source,
        "tip": tip,
        "own_commits": own_commits,
    }
    if own_commits:
        return evidence, None
    refs = integration_refs or ["main"]
    for ref in refs:
        try:
            probe_timeout = _deadline_probe_timeout(timeout_s, deadline)
            if probe_timeout is None:
                return None, "deadline_exceeded"
            integration_result = subprocess.run(
                ["git", "-C", str(repo_root), "rev-parse", "--verify", f"{ref}^{{commit}}"],
                capture_output=True,
                text=True,
                check=False,
                timeout=probe_timeout,
            )
            if deadline is not None and time.monotonic() >= deadline:
                return None, "deadline_exceeded"
            if integration_result.returncode != 0:
                continue
            integration_tip = integration_result.stdout.strip().lower()
            if not _is_sha40(integration_tip):
                continue
            probe_timeout = _deadline_probe_timeout(timeout_s, deadline)
            if probe_timeout is None:
                return None, "deadline_exceeded"
            ancestry = subprocess.run(
                ["git", "-C", str(repo_root), "merge-base", "--is-ancestor", tip, integration_tip],
                capture_output=True,
                text=True,
                check=False,
                timeout=probe_timeout,
            )
            if deadline is not None and time.monotonic() >= deadline:
                return None, "deadline_exceeded"
        except (OSError, subprocess.TimeoutExpired):
            if deadline is not None and time.monotonic() >= deadline:
                return None, "deadline_exceeded"
            return None, "superseded_ancestry_unknown"
        if ancestry.returncode == 0:
            evidence.update(integration_ref=ref, integration_tip=integration_tip)
            return evidence, None
        if ancestry.returncode != 1:
            return None, "superseded_ancestry_unknown"
    return None, "superseded_not_on_integration"


def _short_branch_name(branch: str) -> str:
    return lane_reclaim._short_branch_name(branch.strip())


def _normal_branch(branch: object) -> str:
    value = str(branch or "").strip()
    return _short_branch_name(value)


def _parse_branch_tips(stdout: str) -> dict[str, tuple[str, int]] | None:
    branch_tips: dict[str, tuple[str, int]] = {}
    for line in stdout.splitlines():
        parts = line.split("\0")
        if len(parts) != 3 or not parts[0].startswith("refs/heads/"):
            return None
        short, sha, timestamp = parts
        try:
            committer_unix = int(timestamp)
        except (TypeError, ValueError):
            return None
        branch = _short_branch_name(short)
        if not branch or not _is_sha40(sha):
            return None
        branch_tips[branch] = (sha, committer_unix)
    return branch_tips


def collect_shared(
    repo: str | Path, *, integration_ref: str = "main", timeout_s: float = _GIT_TIMEOUT_S
) -> SharedReads:
    """Read repository-wide evidence once and pin the integration tip for this scan."""

    probes: dict[str, str] = {}
    integration_tip: str | None = None
    result = _git(repo, "rev-parse", "--verify", "--quiet", f"{integration_ref}^{{commit}}", timeout_s=timeout_s)
    if result.returncode == 0:
        candidate = result.stdout.strip().splitlines()
        if candidate and _is_sha40(candidate[0]):
            integration_tip = candidate[0]
        else:
            probes["integration_ref"] = "malformed_commit_sha"
    else:
        probes["integration_ref"] = _error_text(result)

    try:
        policy = load_landing_policy(repo)
    except Exception as exc:  # noqa: BLE001 - unreadable policy is a typed unknown
        policy = None
        probes["landing_policy"] = str(exc)
    if policy is None:
        probes.setdefault("landing_policy", "policy_missing_or_invalid")

    scan: ScanResult | None = None
    if integration_tip is not None:
        scan = scan_first_parent(repo, main_ref=integration_tip, timeout_s=timeout_s)
        if not scan.complete:
            probes["receipt_scan"] = scan.error or "scan_incomplete"

    branch_tips: dict[str, tuple[str, int]] = {}
    result = _git(
        repo,
        "for-each-ref",
        "--format=%(refname)%00%(objectname)%00%(committerdate:unix)",
        "refs/heads/",
        timeout_s=timeout_s,
    )
    if result.returncode == 0:
        parsed = _parse_branch_tips(result.stdout)
        if parsed is None:
            probes["branch_tip"] = "malformed_branch_tips"
        else:
            branch_tips = parsed
    else:
        probes["branch_tip"] = _error_text(result)

    worktrees: Mapping[str, tuple[str, ...]] | None
    try:
        observed_worktrees = lane_reclaim._checked_out_branches(Path(repo))
    except Exception as exc:  # noqa: BLE001 - failed enumeration is not an empty list
        observed_worktrees = None
        probes["worktree_list"] = str(exc)
    if observed_worktrees is None:
        worktrees = None
        probes.setdefault("worktree_list", "worktree_list_failed")
    else:
        worktrees = {ref: tuple(paths) for ref, paths in observed_worktrees.items()}

    return SharedReads(
        integration_ref=integration_ref,
        integration_tip=integration_tip,
        policy=policy,
        scan=scan,
        branch_tips=branch_tips,
        worktrees=worktrees,
        probes=probes,
    )


def _lane_rows_from_envelope(raw: object) -> tuple[tuple[dict[str, Any], ...] | None, str | None]:
    if not lane_reclaim._seam_envelope_ok(raw):
        if isinstance(raw, dict):
            data = raw.get("data")
            error = data.get("error") if isinstance(data, dict) else None
            return None, str(error or "lane_lookup_not_ok")
        return None, "lane_lookup_returned_non_object"
    data = raw.get("data")
    if not isinstance(data, dict):
        return None, "lane_lookup_data_not_object"
    lanes = data.get("lanes")
    returned = data.get("returned")
    total = data.get("total_matching")
    if (
        not isinstance(lanes, list)
        or any(not isinstance(row, dict) for row in lanes)
        or type(returned) is not int
        or type(total) is not int
        or returned != total
        or returned != len(lanes)
    ):
        return None, "lane_lookup_incomplete_or_malformed"
    projected = tuple(
        {key: row.get(key) for key in ("task_ref", "lane_id", "status", "lane_kind", "worktree_path", "branch_tip_sha")}
        for row in lanes
    )
    return projected, None


def _lane_lookup_read_only(task_ref: str, lane_id: str) -> object:
    return lane_reclaim.get_lane(lane_id=lane_id, task_ref=task_ref)


def _gate_lookup_read_only(gate_id: int) -> dict[str, Any]:
    """Return the handoff get_verified_test_by_id envelope using mode=ro."""

    try:
        with lane_reclaim._scan_read_connection() as conn:
            row = conn.execute("SELECT * FROM verified_tests WHERE id = ?", (gate_id,)).fetchone()
        payload = dict(row) if row is not None else None
        if payload is not None:
            payload["passed"] = bool(payload.get("passed"))
        return {
            "ok": True,
            "tool": "get_verified_test_by_id",
            "data": {"test_id": gate_id, "found": payload is not None, "test": payload},
        }
    except Exception as exc:  # noqa: BLE001 - preserve the verified-test seam failure
        return {
            "ok": False,
            "tool": "get_verified_test_by_id",
            "data": {"error": str(exc)},
        }


def _probe_failure(verdict: ReceiptVerdict) -> bool:
    return verdict.failed_check in {"v1_probe_failed", "v2_probe_failed", "v3_lookup_failed", "v4_lookup_failed"} or (
        verdict.detail == "timeout"
    )


def _collect_landing(
    repo: str | Path,
    *,
    tip: str,
    carrier: CarrierResult,
    integration_tip: str,
    lane_lookup: Callable[[str, str], object],
    gate_lookup: Callable[[int], object],
    timeout_s: float,
) -> tuple[ReceiptVerdict | None, tuple[ReceiptVerdict, ...], str | None]:
    if carrier.lineage != "merge_carried":
        return None, (), None
    position = carrier
    unverified: list[ReceiptVerdict] = []
    for _depth in range(4):
        if position.error:
            return None, tuple(unverified), position.error
        if position.lineage != "merge_carried" or not position.carrier:
            break
        shown = _git(repo, "show", "-s", "--format=%P%x00%B", position.carrier, timeout_s=timeout_s)
        if shown.returncode != 0:
            return None, tuple(unverified), _error_text(shown)
        parents_and_message = shown.stdout.split("\0", 1)
        if len(parents_and_message) != 2:
            return None, tuple(unverified), "carrier_message_malformed"
        parents = parents_and_message[0].split()
        parsed = parse_landing_receipt(parents_and_message[1])
        if getattr(parsed, "tip", None) == tip:
            verdict = verify_receipt(
                repo,
                position.carrier,
                main_ref=integration_tip,
                lane_lookup=lane_lookup,
                gate_lookup=gate_lookup,
            )
            if verdict.verified:
                return verdict, tuple(unverified), None
            if _probe_failure(verdict):
                return None, tuple(unverified), verdict.detail or verdict.failed_check or "receipt_probe_failed"
            unverified.append(verdict)
            return None, tuple(unverified), None
        if len(parents) > 2 or len(parents) < 2:
            break
        position = find_carrier(repo, tip, main_ref=parents[1], timeout_s=timeout_s)
        if position.error:
            return None, tuple(unverified), position.error
        if not position.contained or position.lineage != "merge_carried":
            break
    return None, tuple(unverified), None


def _failure_probe(verdict: ReceiptVerdict) -> bool:
    return _probe_failure(verdict)


def _paths_from_diff(
    repo: str | Path, *args: str, timeout_s: float, review_blobs: set[str] | None = None
) -> tuple[tuple[str, ...] | None, str | None]:
    if review_blobs is not None:
        args = tuple("--raw" if arg == "--name-only" else arg for arg in args)
    result = _git(repo, *args, timeout_s=timeout_s)
    if result.returncode != 0:
        return None, _error_text(result)
    if review_blobs is not None:
        # Raw diff records carry the tip mode as well as the path. Blob modes
        # prove presence at this exact tip; deletion/submodule modes do not.
        fields = result.stdout.split("\0")
        if fields[-1] != "" or (len(fields) - 1) % 2:
            return None, "malformed_raw_diff"
        paths = []
        blobs = set()
        for index in range(0, len(fields) - 1, 2):
            header, path = fields[index : index + 2]
            metadata = header.split()
            if len(metadata) != 5 or not metadata[0].startswith(":") or metadata[4] not in {"A", "D", "M", "T"}:
                return None, "malformed_raw_diff"
            path = path.removeprefix("./")
            paths.append(path)
            if metadata[1] in {"100644", "100755", "120000"}:
                blobs.add(path)
        review_blobs.update(blobs)
        return tuple(paths), None
    return tuple(path.removeprefix("./") for path in result.stdout.split("\0") if path), None


def _review_terminal_pass(
    repo: str | Path,
    lane_rows: tuple[dict[str, Any], ...],
    pass_record: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
    """Resolve the latest pass and prove it terminal with a known outcome.

    Returns ``(latest_pass, outcome, {})`` on success, else ``(None, "", evidence)``
    where ``evidence`` types why the pass cannot authorize retirement.
    """
    try:
        from workbay_orchestrator_mcp.orchestration.offload_pass import (  # noqa: PLC0415
            PASS_OUTCOMES,
            _latest_pass_payload,
        )
        from workbay_orchestrator_mcp.orchestration.offload_pass_index import lane_pass_records  # noqa: PLC0415

        if pass_record is not None:
            expected_keys = {(str(row.get("task_ref") or ""), str(row.get("lane_id") or "")) for row in lane_rows}
            pass_key = (str(pass_record.get("task_ref") or ""), str(pass_record.get("lane_id") or ""))
            if pass_key not in expected_keys:
                return None, "", {"reason": "latest_pass_identity_mismatch"}
            latest_pass = pass_record
        else:
            from workbay_handoff_mcp.config import RuntimeConfig  # noqa: PLC0415
            from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError, get_runtime_config  # noqa: PLC0415

            records = []
            try:
                runtime = get_runtime_config()
            except RuntimeNotConfiguredError:
                runtime = None
            # The configured primary repository already pins the state root.
            # Resolve linked/other repositories normally; never borrow their ledger.
            state_dir = (
                runtime.state_dir
                if runtime is not None and runtime.workspace_root == Path(repo).resolve()
                else RuntimeConfig.for_repo(Path(repo)).state_dir
            )
            lane_keys = {(str(row.get("task_ref") or ""), str(row.get("lane_id") or "")) for row in lane_rows}
            for task_ref, lane_id in lane_keys:
                indexed = lane_pass_records(state_dir, task_ref=task_ref, lane_id=lane_id)
                if indexed.status != "ok":
                    return None, "", {"reason": "review_pass_index_unavailable", "detail": indexed.status}
                records.extend(indexed.records)
            latest_pass = _latest_pass_payload(records)
        if not isinstance(latest_pass, dict):
            return None, "", {}
        if latest_pass.get("status") != "done":
            status = latest_pass.get("status")
            return None, "", {"nonterminal_pass_outcome": str(status or "<missing>")}
        result = latest_pass.get("result")
        outcome = result.get("outcome") if isinstance(result, dict) else None

        if not isinstance(outcome, str) or not outcome or outcome not in PASS_OUTCOMES:
            return None, "", {"unknown_pass_outcome": _pass_outcome_detail(outcome)}
        from .pass_terminality import pass_outcome_is_terminal  # noqa: PLC0415

        if not pass_outcome_is_terminal(outcome):
            return None, "", {"nonterminal_pass_outcome": outcome}
        return latest_pass, outcome, {}
    except Exception as exc:  # noqa: BLE001 — an unreadable pass ledger is not proof
        return None, "", {"reason": "review_pass_lookup_failed", "detail": type(exc).__name__}


def _harvest_receipt_refusal(harvest: Mapping[str, Any]) -> str | None:
    """Honor an authoritative run receipt and require no unrecorded findings."""
    if "review_run_receipt" in harvest:
        receipt = harvest["review_run_receipt"]
        if not isinstance(receipt, dict) or not isinstance(receipt.get("status"), str):
            return "receipt_malformed:review_run_receipt"
        if receipt["status"] != "recorded":
            return f"review_run_receipt_{receipt['status']}"
    unrecorded_ids = harvest.get("unrecorded_ids")
    if not isinstance(unrecorded_ids, list):
        return "receipt_malformed:unrecorded_ids"
    if unrecorded_ids or harvest.get("reason") in {"record_failed", "record_pending", "harvest_source_missing"}:
        return "review_unrecorded_findings"
    return None


# Canonical full object id: lowercase SHA-1 or SHA-256 hex, the contract offload_pass and lane_reaping share.
_FULL_OBJECT_ID_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")


def _is_full_object_id(value: object) -> bool:
    return isinstance(value, str) and _FULL_OBJECT_ID_RE.fullmatch(value) is not None


def _review_current_identity_matches(
    branch: str,
    latest_pass: Mapping[str, Any],
    lane_rows: tuple[dict[str, Any], ...],
    authoritative_rows: tuple[dict[str, Any], ...],
) -> bool:
    """Bind caller evidence to the current durable owner, accepting local ref spelling."""
    def normalized(value: object) -> str:
        return value.strip().removeprefix("refs/heads/") if isinstance(value, str) else ""

    current = normalized(branch)
    key = (latest_pass.get("task_ref"), latest_pass.get("lane_id"))
    return bool(
        current
        and all(isinstance(value, str) and value for value in key)
        and normalized(latest_pass.get("branch")) == current
        and len(authoritative_rows) == 1
        and authoritative_rows[0].get("lane_kind") == "review"
        and normalized(authoritative_rows[0].get("branch")) == current
        and all(
            (row.get("task_ref"), row.get("lane_id")) == key
            and ("branch" not in row or normalized(row["branch"]) == current)
            for row in lane_rows
        )
    )


def _legacy_review_harvest_proof(
    repo: str | Path,
    conn: Any,
    branch: str,
    latest_pass: Mapping[str, Any],
    lane_rows: tuple[dict[str, Any], ...],
    deadline: float | None,
) -> str | None:
    """Prove legacy custody from the current owner and immutable SQL/Git source.

    Legacy rows have no report manifest. Their full source commit can supply
    equivalent coverage only when the entire review subtree is unchanged.
    This reads evidence only; it neither upgrades receipts nor repairs state.
    """
    reviewed_tip = latest_pass.get("tip_sha")
    pass_id = latest_pass.get("pass_id")
    if not _is_full_object_id(reviewed_tip) or not isinstance(pass_id, str) or not pass_id:
        return "review_no_harvest_receipt"
    try:
        probe_timeout = _deadline_probe_timeout(5.0, deadline)
        if probe_timeout is None:
            return "deadline_exceeded"
        # Bound legacy SQL lock waits by the same budget as the Git proofs.
        conn.execute(f"PRAGMA busy_timeout = {max(0, int(probe_timeout * 1000))}")
        authoritative_rows = tuple(dict(row) for row in conn.execute(
            "SELECT task_ref, lane_id, branch, lane_kind FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?",
            (latest_pass.get("task_ref"), latest_pass.get("lane_id")),
        ).fetchall())
        if deadline is not None and time.monotonic() >= deadline:
            return "deadline_exceeded"
        if not _review_current_identity_matches(branch, latest_pass, lane_rows, authoritative_rows):
            return "review_current_identity_mismatch"
        probe_timeout = _deadline_probe_timeout(5.0, deadline)
        if probe_timeout is None:
            return "deadline_exceeded"
        conn.execute(f"PRAGMA busy_timeout = {max(0, int(probe_timeout * 1000))}")
        normalized_branch = branch.strip().removeprefix("refs/heads/")
        # A named finding must belong to this lane too. A subject_path alone
        # cannot override a conflicting branch recorded on a review run.
        finding = conn.execute(
            "SELECT 1 FROM review_findings WHERE task_ref = ? AND lane_id = ? AND commit_sha = ? "
            "AND (review_run_id = ? OR review_run_id IS NULL) LIMIT 1",
            (latest_pass.get("task_ref"), latest_pass.get("lane_id"), reviewed_tip, pass_id),
        ).fetchone()
        probe_timeout = _deadline_probe_timeout(5.0, deadline)
        if probe_timeout is None:
            return "deadline_exceeded"
        conn.execute(f"PRAGMA busy_timeout = {max(0, int(probe_timeout * 1000))}")
        review_run = conn.execute(
            "SELECT branch FROM review_runs WHERE task_ref = ? AND review_run_id = ? AND commit_sha = ?",
            (latest_pass.get("task_ref"), pass_id, reviewed_tip),
        ).fetchone()
        if deadline is not None and time.monotonic() >= deadline:
            return "deadline_exceeded"
        run_matches = (
            review_run is not None
            and isinstance(review_run["branch"], str)
            and review_run["branch"].strip().removeprefix("refs/heads/") == normalized_branch
        )
        if finding is None and not run_matches:
            return "review_no_harvest_receipt"
        return _legacy_review_current_report_proof(repo, normalized_branch, reviewed_tip, deadline)
    except Exception:  # noqa: BLE001 — unavailable custody never authorizes retirement
        if deadline is not None and time.monotonic() >= deadline:
            return "deadline_exceeded"
        return "review_legacy_proof_unavailable"


def _legacy_review_current_report_proof(
    repo: str | Path, branch: str, reviewed_tip: str, deadline: float | None,
) -> str | None:
    """Compare complete immutable review trees, including removed/reverted reports."""
    probe_timeout = _deadline_probe_timeout(5.0, deadline)
    if probe_timeout is None:
        return "deadline_exceeded"
    current = _git(repo, "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}", timeout_s=probe_timeout)
    if deadline is not None and time.monotonic() >= deadline:
        return "deadline_exceeded"
    current_tip = current.stdout.strip()
    if current.returncode != 0 or not _is_full_object_id(current_tip):
        return "review_current_identity_unavailable"
    for args in (
        ("merge-base", "--is-ancestor", reviewed_tip, current_tip),
        ("diff", "--quiet", "--no-ext-diff", "--no-textconv", reviewed_tip, current_tip, "--", "docs/reviews/"),
    ):
        probe_timeout = _deadline_probe_timeout(5.0, deadline)
        if probe_timeout is None:
            return "deadline_exceeded"
        proof = _git(repo, *args, timeout_s=probe_timeout)
        if deadline is not None and time.monotonic() >= deadline:
            return "deadline_exceeded"
        if proof.returncode != 0:
            return "review_current_report_identity_mismatch"
    return None


def _harvested_report_receipt(
    repo: str | Path, latest_pass: Mapping[str, Any], *, deadline: float | None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Read the producer's durable native receipt, never reconstruct a missing one."""
    import json
    import os
    import stat

    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError, get_runtime_config
    from .offload_pass import _review_identity_matches

    if deadline is not None and time.monotonic() >= deadline:
        return None, "deadline_exceeded"
    pass_id = latest_pass.get("pass_id")
    if not isinstance(pass_id, str) or not pass_id or "/" in pass_id or "\\" in pass_id:
        return None, "review_report_receipt_identity_mismatch"
    try:
        try:
            runtime = get_runtime_config()
        except RuntimeNotConfiguredError:
            runtime = None
        state_dir = (
            runtime.state_dir
            if runtime is not None and runtime.workspace_root == Path(repo).resolve()
            else RuntimeConfig.for_repo(Path(repo)).state_dir
        )
        # Nonblocking open rejects pipes without waiting for a writer. Refuse
        # symlink replacements; read one atomic-write inode, with a bounded size.
        fd = os.open(state_dir / f"review-receipt-{pass_id}.json", os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            limit = 16 * 1024 * 1024
            if not stat.S_ISREG(info.st_mode) or not info.st_mode & 0o444 or info.st_size > limit:
                return None, "review_report_receipt_unavailable"
            raw = stream.read(limit + 1)
        if deadline is not None and time.monotonic() >= deadline:
            return None, "deadline_exceeded"
        if len(raw) > limit:
            return None, "review_report_receipt_unavailable"
        receipt = json.loads(raw)
    except (OSError, ValueError):
        return None, "review_report_receipt_unavailable"
    if not isinstance(receipt, dict):
        return None, "review_report_receipt_malformed"
    identity, kwargs, saved_harvest = receipt.get("identity"), receipt.get("kwargs"), receipt.get("harvest")
    if (
        receipt.get("status") != "recorded"
        or not isinstance(receipt.get("result"), dict)
        or "report" not in receipt
        or not (receipt.get("report") is None or isinstance(receipt["report"], (dict, str)))
        or not isinstance(receipt.get("operation_token"), str)
        or not receipt["operation_token"]
        or not isinstance(identity, dict)
        or not isinstance(kwargs, dict)
        or not isinstance(saved_harvest, dict)
        or not isinstance(identity.get("branch"), str)
        or not isinstance(kwargs.get("session"), str)
        or not isinstance(kwargs.get("salvage_unstructured"), bool)
        or _harvest_receipt_refusal(saved_harvest) is not None
    ):
        return None, "review_report_receipt_malformed"
    if (
        not _review_identity_matches(identity, dict(latest_pass))
        or identity["branch"].strip().removeprefix("refs/heads/")
        != str(latest_pass.get("branch") or "").strip().removeprefix("refs/heads/")
        or any(kwargs.get(key) != latest_pass.get(key) for key in ("task_ref", "lane_id"))
        or not isinstance(kwargs.get("orchestrator_root"), str)
        or Path(kwargs["orchestrator_root"]).resolve() != Path(repo).resolve()
    ):
        return None, "review_report_receipt_identity_mismatch"
    return receipt, None


def _harvested_report_probe(
    repo: str | Path, *args: str, deadline: float | None,
) -> tuple[subprocess.CompletedProcess[str] | None, str | None]:
    """Every source/manifest probe spends the same remaining retirement budget."""
    timeout = _deadline_probe_timeout(5.0, deadline)
    if timeout is None:
        return None, "deadline_exceeded"
    try:
        result = _git(repo, "--no-replace-objects", "--literal-pathspecs", *args, timeout_s=timeout)
    except (OSError, subprocess.TimeoutExpired):
        result = None
    if deadline is not None and time.monotonic() >= deadline:
        return None, "deadline_exceeded"
    if result is None or result.returncode != 0:
        return None, "review_report_source_unavailable"
    return result, None


def _harvested_report_manifest(
    repo: str | Path, latest_pass: Mapping[str, Any], *, deadline: float | None,
) -> tuple[tuple[str, ...] | None, str | None]:
    """Use the immutable source tree's full blob manifest, not today's net diff.

    Native receipts already pin full base/tip IDs. Git's raw diff supplies each
    harvested report path and its content object ID without a new receipt schema.
    Older legacy proofs lacking a base conservatively cover the entire tip tree.
    """
    tip, base = latest_pass.get("tip_sha"), latest_pass.get("base_sha")
    if not _is_full_object_id(tip):
        return None, "review_report_source_unavailable"
    if _is_full_object_id(base):
        source, reason = _harvested_report_probe(
            repo, "diff", "--raw", "--no-abbrev", "--no-renames", "--no-ext-diff", "--no-textconv",
            "-z", base, tip, "--", "docs/reviews/", deadline=deadline,
        )
        if source is None:
            return None, reason
        fields = source.stdout.split("\0")
        if fields[-1] or (len(fields) - 1) % 2:
            return None, "review_report_source_malformed"
        entries = []
        for index in range(0, len(fields) - 1, 2):
            header, path = fields[index:index + 2]
            metadata = header.split()
            if len(metadata) != 5 or not metadata[0].startswith(":") or metadata[4] not in {"A", "D", "M", "T"}:
                return None, "review_report_source_malformed"
            entries.append((metadata[1], metadata[3], path))
    else:
        source, reason = _harvested_report_probe(
            repo, "ls-tree", "-r", "-z", tip, "--", "docs/reviews/", deadline=deadline,
        )
        if source is None:
            return None, reason
        entries = []
        for entry in source.stdout.split("\0"):
            if not entry:
                continue
            metadata, separator, path = entry.partition("\t")
            fields = metadata.split()
            if not separator or len(fields) != 3:
                return None, "review_report_source_malformed"
            entries.append((fields[0], fields[2], path))
    paths = []
    for mode, object_id, path in entries:
        if not _is_review_doc_path(path):
            continue
        if mode not in {"100644", "100755", "120000"} or not _is_full_object_id(object_id) or set(object_id) == {"0"}:
            return None, "review_report_source_missing"
        blob, reason = _harvested_report_probe(repo, "cat-file", "-t", object_id, deadline=deadline)
        if blob is None:
            return None, reason
        if blob.stdout.strip() != "blob":
            return None, "review_report_source_missing"
        paths.append(path)
    if not paths:
        return None, "review_report_source_missing"
    return tuple(sorted(set(paths))), None


def _review_harvest_snapshot_fact(
    repo: str | Path,
    *,
    branch: str,
    lane_rows: tuple[dict[str, Any], ...],
    changed_paths: tuple[str, ...],
    scratch_globs: tuple[str, ...],
    pass_record: dict[str, Any] | None = None,
    deadline: float | None = None,
    review_blobs: set[str] | None = None,
) -> tuple[bool, dict[str, Any]]:
    """Recover review-preservation evidence for the terminal retire classifier."""
    if not lane_rows or any(
        row.get("lane_kind") != "review" or row.get("status") not in _TERMINAL_STATUSES for row in lane_rows
    ):
        return False, {}
    product_paths = tuple(path for path in changed_paths if not _is_scratch(path, (*scratch_globs, ".review/**")))
    if not product_paths:
        return False, {"reason": "review_report_missing"}
    if any(not _is_review_doc_path(path) for path in product_paths):
        return False, {"reason": "review_product_path_outside_docs"}
    # The pass proves the review finished; settle it before spending git probes.
    latest_pass, outcome, pass_evidence = _review_terminal_pass(repo, lane_rows, pass_record)
    if latest_pass is None:
        return False, pass_evidence
    # A deleted report appears in the diff too; prove each report is a blob
    # preserved at the branch tip before authorizing worktree removal.
    for path in product_paths:
        if review_blobs is not None:
            if path not in review_blobs:
                return False, {"reason": "review_report_missing"}
            continue
        probe_timeout = _deadline_probe_timeout(5.0, deadline)
        if probe_timeout is None:
            return False, {"reason": "deadline_exceeded"}
        try:
            report = _git(repo, "cat-file", "-t", f"{branch}:{path}", timeout_s=probe_timeout)
        except (OSError, subprocess.TimeoutExpired):
            if deadline is not None and time.monotonic() >= deadline:
                return False, {"reason": "deadline_exceeded"}
            return False, {"reason": "review_report_probe_failed"}
        if deadline is not None and time.monotonic() >= deadline:
            return False, {"reason": "deadline_exceeded"}
        if report.returncode != 0 or report.stdout.strip() != "blob":
            return False, {"reason": "review_report_missing"}

    try:
        from workbay_orchestrator_mcp.orchestration.lane_reclaim import _scan_read_connection  # noqa: PLC0415

        result = latest_pass.get("result")
        harvest = result.get("findings_harvest") if isinstance(result, dict) else None
        if isinstance(harvest, dict):
            refusal = _harvest_receipt_refusal(harvest)
            if refusal is not None:
                return False, {"reason": refusal}
        # The terminal-pass parser already proved a known, finished outcome. The producer also harvests
        # failed/stopped passes without changing their outcome. Archiving their preserved docs is not
        # quality approval: require the same durable harvest evidence, retaining the actual outcome.
        # A missing harvest would skip the unrecorded-findings guard.
        if not isinstance(harvest, dict):
            return False, {"reason": "review_no_harvest_receipt"}
        pass_id = str(latest_pass.get("pass_id") or "")
        receipt_found = False
        with _scan_read_connection() as conn:
            if isinstance(harvest, dict) and "review_run_receipt" in harvest:
                review_run = conn.execute(
                    "SELECT branch, commit_sha FROM review_runs WHERE task_ref = ? AND review_run_id = ?",
                    (str(latest_pass.get("task_ref") or ""), pass_id),
                ).fetchone()
                if review_run is None:
                    return False, {"reason": "review_no_harvest_receipt"}
                pass_branch = latest_pass.get("branch")
                # offload_pass records the pre-exec HEAD as the pass's tip_sha; only a full object id can bind it.
                pass_tip = latest_pass.get("tip_sha")
                if (
                    not pass_id
                    or not isinstance(pass_branch, str)
                    or not pass_branch
                    or not _is_full_object_id(pass_tip)
                    or not isinstance(review_run["branch"], str)
                    or review_run["branch"].strip().removeprefix("refs/heads/")
                    != pass_branch.strip().removeprefix("refs/heads/")
                    or review_run["commit_sha"] != pass_tip
                ):
                    return False, {"reason": "review_run_receipt_identity_mismatch"}
                # Bind the receipt to current lane ownership, not just the historical pass.
                authoritative_rows = tuple(dict(row) for row in conn.execute(
                    "SELECT branch, lane_kind FROM worktree_lanes WHERE task_ref = ? AND lane_id = ?",
                    (latest_pass.get("task_ref"), latest_pass.get("lane_id")),
                ).fetchall())
                if not _review_current_identity_matches(branch, latest_pass, lane_rows, authoritative_rows):
                    return False, {"reason": "review_current_identity_mismatch"}
                receipt_found = True
            else:
                legacy_refusal = _legacy_review_harvest_proof(repo, conn, branch, latest_pass, lane_rows, deadline)
                if legacy_refusal is not None:
                    return False, {"reason": legacy_refusal}
                # Legacy path: harvests without review_run_receipt retain the older lookup, bound to this
                # pass and its reviewed tip. The completion gate above already proved this pass finished.
                pass_tip = latest_pass.get("tip_sha")
                reviewed_tip = pass_tip if _is_full_object_id(pass_tip) else None
                if reviewed_tip is None or not pass_id:
                    return False, {"reason": "review_no_harvest_receipt"}
                for row in lane_rows:
                    task_ref = str(row.get("task_ref") or "")
                    lane_id = str(row.get("lane_id") or "")
                    # A finding names this pass on the reviewed tip, or is unnamed on the reviewed tip.
                    # A named finding on another commit, and lane_id alone, match an earlier pass.
                    probe_timeout = _deadline_probe_timeout(5.0, deadline)
                    if probe_timeout is None:
                        return False, {"reason": "deadline_exceeded"}
                    conn.execute(f"PRAGMA busy_timeout = {max(0, int(probe_timeout * 1000))}")
                    finding = conn.execute(
                        "SELECT 1 FROM review_findings WHERE task_ref = ? "
                        "AND ((review_run_id = ? AND commit_sha = ?) "
                        "OR (lane_id = ? AND review_run_id IS NULL AND commit_sha = ?)) "
                        "LIMIT 1",
                        (task_ref, pass_id, reviewed_tip, lane_id, reviewed_tip),
                    ).fetchone()
                    if deadline is not None and time.monotonic() >= deadline:
                        return False, {"reason": "deadline_exceeded"}
                    if finding is not None:
                        receipt_found = True
                        break
                    # A run row from an earlier pass on the same branch never authorizes this pass.
                    probe_timeout = _deadline_probe_timeout(5.0, deadline)
                    if probe_timeout is None:
                        return False, {"reason": "deadline_exceeded"}
                    conn.execute(f"PRAGMA busy_timeout = {max(0, int(probe_timeout * 1000))}")
                    normalized_branch = branch.strip().removeprefix("refs/heads/")
                    review_run = conn.execute(
                        "SELECT 1 FROM review_runs WHERE task_ref = ? AND review_run_id = ? "
                        "AND commit_sha = ? AND (branch = ? OR branch = ? OR subject_path = ?) LIMIT 1",
                        (task_ref, pass_id, reviewed_tip, normalized_branch, f"refs/heads/{normalized_branch}", normalized_branch),
                    ).fetchone()
                    if deadline is not None and time.monotonic() >= deadline:
                        return False, {"reason": "deadline_exceeded"}
                    if review_run is not None:
                        receipt_found = True
                        break
        if not receipt_found:
            return False, {"reason": "review_no_harvest_receipt"}
        # Authenticate current report bytes against the receipt's immutable source.
        # Metadata descendants are compatible only when the harvested reports are unchanged.
        # Git probes run after releasing the read connection; each consumes the remaining deadline.
        if "review_run_receipt" in harvest:
            receipt, reason = _harvested_report_receipt(repo, latest_pass, deadline=deadline)
            if receipt is None:
                return False, {"reason": reason}
            if not _is_full_object_id(latest_pass.get("base_sha")):
                return False, {"reason": "review_report_source_unavailable"}
        source_paths, reason = _harvested_report_manifest(repo, latest_pass, deadline=deadline)
        if source_paths is None:
            return False, {"reason": reason}
        # Added-then-deleted or reverted reports vanish from changed_paths.
        # Coverage therefore comes from the harvested source AND the current tree.
        product_paths = tuple(sorted(set(product_paths).union(source_paths)))
        normalized_branch = branch.strip().removeprefix("refs/heads/")
        current, reason = _harvested_report_probe(
            repo, "rev-parse", "--verify", f"refs/heads/{normalized_branch}^{{commit}}", deadline=deadline,
        )
        if current is None or not _is_full_object_id(current.stdout.strip()):
            return False, {"reason": reason or "review_current_identity_unavailable"}
        current_tip = current.stdout.strip()
        for args in (
            ("merge-base", "--is-ancestor", pass_tip, current_tip),
            ("diff", "--quiet", "--no-ext-diff", "--no-textconv", pass_tip, current_tip, "--", *product_paths),
        ):
            identity, reason = _harvested_report_probe(repo, *args, deadline=deadline)
            if identity is None:
                return False, {"reason": "deadline_exceeded" if reason == "deadline_exceeded" else "review_current_report_identity_mismatch"}
        return True, {
            "pass_id": pass_id,
            "pass_outcome": outcome,
            "harvest_receipt": True,
            "product_paths": list(product_paths),
            "scratch_paths": [path for path in changed_paths if path not in product_paths],
        }
    except Exception:  # noqa: BLE001 — an unreadable pass or handoff ledger is not proof
        return False, {}


def _review_artifacts_receipt(
    repo: str | Path,
    lane_rows: tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Find durable review outcome evidence for one of the terminal lane rows."""
    try:
        with lane_reclaim._scan_read_connection() as conn:
            for row in lane_rows:
                task_ref = str(row.get("task_ref") or "")
                lane_id = str(row.get("lane_id") or "")
                if not task_ref or not lane_id:
                    continue
                finding = conn.execute(
                    "SELECT 1 FROM review_findings WHERE task_ref = ? AND lane_id = ? LIMIT 1",
                    (task_ref, lane_id),
                ).fetchone()
                if finding is not None:
                    return {
                        "kind": "review_findings",
                        "task_ref": task_ref,
                        "lane_id": lane_id,
                    }, {}
    except Exception as exc:  # noqa: BLE001 — a receipt read failure is not absence
        return None, {"reason": "probe_failed:receipt_lookup", "detail": type(exc).__name__}

    lookup_failures: list[dict[str, Any]] = []
    for row in lane_rows:
        if not isinstance(row.get("task_ref"), str) or not row.get("task_ref"):
            continue
        if not isinstance(row.get("lane_id"), str) or not row.get("lane_id"):
            continue
        latest_pass, outcome, pass_evidence = _review_terminal_pass(repo, (row,), None)
        if latest_pass is None:
            reason = pass_evidence.get("reason")
            if reason in {"review_pass_index_unavailable", "review_pass_lookup_failed"}:
                lookup_failures.append(dict(pass_evidence))
            continue
        result = latest_pass.get("result")
        harvest = result.get("findings_harvest") if isinstance(result, dict) else None
        if not isinstance(harvest, dict):
            continue
        refusal = _harvest_receipt_refusal(harvest)
        if refusal == "receipt_malformed:unrecorded_ids":
            return None, {"reason": refusal}
        if refusal is not None:
            continue
        return {
            "kind": "terminal_pass",
            "task_ref": str(row["task_ref"]),
            "lane_id": str(row["lane_id"]),
            "pass_id": str(latest_pass.get("pass_id") or ""),
            "pass_outcome": outcome,
        }, {}
    if lookup_failures:
        return None, {"reason": "probe_failed:pass_lookup", "detail": lookup_failures[0]}
    return None, {"reason": "no_receipt"}


def _review_artifacts_snapshot_fact(
    repo: str | Path,
    *,
    branch: str,
    tip: str | None,
    integration_ref: str,
    lane_rows: tuple[dict[str, Any], ...],
    timeout_s: float,
) -> tuple[bool, dict[str, Any]]:
    """Prove that terminal lane commits contain only preserved review artifacts."""
    if not lane_rows or any(row.get("status") not in _TERMINAL_STATUSES for row in lane_rows):
        return False, {"reason": "non_terminal_row"}

    raw_bases = [row.get("branch_tip_sha") for row in lane_rows]
    if any(not isinstance(value, str) or not value for value in raw_bases) or len(set(raw_bases)) != 1:
        return False, {"reason": "base_disagrees", "base_shas": raw_bases}
    raw_base = raw_bases[0]
    assert isinstance(raw_base, str)
    if len(raw_base) != 40 or any(character not in "0123456789abcdefABCDEF" for character in raw_base):
        return False, {"reason": "base_not_40_hex", "base_sha": raw_base}
    base_sha = raw_base.lower()
    evidence: dict[str, Any] = {"base_sha": base_sha, "base_source": "lane_rows.branch_tip_sha"}
    if tip is None or not _is_sha40(tip):
        return False, {**evidence, "reason": "probe_failed:candidate_tip"}
    if base_sha == tip:
        return False, {**evidence, "tip": tip, "reason": "base_equals_tip"}

    try:
        ancestor = _git(repo, "merge-base", "--is-ancestor", base_sha, tip, timeout_s=timeout_s)
    except Exception as exc:  # noqa: BLE001 — failed Git probes are typed refusals
        return False, {**evidence, "reason": "probe_failed:base_ancestor", "detail": type(exc).__name__}
    if ancestor.returncode == 1:
        return False, {**evidence, "tip": tip, "reason": "base_not_ancestor"}
    if ancestor.returncode != 0:
        return False, {
            **evidence,
            "reason": "probe_failed:base_ancestor",
            "detail": _error_text(ancestor),
        }

    try:
        count_result = _git(
            repo,
            "rev-list",
            "--count",
            f"--max-count={_MAX_REVIEW_ARTIFACT_COMMITS + 1}",
            f"{base_sha}..{tip}",
            timeout_s=timeout_s,
        )
    except Exception as exc:  # noqa: BLE001 — failed Git probes are typed refusals
        return False, {**evidence, "reason": "probe_failed:commit_count", "detail": type(exc).__name__}
    if count_result.returncode != 0:
        return False, {**evidence, "reason": "probe_failed:commit_count", "detail": _error_text(count_result)}
    try:
        commit_count = int(count_result.stdout.strip())
    except ValueError:
        return False, {**evidence, "reason": "probe_failed:commit_count", "detail": "malformed_count"}
    if commit_count > _MAX_REVIEW_ARTIFACT_COMMITS:
        return False, {
            **evidence,
            "reason": "too_many_commits",
            "commit_count": commit_count,
            "commit_limit": _MAX_REVIEW_ARTIFACT_COMMITS,
        }
    if commit_count < 1:
        return False, {**evidence, "reason": "base_not_ancestor"}

    try:
        history = _git(
            repo,
            "rev-list",
            "--parents",
            "--reverse",
            f"{base_sha}..{tip}",
            timeout_s=timeout_s,
        )
    except Exception as exc:  # noqa: BLE001 — failed Git probes are typed refusals
        return False, {**evidence, "reason": "probe_failed:commit_history", "detail": type(exc).__name__}
    if history.returncode != 0:
        return False, {**evidence, "reason": "probe_failed:commit_history", "detail": _error_text(history)}
    commits: list[str] = []
    for line in history.stdout.splitlines():
        parts = line.split()
        if not parts or not _is_sha40(parts[0]):
            return False, {**evidence, "reason": "probe_failed:commit_history", "detail": "malformed_history"}
        if len(parts) > 2:
            return False, {**evidence, "reason": "merge_commit", "commit": parts[0]}
        commits.append(parts[0])
    if len(commits) != commit_count:
        return False, {**evidence, "reason": "probe_failed:commit_history", "detail": "commit_count_changed"}

    artifact_paths: list[str] = []
    artifact_path_total = 0
    for commit in commits:
        try:
            diff = _git(
                repo,
                "diff-tree",
                "--no-commit-id",
                "-r",
                "--raw",
                "-z",
                "--find-renames",
                f"{commit}^",
                commit,
                timeout_s=timeout_s,
            )
        except Exception as exc:  # noqa: BLE001 — failed Git probes are typed refusals
            return False, {**evidence, "reason": "probe_failed:commit_paths", "detail": type(exc).__name__}
        if diff.returncode != 0:
            return False, {**evidence, "reason": "probe_failed:commit_paths", "detail": _error_text(diff)}
        fields = diff.stdout.split("\0")
        if not fields or fields[-1] != "":
            return False, {**evidence, "reason": "probe_failed:commit_paths", "detail": "malformed_raw_diff"}
        index = 0
        while index < len(fields) - 1:
            header = fields[index]
            index += 1
            metadata = header.split()
            if len(metadata) != 5 or not metadata[0].startswith(":"):
                return False, {**evidence, "reason": "probe_failed:commit_paths", "detail": "malformed_raw_diff"}
            status = metadata[4]
            path_count = 2 if status.startswith(("R", "C")) else 1
            if index + path_count > len(fields) - 1:
                return False, {**evidence, "reason": "probe_failed:commit_paths", "detail": "malformed_raw_diff"}
            paths = [fields[index + offset].removeprefix("./") for offset in range(path_count)]
            index += path_count
            outside = [path for path in paths if not path.startswith(_REVIEW_DOC_PREFIX)]
            if outside:
                return False, {
                    **evidence,
                    "reason": "path_outside_reviews",
                    "commit": commit,
                    "paths": outside[:_MAX_EVIDENCE_PATHS],
                }
            if status.startswith("D"):
                return False, {**evidence, "reason": "deletion", "commit": commit, "paths": paths}
            if status.startswith("R"):
                return False, {**evidence, "reason": "rename", "commit": commit, "paths": paths}
            old_mode = metadata[0].removeprefix(":")
            new_mode = metadata[1]
            if old_mode not in {"000000", "100644", "100755"} or new_mode not in {"100644", "100755"}:
                return False, {
                    **evidence,
                    "reason": "non_regular_blob",
                    "commit": commit,
                    "paths": paths,
                    "old_mode": old_mode,
                    "new_mode": new_mode,
                }
            artifact_path_total += len(paths)
            artifact_paths.extend(paths[: max(0, _MAX_EVIDENCE_PATHS - len(artifact_paths))])

    refs_args = ("--format=%(refname)", "refs/heads/", "refs/reclaimed/")
    try:
        base_refs_result = _git(repo, "for-each-ref", "--contains", base_sha, *refs_args, timeout_s=timeout_s)
        tip_refs_result = _git(repo, "for-each-ref", "--contains", tip, *refs_args, timeout_s=timeout_s)
    except Exception as exc:  # noqa: BLE001 — failed Git probes are typed refusals
        return False, {**evidence, "reason": "probe_failed:preserved_refs", "detail": type(exc).__name__}
    if base_refs_result.returncode != 0:
        return False, {
            **evidence,
            "reason": "probe_failed:preserved_refs",
            "detail": _error_text(base_refs_result),
        }
    if tip_refs_result.returncode != 0:
        return False, {
            **evidence,
            "reason": "probe_failed:tip_refs",
            "detail": _error_text(tip_refs_result),
        }
    base_refs = set(base_refs_result.stdout.splitlines())
    tip_refs = set(tip_refs_result.stdout.splitlines())
    candidate_ref = f"refs/heads/{_normal_branch(branch)}"
    holders = sorted(base_refs - tip_refs - {candidate_ref})
    integration_short = _normal_branch(integration_ref)
    preferred_integration_ref = (
        integration_ref if integration_ref.startswith("refs/") else f"refs/heads/{integration_short}"
    )
    holders.sort(key=lambda ref: (ref != preferred_integration_ref, ref.startswith("refs/reclaimed/"), ref))
    if not holders:
        return False, {
            **evidence,
            "reason": "base_unpreserved",
            "candidate_ref": candidate_ref,
            "base_refs": sorted(base_refs)[:_MAX_EVIDENCE_PATHS],
            "tip_refs": sorted(tip_refs)[:_MAX_EVIDENCE_PATHS],
        }

    receipt, receipt_error = _review_artifacts_receipt(repo, lane_rows)
    if receipt is None:
        return False, {**evidence, **receipt_error}
    return True, {
        **evidence,
        "tip": tip,
        "holder_ref": holders[0],
        "receipt": receipt,
        "commit_count": commit_count,
        "artifact_paths": artifact_paths,
        "artifact_paths_total": artifact_path_total,
    }


def _pass_outcome_detail(outcome: object) -> str:
    if outcome is None:
        return "<missing>"
    if isinstance(outcome, str):
        return outcome or "<empty>"
    return repr(outcome)


def _is_scratch(path: str, globs: tuple[str, ...]) -> bool:
    normalized = path.removeprefix("./")
    return any(fnmatch.fnmatchcase(normalized, pattern) for pattern in globs)


def collect_snapshot(
    repo: str | Path,
    branch: str,
    *,
    shared: SharedReads | None = None,
    integration_ref: str = "main",
    now: float | None = None,
    age_floor_s: float = 0.0,
    lanes_by_branch: Callable[..., object] | None = None,
    lane_lookup: Callable[[str, str], object] | None = None,
    gate_lookup: Callable[[int], object] | None = None,
    timeout_s: float = _GIT_TIMEOUT_S,
) -> Snapshot:
    """Collect every available read for one branch under the scan read-only context."""

    with lane_reclaim._scan_read_pass():
        shared_reads = (
            shared if shared is not None else collect_shared(repo, integration_ref=integration_ref, timeout_s=timeout_s)
        )
        normalized_branch = _normal_branch(branch)
        probes = dict(shared_reads.probes)
        tip_info = shared_reads.branch_tips.get(normalized_branch)
        tip = tip_info[0] if tip_info else None
        tip_commit_time = tip_info[1] if tip_info else None
        if "branch_tip" not in probes and tip_info is None:
            probes["branch_tip"] = "branch_missing"
        if shared_reads.worktrees is None:
            worktree_paths: tuple[str, ...] = ()
        else:
            worktree_paths = tuple(shared_reads.worktrees.get(f"refs/heads/{normalized_branch}", ()))

        lane_rows: tuple[dict[str, Any], ...] = ()
        lane_reader = lanes_by_branch or lane_reclaim._list_lanes_by_branch
        try:
            raw_rows = lane_reader(branch=normalized_branch)
            projected_rows, row_error = _lane_rows_from_envelope(raw_rows)
            if row_error:
                probes["list_lanes_by_branch"] = row_error
            else:
                lane_rows = projected_rows or ()
        except Exception as exc:  # noqa: BLE001 - lane registry failure is typed
            probes["list_lanes_by_branch"] = str(exc)

        landing: ReceiptVerdict | None = None
        unverified_receipts: list[ReceiptVerdict] = []
        lane_receipts: list[ReceiptVerdict] = []
        tombstone_groups: list[tuple[str, tuple[TombstoneVerdict, ...]]] = []
        stale_tombstones: list[Tombstone] = []
        carrier = CarrierResult(
            tip or "",
            shared_reads.integration_ref,
            shared_reads.integration_tip,
            False,
            None,
            None,
            "integration_tip_unavailable",
        )
        survivors: tuple[str, ...] = ()
        changed_paths: tuple[str, ...] = ()
        review_blobs: set[str] | None = (
            set()
            if lane_rows
            and all(row.get("lane_kind") == "review" and row.get("status") in _TERMINAL_STATUSES for row in lane_rows)
            else None
        )
        changed_off_main: tuple[str, ...] = ()
        policy = shared_reads.policy
        scratch_globs = tuple(policy.scratch_globs) if policy is not None else ()
        integration_tip = shared_reads.integration_tip

        if tip is not None and integration_tip is not None:
            carrier = find_carrier(repo, tip, main_ref=integration_tip, timeout_s=timeout_s)
            if carrier.error:
                probes["carrier"] = carrier.error

            receipt_lookup = lane_lookup or _lane_lookup_read_only
            gate_reader = gate_lookup or _gate_lookup_read_only
            if not carrier.error:
                landing, failed, receipt_error = _collect_landing(
                    repo,
                    tip=tip,
                    carrier=carrier,
                    integration_tip=integration_tip,
                    lane_lookup=receipt_lookup,
                    gate_lookup=gate_reader,
                    timeout_s=timeout_s,
                )
                unverified_receipts.extend(failed)
                if receipt_error:
                    probes["receipt_verify"] = receipt_error

            if shared_reads.scan is not None and shared_reads.scan.complete:
                lane_identities = {
                    (row.get("task_ref"), row.get("lane_id"))
                    for row in lane_rows
                    if isinstance(row.get("task_ref"), str) and isinstance(row.get("lane_id"), str)
                }
                for _commit, receipt in shared_reads.scan.receipts:
                    if (receipt.task_ref, receipt.lane_id) not in lane_identities or receipt.tip == tip:
                        continue
                    ancestry = _git(repo, "merge-base", "--is-ancestor", receipt.tip, tip, timeout_s=timeout_s)
                    if ancestry.returncode == 1:
                        continue
                    if ancestry.returncode != 0:
                        probes["lane_receipts"] = _error_text(ancestry)
                        continue
                    verdict = verify_receipt(
                        repo,
                        _commit,
                        main_ref=integration_tip,
                        lane_lookup=receipt_lookup,
                        gate_lookup=gate_reader,
                    )
                    if verdict.verified:
                        lane_receipts.append(verdict)
                    elif _failure_probe(verdict):
                        probes["lane_receipts"] = verdict.detail or verdict.failed_check or "receipt_probe_failed"
                    else:
                        unverified_receipts.append(verdict)

                current_groups: dict[str, list[TombstoneVerdict]] = {}
                for commit, tombstone in shared_reads.scan.tombstones:
                    if _short_branch_name(tombstone.branch) != normalized_branch:
                        continue
                    if tombstone.tip != tip:
                        stale_tombstones.append(tombstone)
                        continue
                    # The shared verifier also enforces supersede authority; bound its reads by this probe budget.
                    with tombstone_verification_deadline(time.monotonic() + timeout_s):
                        verdict = verify_tombstone(repo, tombstone, main_ref=integration_tip, current_tip=tip)
                    current_groups.setdefault(commit, []).append(verdict)
                    if verdict.failed_check == "probe_failed":
                        probes["tombstone_verify"] = verdict.detail or "tombstone_probe_failed"
                tombstone_groups = [(commit, tuple(verdicts)) for commit, verdicts in current_groups.items()]

            survivor_proc = _git(
                repo,
                "for-each-ref",
                "--contains",
                tip,
                "--format=%(refname)%00%(objectname)",
                "refs/heads/",
                timeout_s=timeout_s,
            )
            if survivor_proc.returncode != 0:
                probes["survivor_refs"] = _error_text(survivor_proc)
            else:
                integration_short = _short_branch_name(shared_reads.integration_ref)
                found_survivors: set[str] = set()
                for line in survivor_proc.stdout.splitlines():
                    ref, separator, sha = line.partition("\0")
                    if not separator or not ref.startswith("refs/heads/"):
                        continue
                    short = _short_branch_name(ref)
                    if short in {normalized_branch, "main", "master", integration_short} or sha == tip:
                        continue
                    found_survivors.add(ref)
                survivors = tuple(sorted(found_survivors))

            base = _git(repo, "merge-base", tip, integration_tip, timeout_s=timeout_s)
            if base.returncode == 1:
                probes["changed_paths"] = "no_merge_base"
            elif base.returncode != 0:
                probes["changed_paths"] = _error_text(base)
            else:
                base_sha = base.stdout.strip()
                paths, error = _paths_from_diff(
                    repo,
                    "diff-tree",
                    "-r",
                    "--no-renames",
                    "-z",
                    "--name-only",
                    base_sha,
                    tip,
                    timeout_s=timeout_s,
                    review_blobs=review_blobs,
                )
                if error:
                    probes["changed_paths"] = error
                else:
                    changed_paths = paths or ()

        review_archive_available, work_complete_evidence = _review_harvest_snapshot_fact(
            repo,
            branch=normalized_branch,
            lane_rows=lane_rows,
            changed_paths=changed_paths,
            scratch_globs=scratch_globs,
            review_blobs=review_blobs,
        )
        review_artifacts_preserved = False
        review_artifacts_evidence: dict[str, Any] = {}
        if (
            lane_rows
            and all(row.get("lane_kind") == "implement" for row in lane_rows)
            and not probes
            and not review_archive_available
        ):
            review_artifacts_preserved, review_artifacts_evidence = _review_artifacts_snapshot_fact(
                repo,
                branch=normalized_branch,
                tip=tip,
                integration_ref=shared_reads.integration_ref,
                lane_rows=lane_rows,
                timeout_s=timeout_s,
            )
            artifact_reason = review_artifacts_evidence.get("reason")
            if isinstance(artifact_reason, str) and artifact_reason.startswith("probe_failed:"):
                probes["review_artifacts"] = artifact_reason
        # Only content-on-main classification needs this second diff. A proven
        # review archive or a scratch-only diff already decides preservation;
        # do not spend another subprocess against the retirement budget.
        if (
            tip is not None
            and integration_tip is not None
            and "changed_paths" not in probes
            and not review_archive_available
            and not review_artifacts_preserved
            and any(not _is_scratch(path, scratch_globs) for path in changed_paths)
        ):
            main_paths, main_error = _paths_from_diff(
                repo,
                "diff-tree",
                "-r",
                "--no-renames",
                "-z",
                "--name-only",
                tip,
                integration_tip,
                timeout_s=timeout_s,
            )
            if main_error:
                probes["main_diff"] = main_error
            else:
                changed_off_main = tuple(sorted(set(changed_paths) & set(main_paths or ())))
        return Snapshot(
            branch=normalized_branch,
            integration_ref=shared_reads.integration_ref,
            integration_tip=integration_tip,
            tip=tip,
            now=time.time() if now is None else now,
            tip_commit_time=tip_commit_time,
            age_floor_s=max(0.0, float(age_floor_s)),
            lane_rows=lane_rows,
            worktree_paths=worktree_paths,
            landing=landing,
            unverified_receipts=tuple(unverified_receipts),
            lane_receipts=tuple(lane_receipts),
            tombstone_groups=tuple(tombstone_groups),
            stale_tombstones=tuple(stale_tombstones),
            carrier=carrier,
            survivors=survivors,
            changed_paths=changed_paths,
            changed_off_main=changed_off_main,
            scratch_globs=scratch_globs,
            probes=probes,
            review_archive_available=review_archive_available,
            work_complete_evidence=work_complete_evidence,
            review_artifacts_preserved=review_artifacts_preserved,
            review_artifacts_evidence=review_artifacts_evidence,
        )


def _receipt_evidence(verdict: ReceiptVerdict) -> dict[str, Any]:
    return {
        "commit": verdict.commit,
        "failed_check": verdict.failed_check,
        "detail": verdict.detail,
    }


def _tombstone_evidence(verdict: TombstoneVerdict) -> dict[str, Any]:
    tombstone = verdict.tombstone
    return {
        "branch": tombstone.branch,
        "tip": tombstone.tip,
        "disposition": tombstone.disposition,
        "kind": tombstone.kind,
        "ref": tombstone.ref,
        "verified": verdict.verified,
        "failed_check": verdict.failed_check,
        "detail": verdict.detail,
    }


def _path_evidence(name: str, paths: tuple[str, ...]) -> dict[str, Any]:
    return {name: list(paths[:_MAX_EVIDENCE_PATHS]), f"{name}_total": len(paths)}


def _base_evidence(snapshot: Snapshot) -> dict[str, Any]:
    return {
        "branch": snapshot.branch,
        "tip": snapshot.tip,
        "integration_ref": snapshot.integration_ref,
        "integration_tip": snapshot.integration_tip,
        "lane_rows": [dict(row) for row in snapshot.lane_rows],
        "unverified_receipts": [_receipt_evidence(item) for item in snapshot.unverified_receipts],
        "failed_tombstones": [
            _tombstone_evidence(item)
            for _commit, group in snapshot.tombstone_groups
            for item in group
            if not item.verified
        ],
        "stale_tombstones": [
            {
                "branch": item.branch,
                "tip": item.tip,
                "disposition": item.disposition,
                "kind": item.kind,
                "ref": item.ref,
            }
            for item in snapshot.stale_tombstones
        ],
    }


def _match(
    kind: BranchDisposition,
    reason: str,
    work_item: str,
    next_action: str,
    **evidence: Any,
) -> RuleMatch:
    return RuleMatch(kind, reason, work_item, next_action, evidence)


def _probe_failed(snapshot: Snapshot) -> RuleMatch | None:
    if not snapshot.probes:
        return None
    seam = next((name for name in SEAMS if name in snapshot.probes), sorted(snapshot.probes)[0])
    return _match(
        BranchDisposition.PROBE_FAILED,
        f"probe_failed:{seam}",
        f"unknown:probe_failed:{seam}",
        f"repair_seam:{seam}",
        probes=dict(snapshot.probes),
    )


def _live(snapshot: Snapshot) -> RuleMatch | None:
    if snapshot.worktree_paths:
        reason = "worktree_attached"
    elif any(row.get("status") not in _TERMINAL_STATUSES for row in snapshot.lane_rows):
        reason = "row_not_terminal"
    elif (
        snapshot.age_floor_s > 0
        and snapshot.tip_commit_time is not None
        and snapshot.now - snapshot.tip_commit_time < snapshot.age_floor_s
    ):
        reason = "younger_than_age_floor"
    else:
        return None
    return _match(
        BranchDisposition.LIVE,
        f"live:{reason}",
        "unknown:live",
        "wait_until_terminal",
        **_path_evidence("worktree_paths", snapshot.worktree_paths),
    )


def _landed(snapshot: Snapshot) -> RuleMatch | None:
    if snapshot.landing is None or not snapshot.landing.verified:
        return None
    commit = snapshot.landing.commit
    return _match(
        BranchDisposition.LANDED_RECEIPT,
        f"landed:{commit}",
        f"landed:{commit}",
        "retire:delete_d",
        landing={"commit": commit, "tip": snapshot.landing.receipt.tip if snapshot.landing.receipt else None},
    )


def _moved_after_landing(snapshot: Snapshot) -> RuleMatch | None:
    verified_tombstone = any(
        group and all(item.verified for item in group) for _commit, group in snapshot.tombstone_groups
    )
    if not snapshot.lane_receipts or snapshot.carrier.contained or verified_tombstone:
        return None
    old = snapshot.lane_receipts[0]
    old_tip = old.receipt.tip if old.receipt else old.commit
    return _match(
        BranchDisposition.MOVED_AFTER_LANDING,
        f"moved_after_landing:{old_tip}",
        f"landed:{old_tip}",
        "land_again_or_tombstone",
        previous_landing={"commit": old.commit, "tip": old_tip},
    )


def _superseded_tombstone(snapshot: Snapshot) -> RuleMatch | None:
    for commit, group in snapshot.tombstone_groups:
        if not group or not all(item.verified for item in group):
            continue
        tombstone = group[0].tombstone
        reason = f"tombstone:{tombstone.disposition}:{tombstone.kind}"
        return _match(
            BranchDisposition.SUPERSEDED_TOMBSTONE,
            reason,
            f"not_landed:{tombstone.disposition}:{tombstone.kind}:{tombstone.ref}",
            "retire:pin_bundle_then_delete_D",
            tombstone_commit=commit,
            tombstones=[_tombstone_evidence(item) for item in group],
        )
    return None


def _contained(snapshot: Snapshot) -> RuleMatch | None:
    if not snapshot.carrier.contained:
        return None
    lineage = snapshot.carrier.lineage or "unknown"
    carrier = snapshot.carrier.carrier or snapshot.carrier.commit
    return _match(
        BranchDisposition.MERGED_ANCESTRY,
        f"contained:{lineage}:{carrier}",
        f"unknown:contained_no_receipt:{carrier}",
        "retire:delete_d",
        carrier={"commit": carrier, "lineage": lineage},
    )


def _redundant(snapshot: Snapshot) -> RuleMatch | None:
    if not snapshot.survivors:
        return None
    ref = snapshot.survivors[0]
    return _match(
        BranchDisposition.SUPERSEDED_BY_SIBLING,
        f"redundant:{ref}",
        f"unknown:redundant:{ref}",
        "retire:pin_bundle_then_delete_D",
        survivor_refs=list(snapshot.survivors),
    )


def _content_on_main(snapshot: Snapshot) -> RuleMatch | None:
    if (
        snapshot.review_archive_available
        and snapshot.lane_rows
        and all(row.get("status") in _TERMINAL_STATUSES for row in snapshot.lane_rows)
    ):
        return _match(
            BranchDisposition.REVIEW_PERSISTED,
            "review_harvested",
            "not_landed:review_harvested",
            "retire:pin_bundle_then_delete_D",
            work_complete=dict(snapshot.work_complete_evidence),
        )
    if (
        snapshot.review_artifacts_preserved
        and snapshot.lane_rows
        and all(row.get("status") in _TERMINAL_STATUSES for row in snapshot.lane_rows)
    ):
        return _match(
            BranchDisposition.REVIEW_PERSISTED,
            "review_artifacts_over_preserved_base",
            "not_landed:review_artifacts_preserved",
            "retire:pin_bundle_then_delete_D",
            review_artifacts=dict(snapshot.review_artifacts_evidence),
        )
    changed = tuple(path.removeprefix("./") for path in snapshot.changed_paths)
    product = tuple(path for path in changed if not _is_scratch(path, snapshot.scratch_globs))
    off_main = set(path.removeprefix("./") for path in snapshot.changed_off_main)
    if not product or any(path in off_main for path in product):
        return None
    review_docs = (
        all(_is_review_doc_path(path) for path in product)
        and bool(snapshot.lane_rows)
        and all(row.get("lane_kind") == "review" for row in snapshot.lane_rows)
    )
    kind = BranchDisposition.REVIEW_PERSISTED if review_docs else BranchDisposition.CONTENT_LANDED
    suffix = ":review_docs" if review_docs else ""
    item = "unknown:content_on_main:review_docs" if review_docs else "unknown:content_on_main"
    return _match(
        kind,
        f"content_on_main{suffix}",
        item,
        "retire:pin_bundle_then_delete_D",
        **_path_evidence("changed_paths", changed),
        **_path_evidence("product_paths", product),
        **_path_evidence("changed_off_main", tuple(sorted(off_main & set(changed)))),
    )


def _no_product(snapshot: Snapshot) -> RuleMatch | None:
    if not snapshot.changed_paths:
        return _match(
            BranchDisposition.NEVER_STARTED,
            "no_product:empty",
            "not_landed:no_product:empty",
            "retire:pin_bundle_then_delete_D",
            **_path_evidence("changed_paths", ()),
            **_path_evidence("product_paths", ()),
        )
    product = tuple(path for path in snapshot.changed_paths if not _is_scratch(path, snapshot.scratch_globs))
    if product:
        return None
    return _match(
        BranchDisposition.REVIEW_UNPRODUCED,
        "no_product:scratch",
        "not_landed:no_product:scratch",
        "retire:pin_bundle_then_delete_D",
        **_path_evidence("changed_paths", snapshot.changed_paths),
        **_path_evidence("product_paths", ()),
    )


def _unique_work(snapshot: Snapshot) -> RuleMatch:
    return _match(
        BranchDisposition.HELD_UNIQUE_WORK,
        "unique_work_no_receipt",
        "unknown:unique_work_no_receipt",
        "land_or_tombstone",
        review_artifacts=dict(snapshot.review_artifacts_evidence),
        **_path_evidence("changed_paths", snapshot.changed_paths),
        **_path_evidence("changed_off_main", snapshot.changed_off_main),
    )


def _work_complete_empty(snapshot: Snapshot) -> RuleMatch | None:
    if (
        not snapshot.work_complete_empty
        or not snapshot.work_complete_guarded
        or not any(row.get("status") not in _TERMINAL_STATUSES for row in snapshot.lane_rows)
        or snapshot.probes
    ):
        return None
    return _match(
        BranchDisposition.NEVER_STARTED,
        "superseded_empty",
        "not_landed:superseded_empty",
        "retire:pin_bundle_then_delete_D",
        work_complete=dict(snapshot.work_complete_evidence),
    )


def _review_harvested(snapshot: Snapshot) -> RuleMatch | None:
    if (
        not snapshot.review_harvested
        or not snapshot.work_complete_guarded
        or not any(row.get("status") not in _TERMINAL_STATUSES for row in snapshot.lane_rows)
        or snapshot.probes
    ):
        return None
    return _match(
        BranchDisposition.REVIEW_PERSISTED,
        "review_harvested",
        "not_landed:review_harvested",
        "retire:pin_bundle_then_delete_D",
        work_complete=dict(snapshot.work_complete_evidence),
    )


def _unusable_review_pass_outcome(snapshot: Snapshot) -> RuleMatch | None:
    evidence = snapshot.work_complete_evidence
    if "unknown_pass_outcome" in evidence:
        reason = "unknown_pass_outcome"
        detail = evidence["unknown_pass_outcome"]
    elif "nonterminal_pass_outcome" in evidence:
        reason = "review_pass_not_terminal"
        detail = evidence["nonterminal_pass_outcome"]
    else:
        return None
    if not isinstance(detail, str):
        detail = _pass_outcome_detail(detail)
    return _match(
        BranchDisposition.UNDECIDABLE,
        reason,
        f"unknown:{reason}",
        f"keep:{reason}",
        detail=detail,
        work_complete=dict(evidence),
    )


# These sweep-specific facts are gathered only after the caller has checked
# row age, tree cleanliness, and live-owner seams. They precede ordinary
# branch rules because a nonterminal row necessarily has an attached worktree.
# An unusable pass outcome must preempt any positive work-complete disposition.
WORK_COMPLETE_RULES: tuple[Rule, ...] = (
    Rule(12, "unusable_review_pass_outcome", "keep", (BranchDisposition.UNDECIDABLE,), _unusable_review_pass_outcome),
    Rule(10, "work_complete_empty", "delete_D_after_pin", (BranchDisposition.NEVER_STARTED,), _work_complete_empty),
    Rule(11, "review_harvested", "delete_D_after_pin", (BranchDisposition.REVIEW_PERSISTED,), _review_harvested),
)


RULES: tuple[Rule, ...] = (
    Rule(0, "probe_failed", "keep", (BranchDisposition.PROBE_FAILED,), _probe_failed),
    Rule(1, "live", "keep", (BranchDisposition.LIVE,), _live),
    Rule(2, "landed", "delete_d", (BranchDisposition.LANDED_RECEIPT,), _landed),
    Rule(3, "moved_after_landing", "keep", (BranchDisposition.MOVED_AFTER_LANDING,), _moved_after_landing),
    Rule(
        4,
        "superseded_tombstone",
        "delete_D_after_pin",
        (BranchDisposition.SUPERSEDED_TOMBSTONE,),
        _superseded_tombstone,
    ),
    Rule(5, "contained", "delete_d", (BranchDisposition.MERGED_ANCESTRY,), _contained),
    Rule(6, "redundant", "delete_D_after_pin", (BranchDisposition.SUPERSEDED_BY_SIBLING,), _redundant),
    Rule(
        7,
        "content_on_main",
        "delete_D_after_pin",
        (BranchDisposition.CONTENT_LANDED, BranchDisposition.REVIEW_PERSISTED),
        _content_on_main,
    ),
    Rule(
        8,
        "no_product",
        "delete_D_after_pin",
        (BranchDisposition.NEVER_STARTED, BranchDisposition.REVIEW_UNPRODUCED),
        _no_product,
    ),
    Rule(
        9,
        "unique_work_no_receipt",
        "keep",
        (BranchDisposition.HELD_UNIQUE_WORK,),
        lambda snapshot: _unique_work(snapshot),
    ),
)


def decide(snapshot: Snapshot) -> Disposition:
    """Return the first matching rule's disposition without reading external state."""

    base = _base_evidence(snapshot)
    for rule in (*WORK_COMPLETE_RULES, *RULES):
        matched = rule.when(snapshot)
        if matched is None:
            continue
        evidence = dict(base)
        evidence.update(matched.evidence)
        return Disposition(
            kind=matched.kind,
            reason=matched.reason,
            evidence=evidence,
            branch_action=rule.branch_action,
            work_item=matched.work_item,
            next_action=matched.next_action,
            rule=rule.row,
        )
    # RULES ends in a catch-all. This guard catches accidental table edits.
    raise RuntimeError("lane disposition rules are not exhaustive")


def plan_token(snapshot: Snapshot, disposition: Disposition) -> str:
    return f"{snapshot.branch}@{snapshot.tip}:{disposition.rule}:{disposition.kind.value}:{disposition.branch_action}"
