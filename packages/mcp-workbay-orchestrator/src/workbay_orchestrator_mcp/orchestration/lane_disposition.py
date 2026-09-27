"""One read-only snapshot and one precedence table for branch disposition."""

from __future__ import annotations

import fnmatch
import subprocess
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
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
    verify_receipt,
    verify_tombstone,
)
from .lane_terminal_dispose import _TERMINAL_STATUSES, BranchDisposition

_GIT_TIMEOUT_S = 20.0
_MAX_EVIDENCE_PATHS = 50
_REVIEW_DOC_PREFIX = "docs/reviews/"
_REVIEW_DOC_SUFFIX = ".md"

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


def _is_sha40(value: str) -> bool:
    return len(value) == 40 and all(character in "0123456789abcdef" for character in value)


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


def _paths_from_diff(repo: str | Path, *args: str, timeout_s: float) -> tuple[tuple[str, ...] | None, str | None]:
    result = _git(repo, *args, timeout_s=timeout_s)
    if result.returncode != 0:
        return None, _error_text(result)
    return tuple(path.removeprefix("./") for path in result.stdout.split("\0") if path), None


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
                )
                if error:
                    probes["changed_paths"] = error
                else:
                    changed_paths = paths or ()
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
    changed = tuple(path.removeprefix("./") for path in snapshot.changed_paths)
    product = tuple(path for path in changed if not _is_scratch(path, snapshot.scratch_globs))
    off_main = set(path.removeprefix("./") for path in snapshot.changed_off_main)
    if not product or any(path in off_main for path in product):
        return None
    review_docs = (
        all(path.startswith(_REVIEW_DOC_PREFIX) and path.endswith(_REVIEW_DOC_SUFFIX) for path in product)
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
        **_path_evidence("changed_paths", snapshot.changed_paths),
        **_path_evidence("changed_off_main", snapshot.changed_off_main),
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
    for rule in RULES:
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
