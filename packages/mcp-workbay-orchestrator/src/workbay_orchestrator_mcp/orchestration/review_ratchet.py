"""D1 attribution on canonical re-review receipts, using the existing impact bar.

No reviewer-provided listed IDs, source tips or execution booleans are authority.
The host engine loads its preceding harvest and executes both reproduction arms.
"""

from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from workbay_handoff_mcp.shared_write_context import assert_no_write_lock_held
from workbay_protocol.review_impact import is_blocking

from . import worktree_root
from .lane_escalation import _finding_is_in_diff, _line_range_intersects, _normalized_diff_path


class RatchetRefused(ValueError):
    """Named infrastructure failure; never an empty diff or a clean review."""


def _git(repo: Path, *args: str) -> str:
    # Refuse before spawning Git while this thread holds a SQLite write lock.
    assert_no_write_lock_held("subprocess")
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, check=False)
    if result.returncode:
        raise RatchetRefused("review_ratchet_source_unresolvable")
    return result.stdout.strip()


@dataclass(frozen=True)
class ReviewContext:
    listed_findings: tuple[dict[str, Any], ...]
    prior_reviewed_tip: str
    reviewed_tip: str
    baseline_tip: str
    fix_delta: str
    changed_files: tuple[str, ...]
    diff_ranges: dict[str, list[tuple[int, int]]]
    preceding_pass_id: str

    @property
    def listed_finding_ids(self) -> list[str]:
        return [str(item["finding_id"]) for item in self.listed_findings]


def context_from_receipt(
    repo: Path, receipt: dict, state: dict, rows: list[dict], *, task_ref: str, branch: str
) -> ReviewContext:
    """Check a host receipt against its independently retained engine pass state."""
    from .offload_pass import _review_identity_complete

    identity = receipt.get("identity") or {}
    if not _review_identity_complete(identity):
        raise RatchetRefused("review_ratchet_previous_identity_incomplete")
    for key in ("pass_id", "task_ref", "lane_id", "tip_sha", "base_sha", "branch"):
        if not identity.get(key) or state.get(key) != identity[key]:
            raise RatchetRefused("review_ratchet_previous_receipt_mismatch")
    harvest = receipt.get("harvest") or {}
    if (
        identity["task_ref"] != task_ref
        or identity["branch"] != branch
        or receipt.get("status") != "recorded"
        or not isinstance(state.get("findings_harvest"), dict)
        or any(state["findings_harvest"].get(key) != value for key, value in harvest.items())
    ):
        raise RatchetRefused("review_ratchet_previous_receipt_mismatch")
    ids = harvest.get("finding_ids", [])
    if not isinstance(ids, list) or harvest.get("unrecorded_ids"):
        raise RatchetRefused("review_ratchet_previous_harvest_incomplete")
    indexed = {row["finding_id"]: row for row in rows if row.get("task_ref") == task_ref}
    if any(fid not in indexed for fid in ids):
        raise RatchetRefused("review_ratchet_previous_findings_missing")
    # Snapshot classification, rather than later coordinator status, supplies the named list.
    snapshots = harvest.get("blocking_findings")
    if snapshots is None:
        snapshots = [indexed[fid] for fid in ids if is_blocking(indexed[fid])]
    if not isinstance(snapshots, list) or any(item.get("finding_id") not in ids for item in snapshots):
        raise RatchetRefused("review_ratchet_previous_receipt_mismatch")
    prior = identity["tip_sha"]
    if not re.fullmatch("[0-9a-f]{40}", prior) or _git(repo, "rev-parse", f"{prior}^{{commit}}") != prior:
        raise RatchetRefused("review_ratchet_previous_host_tip_unresolvable")
    tip = _git(repo, "rev-parse", "HEAD")
    # Exact current baseline, not a reviewer-declared pass boolean or stale merge base.
    baseline = _git(repo, "rev-parse", "main^{commit}")
    delta = _git(repo, "diff", "--no-ext-diff", "--no-renames", "--unified=0", prior, tip, "--")
    changed = tuple(_git(repo, "diff", "--name-only", "--no-renames", prior, tip, "--").splitlines())
    ranges: dict[str, list[tuple[int, int]]] = {}
    path = None
    for line in delta.splitlines():
        if line.startswith("+++ b/"):
            path = line[6:]
        match = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", line)
        if match and path:
            start, count = int(match[1]), int(match[2] or 1)
            ranges.setdefault(path, []).append((start, start + max(count, 1) - 1))
    return ReviewContext(
        tuple(dict(item) for item in snapshots), prior, tip, baseline, delta, changed, ranges, identity["pass_id"]
    )


# Engine-owned lane projection keys; public lane writers may never author them.
ENGINE_LANE_FIELDS = ("listed_finding_ids", "prior_reviewed_tip")


def _resolve_local_branch(repo: Path, branch: str) -> str | None:
    ref = branch if branch.startswith("refs/heads/") else f"refs/heads/{branch}"
    assert_no_write_lock_held("subprocess")
    result = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _review_subject_lanes(task_ref: str, branch: str) -> list[dict]:
    """Read review lanes through the task/branch-scoped durable port."""
    from workbay_handoff_mcp.lanes_api import list_lanes_by_branch

    result = list_lanes_by_branch(branch, task_ref=task_ref)
    lanes = (result.get("data") or {}).get("lanes") if isinstance(result, dict) else None
    if not isinstance(result, dict) or result.get("ok") is not True or not isinstance(lanes, list):
        raise RatchetRefused("review_ratchet_subject_lookup_failed")
    return [row for row in lanes if row.get("lane_kind") == "review"]


def _durable_review_subject(task_ref: str, branch: str) -> tuple[str, str] | None:
    """Read the validated review subject a public upsert pinned for this review branch."""
    from .secure_sandbox import review_subject_from_lane_row

    subjects = {review_subject_from_lane_row(row) for row in _review_subject_lanes(task_ref, branch)} - {None}
    if len(subjects) > 1:
        raise RatchetRefused("review_ratchet_subject_binding_ambiguous")
    return subjects.pop() if subjects else None


def _subject_has_engine_history(task_ref: str, branch: str, subject: tuple[str, str]) -> bool:
    """A previous public projection is evidence of history, never a named-list source."""
    from .secure_sandbox import review_subject_from_lane_row

    for row in _review_subject_lanes(task_ref, branch):
        if review_subject_from_lane_row(row) != subject:
            continue
        try:
            notes = json.loads(row.get("notes") or "{}")
        except (ValueError, TypeError):
            continue
        if isinstance(notes, dict) and any(key in notes for key in ENGINE_LANE_FIELDS):
            return True
    return False


def _subject_is_ancestor(repo: Path, older: str, newer: str) -> bool:
    assert_no_write_lock_held("subprocess")
    result = subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", older, newer],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode not in (0, 1):
        raise RatchetRefused("review_ratchet_subject_unverified")
    return result.returncode == 0


def _bind_original_subject(
    repo: Path, subject: tuple[str, str] | None, history: dict[str, tuple[str, str, dict]], *, branch: str
) -> str | None:
    """Bind a separate review branch to the original subject branch it reviews.

    Proof is the validated ``review_subject`` plus an immutable engine tip or
    its lineage on the original local branch, with the last engine base.
    Branch names, notes prose and caller-listed IDs never participate.
    """
    if subject is None:
        return None
    base, tip = subject
    if not re.fullmatch("[0-9a-f]{40}", base) or not re.fullmatch("[0-9a-f]{40}", tip):
        raise RatchetRefused("review_ratchet_subject_unverified")
    matches = []
    for name, (_, item_base, identity) in history.items():
        if name == branch:
            continue
        head = _resolve_local_branch(repo, name)
        if identity.get("tip_sha") == tip or (head is not None and _subject_is_ancestor(repo, tip, head)):
            matches.append((name, item_base, identity.get("tip_sha")))
    if not matches:
        tasks = {item[2]["task_ref"] for item in history.values()}
        if any(_subject_has_engine_history(task, branch, subject) for task in tasks):
            raise RatchetRefused("review_ratchet_subject_history_missing")
        return None
    if len(matches) > 1:
        raise RatchetRefused("review_ratchet_subject_binding_ambiguous")
    name, original_base, original_tip = matches[0]
    if original_base != base:
        raise RatchetRefused("review_ratchet_subject_base_mismatch")
    if _git(repo, "rev-parse", "HEAD") != tip:
        raise RatchetRefused("review_ratchet_subject_tip_mismatch")
    if not isinstance(original_tip, str) or not re.fullmatch("[0-9a-f]{40}", original_tip):
        raise RatchetRefused("review_ratchet_subject_unverified")
    if not _subject_is_ancestor(repo, base, tip) or not _subject_is_ancestor(repo, original_tip, tip):
        raise RatchetRefused("review_ratchet_subject_history_mismatch")
    return name


TASK_SUBJECT_FIELD = "task_subject"
# Producer-owned lane notes keys: written only by the registry producer, carried
# across public upserts, and never authored by a public lane writer.
PRODUCER_LANE_FIELDS = (TASK_SUBJECT_FIELD, "registry_correction")
_MAX_TASK_LANES = 500


def _commit(repo: Path, ref: str) -> str | None:
    """Resolve a commit; absence is ``None``, any other Git failure refuses."""
    assert_no_write_lock_held("subprocess")
    result = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 1:
        return None
    if result.returncode:
        raise RatchetRefused("review_ratchet_subject_unverified")
    return result.stdout.strip()


def _short_branch(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().removeprefix("refs/heads/")


def task_record(task_ref: str) -> dict | None:
    """The canonical durable task record (status, target branch/worktree, revision)."""
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    with _get_db_connection() as conn:
        row = conn.execute(
            "SELECT task_ref, status, target_branch, target_worktree_path, revision FROM handoff_state "
            "WHERE task_ref = ?",
            (task_ref,),
        ).fetchone()
    return dict(row) if row is not None else None


def _manifest_lane(orchestrator_root: Path, task_ref: str, lane_id: str) -> tuple[dict, dict] | None:
    if "/" in task_ref or task_ref.startswith("."):
        return None
    try:
        manifest = json.loads((orchestrator_root / "config/lane-orchestration" / f"{task_ref}.json").read_text())
    except (OSError, ValueError):
        return None
    lanes = manifest.get("lanes") if isinstance(manifest, dict) else None
    lane = lanes.get(lane_id) if isinstance(lanes, dict) else None
    return (manifest, lane) if isinstance(lane, dict) and manifest.get("task_ref") == task_ref else None


def _owned_roots(entries: object) -> list[str] | None:
    """Normalized owned roots; whole-tree ownership cannot separate a task's source."""
    from .lane_manifest import _normalize_owned_path

    if not isinstance(entries, list) or not entries or not all(isinstance(item, str) for item in entries):
        return None
    try:
        roots = {_normalize_owned_path(item) for item in entries}
    except ValueError:
        return None
    return None if "" in roots else sorted(roots)


def _task_source(
    repo: Path, orchestrator_root: Path, *, task_ref: str, lane_id: str, branch: str, tip: str | None,
    historical_baseline: bool = False,
) -> dict | None:
    """Evidence that an unfinished canonical task owns ``base..tip`` on a landed baseline.

    Only the durable task record, its manifest lane (owned scope and base) and
    immutable commits participate. Absent or contradictory evidence is ``None``.
    """
    task = task_record(task_ref)
    located = _manifest_lane(orchestrator_root, task_ref, lane_id)
    if task is None or task["status"] == "done" or located is None:
        return None
    manifest, lane = located
    parent = _main_parent(task, manifest)
    if _short_branch(task["target_branch"]) != branch and not parent:
        return None
    roots = _owned_roots(lane.get("owned_paths"))
    base = lane.get("base_sha")
    integration = _short_branch(manifest.get("integration_branch")) or "main"
    if (
        _short_branch(lane.get("branch")) != branch
        or (parent and branch == "main")
        or roots is None
        or not isinstance(base, str)
        or not re.fullmatch("[0-9a-f]{40}", base)
        or tip is None
        or _commit(repo, base) != base
    ):
        return None
    landed = _commit(repo, f"refs/heads/{integration}")
    if landed is None or not _subject_is_ancestor(repo, base, landed) or not _subject_is_ancestor(repo, base, tip):
        return None
    # The first implementation source starts before it has a code commit.
    # This exception requires the task/manifest/owned-scope proof above and
    # the exact current integration baseline, not an arbitrary empty delta.
    if tip == base and landed != base and not historical_baseline:
        return None
    # Keep the work unit's original identity base. A legacy source may have
    # merged newer integration history; those already-landed files do not
    # become its owned implementation delta (nor a new work-unit baseline).
    source_base = _git(repo, "merge-base", landed, tip)
    changed = sorted(_git(repo, "diff", "--name-only", "--no-renames", source_base, tip, "--").splitlines())
    if (not changed and tip != base) or any(
        not any(path == root or path.startswith(f"{root}/") for root in roots) for path in changed
    ):
        return None
    return {
        "task_ref": task_ref,
        "lane_id": lane_id,
        "subject_branch": branch,
        "base_sha": base,
        "tip_sha": tip,
        "integration_branch": integration,
        "owned_paths": roots,
        "changed_files": changed,
    }


def _main_parent(task: dict, manifest: dict) -> bool:
    return (task.get("status") == "in_progress" and _short_branch(task.get("target_branch")) == "main"
            and _short_branch(manifest.get("integration_branch")) == "main")


def _subject_worktree(orchestrator_root: Path, task: dict, task_ref: str, lane_id: str) -> object:
    located = _manifest_lane(orchestrator_root, task_ref, lane_id)
    if located and _main_parent(task, located[0]):
        value = located[1].get("worktree_path")
        return worktree_root.expand_lane_path(value, orchestrator_root=orchestrator_root) if isinstance(value, str) else None
    return task.get("target_worktree_path")


def _same_worktree(left: object, right: object) -> bool:
    if not isinstance(left, str) or not left or not isinstance(right, str) or not right:
        return False
    try:
        return Path(left).expanduser().resolve() == Path(right).expanduser().resolve()
    except (OSError, RuntimeError):
        return False


def _scope_path(repo: Path, binding: dict) -> Path:
    from .review_cycle_dispatch import authority

    digest = hashlib.sha256(json.dumps(binding, sort_keys=True).encode()).hexdigest()
    return authority(repo) / "task_subject_scopes" / (digest + ".json")


def _scope_record(repo: Path, binding: dict) -> dict | None:
    """Private producer metadata, never a path/annotation accepted from notes."""
    path = _scope_path(repo, binding)
    try:
        if path.resolve() != path or path.stat().st_uid != os.getuid() or path.stat().st_mode & 0o077:
            return None
        record = json.loads(path.read_bytes())
        scopes = record["authorized_scopes"]
        if (
            record.get("schema") != "workbay/task-subject-scope@1"
            or record["original"] != binding
            or not scopes
            or scopes[0] != binding["owned_paths"]
            or any(_owned_roots(scope) != scope for scope in scopes)
            or any(not set(left) < set(right) for left, right in zip(scopes, scopes[1:]))
        ):
            return None
        return record
    except (OSError, ValueError, TypeError, KeyError):
        return None


def _publish_scope(repo: Path, binding: dict, worktree_path: str, roots: list[str]) -> bool:
    """Called only by the registry producer on a public, validated upsert.

    One original snapshot plus monotonic authorizations; contraction and path
    replacement cannot erase a resource already owned by this work unit.
    """
    from .review_cycle_dispatch import _save

    path = _scope_path(repo, binding)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.with_suffix(".lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            record = (
                _scope_record(repo, binding)
                if path.exists()
                else {
                    "schema": "workbay/task-subject-scope@1",
                    "original": binding,
                    "worktree_path": str(Path(worktree_path).resolve()),
                    "authorized_scopes": [binding["owned_paths"]],
                }
            )
            if record is None or not _same_worktree(record["worktree_path"], worktree_path):
                return False
            previous = record["authorized_scopes"][-1]
            if roots != previous:
                if not set(previous) < set(roots):
                    return False
                record["authorized_scopes"].append(roots)
            _save(path, record)
            # pass_rescue's atomic writer uses a private temporary by default.
            path.chmod(0o600)
            return True
    except (OSError, ValueError):
        raise RatchetRefused("review_ratchet_scope_publication_unavailable")


def _binding_holds(repo: Path, orchestrator_root: Path, binding: dict, *, historical_baseline: bool = False) -> bool:
    tip = binding.get("tip_sha")
    if not isinstance(tip, str) or not re.fullmatch("[0-9a-f]{40}", tip) or _commit(repo, tip) != tip:
        return False
    current = _task_source(
        repo,
        orchestrator_root,
        task_ref=str(binding.get("task_ref") or ""),
        lane_id=str(binding.get("lane_id") or ""),
        branch=str(binding.get("subject_branch") or ""),
        tip=tip,
        historical_baseline=historical_baseline,
    )
    record = _scope_record(repo, binding)
    if record is None and _scope_path(repo, binding).exists():
        return False
    if record is not None:
        task = task_record(binding["task_ref"]) or {}
        target = _subject_worktree(orchestrator_root, task, binding["task_ref"], binding["lane_id"])
        if not _same_worktree(target, record["worktree_path"]):
            return False
        # Even a reversion to the original scope is a contraction after an
        # authorized expansion. The original remains immutable, current scope
        # must equal the last producer authorization.
        return current == {**binding, "owned_paths": record["authorized_scopes"][-1]} and set(
            binding["owned_paths"]
        ) <= set(record["authorized_scopes"][-1])
    return current == binding


def derive_task_subject(
    repo: Path, orchestrator_root: Path, *, task_ref: str, lane_id: str, branch: str, worktree_path: str
) -> dict | None:
    """Producer proof that a canonical task or main-parent lane owns its source.

    The registered lane must be the task's own target branch and worktree, its
    manifest lane must name the owned scope and a base already on the
    integration branch, and every change in ``base..tip`` must be owned. The
    subject is the task's own branch, never a newer row, timestamp or label.
    """
    task = task_record(task_ref)
    target = _subject_worktree(orchestrator_root, task or {}, task_ref, lane_id)
    if not _same_worktree(target, worktree_path):
        return None
    return _task_source(
        repo,
        orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        tip=_commit(repo, f"refs/heads/{branch}"),
    )


def pin_task_subject(
    repo: Path, orchestrator_root: Path, *, task_ref: str, lane_id: str, branch: str, worktree_path: str, pinned: object
) -> dict | None:
    """Keep an established binding; only a no-longer-valid one for the same subject is re-anchored."""
    fresh = derive_task_subject(
        repo, orchestrator_root, task_ref=task_ref, lane_id=lane_id, branch=branch, worktree_path=worktree_path
    )
    if not isinstance(pinned, dict):
        if fresh is not None:
            _publish_scope(repo, fresh, worktree_path, fresh["owned_paths"])
        return fresh
    if fresh is None:
        return pinned
    record = _scope_record(repo, pinned)
    if fresh == pinned:
        if record is None:
            _publish_scope(repo, pinned, worktree_path, pinned["owned_paths"])
        return pinned
    # Recompute the original tip, not the branch's later implementation tip.
    # Only owned scope may evolve. Public upsert is the authorization point;
    # merely editing the manifest never authorizes new scope.
    current = _task_source(
        repo,
        orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
        branch=branch,
        tip=pinned.get("tip_sha"),
        historical_baseline=True,
    )
    if current is not None and current == {**pinned, "owned_paths": current["owned_paths"]}:
        previous = record["authorized_scopes"][-1] if record else pinned["owned_paths"]
        if set(previous) <= set(current["owned_paths"]) and (
            record is None or _same_worktree(record["worktree_path"], worktree_path)
        ):
            _publish_scope(repo, pinned, worktree_path, current["owned_paths"])
        return pinned
    # Do not turn mutation of base/branch/worktree/history into a fresh source.
    # Explicit registry correction is a separate audited producer operation.
    return pinned


def task_subject_binding(row: dict) -> dict | None:
    notes = row.get("notes")
    try:
        envelope = json.loads(notes) if isinstance(notes, str) and notes.strip().startswith("{") else None
    except ValueError:
        return None
    binding = envelope.get(TASK_SUBJECT_FIELD) if isinstance(envelope, dict) else None
    return binding if isinstance(binding, dict) else None


def _proven_task_subject(repo: Path, orchestrator_root: Path, task_ref: str, *, branch: str | None = None,
                         historical_baseline: bool = False) -> dict | None:
    """The task's producer-pinned original subject, re-verified against canonical evidence.

    Absent, forged or stale bindings are unproven (``None``), so dispatch keeps
    its ancestry hold; two different bindings for one task refuse. The
    dispatching source must descend from the proven baseline.
    """
    from workbay_handoff_mcp.lanes_api import list_lanes

    result = list_lanes(task_ref=task_ref, limit=_MAX_TASK_LANES)
    data = result.get("data") if isinstance(result, dict) else None
    if not isinstance(data, dict) or result.get("ok") is not True or data.get("has_more"):
        raise RatchetRefused("review_ratchet_subject_lookup_failed")
    bindings = [(row, binding) for row in data.get("lanes") or [] if (binding := task_subject_binding(row))]
    task = task_record(task_ref) or {}
    # A main-target task is a parent of work units, not itself one feature.
    # Leaf tasks still have one original subject shared by all their children.
    if _short_branch(task.get("target_branch")) == "main" and branch is not None:
        bindings = [(row, binding) for row, binding in bindings if binding.get("subject_branch") == branch]
    if not bindings:
        return None
    if len({json.dumps(binding, sort_keys=True) for _, binding in bindings}) > 1:
        raise RatchetRefused("review_ratchet_task_subject_conflict")
    row, binding = bindings[0]
    target = _subject_worktree(orchestrator_root, task, task_ref, str(row.get("lane_id") or ""))
    if (
        binding.get("task_ref") != task_ref
        or row.get("task_ref") != task_ref
        or binding.get("lane_id") != row.get("lane_id")
        or binding.get("subject_branch") != _short_branch(row.get("branch"))
        or not _same_worktree(target, row.get("worktree_path"))
        # Review ranges advance independently; this is the original identity
        # proof, not source_review_scope's current integration-to-tip range.
        or not _binding_holds(repo, orchestrator_root, binding, historical_baseline=historical_baseline)
        or not _subject_is_ancestor(repo, binding["tip_sha"], "HEAD")
    ):
        return None
    return binding


def proven_task_subject(repo: Path, orchestrator_root: Path, task_ref: str, *, branch: str | None = None) -> dict | None:
    return _proven_task_subject(repo, orchestrator_root, task_ref, branch=branch)


def source_task_subject_scope(repo: Path, orchestrator_root: Path, snapshot: dict) -> list[str] | None:
    """Authenticate source-time scope separately from current authorization.

    New registrations keep their original binding. For pre-fix producers that
    already re-anchored on expansion, the original protected dispatch and its
    first-publication lane-cycle join are required. A current notes snapshot
    alone cannot retroactively supply source-time scope or worktree identity.
    """
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("task_ref"), str):
        return None
    current = _proven_task_subject(
        repo, orchestrator_root, snapshot["task_ref"], branch=snapshot.get("subject_branch"), historical_baseline=True
    )
    if current is None:
        return None
    record = _scope_record(repo, current)
    if current == snapshot:
        return record["authorized_scopes"][-1] if record else current["owned_paths"]
    # Legacy expansion is accepted only after the current public producer has
    # durably authorized scope. A manually edited manifest/notes pair is inert.
    if record is None or any(
        current.get(k) != snapshot.get(k)
        for k in ("task_ref", "lane_id", "subject_branch", "base_sha", "integration_branch")
    ):
        return None
    roots = record["authorized_scopes"][-1]
    original_roots = _owned_roots(snapshot.get("owned_paths"))
    if original_roots is None or not set(original_roots) < set(roots):
        return None
    original = _task_source(
        repo,
        orchestrator_root,
        task_ref=snapshot["task_ref"],
        lane_id=snapshot["lane_id"],
        branch=snapshot["subject_branch"],
        tip=snapshot.get("tip_sha"),
        historical_baseline=True,
    )
    if original != {**snapshot, "owned_paths": roots}:
        return None
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    from .review_cycle_dispatch import authority
    from .review_cycle_reconciliation import EvidenceHold, _read

    private = authority(repo)
    from workbay_handoff_mcp import RuntimeConfig

    state_dir = RuntimeConfig.for_repo(orchestrator_root).state_dir
    try:
        # Stream bounded records rather than making unrelated lifetime history
        # an authorization limit. Only matching source receipts need a pass join.
        authenticated = False
        for path in (private / "review_cycle_dispatch").glob("*.json"):
            receipt, _ = _read(path, private)
            identity = receipt.get("identity") or {}
            if (
                receipt.get("source_task_subject") != snapshot
                or identity.get("kind") != "implement"
                or identity.get("subject_branch") != snapshot["subject_branch"]
                or receipt.get("source_branch") != snapshot["subject_branch"]
                or receipt.get("source_head") != snapshot["tip_sha"]
                or receipt.get("source_tree") != _git(repo, "rev-parse", snapshot["tip_sha"] + "^{tree}")
                or path.stem != hashlib.sha256(identity.get("dispatch_id", "").encode()).hexdigest()
            ):
                continue
            linked = False
            for pass_path in state_dir.glob("offload-pass-*.json"):
                state, _ = _read(pass_path, state_dir)
                if state.get("review_cycle_dispatch_id") != identity["dispatch_id"]:
                    continue
                if linked:
                    return None
                linked = True
                binding = state.get("lane_cycle_binding") or {}
                if (
                    any(binding.get(k) != snapshot.get(k) for k in ("task_ref", "lane_id"))
                    or binding.get("branch") != snapshot["subject_branch"]
                    or binding.get("source_base_sha") != snapshot["base_sha"]
                    or not _same_worktree(binding.get("worktree_path"), record["worktree_path"])
                ):
                    return None
                with _get_db_connection() as conn:
                    row = conn.execute("SELECT * FROM worktree_lanes WHERE id=?", (binding.get("row_id"),)).fetchone()
                if row is not None and all(
                    dict(row).get(k) == binding.get(k)
                    for k in ("task_ref", "lane_id", "branch", "worktree_path", "cycle_epoch")
                ):
                    authenticated = True
    except (EvidenceHold, OSError, ValueError, TypeError, KeyError):
        return None
    # Do not accept the first valid join before checking for duplicate links.
    return roots if authenticated else None


def source_task_subject_holds(repo: Path, orchestrator_root: Path, snapshot: dict) -> bool:
    return source_task_subject_scope(repo, orchestrator_root, snapshot) is not None


def verified_legacy_source_history(repo: Path, subject_branch: str) -> list[dict]:
    """Authenticate original synchronous and detached sources, never a count.

    The original dispatch_id is authority; a later detached representation is
    not another author. Known history without proof holds. Imported records
    retain the stored route, outcome and byte-level lineage (Canon v0.26.2:
    DATA-14 No dual writes, Designing Data-Intensive Applications, ch. 11;
    GRPH-14 Record provenance as a walkable lineage graph, Building Knowledge
    Graphs, ch. 8 + Mental Models, ch. 8; CARD-02, Principles 9/12).
    """
    import hashlib

    from workbay_handoff_mcp.shared_schema import _get_db_connection

    from .adapters.remote_exec import _source_provenance_from_json
    from .offload_pass import _detached_artifact_paths
    from .remote_job_ledger import RemoteJobLedger, RemoteJobLedgerError
    from .review_cycle_dispatch import enforce_route, registered_lane

    unavailable = "review_ratchet_legacy_source_proof_unavailable"
    mismatch = "review_ratchet_legacy_source_identity_mismatch"
    registered = registered_lane(repo, subject_branch)
    if registered is None:
        from workbay_handoff_mcp import RuntimeConfig, configure_runtime

        config = RuntimeConfig.for_repo(repo)
        if config.db_path.exists():
            configure_runtime(config)
            with _get_db_connection() as conn:
                if conn.execute("SELECT 1 FROM review_runs WHERE branch = ? LIMIT 1", (subject_branch,)).fetchone():
                    raise RatchetRefused(unavailable)
        return []
    config, lane = registered
    with _get_db_connection() as conn:
        known = conn.execute("SELECT 1 FROM review_runs WHERE branch = ? LIMIT 1", (subject_branch,)).fetchone() is not None
    if lane.get("lane_kind") != "implement":
        return []

    def read(path):
        content = path.read_bytes()
        value = json.loads(content)
        if not isinstance(value, dict):
            raise RatchetRefused(unavailable)
        return value, hashlib.sha256(content).hexdigest()

    def original_spool(state):
        # Relocate an intact host spool only through its recorded root-relative
        # locator. No basename search across arbitrary remote-exec directories.
        stream = Path(state["agent_stream_path"])
        relative = stream.relative_to(Path(state["orchestrator_root"]) / ".task-state")
        if len(relative.parts) != 2 or not relative.parts[0].startswith("remote-exec-") or ".." in relative.parts:
            raise RatchetRefused(unavailable)
        return config.state_dir / relative.parent

    def authenticate(state, state_path, *, pass_id, dispatch_id, agent, provenance_path, started_at, job_path=None):
        completion = state.get("completion_config") or {}
        if ((state.get("task_ref"), state.get("lane_id"), state.get("branch")) !=
                (lane["task_ref"], lane["lane_id"], subject_branch)):
            raise RatchetRefused(mismatch)
        head = completion.get("start_head_ref")
        tip = _commit(repo, f"refs/heads/{subject_branch}")
        if (not isinstance(head, str) or not re.fullmatch("[0-9a-f]{40}", head)
                or _commit(repo, head) != head or tip is None or not _subject_is_ancestor(repo, head, tip)):
            raise RatchetRefused("review_ratchet_legacy_source_git_mismatch")
        tree = _git(repo, "rev-parse", head + "^{tree}")
        provenance, failure = _source_provenance_from_json(
            provenance_path, branch=subject_branch, head_sha=head, head_tree=tree)
        if failure or not provenance or provenance.get("model_process_started") is not True:
            raise RatchetRefused(unavailable)
        executed, spec_hash = read(agent)
        argv_path = agent.with_suffix(".argv")
        argv_bytes = argv_path.read_bytes()
        argv = executed.get("argv")
        if (not isinstance(argv, list) or not all(isinstance(arg, str) and "\0" not in arg for arg in argv)
                or argv_bytes != b"".join(arg.encode() + b"\0" for arg in argv)):
            raise RatchetRefused("review_ratchet_legacy_source_route_unavailable")
        route = dict(model=state["model"], effort=state["reasoning_effort"])
        enforce_route(executed, {"required_route": route})
        if (executed.get("branch"), executed.get("head_sha"), executed.get("backend_id")) != (
                subject_branch, head, state["backend"]):
            raise RatchetRefused(mismatch)
        for key, value in (("backend", state["backend"]), ("model", route["model"]),
                           ("reasoning_effort", route["effort"])):
            if key in completion and completion[key] != value:
                raise RatchetRefused(mismatch)
        # Requested routing describes intent, which can differ from execution.
        # State + argv + provenance authenticate the charged route; an effective
        # receipt, when present, must still agree with that execution.
        if "routing_requested" in executed:
            requested = executed["routing_requested"]
            if not isinstance(requested, dict) or any(
                    requested.get(k) is not None and not isinstance(requested[k], str)
                    for k in ("backend", "model", "effort")):
                raise RatchetRefused(mismatch)
        if "routing_effective" in executed:
            effective = executed["routing_effective"]
            if not isinstance(effective, dict) or any(effective.get(k) != v for k, v in
                    (("backend", state["backend"]), ("model", route["model"]), ("effort", route["effort"]))):
                raise RatchetRefused(mismatch)
        lane_spec = state.get("lane_spec") or {}
        for key, expected in (("backend", state["backend"]), ("model", route["model"]), ("effort", route["effort"])):
            if key in lane_spec and lane_spec[key] != expected:
                raise RatchetRefused(mismatch)
        binding = state.get("lane_cycle_binding")
        if binding is not None:
            if any(binding.get(k) != state.get(k) for k in ("task_ref", "lane_id", "branch", "worktree_path")):
                raise RatchetRefused(mismatch)
            base = binding["source_base_sha"]
            if _commit(repo, base) != base or not _subject_is_ancestor(repo, base, head):
                raise RatchetRefused("review_ratchet_legacy_source_git_mismatch")
        result = state.get("result") or {}
        for key, expected in (("dispatch_id", dispatch_id), ("pass_id", pass_id),
                              ("task_ref", lane["task_ref"]), ("lane_id", lane["lane_id"]),
                              ("branch", subject_branch), ("backend", state["backend"]),
                              ("model", route["model"]), ("reasoning_effort", route["effort"])):
            if key in result and result[key] != expected:
                raise RatchetRefused(mismatch)
        checkpoint = state.get("tip_sha")
        if checkpoint and (_commit(repo, checkpoint) != checkpoint or not _subject_is_ancestor(repo, head, checkpoint)
                           or not _subject_is_ancestor(repo, checkpoint, tip)):
            raise RatchetRefused("review_ratchet_legacy_source_git_mismatch")
        if not all(isinstance(x, str) and x for x in (pass_id, dispatch_id, started_at)):
            raise RatchetRefused(mismatch)
        producer = dict(pass_state=str(state_path), pass_state_sha256=hashlib.sha256(state_path.read_bytes()).hexdigest(),
                        provenance=str(provenance_path),
                        provenance_sha256=hashlib.sha256(provenance_path.read_bytes()).hexdigest(),
                        dispatch_nonce=provenance["dispatch_nonce"], agent_spec=str(agent), agent_spec_sha256=spec_hash,
                        agent_argv=str(argv_path), agent_argv_sha256=hashlib.sha256(argv_bytes).hexdigest())
        if job_path is not None:
            producer["job"] = str(job_path)
            producer["job_sha256"] = hashlib.sha256(job_path.read_bytes()).hexdigest()
        if state.get("agent_stream_path"):
            producer["original_agent_stream"] = state["agent_stream_path"]
        return dict(dispatch_id=dispatch_id, pass_id=pass_id, kind="implement", subject_branch=subject_branch,
                    task_ref=lane["task_ref"], lane_id=lane["lane_id"], backend=state["backend"],
                    required_route=route, charged=True, outcome=result.get("outcome", "remote_job_pending"),
                    source_head=head, source_tree=tree, started_at=started_at,
                    checkpoint_tip=checkpoint, producer=producer)

    records = []
    detached_states = set()
    try:
        ledger = RemoteJobLedger(config.state_dir / "remote-jobs")
        for path in sorted(ledger.ledger_dir.glob("j*.json")):
            job = ledger.read_job(path.stem)
            spec = job["spec"]
            if spec.get("branch") != subject_branch or job.get("stage") == "intent":
                continue
            state_path = config.state_dir / f"offload-pass-{spec['pass_id']}.json"
            state, _ = read(state_path)
            if (state.get("lane_spec") or {}).get("lane_kind", "implement") != "implement":
                continue
            known = True
            if any(state.get(key) != spec.get(key) for key in ("task_ref", "lane_id", "branch", "backend")):
                raise RatchetRefused(mismatch)
            for key in ("job_id", "pass_id", "attempt"):
                if key in state and state[key] != spec[key]:
                    raise RatchetRefused(mismatch)
            completion = state.get("completion_config") or {}
            if "pass_id" in completion and completion["pass_id"] != spec["pass_id"]:
                raise RatchetRefused(mismatch)
            argv = spec["argv"]
            positions = [i + 1 for i, arg in enumerate(argv[:-1]) if arg == "--agent-spec"]
            if len(positions) != 1:
                raise RatchetRefused("review_ratchet_legacy_source_route_unavailable")
            agent = Path(argv[positions[0]])
            if not agent.is_absolute():
                agent = Path(state["worktree_path"]) / agent
            paths = _detached_artifact_paths(argv, Path(state["worktree_path"]))
            records.append(authenticate(state, state_path, pass_id=spec["pass_id"],
                dispatch_id=state.get("dispatch_id") or spec["job_id"], agent=agent,
                provenance_path=paths["provenance"], started_at=state.get("started_at") or spec["created_at"], job_path=path))
            detached_states.add(state_path)
        for path in sorted(config.state_dir.glob("offload-pass-*.json")):
            state, _ = read(path)
            if state.get("branch") != subject_branch:
                continue
            kind = (state.get("lane_spec") or {}).get("lane_kind")
            if kind not in {None, "implement"}:
                continue
            known = True
            if state.get("job_id"):
                if path not in detached_states:
                    raise RatchetRefused(unavailable)  # an orphan is known history, not a fresh source1
                continue
            if kind is None:
                raise RatchetRefused(unavailable)
            completion = state.get("completion_config") or {}
            pass_id = completion["pass_id"]
            if (path.name != f"offload-pass-{pass_id}.json" or not state.get("lane_cycle_binding")
                    or completion.get("dispatch_id") != f"offload-pass:{state['task_ref']}:{state['lane_id']}:{pass_id}"):
                raise RatchetRefused(mismatch)
            spool = original_spool(state)
            records.append(authenticate(state, path, pass_id=pass_id, dispatch_id=state["dispatch_id"],
                agent=spool / "spec.json", provenance_path=spool / "source-provenance.json", started_at=state["started_at"]))
    except RatchetRefused:
        raise
    except (OSError, ValueError, KeyError, TypeError, AttributeError, RemoteJobLedgerError) as exc:
        raise RatchetRefused(unavailable) from exc
    if known and not records:
        raise RatchetRefused(unavailable)
    return _deduplicate_legacy_sources(records)


def _deduplicate_legacy_sources(records: list[dict]) -> list[dict]:
    # All three identities must agree to collapse two representations. A reused
    # nonce, dispatch ID or pass ID with different source facts is a hold.
    unique = []
    for record in sorted(records, key=lambda row: (row["started_at"], row["dispatch_id"])):
        for prior in unique:
            if (record["dispatch_id"] == prior["dispatch_id"] or record["pass_id"] == prior["pass_id"]
                    or record["producer"]["dispatch_nonce"] == prior["producer"]["dispatch_nonce"]):
                facts = {k: v for k, v in record.items() if k != "producer"}
                if (facts != {k: v for k, v in prior.items() if k != "producer"}
                        or record["producer"]["dispatch_nonce"] != prior["producer"]["dispatch_nonce"]
                        or any(record["producer"][k] != prior["producer"][k] for k in
                               ("provenance_sha256", "agent_spec_sha256", "agent_argv_sha256"))):
                    raise RatchetRefused("review_ratchet_legacy_source_identity_mismatch")
                prior["producer"].setdefault("representations", []).append(record["producer"])
                break
        else:
            unique.append(record)
    return unique


def load_context(
    repo: Path,
    state_dir: Path,
    *,
    task_ref: str,
    branch: str,
    current_pass_id: str | None = None,
    before: str | None = None,
    orchestrator_root: Path | None = None,
    review_subject: tuple[str, str] | None = None,
    configure: bool = True,
) -> ReviewContext | None:
    """Discover only existing canonical engine receipts for this host subject.

    A separate review branch inherits its original subject's receipts only through
    the durable validated review subject (or ``review_subject`` supplied by the
    public upsert validating it). ``configure=False`` keeps the caller's runtime.
    """
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    from .offload_pass import read_pass_state

    def eligible(identity: dict) -> bool:
        return (
            identity.get("task_ref") == task_ref
            and identity.get("pass_id") != current_pass_id
            and (before is None or str(identity.get("started_at") or "") < before)
        )

    receipts = []
    for path in state_dir.glob("review-receipt-*.json"):
        try:
            receipt = json.loads(path.read_text())
            identity = receipt.get("identity") or {}
        except (OSError, ValueError, AttributeError) as exc:
            raise RatchetRefused("review_ratchet_receipt_unreadable") from exc
        if eligible(identity) and isinstance(identity.get("branch"), str):
            receipts.append((str(identity.get("started_at") or ""), path.name, receipt))
    retained = []
    for path in state_dir.glob("offload-pass-*.json"):
        try:
            prior = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if (
            isinstance(prior, dict)
            and eligible(prior)
            and prior.get("findings_harvest")
            and isinstance(prior.get("branch"), str)
        ):
            retained.append(prior)
    # Latest engine base per history branch, from receipts and retained pass state.
    history: dict[str, tuple[str, str, dict]] = {}
    for started, identity in [(item[0], item[2]["identity"]) for item in receipts] + [
        (str(item.get("started_at") or ""), item) for item in retained
    ]:
        name = identity["branch"]
        if name not in history or started >= history[name][0]:
            history[name] = (started, str(identity.get("base_sha") or ""), identity)
    configured = False
    subject_branches = {branch}
    if any(name != branch for name in history):
        if review_subject is None:
            if configure:
                configure_runtime(RuntimeConfig.for_repo(orchestrator_root or repo))
                configured = True
            review_subject = _durable_review_subject(task_ref, branch)
        bound = _bind_original_subject(repo, review_subject, history, branch=branch)
        if bound is not None:
            subject_branches.add(bound)
    candidates = [item for item in receipts if item[2]["identity"]["branch"] in subject_branches]
    if not candidates:
        if any(prior["branch"] in subject_branches for prior in retained):
            raise RatchetRefused("review_ratchet_previous_receipt_missing")
        return None
    _, _, receipt = max(candidates, key=lambda item: item[:2])
    # Publication can fail after independent state is saved. An older receipt
    # must not hide that latest review when a worker loses its in-memory pointer.
    latest_started = str(receipt["identity"].get("started_at") or "")
    for prior in retained:
        if prior["branch"] not in subject_branches or str(prior.get("started_at") or "") < latest_started:
            continue
        if not any(
            all(
                candidate[2]["identity"].get(key) == prior.get(key)
                for key in ("pass_id", "task_ref", "lane_id", "tip_sha", "base_sha", "branch")
            )
            for candidate in candidates
        ):
            raise RatchetRefused("review_ratchet_previous_receipt_missing")
    state = read_pass_state(state_dir, receipt["identity"].get("pass_id", "")) or {}
    if configure and not configured:
        configure_runtime(RuntimeConfig.for_repo(orchestrator_root or repo))
    with _get_db_connection() as conn:
        rows = [dict(row) for row in conn.execute("SELECT * FROM review_findings WHERE task_ref = ?", (task_ref,))]
    return context_from_receipt(repo, receipt, state, rows, task_ref=task_ref, branch=receipt["identity"]["branch"])


def render_brief(context: ReviewContext, *, questions: bool = True) -> str:
    prompt = "\nD1 NAMED RE-REVIEW\n" + json.dumps(
        {"listed_finding_ids": context.listed_finding_ids, "prior_reviewed_tip": context.prior_reviewed_tip},
        sort_keys=True,
    )
    if questions:
        prompt += (
            "\nDamage questions (yes/no):\n"
            "1. Is each named finding still open after executing its correction?\n"
            "2. Did the fix delta introduce realistic damage?\n"
            "3. Does new outside-delta damage execute on the merge tree and pass exact main?\n"
        )
    prompt += (
        "Not your problem: unrelated refinements, formatting, and reasoning-only outside-delta candidates. "
        "Keep original finding text and tags. Never claim execution from a written command alone.\n"
        "ACTUAL FIX DELTA\n" + context.fix_delta
    )
    return prompt


@dataclass(frozen=True)
class ExecutedDamage:
    """Created by the engine after actual both-arm execution; never decoded from JSON."""

    tip: str
    baseline: str
    node_id: str
    merge_exit: int
    baseline_exit: int
    merge_output: str
    baseline_output: str


def execute_damage(repo: Path, context: ReviewContext, finding: dict) -> ExecutedDamage | None:
    """Execute an existing pytest scenario on the lane merge tree and exact baseline.

    The reviewer may nominate a test, never supply its result. A test absent from
    baseline is not comparable evidence. Collection/environment errors are not damage.
    """
    evidence = finding.get("evidence")
    node = evidence.get("test_node_id") if isinstance(evidence, dict) else None
    scenario = finding.get("failure_scenario")
    if not isinstance(scenario, str) or not scenario.strip() or not isinstance(node, str):
        return None
    test_file = node.split("::", 1)[0]
    if (
        not re.fullmatch(r"[A-Za-z0-9_./-]+\.py(?:::[A-Za-z0-9_\[\].-]+)+", node)
        or ".." in Path(test_file).parts
        or node.startswith("/")
    ):
        return None
    # The scenario must be the same code on both arms; a newly written failing test
    # may accompany a report, but is not itself a sealed comparative reproduction.
    try:
        baseline_blob = _git(repo, "rev-parse", f"{context.baseline_tip}:{test_file}")
        merge_tree = _git(repo, "merge-tree", "--write-tree", context.baseline_tip, context.reviewed_tip).splitlines()[
            0
        ]
        if _git(repo, "rev-parse", f"{merge_tree}:{test_file}") != baseline_blob:
            return None
    except RatchetRefused:
        return None
    # Both arms spawn git archive and pytest; neither may run under a held write lock.
    assert_no_write_lock_held("subprocess")
    results = []
    for tree in (merge_tree, context.baseline_tip):
        root = Path(tempfile.mkdtemp(prefix="workbay-d1-repro-"))
        archive = subprocess.run(["git", "-C", str(repo), "archive", tree], capture_output=True, check=False)
        if archive.returncode:
            return None
        with tarfile.open(fileobj=io.BytesIO(archive.stdout)) as bundle:
            bundle.extractall(root, filter="data")
        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = ""
        env["PYTHONPATH"] = os.pathsep.join([str(root), *(str(p) for p in sorted((root / "packages").glob("*/src")))])
        command = [
            sys.executable,
            "-m",
            "pytest",
            "-n0",
            "-q",
            "-p",
            "no:cacheprovider",
            "--basetemp=" + tempfile.mkdtemp(prefix="workbay-d1-pytest-"),
            node,
        ]
        try:
            result = subprocess.run(command, cwd=root, env=env, capture_output=True, text=True, timeout=60, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return None
        output = result.stdout + result.stderr
        # pytest exit 1 means a test failed; other codes are infrastructure/collection.
        if result.returncode not in (0, 1) or (result.returncode == 1 and "FAILED " not in output):
            return None
        results.append((result.returncode, output))
    return ExecutedDamage(
        context.reviewed_tip, context.baseline_tip, node, results[0][0], results[1][0], results[0][1], results[1][1]
    )


def attribute(
    findings: list[dict], context: ReviewContext, *, executed: dict[int, ExecutedDamage] | None = None
) -> list[dict]:
    """Apply D1 without altering severity, original tags, or listed judgments."""
    from workbay_handoff_mcp.review_findings_recording import _finding_content_fingerprint

    listed = {
        _finding_content_fingerprint(item["file_path"], item["description"]): item["finding_id"]
        for item in context.listed_findings
    }
    out = []
    for index, finding in enumerate(findings):
        item = dict(finding)
        fingerprint = _finding_content_fingerprint(
            item.get("file_path", ""), f"[{item.get('category') or 'GAP'}] {item.get('description') or ''}"
        )
        if fingerprint in listed:
            item["origin"] = "listed_open"
        elif is_blocking({**item, "origin": "introduced"}):
            path = _normalized_diff_path(item.get("file_path"))
            in_delta = (
                _line_range_intersects(item, path, context.diff_ranges)
                if item.get("line_start") is not None
                else _finding_is_in_diff(item, context.changed_files, context.diff_ranges)
            )
            proof = (executed or {}).get(index)
            valid = (
                isinstance(proof, ExecutedDamage)
                and proof.tip == context.reviewed_tip
                and proof.baseline == context.baseline_tip
                and proof.merge_exit == 1
            )
            if in_delta or (valid and proof.baseline_exit == 0):
                item["origin"] = "introduced"
                item["impact"] = "blocking"
                item["impact_reason"] = "fix_delta" if in_delta else "executed_lane_damage"
            elif valid and proof.baseline_exit == 1:
                item["origin"] = "pre_existing"
                # Keep the existing genuine-critical policy through is_blocking.
                if not is_blocking(item):
                    item["impact"] = "follow_up"
                item["impact_reason"] = "baseline_damage"
            else:
                item["impact"] = "follow_up"
                item["impact_reason"] = "round_n_candidate"
        out.append(item)
    return out


def apply(
    repo: Path, findings: list[dict], context: ReviewContext | None, *, evidence_log: list[dict] | None = None
) -> list[dict]:
    if context is None:
        return findings
    executed = {}
    for index, item in enumerate(findings):
        proof = execute_damage(repo, context, item)
        if proof is not None:
            executed[index] = proof
            if evidence_log is not None:
                evidence_log.append({"finding_index": index, **asdict(proof)})
    return attribute(findings, context, executed=executed)


def reopen_new_damage(root: Path, findings: list[dict], context: ReviewContext, *, task_ref: str) -> None:
    """Use controlled lifecycle writes before D5's durable disposition lookup.

    Only engine-attributed fresh facts enter this function. Repeated reports at
    the same tip keep their old disposition; the reopen reason retains provenance.
    """
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime, review_findings
    from workbay_handoff_mcp.review_findings_recording import _reusable_review_finding
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    configure_runtime(RuntimeConfig.for_repo(root))
    for item in findings:
        if item.get("impact_reason") not in ("fix_delta", "executed_lane_damage") or not is_blocking(item):
            continue
        with _get_db_connection() as conn:
            row = _reusable_review_finding(
                conn, task_ref, item["file_path"], f"[{item.get('category') or 'GAP'}] {item['description']}"
            )
        if row is None:
            continue
        reason = f"D1 {item['impact_reason']}: {context.prior_reviewed_tip}..{context.reviewed_tip} baseline={context.baseline_tip}"
        if row["last_reopen_reason"] == reason or (
            context.reviewed_tip in str(row["last_reopen_reason"] or "")
            and row["impact_reason"] == item["impact_reason"]
        ):
            continue
        result = review_findings(
            review={
                "operation": "update",
                "status": "open",
                "finding_id": row["finding_id"],
                "task_ref": task_ref,
                "reopen_reason": reason,
                "impact": "blocking",
                "origin": "introduced",
                "impact_reason": item["impact_reason"],
                "failure_scenario": item.get("failure_scenario"),
            }
        )
        if not result.get("ok"):
            raise RatchetRefused("review_ratchet_controlled_reopen_failed")


def unresolved_listed(root: Path, context: ReviewContext, *, task_ref: str) -> list[str]:
    """A silent/empty review cannot close a previously named blocker."""
    from workbay_handoff_mcp import RuntimeConfig, configure_runtime
    from workbay_handoff_mcp.shared_schema import _get_db_connection

    configure_runtime(RuntimeConfig.for_repo(root))
    with _get_db_connection() as conn:
        rows = {
            row["finding_id"]: dict(row)
            for row in conn.execute("SELECT * FROM review_findings WHERE task_ref = ?", (task_ref,))
        }
    return [
        fid
        for fid in context.listed_finding_ids
        if fid not in rows
        or (rows[fid]["status"] == "open" and is_blocking(rows[fid]))
        or (
            rows[fid]["status"] in ("fixed", "resolved_on_branch", "integrated")
            and not rows[fid].get("verification_evidence")
        )
    ]
