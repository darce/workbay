"""D1 attribution on canonical re-review receipts, using the existing impact bar.

No reviewer-provided listed IDs, source tips or execution booleans are authority.
The host engine loads its preceding harvest and executes both reproduction arms.
"""

from __future__ import annotations

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
    repo: Path, orchestrator_root: Path, *, task_ref: str, lane_id: str, branch: str, tip: str | None
) -> dict | None:
    """Evidence that an unfinished canonical task owns ``base..tip`` on a landed baseline.

    Only the durable task record, its manifest lane (owned scope and base) and
    immutable commits participate. Absent or contradictory evidence is ``None``.
    """
    task = task_record(task_ref)
    located = _manifest_lane(orchestrator_root, task_ref, lane_id)
    if task is None or task["status"] == "done" or _short_branch(task["target_branch"]) != branch or located is None:
        return None
    manifest, lane = located
    roots = _owned_roots(lane.get("owned_paths"))
    base = lane.get("base_sha")
    integration = _short_branch(manifest.get("integration_branch")) or "main"
    if (
        _short_branch(lane.get("branch")) != branch
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
    if tip == base and landed != base:
        return None
    changed = sorted(_git(repo, "diff", "--name-only", "--no-renames", base, tip, "--").splitlines())
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


def _binding_holds(repo: Path, orchestrator_root: Path, binding: dict) -> bool:
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
    )
    return current == binding


def derive_task_subject(
    repo: Path, orchestrator_root: Path, *, task_ref: str, lane_id: str, branch: str, worktree_path: str
) -> dict | None:
    """Producer proof that a separate canonical task owns its new source.

    The registered lane must be the task's own target branch and worktree, its
    manifest lane must name the owned scope and a base already on the
    integration branch, and every change in ``base..tip`` must be owned. The
    subject is the task's own branch, never a newer row, timestamp or label.
    """
    task = task_record(task_ref)
    target = task.get("target_worktree_path") if task else None
    try:
        same_worktree = (
            bool(target) and Path(target).expanduser().resolve() == Path(worktree_path).expanduser().resolve()
        )
    except (OSError, RuntimeError):
        same_worktree = False
    if not same_worktree:
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
        return fresh
    if fresh is None or fresh == pinned:
        return pinned
    same_subject = all(pinned.get(key) == fresh[key] for key in ("task_ref", "lane_id", "subject_branch"))
    return fresh if same_subject and not _binding_holds(repo, orchestrator_root, pinned) else pinned


def task_subject_binding(row: dict) -> dict | None:
    notes = row.get("notes")
    try:
        envelope = json.loads(notes) if isinstance(notes, str) and notes.strip().startswith("{") else None
    except ValueError:
        return None
    binding = envelope.get(TASK_SUBJECT_FIELD) if isinstance(envelope, dict) else None
    return binding if isinstance(binding, dict) else None


def proven_task_subject(repo: Path, orchestrator_root: Path, task_ref: str) -> dict | None:
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
    if not bindings:
        return None
    if len({json.dumps(binding, sort_keys=True) for _, binding in bindings}) > 1:
        raise RatchetRefused("review_ratchet_task_subject_conflict")
    row, binding = bindings[0]
    if (
        binding.get("task_ref") != task_ref
        or row.get("task_ref") != task_ref
        or binding.get("lane_id") != row.get("lane_id")
        or binding.get("subject_branch") != _short_branch(row.get("branch"))
        or not _binding_holds(repo, orchestrator_root, binding)
        or not _subject_is_ancestor(repo, binding["tip_sha"], "HEAD")
    ):
        return None
    return binding


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
