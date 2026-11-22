"""Retire review-stage worktrees after their subject is safely on ``main``."""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

from workbay_system.payload.scripts import review_pipeline as stage_evidence

from .. import lane_reaping
from . import branch_reclaim_delete, landing_log
from .post_merge_reap import REVIEW_INVOCATION_LOCK_HELD, review_sweep_action

# Mirrors ``_review_branch`` / ``_branch_file_slug`` in
# packages/workbay-system/workbay_system/payload/scripts/review_pipeline.py.
_REVIEW_BRANCH_RE = re.compile(r"^feature/rev(?P<rev>[0-9]+)-(?P<slug>[A-Za-z0-9._-]+)$")
_FULL_SHA_RE = stage_evidence.FULL_OBJECT_ID
_STAGE_TITLE = stage_evidence.STAGE_TITLE
_LEGACY_STAGE_FIELDS = {"slug", "subject", "subject_tip", "patch", "keys", "staged_commit", "ts"}
_ARCHIVE_STAGING_DIR = ".task-state/branch-archive/.review-stage-staging"


def _git(root: Path, *args: str):
    return lane_reaping._run_reclaim_command(["git", "-C", str(root), *args])


def _stdout(result: Any) -> str:
    return lane_reaping._probe_command_stdout(result)


def _resolve_commit(root: Path, ref: str) -> str | None:
    result = _git(root, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
    if result is None or result.returncode != 0:
        return None
    value = _stdout(result).strip().lower()
    return value if _FULL_SHA_RE.fullmatch(value) else None


def _is_ancestor(root: Path, commit: str, target: str) -> tuple[bool | None, str]:
    try:
        carrier = landing_log.find_carrier(root, commit, main_ref=target, timeout_s=10.0)
    except Exception as exc:  # noqa: BLE001 - containment failures must stay unknown
        return None, f"carrier_probe_failed:{type(exc).__name__}"
    if carrier.error:
        return None, carrier.error
    if carrier.main_tip is None:
        return None, "main_ref_unresolved"
    if carrier.contained is True:
        return True, ""
    if carrier.contained is False:
        return False, ""
    return None, "carrier_result_unresolved"


def _review_branch_parts(branch: str) -> tuple[int, str] | None:
    match = _REVIEW_BRANCH_RE.fullmatch(branch)
    if match is None or not match.group("slug").strip("-."):
        return None
    return int(match.group("rev")), match.group("slug")


def _stage_git(root: Path, *args: str) -> str | None:
    result = _git(root, *args)
    return _stdout(result) if result is not None and result.returncode == 0 else None


def _latest_stage(root: Path, review_tip: str) -> tuple[tuple[str, str, str] | None, str]:
    return stage_evidence.latest_review_stage(root, review_tip, git=_stage_git)


def _git_bytes(root: Path, *args: str) -> bytes | None:
    # Patch hashes cover raw bytes, including CRLF and non-UTF-8 data.
    lane_reaping._assert_no_reaper_write_lock(reason="review_stage_patch_probe")
    remaining = lane_reaping._registry_reap_remaining()
    timeout = 10 if remaining is None else min(10, remaining)
    try:
        result = subprocess.run(  # noqa: S603 - fixed Git command with validated object names
            ["git", "-C", str(root), *args], capture_output=True, check=False, timeout=timeout
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout if result.returncode == 0 else None


def _stage_generation_proven(root: Path, branch: str, review_tip: str, stage: str, title: str) -> bool:
    # A copy carries the source's entire reflog. Read the native transaction
    # chain, including its creation and any copy/rename boundary, in order.
    log = lane_reaping._reclaim_git_path(root, "--git-path", f"logs/refs/heads/{branch}")
    lines = lane_reaping._reclaim_reflog_lines(log) if log else None
    if not lines:
        return False
    previous = "0" * len(stage)
    stage_parent = _resolve_commit(root, f"{stage}^")
    seen_stage = False
    for line in lines:
        head, tab, message = line.partition("\t")
        fields = lane_reaping._BRANCH_REFLOG_HEAD.fullmatch(head)
        if not tab or fields is None:
            return False
        old, new = fields.group(1, 2)
        if old != previous or not _FULL_SHA_RE.fullmatch(new) or len(new) != len(stage):
            return False
        previous = new
        if old == "0" * len(stage) and not message.startswith("branch: Created from "):
            return False
        if message.startswith(("Branch: copied ", "Branch: renamed ")):
            seen_stage = False
        elif (old, new, message) == (stage_parent, stage, f"commit: {title}"):
            seen_stage = True
    return seen_stage and previous == review_tip


def _stage_record_subject(
    root: Path, branch: str, rev: int, review_tip: str, latest: tuple[str, str, str]
) -> tuple[str | None, bool, str, str]:
    stage, label, short = latest
    record, reason, sidecar_path = stage_evidence.read_review_stage(root, stage, label, short)
    sidecar = str(sidecar_path)
    if record is None:
        return None, False, reason, sidecar
    patch = f"docs/reviews/_input-{label}-{short}.patch"
    subject, subject_tip = record.get("subject"), record.get("subject_tip")
    if (
        not isinstance(subject, str)
        or not isinstance(subject_tip, str)
        or not _FULL_SHA_RE.fullmatch(subject_tip)
        or not subject_tip.startswith(short)
        or record.get("slug") != label
        or record.get("patch") != patch
        or record.get("staged_commit") != stage
    ):
        return None, False, "stage_sidecar_mismatch", sidecar
    subject_slug = re.sub(r"[^A-Za-z0-9._-]+", "-", subject.split("/", 1)[-1]).strip("-")
    if branch != f"feature/rev{rev}-{subject_slug}":
        return None, False, "stage_subject_mismatch", sidecar

    legacy = record.keys() == _LEGACY_STAGE_FIELDS
    schema = record.get("schema_version")
    if legacy:
        base_result = _git(root, "merge-base", f"{stage}^", subject_tip)
        if base_result is None or base_result.returncode != 0:
            return None, False, "legacy_stage_base_unavailable", sidecar
        base = _stdout(base_result).strip()
    else:
        if (
            type(record.get("schema_version")) is not int
            or schema not in {2, stage_evidence.STAGE_SCHEMA}
            or type(record.get("rev")) is not int
            or record["rev"] != rev
            or record.get("review_branch") != branch
        ):
            return None, False, "stage_scope_mismatch", sidecar
        if not {"integration", "keys", "worktree", "judge_provenance"}.issubset(record):
            return None, False, "stage_sidecar_malformed", sidecar
        base = record.get("base")
    title = f"{_STAGE_TITLE}{label} at {short}"
    if not _stage_generation_proven(root, branch, review_tip, stage, title):
        reason = "legacy_stage_branch_unbound" if legacy else "stage_generation_unbound"
        return None, False, reason, sidecar
    if not isinstance(base, str) or not _FULL_SHA_RE.fullmatch(base):
        return None, False, "stage_base_invalid", sidecar
    if _resolve_commit(root, subject_tip) != subject_tip:
        return None, False, "stage_subject_tip_unavailable", subject_tip
    base_ancestry = _git(root, "merge-base", "--is-ancestor", base, subject_tip)
    if base_ancestry is None or base_ancestry.returncode != 0:
        return None, False, "stage_base_unproven", base

    reason, detail = _verify_stage_patch(root, stage, patch, record, base, subject_tip, sidecar, legacy=legacy)
    if reason:
        return None, False, reason, detail
    return subject_tip, legacy, "", ""


def _verify_stage_patch(
    root: Path, stage: str, patch: str, record: dict, base: str, subject_tip: str, sidecar: str, *, legacy: bool
) -> tuple[str, str]:
    added = _git(
        root,
        "diff-tree",
        "--no-commit-id",
        "--name-only",
        "--no-renames",
        "--diff-filter=A",
        "-r",
        "-z",
        stage,
        "--",
        patch,
    )
    if added is None or added.returncode != 0:
        return "stage_patch_probe_failed", stage
    if _stdout(added) != f"{patch}\0":
        return "stage_patch_not_added", sidecar
    staged_patch = _git_bytes(root, "cat-file", "blob", f"{stage}:{patch}")
    if staged_patch is None:
        return "stage_patch_probe_failed", stage
    if not legacy and record.get("patch_sha256") != hashlib.sha256(staged_patch).hexdigest():
        return "stage_patch_digest_mismatch", sidecar
    message = _git_bytes(root, "show", "-s", "--format=%B", stage)
    if message is None:
        return "stage_commit_probe_failed", stage
    if record.get("schema_version") == stage_evidence.STAGE_SCHEMA:
        if base == subject_tip:
            return "stage_subject_patch_mismatch", sidecar
        blob = _git(root, "rev-parse", "--verify", f"{stage}:{patch}")
        if blob is None or blob.returncode != 0:
            return "stage_patch_probe_failed", stage
        if _stdout(blob).strip() != record.get("patch_blob"):
            return "stage_patch_digest_mismatch", sidecar
    if not stage_evidence.stage_binding_matches(message, record):
        return "stage_commit_binding_mismatch", sidecar
    if record.get("schema_version") != stage_evidence.STAGE_SCHEMA:
        lane_reaping._assert_no_reaper_write_lock(reason="legacy_review_stage_proof")
        remaining = lane_reaping._registry_reap_remaining()
        timeout = 10 if remaining is None else min(10, remaining)
        matches = stage_evidence.patch_matches_trees(
            root, base, subject_tip, staged_patch, timeout=timeout, historical=True
        )
        if matches is None:
            return "stage_subject_diff_probe_failed", subject_tip
        if not matches:
            return "stage_subject_patch_mismatch", sidecar
    return "", ""


def _subject_merge_proof(
    root: Path, branch: str, rev: int, review_tip: str, main_tip: str
) -> tuple[bool | None, str, str]:
    latest, reason = _latest_stage(root, review_tip)
    if latest is None:
        return None, reason, review_tip
    subject_tip, legacy, reason, detail = _stage_record_subject(root, branch, rev, review_tip, latest)
    if subject_tip is None:
        return None, reason, detail
    merged, detail = _is_ancestor(root, subject_tip, main_tip)
    if merged is True:
        return True, "legacy_stage_subject_merged" if legacy else "stage_subject_merged", subject_tip
    if merged is False:
        return False, "subject_not_merged", subject_tip
    return None, detail, subject_tip


def _parse_worktree_registry(text: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in text.splitlines():
        if not line:
            if current is not None:
                entries.append(current)
                current = None
            continue
        if line.startswith("worktree "):
            if current is not None:
                entries.append(current)
            current = {"path": line[len("worktree ") :]}
        elif current is None:
            continue
        elif line.startswith("HEAD "):
            current["head"] = line[len("HEAD ") :].strip().lower()
        elif line.startswith("branch "):
            current["branch"] = line[len("branch ") :].removeprefix("refs/heads/")
        elif line == "locked" or line.startswith("locked "):
            current["locked"] = True
        elif line == "prunable" or line.startswith("prunable "):
            current["prunable"] = True
    if current is not None:
        entries.append(current)
    return entries


def _worktree_registry(root: Path) -> tuple[list[dict[str, Any]] | None, str]:
    result = _git(root, "worktree", "list", "--porcelain")
    if result is None or result.returncode != 0:
        return None, "worktree_registry_unavailable"
    entries = _parse_worktree_registry(_stdout(result))
    if not entries:
        return None, "worktree_registry_empty"
    return entries, ""


def _local_review_branches(root: Path) -> tuple[dict[str, str] | None, str]:
    result = _git(root, "for-each-ref", "--format=%(refname:short)%09%(objectname)", "refs/heads/feature/rev*")
    if result is None or result.returncode != 0:
        return None, "review_branch_listing_failed"
    branches: dict[str, str] = {}
    for line in _stdout(result).splitlines():
        branch, separator, tip = line.partition("\t")
        if separator and _review_branch_parts(branch) is not None and _FULL_SHA_RE.fullmatch(tip.lower()):
            branches[branch] = tip.lower()
    return branches, ""


def _review_branches(
    local: dict[str, str], registry: list[dict[str, Any]], *, include_ref_only: bool
) -> dict[str, str]:
    branches = dict(local) if include_ref_only else {}
    for entry in registry:
        branch = entry.get("branch")
        if isinstance(branch, str) and _review_branch_parts(branch) is not None:
            branches.setdefault(branch, local.get(branch, ""))
    return branches


def _worktree_clean(path: Path) -> tuple[bool | None, str]:
    clean, detail = lane_reaping._probe_worktree_clean(str(path))
    if clean is True:
        return True, ""
    if clean is False:
        return False, "worktree_dirty"
    return None, detail or "worktree_status_unknown"


def _worktree_guard(
    root: Path,
    branch: str,
    tip: str,
    registry: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, str, str]:
    matches = [entry for entry in registry if entry.get("branch") == branch]
    if not matches:
        return None, "", ""
    if len(matches) != 1:
        return None, "worktree_ambiguous", f"registered_worktrees={len(matches)}"
    entry = matches[0]
    raw_path = entry.get("path")
    if not isinstance(raw_path, str) or not raw_path:
        return None, "worktree_path_missing", "registered worktree has no path"
    path = Path(raw_path)
    record = {"worktree_path": str(path)}
    try:
        resolved = path.resolve()
        is_primary = resolved == root.resolve()
        is_current = Path.cwd().is_relative_to(resolved)
    except OSError as exc:
        return record, "worktree_path_unreadable", str(exc)[:400]
    if is_primary:
        return record, "primary_checkout", "review branch is checked out in the repository root"
    if is_current:
        return record, "current_checkout", "review branch contains the invoking directory"
    if entry.get("locked"):
        return record, "worktree_locked", "git worktree registry marks this worktree locked"
    if entry.get("prunable"):
        return record, "worktree_prunable", "git worktree registry marks this worktree prunable"
    try:
        if not path.is_dir():
            return record, "worktree_missing", "registered worktree path is absent"
    except OSError as exc:
        return record, "worktree_path_unreadable", str(exc)[:400]
    if entry.get("head") != tip:
        return record, "worktree_head_mismatch", "worktree or branch tip changed during inspection"
    head = _resolve_commit(root, f"{branch}^{{commit}}")
    if head is None:
        return record, "worktree_head_probe_failed", "review branch tip could not be resolved"
    worktree_head = _resolve_commit(path, "HEAD")
    if worktree_head is None:
        return record, "worktree_head_probe_failed", "worktree HEAD could not be resolved"
    if head != tip or worktree_head != tip:
        return record, "worktree_head_mismatch", "worktree or branch tip changed during inspection"
    clean, detail = _worktree_clean(path)
    if clean is not True:
        return record, "worktree_dirty" if clean is False else "worktree_status_unknown", detail
    ignored_ok, ignored_detail = lane_reaping._probe_worktree_ignored(str(path))
    if ignored_ok is None:
        return record, "worktree_ignored_probe_unknown", ignored_detail
    if ignored_ok is False:
        return record, "worktree_has_unpreserved_ignored_content", ignored_detail
    owner_state, owner_detail = lane_reaping._probe_worktree_process_owner(str(path))
    if owner_state == "owned":
        return record, "worktree_process_live", owner_detail
    if owner_state != "free":
        return record, "worktree_owner_probe_unknown", owner_detail
    shared_blocks, shared_detail = lane_reaping._shared_path_blocks_reclaim(
        worktree_path=str(path),
        task_ref=None,
        lane_id=None,
        repo_root=root,
    )
    if shared_blocks:
        if shared_detail.startswith("shared_with_lane:"):
            return record, "worktree_lane_active", shared_detail
        return record, "worktree_lane_probe_unknown", shared_detail
    heartbeat_blocks, heartbeat_detail = lane_reaping._session_heartbeat_blocks_reclaim(
        repo_root=root,
        worktree_path=str(path),
    )
    if heartbeat_blocks:
        if heartbeat_detail == "session_heartbeat_live":
            return record, "worktree_session_live", heartbeat_detail
        return record, "worktree_session_probe_unknown", heartbeat_detail
    return record, "", ""


def _archive_path(root: Path, rev: int, slug: str, tip: str) -> Path:
    return root / ".task-state" / "branch-archive" / f"rev-{rev}-{slug}-{tip[:10]}.bundle"


def _ensure_archive(root: Path, branch: str, rev: int, slug: str, tip: str) -> tuple[Path | None, str, str]:
    archive = _archive_path(root, rev, slug, tip)
    errors: list[str] = []
    if archive.exists() or archive.is_symlink():
        if lane_reaping._verified_bundle_contains(root, archive, tip, errors=errors):
            return archive, "", ""
        return None, "bundle_existing_invalid", errors[0] if errors else str(archive)

    staging = root / _ARCHIVE_STAGING_DIR
    result = lane_reaping._bundle_before_reap(root, branch, retention_count=1, bundle_dir=staging)
    if result.get("ok") is not True:
        return None, "bundle_failed", str(result.get("error") or "verified_bundle_required")
    if result.get("tip_sha") != tip:
        return None, "branch_tip_changed", "branch tip moved while its bundle was written"
    source = Path(str(result.get("bundle_path") or ""))
    try:
        archive.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(source, archive)
        except FileExistsError:
            pass
        from .worktree_tombstone import _fsync_directory  # noqa: PLC0415

        _fsync_directory(archive.parent)
        source.unlink()
    except OSError as exc:
        return None, "bundle_archive_publish_failed", str(exc)[:400]
    errors.clear()
    if not lane_reaping._verified_bundle_contains(root, archive, tip, errors=errors):
        return None, "bundle_verification_failed", errors[0] if errors else str(archive)
    return archive, "", ""


def _review_evidence_root(root: Path) -> Path | None:
    if (root / ".git").is_dir():
        return root
    from workbay_handoff_mcp.config import _resolve_primary_worktree_root  # noqa: PLC0415

    return _resolve_primary_worktree_root(root)


def review_stage_deferral(root: Path, branch: str, tip: str | None = None) -> str:
    """Return the native review hold for generic reclaim, or an empty decision for ordinary lanes."""
    branch = branch.removeprefix("refs/heads/")
    parts = _review_branch_parts(branch)
    if parts is None:
        return ""
    evidence_root = _review_evidence_root(root)
    if evidence_root is None:
        return "review_stage_evidence_root_unknown"
    root = evidence_root
    if lane_reaping._review_removal_pending(root, branch):
        return "review_removal_intent_pending"
    if tip is None:
        exists, tip, detail = _branch_ref_state(root, branch)
        if exists is False:
            return ""
        if exists is not True or tip is None:
            return detail or "review_stage_tip_unknown"

    # A copied or interrupted stage is still a review candidate. Only the
    # proof consumer can authorize that branch generation.
    reflog = _git(root, "reflog", "show", "--format=%H%x09%gs", f"refs/heads/{branch}", "--")
    if reflog is None or reflog.returncode != 0:
        return "review_stage_reflog_unknown"
    events = _stdout(reflog).splitlines()
    if any(event.partition("\t")[2].startswith(f"commit: {_STAGE_TITLE}") for event in events):
        return "review_stage_native_reclaim_required"

    latest, reason = _latest_stage(root, tip)
    if latest is None and reason == "stage_binding_missing":
        return ""
    if latest is None and reason != "stage_binding_ambiguous":
        return reason
    if latest is None:
        return "" if _review_history_inherited(root, tip, events) else reason
    stage, label, short = latest
    if stage == tip:
        return "review_stage_native_reclaim_required"
    evidence, reason, _ = stage_evidence.read_review_stage(root, stage, label, short)
    if evidence is None and reason != "stage_sidecar_missing":
        return "review_stage_ownership_unknown"
    if evidence is not None and evidence.get("staged_commit") == stage:
        subject = evidence.get("subject")
        subject_slug = (
            re.sub(r"[^A-Za-z0-9._-]+", "-", subject.split("/", 1)[-1]).strip("-") if isinstance(subject, str) else None
        )
        recorded_branch = evidence.get("review_branch")
        if recorded_branch == branch or (recorded_branch is None and parts[1] == subject_slug):
            return "review_stage_native_reclaim_required"

    return "" if _review_history_inherited(root, tip, events) else "review_stage_ownership_unknown"


def _review_history_inherited(root: Path, tip: str, events: list[str]) -> bool:
    """Establish that all stage markers predate this branch generation."""
    for event in reversed(events):
        origin, _, message = event.partition("\t")
        if not message.startswith("branch: Created from ") or not _FULL_SHA_RE.fullmatch(origin):
            continue
        ancestor = _git(root, "merge-base", "--is-ancestor", origin, tip)
        if ancestor is None or ancestor.returncode != 0:
            return False
        new_stages = _git(root, "log", "-1", "--format=%H", f"--grep=^{_STAGE_TITLE}", tip, f"^{origin}", "--")
        return new_stages is not None and new_stages.returncode == 0 and not _stdout(new_stages).strip()
    return False


def _review_archive_binding(root: Path, branch: str, tip: str, archive: Path, pin: str) -> dict[str, object] | None:
    if archive.is_symlink() or not archive.is_file():
        return None
    if not lane_reaping._verified_bundle_contains(root, archive, tip) or not branch_reclaim_delete.review_pin_matches(
        root, pin, tip
    ):
        return None
    prerequisites = sorted(set(lane_reaping._bundle_prerequisite_shas(archive)))
    identity = lane_reaping._bundle_verification_identity(archive, tip, prerequisites)
    if identity is None or not lane_reaping._bundle_verification_receipt_matches(archive, identity):
        return None
    return {"branch": branch, "tip": tip, "bundle_path": str(archive), "bundle_identity": identity, "pin_ref": pin}


def review_branch_delete_guard(root: Path, branch: str, tip: str) -> tuple[bool, str, str]:
    """Require native stage evidence and preservation before the shared monitor deletes a review ref."""
    if not review_stage_deferral(root, branch, tip):
        return False, "", ""
    evidence_root = _review_evidence_root(root)
    if evidence_root is None:
        return True, "review_stage_evidence_root_unknown", ""
    root = evidence_root
    if lane_reaping._review_removal_pending(root, branch, include_legacy=True):
        return True, "review_removal_intent_pending", ""
    parts = _review_branch_parts(branch)
    if parts is None:
        return True, "review_stage_ownership_unknown", ""
    rev, slug = parts
    main = _resolve_commit(root, "refs/heads/main")
    if main is None:
        return True, "main_ref_unavailable", ""
    proof, reason, subject = _subject_merge_proof(root, branch, rev, tip, main)
    if proof is not True:
        return True, reason, ""
    pin = branch_reclaim_delete.review_preservation_ref(root, branch, tip)
    if _review_archive_binding(root, branch, tip, _archive_path(root, rev, slug, tip), pin) is None:
        return True, "review_stage_preservation_unverified", ""
    return True, "", subject


def _review_proof_binding(root: Path, review_tip: str, subject_tip: str, integration_tip: str) -> dict[str, str] | None:
    """Record the objects admitted by the proof consumer for later inspection."""
    latest, _ = _latest_stage(root, review_tip)
    if latest is None:
        return None
    stage, label, short = latest
    record, _, _ = stage_evidence.read_review_stage(root, stage, label, short)
    if record is None or (record.get("staged_commit"), record.get("subject_tip")) != (stage, subject_tip):
        return None
    patch = f"docs/reviews/_input-{label}-{short}.patch"
    blob = _git_bytes(root, "cat-file", "blob", f"{stage}:{patch}")
    if blob is None:
        return None
    base = record.get("base")
    if record.keys() == _LEGACY_STAGE_FIELDS:
        parent = _git(root, "merge-base", f"{stage}^", subject_tip)
        if parent is None or parent.returncode != 0:
            return None
        base = _stdout(parent).strip()
    if not isinstance(base, str) or not _FULL_SHA_RE.fullmatch(base):
        return None
    return {
        "stage_commit": stage,
        "patch_sha256": hashlib.sha256(blob).hexdigest(),
        "base": base,
        "subject_tip": subject_tip,
        "integration_tip": integration_tip,
    }


def _live_review_removal_proof(root: Path, preservation: Mapping[str, object]) -> bool:
    branch, tip = str(preservation["branch"]), str(preservation["tip"])
    parts = _review_branch_parts(branch)
    stage = preservation.get("stage")
    integration_tip = _resolve_commit(root, "refs/heads/main")
    if parts is None or not isinstance(stage, dict) or integration_tip is None:
        return False
    proven, _, subject = _subject_merge_proof(root, branch, parts[0], tip, integration_tip)
    if proven is not True:
        return False
    current = _review_proof_binding(root, tip, subject, integration_tip)
    return current is not None and all(current[key] == stage.get(key) for key in current if key != "integration_tip")


def _keep(
    kept: list[dict[str, Any]],
    candidate: dict[str, Any],
    reason: str,
    detail: str = "",
) -> None:
    kept.append({**candidate, "reason": reason, **({"detail": detail} if detail else {})})


def _reason_is_probe_failure(reason: str) -> bool:
    return "_probe_" in reason or reason.endswith(("unknown", "unavailable", "failed", "unreadable"))


def _branch_ref_state(root: Path, branch: str) -> tuple[bool | None, str | None, str]:
    ref = f"refs/heads/{branch}"
    result = _git(root, "show-ref", "--verify", "--quiet", ref)
    if result is None:
        return None, None, "branch_ref_probe_unavailable"
    if result.returncode == 1:
        return False, None, ""
    if result.returncode != 0:
        return None, None, "branch_ref_probe_failed"
    tip = _resolve_commit(root, ref)
    if tip is None:
        return None, None, "branch_tip_probe_failed"
    return True, tip, ""


def _reference_monitor_result(result: Any) -> tuple[bool | None, str, str]:
    if isinstance(result, dict):
        raw_deleted = result.get("deleted")
        reason = result.get("reason")
        detail = result.get("detail")
    else:
        raw_deleted = getattr(result, "deleted", None)
        reason = getattr(result, "reason", None)
        detail = getattr(result, "detail", None)
    if not isinstance(raw_deleted, bool):
        return None, "reference_monitor_result_unknown", str(detail or "monitor returned no boolean deleted result")
    return raw_deleted, str(reason or "reference_monitor_refused"), str(detail or "")


def _monitor_refusal_reason(reason: str) -> str:
    if reason in {"invalid_branch", "live_proof_failed"}:
        return "branch_ref_kept_reference_monitor_refused"
    return reason


def reap_review_stage_worktrees(
    repo: Path | str,
    *,
    apply: bool,
    slug: str | None = None,
    deadline: float | None = None,
    schedule_candidates: Callable[[Iterable[str]], Iterable[str]] | None = None,
) -> dict[str, Any]:
    """Report or retire review-stage branches whose subject proof is on ``main``.

    Apply preserves each branch tip in a verified bundle and a CAS-written
    ``refs/reclaimed/`` pin before removing its clean, unlocked, unowned linked
    worktree. Branch removal is a final compare-and-swap against the archived tip.
    ``deadline`` is absolute monotonic time. The optional scheduler claims each
    branch name before yielding it. Omitted candidates wait for a later run.
    The foreground hook supplies both arguments to schedule only Git-registered
    review checkouts after removal-intent recovery. Explicit CLI runs also
    inspect review refs that have no registered checkout.
    """
    retired: list[dict[str, Any]] = []
    kept: list[dict[str, Any]] = []
    would_retire: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    payload: dict[str, Any] = {
        "ok": True,
        "applied": apply,
        "repo_root": str(repo),
        "retired": retired,
        "kept": kept,
        "would_retire": would_retire,
        "failures": failures,
    }

    def note_release(branch: str, step: str, release: str) -> None:
        # A fenced step that ran but left the lane row's fence unreleased, beside retired/failures [AGT-10].
        if release and release != "released":
            payload.setdefault("fence_release", []).append({"branch": branch, "step": step, "release": release})
            payload["cleanup_pending"] = "reclaim_fence_release"

    token = lane_reaping._REGISTRY_REAP_DEADLINE.set(deadline) if deadline is not None else None
    try:
        root = _review_evidence_root(Path(repo).resolve())
        if root is None:
            return {**payload, "ok": False, "error": "review_stage_evidence_root_unknown"}
        if slug is not None and not slug.strip():
            return {**payload, "ok": False, "error": "blank_slug"}
        recovery = lane_reaping._bounded_registry_removal_recovery(
            root, apply=apply, review_only=True, review_slug=slug.strip() if slug is not None else None
        )
        if recovery:
            payload["removal_intents"] = recovery
        pending_branches = {str(item["branch"]) for item in recovery if item["state"] == "pending" and "branch" in item}
        for item in recovery:
            if item["state"] == "pending" and apply:
                failures.append({**item, "reason": "review_removal_intent_pending"})
        local_branches, branch_error = _local_review_branches(root)
        if local_branches is None:
            return {**payload, "ok": False, "error": branch_error}
        registry, registry_error = _worktree_registry(root)
        if registry is None:
            for branch, tip in sorted(local_branches.items()):
                parts = _review_branch_parts(branch)
                if parts is None or (slug is not None and parts[1] != slug.strip()):
                    continue
                _keep(
                    kept,
                    {"branch": branch, "rev": parts[0], "slug": parts[1], "tip_sha": tip, "worktree_path": None},
                    registry_error,
                )
            return {**payload, "ok": False, "error": registry_error}
        branches = _review_branches(
            local_branches, registry, include_ref_only=deadline is None or schedule_candidates is None
        )
        main_tip = _resolve_commit(root, "refs/heads/main")
        if main_tip is None:
            for branch, tip in sorted(branches.items()):
                parts = _review_branch_parts(branch)
                if parts is None or (slug is not None and parts[1] != slug.strip()):
                    continue
                _keep(
                    kept,
                    {"branch": branch, "rev": parts[0], "slug": parts[1], "tip_sha": tip or None},
                    "main_ref_unavailable",
                )
            return {**payload, "ok": False, "error": "main_ref_unavailable"}

        keys = [
            branch for branch in sorted(branches) if slug is None or _review_branch_parts(branch)[1] == slug.strip()
        ]
        scheduled = schedule_candidates(keys) if schedule_candidates is not None else keys
        for branch in scheduled:
            lane_reaping._registry_reap_remaining()
            tip = branches[branch]
            parts = _review_branch_parts(branch)
            if parts is None:
                continue
            rev, candidate_slug = parts
            if slug is not None and candidate_slug != slug.strip():
                continue
            candidate: dict[str, Any] = {
                "branch": branch,
                "rev": rev,
                "slug": candidate_slug,
                "tip_sha": tip or None,
                "worktree_path": None,
            }
            if branch in pending_branches:
                _keep(kept, candidate, "review_removal_intent_pending")
                continue
            if not tip:
                _keep(kept, candidate, "branch_tip_unavailable")
                failures.append({**candidate, "reason": "branch_tip_unavailable"})
                continue

            proof, proof_reason, proof_detail = _subject_merge_proof(root, branch, rev, tip, main_tip)
            if proof is not True:
                _keep(kept, candidate, proof_reason, proof_detail)
                if proof is None:
                    failures.append({**candidate, "reason": proof_reason, "detail": proof_detail})
                continue
            candidate["subject_tip"] = proof_detail
            worktree_record, guard_reason, guard_detail = _worktree_guard(root, branch, tip, registry)
            if worktree_record is not None:
                candidate.update(worktree_record)
            if guard_reason:
                _keep(kept, candidate, guard_reason, guard_detail)
                if _reason_is_probe_failure(guard_reason):
                    failures.append({**candidate, "reason": guard_reason, "detail": guard_detail})
                continue

            eligible = {**candidate, "reason": "eligible"}
            if not apply:
                would_retire.append({**eligible, "reason": "would_retire"})
                _keep(kept, candidate, "preview_only", "eligible; --apply is required to retire it")
                continue

            original_path = candidate.get("worktree_path")
            incarnation = None
            if original_path:
                incarnation, detail = lane_reaping._observe_registry_removal(
                    Path(original_path).resolve(), verify_links=True
                )
                if incarnation is None or (incarnation["branch"], incarnation["head_sha"]) != (branch, tip):
                    _keep(kept, candidate, "review_removal_incarnation_unverified", detail)
                    failures.append({**candidate, "reason": "review_removal_incarnation_unverified", "detail": detail})
                    continue

            # A terminal lane row may still name this branch: act only as that lane, under its
            # current close authority; a rowless branch acts on re-proved absence [SECD-01].
            identity_refusal, lane_task, lane_id = branch_reclaim_delete.lane_identity_for_branch(branch)
            if not identity_refusal:
                binding = branch_reclaim_delete.bind_close_authority(
                    task_ref=lane_task,
                    lane_id=lane_id or f"rev{rev}-{candidate_slug}",
                    branch=branch,
                    tip=tip,
                )
                identity_refusal = binding.refusal
            if identity_refusal:
                _keep(kept, candidate, identity_refusal)
                failures.append({**candidate, "reason": identity_refusal})
                continue

            archive, archive_error, archive_detail = _ensure_archive(root, branch, rev, candidate_slug, tip)
            if archive is None:
                _keep(kept, candidate, archive_error, archive_detail)
                failures.append({**candidate, "reason": archive_error, "detail": archive_detail})
                continue
            pin_ok, pin_result = branch_reclaim_delete.pin_review_tip(root, branch, tip)
            if not pin_ok:
                _keep(kept, candidate, "pin_failed", pin_result)
                failures.append({**candidate, "reason": "pin_failed", "detail": pin_result})
                continue
            preservation = _review_archive_binding(root, branch, tip, archive, pin_result)
            if preservation is None:
                _keep(kept, candidate, "bundle_verification_failed")
                failures.append({**candidate, "reason": "bundle_verification_failed"})
                continue

            # Re-prove stage identity, integration and worktree guards after
            # preservation, immediately before removal.
            current_main = _resolve_commit(root, "refs/heads/main")
            current_proof = _subject_merge_proof(root, branch, rev, tip, current_main) if current_main else None
            if current_proof != (proof, proof_reason, proof_detail):
                detail = current_proof[1] if current_proof else "main_ref_unavailable"
                _keep(kept, candidate, "stage_proof_changed", detail)
                failures.append({**candidate, "reason": "stage_proof_changed", "detail": detail})
                continue
            current_tip = _resolve_commit(root, f"refs/heads/{branch}")
            if current_tip != tip:
                _keep(kept, candidate, "branch_tip_changed", "branch ref changed after its archive and pin")
                failures.append({**candidate, "reason": "branch_tip_changed"})
                continue
            current_registry, current_registry_error = _worktree_registry(root)
            if current_registry is None:
                _keep(kept, candidate, current_registry_error)
                failures.append({**candidate, "reason": current_registry_error})
                continue
            current_worktree, current_guard, current_detail = _worktree_guard(root, branch, tip, current_registry)
            if current_guard:
                _keep(kept, candidate, current_guard, current_detail)
                if _reason_is_probe_failure(current_guard):
                    failures.append({**candidate, "reason": current_guard, "detail": current_detail})
                continue
            if current_worktree is not None and current_worktree.get("worktree_path") != original_path:
                _keep(kept, candidate, "worktree_changed", "registered worktree path changed during retirement")
                failures.append({**candidate, "reason": "worktree_changed"})
                continue
            if current_worktree is None and original_path:
                try:
                    if Path(str(original_path)).exists():
                        _keep(
                            kept,
                            candidate,
                            "worktree_registry_changed",
                            "path remains after registry entry disappeared",
                        )
                        failures.append({**candidate, "reason": "worktree_registry_changed"})
                        continue
                except OSError as exc:
                    _keep(kept, candidate, "worktree_path_unreadable", str(exc)[:400])
                    failures.append({**candidate, "reason": "worktree_path_unreadable"})
                    continue

            stage = None
            if original_path and current_worktree is not None:
                stage = _review_proof_binding(root, tip, proof_detail, current_main) if current_main else None
                if stage is None:
                    _keep(kept, candidate, "review_removal_proof_unrecordable")
                    failures.append({**candidate, "reason": "review_removal_proof_unrecordable"})
                    continue
            with review_sweep_action(root, branch=branch, worktree=original_path, deadline=deadline) as refusal:
                if refusal:
                    _keep(kept, candidate, refusal)
                    if refusal == REVIEW_INVOCATION_LOCK_HELD:
                        payload.setdefault("cleanup_pending", refusal)
                    continue
                if stage is not None:
                    removed, remove_detail = branch_reclaim_delete.remove_worktree_under_close_authority(
                        root,
                        str(original_path),
                        binding,
                        review={**preservation, "stage": stage},
                        incarnation=incarnation,
                    )
                    note_release(branch, "worktree_remove", branch_reclaim_delete.fence_release_of(remove_detail))
                    if not removed:
                        detail = (remove_detail or "git_worktree_remove_failed").strip()[:400]
                        _keep(kept, candidate, "worktree_remove_failed", detail)
                        failures.append({**candidate, "reason": "worktree_remove_failed", "detail": detail})
                        continue

                if not branch_reclaim_delete.review_pin_matches(root, pin_result, tip):
                    reason = "preservation_pin_verification_failed"
                    detail = f"expected={tip} pin={pin_result}"
                    _keep(kept, candidate, reason, detail)
                    failures.append({**candidate, "reason": reason, "detail": detail})
                    if original_path:
                        retired.append(
                            {
                                **candidate,
                                "reason": "worktree_retired",
                                "worktree_removed": True,
                                "branch_ref_retired": False,
                                "bundle_path": str(archive),
                                "pin_ref": str(pin_result),
                                "subject_proof": proof_reason,
                            }
                        )
                    continue

                try:
                    monitor_result = branch_reclaim_delete.delete_authorized_branch(
                        orchestrator_root=root,
                        lane_id=str(binding.lane_id),
                        branch=branch,
                        authorized_sha=tip,
                        apply=True,
                        # A rowless branch keeps the monitor's task-less lookup; a bound lane names its task.
                        **({"task_ref": binding.task_ref} if binding.task_ref else {}),
                        integration_ref="main",
                    )
                except Exception as exc:  # noqa: BLE001 - monitor failures preserve the ref
                    monitor_result = None
                    monitor_exception = f"{type(exc).__name__}: {exc}"[:400]
                    note_release(branch, "branch_delete", branch_reclaim_delete.exception_fence_release(exc))
                else:
                    monitor_exception = ""
                    note_release(branch, "branch_delete", str(getattr(monitor_result, "fence_release", "") or ""))
            monitor_deleted, monitor_reason, monitor_detail = _reference_monitor_result(monitor_result)
            if monitor_exception:
                monitor_deleted = None
                monitor_reason = "reference_monitor_probe_failed"
                monitor_detail = monitor_exception

            ref_exists, current_branch_tip, ref_error = _branch_ref_state(root, branch)
            if monitor_deleted is True:
                if ref_exists is not False:
                    reason = "branch_delete_verification_failed"
                    detail = (
                        ref_error
                        or f"reference monitor reported deletion; exists={ref_exists} tip={current_branch_tip}"
                    )
                    _keep(kept, candidate, reason, detail)
                    failures.append({**candidate, "reason": reason, "detail": detail})
                    if original_path:
                        retired.append(
                            {
                                **candidate,
                                "reason": "worktree_retired",
                                "worktree_removed": True,
                                "branch_ref_retired": False,
                                "bundle_path": str(archive),
                                "pin_ref": str(pin_result),
                                "subject_proof": proof_reason,
                            }
                        )
                    continue
                retired.append(
                    {
                        **candidate,
                        "reason": "retired",
                        "worktree_removed": bool(original_path),
                        "branch_ref_retired": True,
                        "bundle_path": str(archive),
                        "pin_ref": str(pin_result),
                        "subject_proof": proof_reason,
                    }
                )
                continue

            if ref_exists is not True or current_branch_tip != tip:
                reason = "branch_ref_state_unknown" if ref_exists is None else "branch_ref_changed_after_monitor"
                detail = ref_error or f"expected={tip} exists={ref_exists} actual={current_branch_tip}"
                _keep(kept, candidate, reason, detail)
                failures.append({**candidate, "reason": reason, "detail": detail})
                if original_path:
                    retired.append(
                        {
                            **candidate,
                            "reason": "worktree_retired",
                            "worktree_removed": True,
                            "branch_ref_retired": False,
                            "bundle_path": str(archive),
                            "pin_ref": str(pin_result),
                            "subject_proof": proof_reason,
                        }
                    )
                continue

            keep_reason = _monitor_refusal_reason(monitor_reason)
            detail = f"reference monitor reason: {monitor_reason}"
            if monitor_detail:
                detail = f"{detail}; {monitor_detail}"
            _keep(
                kept,
                {**candidate, "reference_monitor_reason": monitor_reason},
                keep_reason,
                detail,
            )
            if monitor_deleted is None or (
                _reason_is_probe_failure(monitor_reason) and monitor_reason != "live_proof_failed"
            ):
                failures.append({**candidate, "reason": keep_reason, "detail": detail})
            if original_path:
                retired.append(
                    {
                        **candidate,
                        "reason": "worktree_retired",
                        "worktree_removed": True,
                        "branch_ref_retired": False,
                        "reference_monitor_reason": monitor_reason,
                        "bundle_path": str(archive),
                        "pin_ref": str(pin_result),
                        "subject_proof": proof_reason,
                    }
                )
        payload["ok"] = not failures
        payload["repo_root"] = str(root)
        return payload
    except TimeoutError as exc:
        return {**payload, "ok": False, "error": "deadline_exceeded", "detail": str(exc)}
    except Exception as exc:  # noqa: BLE001 — a probe failure must keep candidates
        payload["ok"] = False
        payload["error"] = "review_stage_reap_failed"
        payload["detail"] = str(exc)[:400]
        return payload
    finally:
        if token is not None:
            lane_reaping._REGISTRY_REAP_DEADLINE.reset(token)
