"""Retire review-stage worktrees after their subject is safely on ``main``."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from .. import lane_reaping
from . import branch_reclaim_delete, landing_log
from .orphan_reclaim import _pin_branch

# Mirrors ``_review_branch`` / ``_branch_file_slug`` in
# packages/workbay-system/workbay_system/payload/scripts/review_pipeline.py.
_REVIEW_BRANCH_RE = re.compile(r"^feature/rev(?P<rev>[0-9]+)-(?P<slug>[A-Za-z0-9._-]+)$")
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
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


def _artifact_slug(subject: str) -> str:
    # Matches ``review_pipeline._branch_file_slug`` for the adjudication path.
    return re.sub(r"[^A-Za-z0-9._-]+", "-", subject).strip("-")


def _subject_merge_proof(root: Path, slug: str, main_tip: str) -> tuple[bool | None, str, str]:
    subject = f"feature/{slug}"
    subject_tip = _resolve_commit(root, f"refs/heads/{subject}")
    if subject_tip is not None:
        merged, detail = _is_ancestor(root, subject_tip, main_tip)
        if merged is True:
            return True, "subject_branch_merged", subject_tip
        if merged is None:
            branch_error = detail
        else:
            branch_error = "subject_not_merged"
    else:
        branch_error = "subject_branch_unavailable"

    artifact = root / ".task-state" / "review-adjudication" / f"{_artifact_slug(subject)}.json"
    try:
        if not artifact.is_file():
            proof = False if branch_error == "subject_not_merged" else None
            return proof, branch_error, str(artifact)
        record = json.loads(artifact.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return None, "adjudication_artifact_unreadable", str(exc)[:400]
    if not isinstance(record, dict) or record.get("subject") != subject or record.get("verdict") != "MERGE":
        return False, "adjudication_not_merge", str(artifact)

    fenced_tip = record.get("subject_tip")
    if not isinstance(fenced_tip, str) or not _FULL_SHA_RE.fullmatch(fenced_tip.lower()):
        return None, "adjudication_tip_invalid", str(artifact)
    resolved_fence = _resolve_commit(root, fenced_tip)
    if resolved_fence is None or resolved_fence != fenced_tip.lower():
        return None, "adjudication_tip_unavailable", str(artifact)
    merged, detail = _is_ancestor(root, resolved_fence, main_tip)
    if merged is True:
        return True, "adjudicated_tip_merged", resolved_fence
    if merged is False:
        return False, "adjudicated_tip_not_merged", str(artifact)
    return None, "adjudication_ancestry_probe_failed", detail


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


def _review_branches(local: dict[str, str], registry: list[dict[str, Any]]) -> dict[str, str]:
    branches = dict(local)
    for entry in registry:
        branch = entry.get("branch")
        if isinstance(branch, str) and _review_branch_parts(branch) is not None:
            branches.setdefault(branch, "")
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
        is_primary = path.resolve() == root.resolve()
    except OSError as exc:
        return record, "worktree_path_unreadable", str(exc)[:400]
    if is_primary:
        return record, "primary_checkout", "review branch is checked out in the repository root"
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
        os.replace(source, archive)
    except OSError as exc:
        return None, "bundle_archive_publish_failed", str(exc)[:400]
    errors.clear()
    if not lane_reaping._verified_bundle_contains(root, archive, tip, errors=errors):
        return None, "bundle_verification_failed", errors[0] if errors else str(archive)
    return archive, "", ""


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
) -> dict[str, Any]:
    """Report or retire review-stage branches whose subject proof is on ``main``.

    Apply preserves each branch tip in a verified bundle and a CAS-written
    ``refs/reclaimed/`` pin before removing its clean, unlocked, unowned linked
    worktree. Branch removal is a final compare-and-swap against the archived tip.
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
    try:
        root = Path(repo).resolve()
        if slug is not None and not slug.strip():
            return {**payload, "ok": False, "error": "blank_slug"}
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
        branches = _review_branches(local_branches, registry)
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

        for branch, tip in sorted(branches.items()):
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
            if not tip:
                _keep(kept, candidate, "branch_tip_unavailable")
                failures.append({**candidate, "reason": "branch_tip_unavailable"})
                continue

            proof, proof_reason, proof_detail = _subject_merge_proof(root, candidate_slug, main_tip)
            if proof is not True:
                _keep(kept, candidate, proof_reason, proof_detail)
                if proof is None:
                    failures.append({**candidate, "reason": proof_reason, "detail": proof_detail})
                continue
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

            archive, archive_error, archive_detail = _ensure_archive(root, branch, rev, candidate_slug, tip)
            if archive is None:
                _keep(kept, candidate, archive_error, archive_detail)
                failures.append({**candidate, "reason": archive_error, "detail": archive_detail})
                continue
            pin_ok, pin_result = _pin_branch(root, f"rev{rev}-{candidate_slug}", tip)
            if not pin_ok:
                _keep(kept, candidate, "pin_failed", pin_result)
                failures.append({**candidate, "reason": "pin_failed", "detail": pin_result})
                continue

            # Re-read all worktree guards after preservation, immediately before
            # removal. A new lock, owner, dirty file, or moved branch leaves the
            # already-preserved branch available for a later retry.
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
            original_path = candidate.get("worktree_path")
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

            if original_path:
                removed = _git(root, "worktree", "remove", str(original_path))
                if removed is None or removed.returncode != 0:
                    detail = _stdout(removed).strip() if removed is not None else "git command unavailable"
                    if removed is not None:
                        detail = (removed.stderr or removed.stdout or detail).strip()[:400]
                    _keep(kept, candidate, "worktree_remove_failed", detail)
                    failures.append({**candidate, "reason": "worktree_remove_failed", "detail": detail})
                    continue

            pinned_tip = _resolve_commit(root, pin_result)
            if pinned_tip != tip:
                reason = "preservation_pin_verification_failed"
                detail = f"expected={tip} actual={pinned_tip}"
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
                    lane_id=f"rev{rev}-{candidate_slug}",
                    branch=branch,
                    authorized_sha=tip,
                    apply=True,
                    integration_ref="main",
                )
            except Exception as exc:  # noqa: BLE001 - monitor failures preserve the ref
                monitor_result = None
                monitor_exception = f"{type(exc).__name__}: {exc}"[:400]
            else:
                monitor_exception = ""
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
    except Exception as exc:  # noqa: BLE001 — a probe failure must keep candidates
        payload["ok"] = False
        payload["error"] = "review_stage_reap_failed"
        payload["detail"] = str(exc)[:400]
        return payload
