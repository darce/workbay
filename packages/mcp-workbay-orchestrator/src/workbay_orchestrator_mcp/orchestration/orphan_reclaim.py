"""Fail-closed reclamation of merged branches with no live registry owner.

This surface is deliberately separate from the lane reaper.  A branch with a
task row belongs to ``task-finish`` and a branch with a non-terminal lane row
belongs to the lane reaper; only a merged, clean, registry-free branch reaches
the destructive path.  Every uncertainty is represented by an
``unknown_*`` classification and therefore cannot authorize deletion [AGT-10].
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .branch_reclaim_delete import delete_authorized_branch

_DEFAULT_GIT_TIMEOUT_S = 20.0
_LOCK_MAX_AGE_S = 24 * 60 * 60
_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_TERMINAL_LANE_STATUSES = frozenset({"closed", "merged", "closed_stale"})
_INTEGRATION_NAMES = frozenset({"main", "master"})


@dataclass(frozen=True)
class OrphanBranch:
    """One branch identity and its typed, evidence-backed classification."""

    branch: str
    sha: str | None
    worktree: str | None
    classification: str
    evidence: tuple[str, ...] = ()

    @property
    def reason(self) -> str:
        """Compatibility spelling for callers that use reason-based rows."""

        return self.classification

    def to_dict(self) -> dict[str, object]:
        return {
            "branch": self.branch,
            "sha": self.sha,
            "worktree": self.worktree,
            "classification": self.classification,
            "reason": self.classification,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True)
class OrphanScan:
    """Pure scan output; no branch, worktree, ref, or ledger mutation occurs."""

    repo_root: str
    integration_ref: str
    rows: tuple[OrphanBranch, ...] = ()
    error: str | None = None
    state_dir: str | None = None

    @property
    def branches(self) -> tuple[OrphanBranch, ...]:
        """Alias used by reporting callers."""

        return self.rows

    @property
    def candidates(self) -> tuple[OrphanBranch, ...]:
        """Alias used by older scan consumers."""

        return self.rows

    @property
    def unknown_rows(self) -> tuple[OrphanBranch, ...]:
        return tuple(row for row in self.rows if row.classification.startswith("unknown_"))

    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self.rows:
            counts[row.classification] = counts.get(row.classification, 0) + 1
        if self.error is not None:
            counts[self.error] = counts.get(self.error, 0) + 1
        return dict(sorted(counts.items()))

    def to_dict(self) -> dict[str, object]:
        return {
            "repo_root": self.repo_root,
            "integration_ref": self.integration_ref,
            "summary": self.summary(),
            "rows": [row.to_dict() for row in self.rows],
            "error": self.error,
        }


def _git_timeout() -> float:
    raw = os.environ.get("WORKBAY_ORPHAN_RECLAIM_GIT_TIMEOUT", "")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return _DEFAULT_GIT_TIMEOUT_S
    return value if value > 0 else _DEFAULT_GIT_TIMEOUT_S


def _decode_output(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return "" if value is None else str(value)


def _git(root: Path, *args: str, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    """Run a bounded git probe and turn timeout/I/O failures into typed output."""

    timeout_s = _git_timeout() if timeout is None else timeout
    argv = ["git", "-C", str(root), *args]
    try:
        return subprocess.run(
            argv,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        return subprocess.CompletedProcess(
            argv,
            124,
            _decode_output(exc.stdout),
            _decode_output(exc.stderr) or f"git timeout after {timeout_s:g}s",
        )
    except OSError as exc:
        return subprocess.CompletedProcess(argv, 125, "", f"{type(exc).__name__}: {exc}")


def _detail(proc: subprocess.CompletedProcess[str]) -> str:
    return (_decode_output(proc.stderr) or _decode_output(proc.stdout)).strip()


def _short_branch(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    branch = value.strip()
    if branch.startswith("refs/heads/"):
        branch = branch.removeprefix("refs/heads/")
    elif branch.startswith("refs/"):
        return None
    return branch or None


def _integration_name(value: str) -> str:
    return value.strip().removeprefix("refs/heads/")


def _is_timeout(proc: subprocess.CompletedProcess[str]) -> bool:
    return proc.returncode == 124 or "timeout" in _detail(proc).casefold()


def _failure_classification(proc: subprocess.CompletedProcess[str], *, operation: str) -> str:
    if _is_timeout(proc):
        return "unknown_git_timeout"
    return f"unknown_{operation}"


def _evidence(*items: object) -> tuple[str, ...]:
    return tuple(str(item) for item in items if str(item).strip())


def _local_branches(root: Path) -> tuple[list[str] | None, str | None]:
    proc = _git(root, "for-each-ref", "--format=%(refname:short)", "refs/heads/")
    if proc.returncode != 0:
        return None, _failure_classification(proc, operation="branch_list")
    branches: list[str] = []
    for raw in _decode_output(proc.stdout).splitlines():
        branch = _short_branch(raw)
        if branch is not None and branch not in branches:
            branches.append(branch)
    return branches, None


def _merged_branches(root: Path, integration_ref: str) -> tuple[set[str] | None, str | None]:
    # One batched merged listing; the per-branch merge-base below is the
    # act-time content confirmation [OBS-08].
    proc = _git(root, "branch", "--merged", integration_ref, "--format=%(refname:short)")
    if proc.returncode != 0:
        return None, _failure_classification(proc, operation="merged_list")
    merged: set[str] = set()
    for raw in _decode_output(proc.stdout).splitlines():
        branch = _short_branch(raw.removeprefix("* "))
        if branch is not None:
            merged.add(branch)
    return merged, None


def _branch_tip(root: Path, branch: str) -> tuple[str | None, str | None, str]:
    proc = _git(root, "rev-parse", "--verify", f"refs/heads/{branch}^{{commit}}")
    value = _decode_output(proc.stdout).strip().lower()
    if proc.returncode == 0 and _FULL_SHA_RE.fullmatch(value):
        return value, None, "branch tip resolved"
    return None, _failure_classification(proc, operation="branch_tip"), _detail(proc)


def _worktree_index(root: Path) -> tuple[dict[str, list[Path]] | None, str | None]:
    proc = _git(root, "worktree", "list", "--porcelain")
    if proc.returncode != 0:
        return None, _failure_classification(proc, operation="worktree_list")

    indexed: dict[str, list[Path]] = {}
    current_path: Path | None = None
    current_branch: str | None = None

    def flush() -> None:
        nonlocal current_path, current_branch
        if current_path is not None and current_branch is not None:
            short = _short_branch(current_branch)
            if short is not None:
                indexed.setdefault(short, []).append(current_path)
        current_path = None
        current_branch = None

    for raw in _decode_output(proc.stdout).splitlines():
        if raw.startswith("worktree "):
            flush()
            current_path = Path(raw.removeprefix("worktree ").strip())
        elif raw.startswith("branch "):
            current_branch = raw.removeprefix("branch ").strip()
        elif not raw.strip():
            flush()
    flush()
    return indexed, None


def _git_dir_from(path: Path) -> Path | None:
    dot_git = path / ".git"
    try:
        if dot_git.is_dir():
            return dot_git
        if dot_git.is_file():
            marker = dot_git.read_text(encoding="utf-8", errors="replace").strip()
            if marker.startswith("gitdir:"):
                raw = marker.removeprefix("gitdir:").strip()
                git_dir = Path(raw)
                return git_dir if git_dir.is_absolute() else (path / git_dir).resolve()
    except OSError:
        return None
    return None


def _lock_paths(root: Path, worktree: Path) -> tuple[list[Path], str | None]:
    """Find lifecycle/index sentinels without walking production content."""

    git_dirs: list[Path] = []
    for candidate in (_git_dir_from(root), _git_dir_from(worktree)):
        if candidate is not None and candidate not in git_dirs:
            git_dirs.append(candidate)
    # A linked-worktree git dir is ``<common>/.git/worktrees/<name>``.
    for git_dir in tuple(git_dirs):
        if git_dir.parent.name == "worktrees" and git_dir.parent.parent not in git_dirs:
            git_dirs.append(git_dir.parent.parent)

    paths: list[Path] = []
    bases = [worktree, *git_dirs]
    for base in bases:
        for name in ("index.lock", "lock"):
            candidate = base / name
            if candidate not in paths:
                paths.append(candidate)
        try:
            matches = sorted(base.glob(".lane-live-*"))
        except OSError as exc:
            return [], f"unknown_lock_probe:{type(exc).__name__}"
        for candidate in matches:
            if candidate not in paths:
                paths.append(candidate)
    return paths, None


def _recent_lock_evidence(root: Path, worktree: Path) -> tuple[bool, str | None, tuple[str, ...]]:
    paths, error = _lock_paths(root, worktree)
    if error is not None:
        return False, error, ()
    cutoff = time.time() - _LOCK_MAX_AGE_S
    recent: list[str] = []
    for path in paths:
        try:
            if path.exists() and path.stat().st_mtime >= cutoff:
                recent.append(f"recent lock sentinel: {path}")
        except OSError as exc:
            return False, f"unknown_lock_probe:{type(exc).__name__}", ()
    return not recent, None, tuple(recent)


def _worktree_classification(
    root: Path, worktree_paths: list[Path] | None
) -> tuple[str | None, tuple[str, ...], str | None]:
    if worktree_paths is None:
        return "unknown_worktree_list", (), None
    if len(worktree_paths) > 1:
        paths = ", ".join(str(path) for path in worktree_paths)
        return "unknown_worktree_identity", (f"branch appears in multiple worktrees: {paths}",), None
    if not worktree_paths:
        return None, ("no linked worktree checks out this branch",), None

    worktree = worktree_paths[0]
    status = _git(worktree, "status", "--porcelain")
    if status.returncode != 0:
        return _failure_classification(status, operation="worktree_status"), (_detail(status),), str(worktree)
    porcelain = _decode_output(status.stdout)
    if porcelain:
        return "dirty_worktree", (f"worktree porcelain is non-empty: {porcelain.strip()}",), str(worktree)

    clean, error, lock_evidence = _recent_lock_evidence(root, worktree)
    if error is not None:
        return error.split(":", 1)[0], (error,), str(worktree)
    if not clean:
        return "unknown_lock", lock_evidence, str(worktree)
    return None, _evidence(f"clean worktree: {worktree}", "no lock sentinel younger than 24h"), str(worktree)


def _registry_snapshot() -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
    """Read task/lane ownership through the handoff API, never an empty fallback."""

    try:
        from workbay_handoff_mcp import list_active_tasks  # noqa: PLC0415
        from workbay_handoff_mcp.lanes_recording import list_lanes  # noqa: PLC0415
    except (ImportError, AttributeError) as exc:
        return [], [], f"unknown_registry:{type(exc).__name__}"
    try:
        tasks = list_active_tasks()
        lanes_envelope = list_lanes(all_tasks=True, limit=10_000, offset=0)
    except Exception as exc:  # noqa: BLE001 - registry uncertainty is fail-closed
        return [], [], f"unknown_registry:{type(exc).__name__}"
    if not isinstance(tasks, list) or not isinstance(lanes_envelope, dict) or lanes_envelope.get("ok") is not True:
        return [], [], "unknown_registry:malformed_envelope"
    data = lanes_envelope.get("data")
    lanes = data.get("lanes") if isinstance(data, dict) else None
    if not isinstance(lanes, list) or data.get("has_more") is True:
        return [], [], "unknown_registry:lane_listing_truncated"
    if not all(isinstance(row, dict) for row in tasks + lanes):
        return [], [], "unknown_registry:malformed_row"
    return tasks, lanes, None


def _registry_classification(
    branch: str,
    tasks: list[dict[str, Any]],
    lanes: list[dict[str, Any]],
) -> tuple[str | None, tuple[str, ...], str | None]:
    task_rows = [row for row in tasks if _short_branch(row.get("target_branch")) == branch]
    if task_rows:
        refs = ", ".join(str(row.get("task_ref", "?")) for row in task_rows)
        return "has_task_row", (f"live handoff task row(s): {refs}",), None

    for row in lanes:
        row_branch = row.get("branch")
        status = row.get("status")
        if row_branch is None:
            if status not in _TERMINAL_LANE_STATUSES:
                return "unknown_registry", ("non-terminal lane row has no readable branch",), "unknown_registry"
            continue
        if _short_branch(row_branch) == branch and status not in _TERMINAL_LANE_STATUSES:
            return (
                "has_lane_row",
                (f"non-terminal lane row: {row.get('task_ref', '?')}/{row.get('lane_id', '?')}",),
                None,
            )
    return None, ("no live handoff task row or non-terminal lane row",), None


def _runtime_state_dir(root: Path) -> Path:
    try:
        from workbay_handoff_mcp import get_runtime_config  # noqa: PLC0415

        configured = getattr(get_runtime_config(), "state_dir", None)
        if configured is not None:
            return Path(configured)
    except Exception:  # noqa: BLE001 - local fallback is read-only here
        pass
    return root / ".task-state"


def scan_orphan_branches(repo_root: Path | str, integration_ref: str = "main") -> OrphanScan:
    """Classify every local branch without mutating Git or the handoff ledger."""

    root = Path(repo_root).resolve()
    integration_name = _integration_name(integration_ref)
    branches, error = _local_branches(root)
    if branches is None:
        return OrphanScan(str(root), integration_name, error=error, state_dir=str(_runtime_state_dir(root)))
    merged, merged_error = _merged_branches(root, integration_ref)
    worktrees, worktree_error = _worktree_index(root)
    rows: list[OrphanBranch] = []
    if merged_error is not None:
        for branch in branches:
            if branch == integration_name:
                continue
            rows.append(OrphanBranch(branch, None, None, merged_error, (f"git branch --merged probe: {merged_error}",)))
        return OrphanScan(str(root), integration_name, tuple(rows), state_dir=str(_runtime_state_dir(root)))
    if worktrees is None:
        # Worktree identity is required only for otherwise merged candidates,
        # but retaining a row for each branch keeps degraded output complete.
        worktree_error = worktree_error or "unknown_worktree_list"

    registry: tuple[list[dict[str, Any]], list[dict[str, Any]], str | None] | None = None
    for branch in branches:
        if branch == integration_name or branch in _INTEGRATION_NAMES and branch == integration_name:
            continue
        sha, tip_error, tip_detail = _branch_tip(root, branch)
        if sha is None:
            rows.append(OrphanBranch(branch, None, None, tip_error or "unknown_branch_tip", _evidence(tip_detail)))
            continue

        confirmation = _git(root, "merge-base", "--is-ancestor", sha, integration_ref)
        if confirmation.returncode == 1:
            rows.append(
                OrphanBranch(
                    branch,
                    sha,
                    None,
                    "unmerged",
                    _evidence(
                        f"tip {sha} is not an ancestor of {integration_ref}",
                        f"batch merged listing={'yes' if branch in (merged or set()) else 'no'}",
                    ),
                )
            )
            continue
        if confirmation.returncode != 0:
            rows.append(
                OrphanBranch(
                    branch,
                    sha,
                    None,
                    _failure_classification(confirmation, operation="merge_base"),
                    _evidence(_detail(confirmation)),
                )
            )
            continue

        if branch not in (merged or set()):
            rows.append(
                OrphanBranch(
                    branch,
                    sha,
                    None,
                    "unknown_merged_listing",
                    (f"SHA confirmation merged but branch absent from batched listing for {integration_ref}",),
                )
            )
            continue

        if registry is None:
            registry = _registry_snapshot()
        tasks, lanes, registry_error = registry
        if registry_error is not None:
            rows.append(OrphanBranch(branch, sha, None, registry_error.split(":", 1)[0], (registry_error,)))
            continue
        owner_class, owner_evidence, owner_error = _registry_classification(branch, tasks, lanes)
        if owner_error is not None:
            rows.append(OrphanBranch(branch, sha, None, owner_class or "unknown_registry", owner_evidence))
            continue
        if owner_class is not None:
            rows.append(OrphanBranch(branch, sha, None, owner_class, owner_evidence))
            continue

        checked_out = None if worktrees is None else worktrees.get(branch, [])
        classification, worktree_evidence, worktree = _worktree_classification(root, checked_out)
        if worktree_error is not None:
            rows.append(OrphanBranch(branch, sha, worktree, worktree_error, _evidence(worktree_error)))
            continue
        if classification is not None:
            rows.append(OrphanBranch(branch, sha, worktree, classification, worktree_evidence))
            continue
        rows.append(
            OrphanBranch(
                branch,
                sha,
                worktree,
                "eligible",
                _evidence(
                    f"tip {sha} is an ancestor of {integration_ref}",
                    f"batch merged listing confirmed {branch}",
                    *worktree_evidence,
                ),
            )
        )
    return OrphanScan(str(root), integration_name, tuple(rows), state_dir=str(_runtime_state_dir(root)))


def _resolve_ref(root: Path, ref: str) -> tuple[str | None, str | None]:
    proc = _git(root, "rev-parse", "--verify", f"{ref}^{{commit}}")
    value = _decode_output(proc.stdout).strip().lower()
    if proc.returncode == 0 and _FULL_SHA_RE.fullmatch(value):
        return value, None
    if proc.returncode == 128:
        detail = _detail(proc).casefold()
        if any(
            marker in detail
            for marker in (
                "needed a single revision",
                "unknown revision",
                "not a valid object name",
                "ambiguous argument",
                "does not exist",
            )
        ):
            return None, None
    return None, _failure_classification(proc, operation="ref_probe")


def _reclaim_ref(branch: str) -> str:
    return f"refs/reclaimed/{branch}"


def _pin_branch(root: Path, branch: str, sha: str) -> tuple[bool, str]:
    ref = _reclaim_ref(branch)
    existing, error = _resolve_ref(root, ref)
    if error is not None:
        return False, error
    if existing is not None and existing != sha:
        return False, "pin_conflict"
    expected = existing or "0" * 40
    proc = _git(root, "update-ref", ref, sha, expected)
    if proc.returncode != 0:
        return False, "pin_failed"
    verified, verify_error = _resolve_ref(root, ref)
    if verify_error is not None or verified != sha:
        return False, verify_error or "pin_failed"
    return True, ref


def _receipt_entries(path: Path) -> tuple[set[tuple[str, str]], str | None]:
    if not path.exists():
        return set(), None
    entries: set[tuple[str, str]] = set()
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            value = json.loads(raw)
            if isinstance(value, dict) and isinstance(value.get("branch"), str) and isinstance(value.get("sha"), str):
                entries.add((value["branch"], value["sha"]))
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        return set(), f"receipt_read_failed:{type(exc).__name__}"
    return entries, None


def _append_receipt(path: Path, row: OrphanBranch) -> tuple[bool, str | None]:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {"branch": row.branch, "sha": row.sha, "worktree": row.worktree},
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n"
            )
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        return False, f"receipt_write_failed:{type(exc).__name__}"
    return True, None


def _delete_result_fields(result: object) -> tuple[bool, str]:
    if isinstance(result, dict):
        return result.get("deleted") is True, str(result.get("reason") or "delete_failed")
    return getattr(result, "deleted", False) is True, str(getattr(result, "reason", "delete_failed"))


def _record_reclaim_decision(root: Path, row: OrphanBranch) -> bool:
    try:
        from workbay_handoff_mcp import record_decision  # noqa: PLC0415

        raw = record_decision(
            session=f"orphan-reclaim-{row.sha[:12] if row.sha else 'unknown'}",
            decision="orphan_branch_reclaimed",
            rationale=json.dumps(
                {"branch": row.branch, "sha": row.sha, "worktree": row.worktree},
                sort_keys=True,
                separators=(",", ":"),
            ),
            actor={"agent": "orchestrator-daemon", "branch": row.branch},
            task_ref=None,
            decision_origin="system",
            event_id=f"orphan-branch-reclaimed-{row.branch}-{row.sha}",
        )
    except Exception:  # noqa: BLE001 - receipt remains authoritative when registry is absent
        return False
    return isinstance(raw, dict) and raw.get("ok") is True


def apply_orphan_reclaim(
    scan: OrphanScan,
    *,
    apply: bool = False,
    state_dir: Path | str | None = None,
) -> dict[str, object]:
    """Apply only eligible rows, recording a reversible receipt before deletion."""

    root = Path(scan.repo_root)
    eligible = [row for row in scan.rows if row.classification == "eligible"]
    result: dict[str, object] = {
        "ok": scan.error is None,
        "apply": apply,
        "summary": scan.summary(),
        "rows": [row.to_dict() for row in scan.rows],
        "would_apply": [row.branch for row in eligible],
        "applied": [],
        "skipped": [],
        "errors": [],
    }
    if not apply or not eligible or scan.error is not None:
        return result

    destination = Path(state_dir) if state_dir is not None else Path(scan.state_dir or _runtime_state_dir(root))
    receipt = destination / "orphan-reclaim.jsonl"
    seen, receipt_error = _receipt_entries(receipt)
    if receipt_error is not None:
        result["ok"] = False
        result["errors"] = [receipt_error]
        return result

    applied = result["applied"]
    skipped = result["skipped"]
    errors = result["errors"]
    assert isinstance(applied, list) and isinstance(skipped, list) and isinstance(errors, list)

    for row in eligible:
        if row.sha is None:
            skipped.append({"branch": row.branch, "reason": "unknown_branch_tip"})
            continue
        current_sha, probe_error = _resolve_ref(root, f"refs/heads/{row.branch}")
        if current_sha is None:
            reason = "already_reclaimed" if (row.branch, row.sha) in seen else (probe_error or "branch_missing")
            skipped.append({"branch": row.branch, "reason": reason})
            continue
        if current_sha != row.sha:
            skipped.append({"branch": row.branch, "reason": "authorized_sha_changed"})
            continue

        if (row.branch, row.sha) not in seen:
            written, write_error = _append_receipt(receipt, row)
            if not written:
                result["ok"] = False
                errors.append({"branch": row.branch, "reason": write_error or "receipt_write_failed"})
                continue
            seen.add((row.branch, row.sha))

        pinned, pin_result = _pin_branch(root, row.branch, row.sha)
        if not pinned:
            result["ok"] = False
            errors.append({"branch": row.branch, "reason": pin_result})
            continue

        if row.worktree is not None:
            removed = _git(root, "worktree", "remove", "--", row.worktree)
            if removed.returncode != 0:
                result["ok"] = False
                errors.append(
                    {
                        "branch": row.branch,
                        "reason": "worktree_remove_failed",
                        "detail": _detail(removed),
                    }
                )
                continue

        deletion = delete_authorized_branch(
            orchestrator_root=root,
            lane_id=f"orphan-{row.sha[:12]}",
            branch=row.branch,
            authorized_sha=row.sha,
            apply=True,
            task_ref=None,
            integration_ref=scan.integration_ref,
        )
        deleted, delete_reason = _delete_result_fields(deletion)
        if not deleted:
            result["ok"] = False
            errors.append({"branch": row.branch, "reason": delete_reason})
            continue
        applied.append(row.branch)
        _record_reclaim_decision(root, row)
    return result


__all__ = [
    "OrphanBranch",
    "OrphanScan",
    "apply_orphan_reclaim",
    "scan_orphan_branches",
]
