"""Derive ``changed_files`` for ``close_slice`` decisions (internal).

S0 records the derivation locus: primary in lifecycle ``slice_commit`` (worktree-local
git at commit time), with server ``close_slice`` as fallback when ``commit_sha`` is
reachable from the MCP workspace root.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

DEFAULT_DERIVE_TIMEOUT_SECONDS = 5.0

DERIVATION_LOCUS_DECISION_ID = "wb_checklist_durable_sync_derivation_locus_s0"


@dataclass(frozen=True, slots=True)
class DerivationLocusProbe:
    """S0 spike probe: documents where derivation runs."""

    primary_locus: str
    fallback_locus: str
    decision_id: str


@dataclass(frozen=True, slots=True)
class DerivationResult:
    """Outcome of a single-commit ``changed_files`` derivation."""

    paths: tuple[str, ...]
    warning: str | None = None
    derived: bool = False


def derivation_locus_probe() -> DerivationLocusProbe:
    """Return the recorded S0 derivation-locus decision (probe surface)."""

    return DerivationLocusProbe(
        primary_locus="slice_commit",
        fallback_locus="close_slice",
        decision_id=DERIVATION_LOCUS_DECISION_ID,
    )


def derive_changed_files_from_commit(
    repo_root: Path,
    commit_sha: str,
    *,
    timeout_seconds: float = DEFAULT_DERIVE_TIMEOUT_SECONDS,
) -> DerivationResult:
    """Derive monorepo-relative paths from ``git diff --name-only <sha>^..<sha>``."""

    sha = (commit_sha or "").strip()
    if not sha:
        return DerivationResult(paths=(), warning="missing_commit_sha")

    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "diff", "--name-only", f"{sha}^..{sha}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return DerivationResult(paths=(), warning=f"git_diff_failed:{exc}")

    if proc.returncode != 0:
        stderr = (proc.stderr or proc.stdout or "").strip()[:200]
        return DerivationResult(paths=(), warning=f"git_diff_rc_{proc.returncode}:{stderr}")

    paths = tuple(line.strip().replace("\\", "/") for line in proc.stdout.splitlines() if line.strip())
    return DerivationResult(paths=paths, derived=True)


def resolve_omitted_changed_files(
    repo_root: Path | None,
    commit_sha: str | None,
    *,
    timeout_seconds: float = DEFAULT_DERIVE_TIMEOUT_SECONDS,
) -> tuple[list[str] | None, str | None]:
    """Derive ``changed_files`` when the caller omitted the arg.

    Returns ``(paths, warning)``. ``paths`` is ``None`` when derivation could not run.
    """

    if repo_root is None or not repo_root.is_dir():
        return None, "derivation_repo_unavailable"
    if not commit_sha:
        return None, "derivation_commit_sha_missing"
    result = derive_changed_files_from_commit(repo_root, commit_sha, timeout_seconds=timeout_seconds)
    if result.warning and not result.paths:
        return None, result.warning
    return list(result.paths), result.warning
