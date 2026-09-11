"""Harvest-driven reaper for finished remote VM lane sandboxes.

This is the row-aware orchestrator complement to the standalone VM sweep in
``scripts/remote_agent.sh`` (RES-07). Marker-owned sandboxes with a row require
a terminal status (``closed`` / ``merged`` / ``closed_stale``). A sandbox
without a row is harvested only when its key maps to a branch known to this
repository and a bounded probe proves the clean, idle sandbox's exported blobs
equal that host branch tip. A non-terminal row is never harvested. Callers also
pass ``exclude_keys`` for the invoking pass.
``review`` is openable and is not a status shortcut. Rowless sandboxes whose
keys cannot be derived from this repository's refs or registry branches are
foreign and report-only. ``sandbox_bytes`` excludes those foreign bytes.

Liveness is never taken from a local snapshot. Listing reports lease / lock /
pid for classification. Deletion acquires ``$ROOT/.lane-lock-<key>``
non-blocking and holds it across the re-probe (marker, lease, pid) and the
``rm`` inside one ``flock -n`` invocation. Occupancy coordination with
dispatch is that per-lane lock: ``scripts/remote_agent.sh`` holds
``$ROOT/.lane-lock-<key>`` for the dispatch lifetime and does not take
``$ROOT/.reap.lock``. A probe that does not retain the lock is not a lock.

``echo REAPED`` is emitted only after the sandbox path is absent
(``[ ! -e "$_sd" ]``, the same shape as the TTL reaper). A surviving path
emits ``SKIPPED_GONE_FAILED`` and is not recorded as reaped.

Residual tech-debt:
- Dispatch materialization does not take ``$ROOT/.reap.lock``; per-lane locks
  remain the shared check/act coordination boundary.
- Listing still uses the self-releasing ``_lane_lock_held`` probe
  (``flock -n ... true``); only delete holds the per-lane lock across
  check and act.
- Production apply-path caller lives in ``lane_census.census_lanes``.
  ``TERMINAL_ROW_STATUSES`` is an alias of
  ``lane_worktree._TERMINAL_LANE_STATUSES``.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
import tarfile
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration.lane_worktree import (
    _TERMINAL_LANE_STATUSES,
)

__all__ = (
    "KIND_LOCK_HELD",
    "KIND_PROBE_FAILED",
    "KIND_SANDBOX_HARVESTED",
    "KIND_SANDBOX_FOREIGN",
    "KIND_SANDBOX_LIVE",
    "KIND_SANDBOX_ROWLESS_OURS",
    "KIND_SANDBOX_UNHARVESTED",
    "KIND_SANDBOX_UNMAPPED",
    "RemoteSandboxReapReport",
    "SSH_TIMEOUT_S",
    "SandboxVerdict",
    "TERMINAL_ROW_STATUSES",
    "plan_remote_sandboxes",
    "reap_remote_sandboxes",
)

KIND_SANDBOX_HARVESTED = "sandbox_harvested"
KIND_SANDBOX_FOREIGN = "sandbox_foreign"
KIND_SANDBOX_LIVE = "sandbox_live"
KIND_SANDBOX_ROWLESS_OURS = "sandbox_rowless_ours"
KIND_SANDBOX_UNMAPPED = "sandbox_unmapped"
KIND_SANDBOX_UNHARVESTED = "sandbox_unharvested"
KIND_LOCK_HELD = "lock_held"
KIND_PROBE_FAILED = "probe_failed"

TERMINAL_ROW_STATUSES = _TERMINAL_LANE_STATUSES

SSH_TIMEOUT_S = 30.0
GIT_TIMEOUT_S = 5.0

_DEFAULT_AGENT_ROOT = "grok-sandbox"
_SANDBOX_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}$")
_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
_LISTING_RE = re.compile(
    r"^SANDBOX (?P<key>\S+) tip=(?P<tip>\S+) occupant=(?P<occupant>live|idle) "
    r"lock=(?P<lock>held|free)(?: idle=(?P<idle>[0-9]+) bytes=(?P<bytes>[0-9]+))?$"
)
_REAP_LINE_RE = re.compile(
    r"^(?P<status>REAPED|SKIPPED_LIVE|SKIPPED_LOCK|SKIPPED_GONE_FAILED|SKIPPED_UNMARKED|SKIPPED_CHANGED) (?P<key>\S+)(?: (?P<detail>[a-z_]+))?$"
)
_HEADER_LIST = "# WORKBAY_REMOTE_SANDBOX_REAP op=list"
_HEADER_PROBE_PREFIX = "# WORKBAY_REMOTE_SANDBOX_REAP op=probe key="
_HEADER_REAP_PREFIX = "# WORKBAY_REMOTE_SANDBOX_REAP op=reap keys="
DEFAULT_IDLE_SECONDS = 21_600

SshRunner = Callable[..., Any]
RefLister = Callable[[Path], Sequence[str]]
TreeEntries = dict[str, tuple[str, str, str]]


@dataclass(frozen=True)
class _RowlessProof:
    tree: str
    generation: str
    host_tip: str = ""


@dataclass(frozen=True)
class SandboxVerdict:
    sandbox_key: str
    lane_id: str | None
    branch: str | None
    kind: str
    tip_sha: str | None
    reason: str


@dataclass
class RemoteSandboxReapReport:
    verdicts: list[SandboxVerdict] = field(default_factory=list)
    reaped: list[str] = field(default_factory=list)
    dry_run: bool = True
    probe_error: str | None = None
    skipped: bool = False
    skip_reason: str | None = None
    gauge: dict[str, int] = field(
        default_factory=lambda: {
            "sandbox_count": 0,
            "sandbox_bytes": 0,
            "oldest_idle_seconds": 0,
            "foreign_sandbox_count": 0,
            "foreign_sandbox_bytes": 0,
            "rowless_ours_count": 0,
        }
    )


@dataclass(frozen=True)
class _ListedSandbox:
    key: str
    tip_sha: str | None
    occupant_live: bool
    lock_held: bool
    idle_seconds: int
    bytes_used: int


@dataclass(frozen=True)
class _MappedRow:
    lane_id: str | None
    branch: str | None
    status: str | None


@dataclass(frozen=True)
class _AmbiguousRows:
    rows: tuple[_MappedRow, ...]


def plan_remote_sandboxes(
    task_ref: str,
    *,
    ssh_runner: SshRunner,
    primary_repo: Path | str,
    rows: Sequence[Mapping[str, Any] | Any],
    sandbox_root: str | None = None,
    actor: Any = None,
    exclude_keys: Sequence[str] | None = None,
    idle_seconds: int = DEFAULT_IDLE_SECONDS,
    ref_lister: RefLister | None = None,
) -> RemoteSandboxReapReport:
    """Report harvest verdicts for marker-gated remote VM sandboxes; never delete."""
    return _reap_remote_sandbox_pass(
        task_ref,
        ssh_runner=ssh_runner,
        primary_repo=primary_repo,
        rows=rows,
        sandbox_root=sandbox_root,
        dry_run=True,
        actor=actor,
        exclude_keys=exclude_keys,
        idle_seconds=idle_seconds,
        ref_lister=ref_lister,
    )


def reap_remote_sandboxes(
    task_ref: str,
    *,
    ssh_runner: SshRunner,
    primary_repo: Path | str,
    rows: Sequence[Mapping[str, Any] | Any],
    sandbox_root: str | None = None,
    actor: Any = None,
    exclude_keys: Sequence[str] | None = None,
    idle_seconds: int = DEFAULT_IDLE_SECONDS,
    ref_lister: RefLister | None = None,
) -> RemoteSandboxReapReport:
    """Reap harvested marker-gated remote VM sandboxes.

    Only ``sandbox_harvested`` keys are sent to the remote delete script.
    A probe/parse failure refuses the whole pass: ``probe_error`` is set
    and ``reaped`` is empty. Call ``plan_remote_sandboxes`` to preview.
    """
    return _reap_remote_sandbox_pass(
        task_ref,
        ssh_runner=ssh_runner,
        primary_repo=primary_repo,
        rows=rows,
        sandbox_root=sandbox_root,
        dry_run=False,
        actor=actor,
        exclude_keys=exclude_keys,
        idle_seconds=idle_seconds,
        ref_lister=ref_lister,
    )


def _reap_remote_sandbox_pass(
    task_ref: str,
    *,
    ssh_runner: SshRunner,
    primary_repo: Path | str,
    rows: Sequence[Mapping[str, Any] | Any],
    sandbox_root: str | None = None,
    dry_run: bool,
    actor: Any = None,
    exclude_keys: Sequence[str] | None = None,
    idle_seconds: int,
    ref_lister: RefLister | None,
) -> RemoteSandboxReapReport:
    """Classify sandboxes and, when ``dry_run`` is false, delete harvested keys."""
    report = RemoteSandboxReapReport(dry_run=dry_run)
    scoped = (task_ref or "").strip()
    if not scoped:
        return _fail(report, "task_ref is empty")
    root = _resolve_sandbox_root(sandbox_root)
    if root is None:
        return _fail(report, "sandbox_root is empty or unsafe")
    repo = Path(primary_repo)
    excluded = frozenset(str(key) for key in (exclude_keys or ()) if str(key))

    listed, list_error = _list_remote_sandboxes(ssh_runner, root)
    if list_error is not None:
        return _fail(report, list_error)
    if not listed:
        return report
    mapped_rows = _index_rows(rows, task_ref=scoped)
    ref_branches: list[str] = []
    if any(_map_key(item.key, mapped_rows) is None for item in listed):
        try:
            ref_branches = list((ref_lister or _list_repo_branches)(repo))
        except Exception as exc:  # noqa: BLE001 — ownership ambiguity must fail closed
            return _fail(report, f"known branch listing failed: {exc}")
    known_keys = _known_branch_keys(repo, rows, ref_lister=lambda _repo: ref_branches)
    known_branches = _known_branches(rows, ref_branches)
    branches_by_key: dict[str, list[str]] = {}
    for branch in known_branches:
        branches_by_key.setdefault(_derive_lane_key(_branch_identity(branch)), []).append(branch)

    foreign_keys: set[str] = set()
    rowless_ours_keys: set[str] = set()
    for item in listed:
        if _map_key(item.key, mapped_rows) is not None:
            continue
        if item.key in known_keys:
            rowless_ours_keys.add(item.key)
        else:
            foreign_keys.add(item.key)
    report.gauge = _gauge_for(
        listed,
        foreign_keys=frozenset(foreign_keys),
        rowless_ours_keys=frozenset(rowless_ours_keys),
    )

    harvested_keys: list[str] = []
    rowless_proofs: dict[str, _RowlessProof] = {}
    for item in listed:
        candidates = branches_by_key.get(item.key, [])
        verdict = _classify(
            item,
            mapped_rows,
            repo,
            ssh_runner=ssh_runner,
            sandbox_root=root,
            rowless_branch=candidates[0] if len(candidates) == 1 else None,
            rowless_ours=item.key in known_keys,
            exclude_keys=excluded,
            idle_seconds=max(0, int(idle_seconds)),
            rowless_proofs=rowless_proofs,
        )
        report.verdicts.append(verdict)
        if verdict.kind == KIND_SANDBOX_HARVESTED:
            harvested_keys.append(item.key)

    if dry_run or not harvested_keys:
        return report

    reap_lines, reap_error = _reap_remote_sandboxes(
        ssh_runner, root, harvested_keys, rowless_proofs=rowless_proofs, idle_seconds=max(0, int(idle_seconds))
    )
    if reap_error is not None:
        report.verdicts = [
            SandboxVerdict(
                sandbox_key=verdict.sandbox_key,
                lane_id=verdict.lane_id,
                branch=verdict.branch,
                kind=KIND_PROBE_FAILED if verdict.kind == KIND_SANDBOX_HARVESTED else verdict.kind,
                tip_sha=verdict.tip_sha,
                reason=reap_error if verdict.kind == KIND_SANDBOX_HARVESTED else verdict.reason,
            )
            for verdict in report.verdicts
        ]
        if not any(verdict.kind == KIND_PROBE_FAILED for verdict in report.verdicts):
            report.verdicts.append(
                SandboxVerdict(
                    sandbox_key="",
                    lane_id=None,
                    branch=None,
                    kind=KIND_PROBE_FAILED,
                    tip_sha=None,
                    reason=reap_error,
                )
            )
        report.probe_error = reap_error
        report.reaped = []
        return report

    if reap_lines == ["LOCK_HELD"]:
        report.verdicts = [
            SandboxVerdict(
                sandbox_key=verdict.sandbox_key,
                lane_id=verdict.lane_id,
                branch=verdict.branch,
                kind=KIND_LOCK_HELD if verdict.kind == KIND_SANDBOX_HARVESTED else verdict.kind,
                tip_sha=verdict.tip_sha,
                reason="remote .reap.lock is held" if verdict.kind == KIND_SANDBOX_HARVESTED else verdict.reason,
            )
            for verdict in report.verdicts
        ]
        if not any(verdict.kind == KIND_LOCK_HELD for verdict in report.verdicts):
            report.verdicts.append(
                SandboxVerdict(
                    sandbox_key="",
                    lane_id=None,
                    branch=None,
                    kind=KIND_LOCK_HELD,
                    tip_sha=None,
                    reason="remote .reap.lock is held",
                )
            )
        report.reaped = []
        return report

    by_key = {verdict.sandbox_key: verdict for verdict in report.verdicts}
    reaped: list[str] = []
    updated: list[SandboxVerdict] = []
    seen_reap_keys: set[str] = set()
    for line in reap_lines:
        match = _REAP_LINE_RE.match(line)
        if match is None:
            return _fail(report, f"unexpected reap output: {line!r}")
        key = match.group("key")
        status = match.group("status")
        seen_reap_keys.add(key)
        prior = by_key.get(key)
        lane_id = prior.lane_id if prior else None
        branch = prior.branch if prior else None
        tip_sha = prior.tip_sha if prior else None
        if status == "REAPED":
            updated.append(
                SandboxVerdict(
                    sandbox_key=key,
                    lane_id=lane_id,
                    branch=branch,
                    kind=KIND_SANDBOX_HARVESTED,
                    tip_sha=tip_sha,
                    reason=prior.reason if prior else "reaped",
                )
            )
            reaped.append(key)
            _record_keyed_decision(
                task_ref=scoped,
                lane_id=lane_id or "unmapped",
                tip_sha=tip_sha,
                actor=actor,
                sandbox_key=key,
                reason=prior.reason if prior else "reaped",
            )
        elif status == "SKIPPED_CHANGED":
            updated.append(
                SandboxVerdict(
                    sandbox_key=key,
                    lane_id=lane_id,
                    branch=branch,
                    kind=KIND_SANDBOX_UNHARVESTED,
                    tip_sha=tip_sha,
                    reason="remote re-probe: rowless generation, tree, cleanliness or idle proof changed"
                    + (f": {match.group('detail')}" if match.group("detail") else ""),
                )
            )
        elif status == "SKIPPED_LIVE":
            updated.append(
                SandboxVerdict(
                    sandbox_key=key,
                    lane_id=lane_id,
                    branch=branch,
                    kind=KIND_SANDBOX_LIVE,
                    tip_sha=tip_sha,
                    reason="remote re-probe: lease or pid live",
                )
            )
        elif status == "SKIPPED_GONE_FAILED":
            updated.append(
                SandboxVerdict(
                    sandbox_key=key,
                    lane_id=lane_id,
                    branch=branch,
                    kind=KIND_PROBE_FAILED,
                    tip_sha=tip_sha,
                    reason="SKIPPED_GONE_FAILED: remote delete left sandbox path in place",
                )
            )
        elif status == "SKIPPED_UNMARKED":
            updated.append(
                SandboxVerdict(
                    sandbox_key=key,
                    lane_id=lane_id,
                    branch=branch,
                    kind=KIND_SANDBOX_UNMAPPED,
                    tip_sha=tip_sha,
                    reason="remote re-probe: sandbox marker missing",
                )
            )
        else:
            updated.append(
                SandboxVerdict(
                    sandbox_key=key,
                    lane_id=lane_id,
                    branch=branch,
                    kind=KIND_LOCK_HELD,
                    tip_sha=tip_sha,
                    reason="remote re-probe: lane lock held",
                )
            )

    kept = [verdict for verdict in report.verdicts if verdict.sandbox_key not in seen_reap_keys]
    report.verdicts = kept + updated
    report.reaped = reaped
    reaped_set = set(reaped)
    report.gauge = _gauge_for(
        [item for item in listed if item.key not in reaped_set],
        foreign_keys=frozenset(foreign_keys),
        rowless_ours_keys=frozenset(rowless_ours_keys),
    )
    return report


def _gauge_for(
    items: Sequence[_ListedSandbox],
    *,
    foreign_keys: frozenset[str] = frozenset(),
    rowless_ours_keys: frozenset[str] = frozenset(),
) -> dict[str, int]:
    """Return repository-owned pressure separately from foreign VM usage."""
    foreign = [item for item in items if item.key in foreign_keys]
    return {
        "sandbox_count": len(items),
        "sandbox_bytes": sum(item.bytes_used for item in items if item.key not in foreign_keys),
        "oldest_idle_seconds": max((item.idle_seconds for item in items), default=0),
        "foreign_sandbox_count": len(foreign),
        "foreign_sandbox_bytes": sum(item.bytes_used for item in foreign),
        "rowless_ours_count": sum(item.key in rowless_ours_keys for item in items),
    }


def _fail(report: RemoteSandboxReapReport, error: str) -> RemoteSandboxReapReport:
    report.probe_error = error
    report.reaped = []
    if not any(verdict.kind == KIND_PROBE_FAILED for verdict in report.verdicts):
        report.verdicts.append(
            SandboxVerdict(
                sandbox_key="",
                lane_id=None,
                branch=None,
                kind=KIND_PROBE_FAILED,
                tip_sha=None,
                reason=error,
            )
        )
    else:
        report.verdicts = [
            SandboxVerdict(
                sandbox_key=verdict.sandbox_key,
                lane_id=verdict.lane_id,
                branch=verdict.branch,
                kind=KIND_PROBE_FAILED,
                tip_sha=verdict.tip_sha,
                reason=error,
            )
            if verdict.kind == KIND_SANDBOX_HARVESTED
            else verdict
            for verdict in report.verdicts
        ]
    return report


def _resolve_sandbox_root(sandbox_root: str | None) -> str | None:
    raw = (
        sandbox_root if sandbox_root is not None else os.environ.get("WORKBAY_REMOTE_AGENT_ROOT") or _DEFAULT_AGENT_ROOT
    )
    value = raw.strip()
    if not value or ".." in value or "\n" in value or "\r" in value:
        return None
    return value


def _index_rows(
    rows: Sequence[Mapping[str, Any] | Any],
    *,
    task_ref: str,
) -> list[tuple[_MappedRow, str]]:
    del task_ref  # classification still treats repository absence as unproven globally
    indexed: list[tuple[_MappedRow, str]] = []
    for row in rows:
        branch = _row_text(row, "branch")
        mapped = _MappedRow(
            lane_id=_row_text(row, "lane_id"),
            branch=branch,
            status=_row_text(row, "status"),
        )
        derived = _derive_lane_key(branch) if branch else ""
        indexed.append((mapped, derived))
    return indexed


def _list_repo_branches(primary_repo: Path) -> list[str]:
    """List every local, salvage, and remote branch known to this repository."""
    try:
        proc = subprocess.run(
            [
                "git",
                "-C",
                str(primary_repo),
                "for-each-ref",
                "--format=%(refname) %(symref)",
                "refs/heads",
                "refs/salvage",
                "refs/remotes",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=GIT_TIMEOUT_S,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(str(exc)) from exc
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or f"git exited {proc.returncode}").strip()[:300])
    # Symbolic remote HEAD aliases are not original branch identities.
    return [line.strip() for line in proc.stdout.splitlines() if len(line.split()) == 1]


def _branch_identity(ref: str) -> str:
    """Keep the qualified lookup ref separate from the dispatcher's branch name."""
    if ref.startswith("refs/remotes/"):
        return ref.removeprefix("refs/remotes/").partition("/")[2]
    if ref.startswith("refs/heads/"):
        return ref.removeprefix("refs/heads/")
    if ref.startswith("refs/salvage/"):
        return ref.removeprefix("refs/")
    # Injected short branch names have no namespace evidence to strip safely.
    return ref


def _known_branches(
    rows: Sequence[Mapping[str, Any] | Any],
    ref_branches: Sequence[str],
) -> list[str]:
    branches: list[str] = []
    seen: set[str] = set()
    for branch in [*ref_branches, *(_row_text(row, "branch") for row in rows)]:
        if not branch or branch in seen:
            continue
        seen.add(branch)
        branches.append(branch)
    return branches


def _known_branch_keys(
    primary_repo: Path | str,
    rows: Sequence[Mapping[str, Any] | Any],
    *,
    ref_lister: RefLister | None = None,
) -> frozenset[str]:
    """Return keys derived from all repository refs and all registry rows."""
    repo = Path(primary_repo)
    ref_branches = (ref_lister or _list_repo_branches)(repo)
    return frozenset(_derive_lane_key(_branch_identity(branch)) for branch in _known_branches(rows, ref_branches))


def _classify(
    item: _ListedSandbox,
    indexed_rows: Sequence[tuple[_MappedRow, str]],
    repo: Path,
    *,
    ssh_runner: SshRunner,
    sandbox_root: str,
    rowless_branch: str | None = None,
    rowless_ours: bool = False,
    exclude_keys: frozenset[str] = frozenset(),
    idle_seconds: int = DEFAULT_IDLE_SECONDS,
    rowless_proofs: dict[str, _RowlessProof] | None = None,
) -> SandboxVerdict:
    mapped = _map_key(item.key, indexed_rows)
    if mapped is None and not rowless_ours:
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=None,
            branch=None,
            kind=KIND_SANDBOX_FOREIGN,
            tip_sha=item.tip_sha,
            reason="key is not derived from any branch known to this repository",
        )
    if item.occupant_live:
        row = mapped if isinstance(mapped, _MappedRow) else None
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=row.lane_id if row else None,
            branch=row.branch if row else rowless_branch,
            kind=KIND_SANDBOX_LIVE,
            tip_sha=item.tip_sha,
            reason="unexpired lease or live pid under sandbox",
        )
    if item.lock_held:
        row = mapped if isinstance(mapped, _MappedRow) else None
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=row.lane_id if row else None,
            branch=row.branch if row else rowless_branch,
            kind=KIND_LOCK_HELD,
            tip_sha=item.tip_sha,
            reason="lane lock held",
        )
    if item.key in exclude_keys:
        row = mapped if isinstance(mapped, _MappedRow) else None
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=row.lane_id if row else None,
            branch=row.branch if row else rowless_branch,
            kind=KIND_SANDBOX_UNHARVESTED,
            tip_sha=item.tip_sha,
            reason="excluded invoking-pass identity",
        )
    if isinstance(mapped, _AmbiguousRows):
        statuses = sorted({(row.status or "unknown").strip().lower() for row in mapped.rows})
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=None,
            branch=None,
            kind=KIND_SANDBOX_UNHARVESTED,
            tip_sha=item.tip_sha,
            reason=f"ambiguous ownership across {len(mapped.rows)} lane rows (statuses: {','.join(statuses)})",
        )
    row = mapped
    if row is None:
        if rowless_branch is None:
            return SandboxVerdict(
                sandbox_key=item.key,
                lane_id=None,
                branch=None,
                kind=KIND_SANDBOX_UNHARVESTED,
                tip_sha=item.tip_sha,
                reason="ambiguous known-branch key collision",
            )
        if item.idle_seconds < idle_seconds:
            return SandboxVerdict(
                sandbox_key=item.key,
                lane_id=None,
                branch=rowless_branch,
                kind=KIND_SANDBOX_ROWLESS_OURS,
                tip_sha=item.tip_sha,
                reason=f"rowless ours idle {item.idle_seconds}s below threshold {idle_seconds}s",
            )
        return _classify_rowless_ours(
            item,
            repo,
            rowless_branch,
            ssh_runner=ssh_runner,
            sandbox_root=sandbox_root,
            rowless_proofs=rowless_proofs,
        )
    status = (row.status or "").strip().lower()
    if status in TERMINAL_ROW_STATUSES:
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=row.lane_id,
            branch=row.branch,
            kind=KIND_SANDBOX_HARVESTED,
            tip_sha=item.tip_sha,
            reason=f"lane row status {status}",
        )
    return SandboxVerdict(
        sandbox_key=item.key,
        lane_id=row.lane_id,
        branch=row.branch,
        kind=KIND_SANDBOX_UNHARVESTED,
        tip_sha=item.tip_sha,
        reason="row is non-terminal",
    )


def _classify_rowless_ours(
    item: _ListedSandbox,
    repo: Path,
    branch: str,
    *,
    ssh_runner: SshRunner,
    sandbox_root: str,
    rowless_proofs: dict[str, _RowlessProof] | None = None,
) -> SandboxVerdict:
    host_tip, host_entries, host_error = _host_branch_blobs(repo, branch)
    if host_error is not None:
        return _rowless_verdict(item, branch, KIND_PROBE_FAILED, host_error)

    raw, probe_error = _run_ssh(
        ssh_runner, _build_rowless_probe_script(sandbox_root, item.key, host_tip=host_tip or "")
    )
    if probe_error is not None:
        return _rowless_verdict(item, branch, KIND_PROBE_FAILED, probe_error)
    remote_entries, proof, clean, parse_error = _parse_rowless_probe(raw)
    if parse_error is not None:
        return _rowless_verdict(item, branch, KIND_PROBE_FAILED, parse_error)
    if not clean:
        return _rowless_verdict(item, branch, KIND_SANDBOX_UNHARVESTED, f"rowless ours sandbox is dirty: {raw.strip()}")

    assert host_entries is not None
    assert remote_entries is not None
    for path in sorted(host_entries.keys() | remote_entries.keys()):
        if host_entries.get(path) != remote_entries.get(path):
            return _rowless_verdict(
                item,
                branch,
                KIND_SANDBOX_UNHARVESTED,
                f"rowless ours differs from host tip at {path}",
            )
    assert proof is not None
    if rowless_proofs is not None:
        rowless_proofs[item.key] = _RowlessProof(proof.tree, proof.generation, host_tip or "")
    return _rowless_verdict(
        item,
        branch,
        KIND_SANDBOX_HARVESTED,
        f"rowless_ours_content_equal_to_host_tip {branch}@{host_tip}",
    )


def _rowless_verdict(item: _ListedSandbox, branch: str, kind: str, reason: str) -> SandboxVerdict:
    return SandboxVerdict(
        sandbox_key=item.key,
        lane_id=None,
        branch=branch,
        kind=kind,
        tip_sha=item.tip_sha,
        reason=reason,
    )


def _host_branch_blobs(repo: Path, branch: str) -> tuple[str | None, TreeEntries | None, str | None]:
    if not branch or branch.startswith("-") or ".." in branch or "\n" in branch or "\r" in branch:
        return None, None, f"unsafe known branch name: {branch!r}"
    shallow, shallow_error = _run_local_git(repo, "rev-parse", "--is-shallow-repository")
    if shallow_error is not None or shallow.strip() != "false":
        return None, None, "host ancestry cannot be verified: shallow or unavailable repository"
    tip_proc, tip_error = _run_local_git(repo, "rev-parse", "--verify", "--end-of-options", f"{branch}^{{commit}}")
    if tip_error is not None:
        return None, None, f"host tip probe failed for {branch}: {tip_error}"
    tip = tip_proc.strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", tip):
        return None, None, f"host tip probe returned malformed sha for {branch}"
    tree_output, tree_error = _run_local_git(repo, "ls-tree", "-r", "--full-tree", tip)
    if tree_error is not None:
        return None, None, f"host tree probe failed for {branch}: {tree_error}"
    entries, parse_error = _parse_ls_tree_lines(tree_output.splitlines())
    if parse_error is not None:
        return None, None, f"host tree parse failed for {branch}: {parse_error}"
    # Use the dispatch exporter itself: arbitrary missing paths are not proof of
    # export exclusion. Do not extract the archive or load its contents in RAM.
    try:
        with tempfile.TemporaryFile() as archive:
            proc = subprocess.run(
                ["git", "-C", str(repo), "archive", "--format=tar", tip],
                stdout=archive,
                stderr=subprocess.PIPE,
                timeout=GIT_TIMEOUT_S,
                stdin=subprocess.DEVNULL,
                check=False,
            )
            if proc.returncode:
                return None, None, f"host export failed for {branch}: git exited {proc.returncode}"
            archive.seek(0)
            with tarfile.open(fileobj=archive, mode="r|") as exported:
                paths = {member.name for member in exported if not member.isdir()}
        if not paths <= entries.keys():
            return None, None, f"host export contains unknown paths for {branch}"
    except (OSError, subprocess.TimeoutExpired, tarfile.TarError) as exc:
        return None, None, f"host export failed for {branch}: {exc}"
    return tip, {path: entries[path] for path in paths}, None


def _run_local_git(repo: Path, *args: str) -> tuple[str, str | None]:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=GIT_TIMEOUT_S,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return "", str(exc)
    if proc.returncode != 0:
        return "", (proc.stderr or proc.stdout or f"git exited {proc.returncode}").strip()[:300]
    return proc.stdout, None


def _parse_rowless_probe(raw: str) -> tuple[TreeEntries | None, _RowlessProof | None, bool, str | None]:
    lines = raw.splitlines()
    if len(lines) == 1 and re.fullmatch(r"DIRTY(?: [a-z_]+)?", lines[0]):
        return {}, None, False, None
    if len(lines) == 1 and re.fullmatch(r"PROBE_FAILED [a-z_]+", lines[0]):
        return None, None, False, lines[0]
    if (
        len(lines) < 3
        or lines[0] != "CLEAN"
        or not re.fullmatch(r"TREE [0-9a-f]{40}", lines[1])
        or not re.fullmatch(r"GENERATION [0-9a-f]{64}", lines[2])
    ):
        return None, None, False, "unexpected rowless sandbox probe output"
    proof = _RowlessProof(tree=lines[1].split()[1], generation=lines[2].split()[1])
    entries, error = _parse_ls_tree_lines(lines[3:])
    if error is not None:
        return None, None, False, f"unexpected rowless sandbox tree output: {error}"
    return entries, proof, True, None


def _parse_ls_tree_lines(lines: Sequence[str]) -> tuple[TreeEntries, str | None]:
    entries: TreeEntries = {}
    for line in lines:
        match = re.fullmatch(r"(100644|100755|120000) (blob) ([0-9a-f]{40})\t([^\r\n]+)", line)
        if match is None:
            return {}, repr(line)
        path = match.group(4)
        if path.startswith('"'):
            return {}, f"unsupported quoted path {path!r}"
        if path in entries:
            return {}, f"duplicate path {path!r}"
        entries[path] = (match.group(1), match.group(2), match.group(3))
    return entries, None


def _exclusion_keys_for_rows(rows: Sequence[Mapping[str, Any] | Any]) -> list[str]:
    """Derived sandbox keys for non-terminal rows in the invoking pass."""
    keys: list[str] = []
    for row in rows:
        status = (_row_text(row, "status") or "").strip().lower()
        if status in TERMINAL_ROW_STATUSES:
            continue
        branch = _row_text(row, "branch")
        if not branch:
            continue
        keys.append(_derive_lane_key(branch))
    return keys


def _map_key(key: str, indexed_rows: Sequence[tuple[_MappedRow, str]]) -> _MappedRow | _AmbiguousRows | None:
    exact = [row for row, derived in indexed_rows if derived and derived == key]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        return _AmbiguousRows(tuple(exact))
    return None


def _derive_lane_key(branch: str) -> str:
    """Mirror ``remote_agent.sh`` / ``offload_pass._remote_lane_key``."""
    digest = hashlib.sha256(branch.encode("utf-8", errors="surrogatepass")).hexdigest()[:8]
    key = re.sub(r"[^A-Za-z0-9-]", "-", branch)[:40]
    while key.startswith("-"):
        key = key[1:]
    return f"{key or 'lane'}-{digest}"


def _row_text(row: Mapping[str, Any] | Any, key: str) -> str | None:
    if isinstance(row, Mapping):
        raw = row.get(key)
    else:
        raw = getattr(row, key, None)
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    return value or None


def _tip_is_ancestor(repo: Path, tip_sha: str, branch: str) -> bool:
    if not _SHA_RE.fullmatch(tip_sha):
        return False
    if not branch or branch.startswith("-") or ".." in branch or "\n" in branch:
        return False
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo), "merge-base", "--is-ancestor", tip_sha, branch],
            capture_output=True,
            text=True,
            check=False,
            timeout=GIT_TIMEOUT_S,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _list_remote_sandboxes(
    ssh_runner: SshRunner,
    root: str,
) -> tuple[list[_ListedSandbox], str | None]:
    result, error = _run_ssh(ssh_runner, _build_list_script(root))
    if error is not None:
        return [], error
    listed: list[_ListedSandbox] = []
    for raw_line in result.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _LISTING_RE.match(line)
        if match is None:
            return [], f"unexpected listing line: {line!r}"
        key = match.group("key")
        if not _SANDBOX_KEY_RE.fullmatch(key):
            return [], f"unsafe sandbox key in listing: {key!r}"
        tip_raw = match.group("tip")
        tip_sha = tip_raw.lower() if _SHA_RE.fullmatch(tip_raw.lower()) else None
        listed.append(
            _ListedSandbox(
                key=key,
                tip_sha=tip_sha,
                occupant_live=match.group("occupant") == "live",
                lock_held=match.group("lock") == "held",
                idle_seconds=int(match.group("idle") or 0),
                bytes_used=int(match.group("bytes") or 0),
            )
        )
    return listed, None


def _reap_remote_sandboxes(
    ssh_runner: SshRunner,
    root: str,
    keys: Sequence[str],
    *,
    rowless_proofs: Mapping[str, _RowlessProof] | None = None,
    idle_seconds: int = DEFAULT_IDLE_SECONDS,
) -> tuple[list[str], str | None]:
    safe_keys = [key for key in keys if _SANDBOX_KEY_RE.fullmatch(key)]
    if len(safe_keys) != len(keys):
        return [], "refusing to reap unsafe sandbox key"
    result, error = _run_ssh(
        ssh_runner, _build_reap_script(root, safe_keys, rowless_proofs=rowless_proofs, idle_seconds=idle_seconds)
    )
    if error is not None:
        return [], error
    lines = [line.strip() for line in result.splitlines() if line.strip()]
    if lines == ["LOCK_HELD"]:
        return lines, None
    for line in lines:
        if line == "LOCK_HELD":
            return [], "mixed LOCK_HELD with per-key reap lines"
        if _REAP_LINE_RE.match(line) is None:
            return [], f"unexpected reap output: {line!r}"
    return lines, None


def _run_ssh(ssh_runner: SshRunner, script: str) -> tuple[str, str | None]:
    try:
        raw = ssh_runner(script, timeout=SSH_TIMEOUT_S)
    except (TimeoutError, subprocess.TimeoutExpired) as exc:
        return "", f"ssh timed out: {exc}"
    except OSError as exc:
        return "", f"ssh failed: {exc}"
    except TypeError:
        try:
            raw = ssh_runner(script)
        except (TimeoutError, subprocess.TimeoutExpired) as exc:
            return "", f"ssh timed out: {exc}"
        except OSError as exc:
            return "", f"ssh failed: {exc}"
    stdout, stderr, returncode = _coerce_ssh_result(raw)
    if returncode not in (0, None):
        detail = (stderr or stdout or f"exit {returncode}").strip()
        return "", f"ssh exited {returncode}: {detail[:300]}"
    return stdout, None


def _coerce_ssh_result(raw: Any) -> tuple[str, str, int | None]:
    if isinstance(raw, str):
        return raw, "", 0
    stdout = getattr(raw, "stdout", "") or ""
    stderr = getattr(raw, "stderr", "") or ""
    returncode = getattr(raw, "returncode", 0)
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="replace")
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    try:
        code: int | None = int(returncode) if returncode is not None else 0
    except (TypeError, ValueError):
        code = 0
    return str(stdout), str(stderr), code


def _sh_single_quote(value: str) -> str:
    return "'" + value.replace("'", "'\\''") + "'"


def _remote_helpers(root: str) -> str:
    quoted_root = _sh_single_quote(root)
    return f"""
ROOT={quoted_root}
# Keep replacement refs planted after the guard from changing object inspection.
export GIT_NO_REPLACE_OBJECTS=1
case "$ROOT" in
  /*) ;;
  *) ROOT="$HOME/$ROOT" ;;
esac
_lane_occupant_live() {{
  _lk="${{1:-}}"
  [ -n "$_lk" ] || return 0
  _lf="$ROOT/.lane-live-$_lk"
  [ -f "$_lf" ] || return 1
  _expiry=
  _issued=
  while IFS= read -r _lline || [ -n "$_lline" ]; do
    case "$_lline" in
      expiry=*) _expiry="${{_lline#expiry=}}" ;;
      issued=*) _issued="${{_lline#issued=}}" ;;
    esac
  done < "$_lf" || return 0
  case "$_expiry" in
    ''|*[!0-9]*) return 0 ;;
  esac
  case "$_issued" in
    ''|*[!0-9]*) return 0 ;;
  esac
  _now=$(date +%s)
  if [ "$_now" -ge "$_expiry" ]; then
    return 1
  fi
  return 0
}}
_lane_lock_held() {{
  _lk="${{1:-}}"
  [ -n "$_lk" ] || return 1
  _llf="$ROOT/.lane-lock-$_lk"
  if [ ! -f "$_llf" ]; then
    return 1
  fi
  if flock -n "$_llf" true 2>/dev/null; then
    return 1
  fi
  return 0
}}
_pid_in_sandbox() {{
  _sd="${{1:-}}"
  [ -n "$_sd" ] || return 1
  for _cwd in /proc/[0-9]*/cwd; do
    [ -L "$_cwd" ] || continue
    _target=$(readlink "$_cwd" 2>/dev/null) || continue
    case "$_target" in
      "$_sd"|"$_sd"/*) return 0 ;;
    esac
  done
  return 1
}}
_sandbox_generation() {{
  _identity=$(stat -c '%d:%i:%w' "$1" "$1/.git") || return 1
  _marker_identity=$(stat -c '%d:%i:%z' "$1/.workbay-lane-sandbox") || return 1
  printf '%s\\n%s\\n' "$_identity" "$_marker_identity" | sha256sum | cut -d ' ' -f 1
}}
_git_admin_shape_safe() {{
  # Only a plain, self-contained repository can support the salvage proof.
  # Check filesystem redirections before invoking Git at all.
  _worktree_reason=git_directory_unverified
  [ -d "$1/.git" ] && [ ! -L "$1/.git" ] || return 1
  _worktree_reason=commondir_present
  [ ! -e "$1/.git/commondir" ] && [ ! -L "$1/.git/commondir" ] || return 1
  _worktree_reason=git_admin_shape_unverified
  for _admin_path in refs objects info refs/replace objects/info; do
    [ ! -L "$1/.git/$_admin_path" ] || return 1
  done
  # Superproject fsck cannot prove retained submodule objects were harvested.
  _worktree_reason=submodule_repositories_present
  [ ! -L "$1/.git/modules" ] || return 1
  if [ -e "$1/.git/modules" ]; then
    [ -d "$1/.git/modules" ] || return 1
    ls -A "$1/.git/modules" >/dev/null || return 1
    for _module in "$1/.git/modules/"* "$1/.git/modules/".[!.]* "$1/.git/modules/"..?*; do
      [ ! -e "$_module" ] && [ ! -L "$_module" ] || return 1
    done
  fi
  _worktree_reason=linked_worktrees_present
  [ ! -L "$1/.git/worktrees" ] || return 1
  for _linked in "$1/.git/worktrees/"* "$1/.git/worktrees/".[!.]* "$1/.git/worktrees/"..?*; do
    [ ! -e "$_linked" ] && [ ! -L "$_linked" ] || return 1
  done
  _worktree_reason=grafts_present
  [ ! -e "$1/.git/info/grafts" ] && [ ! -L "$1/.git/info/grafts" ] || return 1
  _worktree_reason=replace_refs_present
  [ ! -e "$1/.git/refs/replace" ] || return 1
  [ ! -L "$1/.git/packed-refs" ] || return 1
  if [ -e "$1/.git/packed-refs" ]; then
    awk 'index($2, "refs/replace/") == 1 {{ found=1 }} END {{ exit found }}' "$1/.git/packed-refs" || return 1
  fi
  _worktree_reason=alternates_present
  for _alternate in alternates http-alternates; do
    [ ! -e "$1/.git/objects/info/$_alternate" ] && [ ! -L "$1/.git/objects/info/$_alternate" ] || return 1
  done
  _worktree_reason=shallow_present
  [ ! -e "$1/.git/shallow" ] && [ ! -L "$1/.git/shallow" ] || return 1
  _worktree_reason=git_common_dir_unverified
  _common=$(git -C "$1" --work-tree="$1" --git-dir="$1/.git" rev-parse --path-format=absolute --git-common-dir) || return 1
  [ "$_common" = "$1/.git" ] || return 1
  _worktree_reason=linked_worktrees_present
  _worktrees=$(git -C "$1" --work-tree="$1" --git-dir="$1/.git" worktree list --porcelain) || return 1
  [ "$(printf '%s\\n' "$_worktrees" | awk '/^worktree / {{ n++ }} END {{ print n+0 }}')" -eq 1 ] || return 1
  return 0
}}
_rowless_worktree_safe() {{
  _git_admin_shape_safe "$1" || return 1
  _worktree_reason=replace_refs_present
  _replace_refs=$(git -C "$1" --work-tree="$1" --git-dir="$1/.git" for-each-ref --format='%(refname)' refs/replace/) || return 1
  [ -z "$_replace_refs" ] || return 1
  _worktree_reason=redirect_config
  [ ! -e "$1/.git/objects/info/alternates" ] || return 1
  [ ! -e "$1/.git/objects/info/http-alternates" ] || return 1
  _redirects=$(git -C "$1" --work-tree="$1" --git-dir="$1/.git" config --no-includes --get-regexp '^(core\\.(worktree|hookspath)|include\\..*|includeif\\..*)$')
  _rc=$?
  [ "$_rc" -eq 1 ] && [ -z "$_redirects" ]
}}
_rowless_inventory_git() {{
  GIT_OPTIONAL_LOCKS=0 git -c core.ignorecase=false -c core.filemode=true -c core.symlinks=true -c core.fsmonitor=false -c core.untrackedCache=false -c core.trustctime=true -c core.checkStat=default -C "$_inventory_root" --work-tree="$_inventory_root" --git-dir="$_inventory_root/.git" "$@"
}}
_rowless_inventory_safe() {{
  _inventory_root="$1"
  _inventory_reason=worktree_inventory
  # The dispatcher-owned marker is the sole runtime artifact exemption.
  _inventory_status=$(_rowless_inventory_git status --porcelain=v2 --ignored=matching --untracked-files=all -- . ':(top,exclude,literal).workbay-lane-sandbox') || return 1
  [ -z "$_inventory_status" ] || return 1
  _inventory_reason=extra_refs
  _head_ref=$(_rowless_inventory_git symbolic-ref -q HEAD) || return 1
  _refs=$(_rowless_inventory_git for-each-ref --format='%(refname)') || return 1
  # No host ref inventory is transferred: conservatively refuse every extra ref.
  [ "$_refs" = "$_head_ref" ] || return 1
  _inventory_reason=stash
  _stash=$(_rowless_inventory_git stash list) || return 1
  [ -z "$_stash" ] || return 1
  _inventory_reason=reachable_history
  [ "$(_rowless_inventory_git rev-parse --is-shallow-repository)" = false ] || return 1
  _inventory_reason=unreachable_objects
  _objects=$(_rowless_inventory_git fsck --no-progress --unreachable --no-reflogs 2>&1) || return 1
  [ -z "$_objects" ] || return 1
  _inventory_reason=reachable_history
  # Fail closed: tree equality cannot prove reverted implementation was harvested.
  _host_tip="$2"
  case "$_host_tip" in ''|*[!0-9a-f]*) return 1 ;; esac
  _rowless_inventory_git cat-file -e "$_host_tip^{{commit}}" 2>/dev/null || return 1
  _additional=$(_rowless_inventory_git rev-list HEAD --not "$_host_tip" --) || return 1
  [ -z "$_additional" ] || return 1
  _inventory_reason=index_diff
  _rowless_inventory_git diff --cached --quiet || return 1
  _inventory_reason=worktree_diff
  _rowless_inventory_git diff --quiet || return 1
}}
_rowless_index_safe() {{
  # RV4: status hides skip-worktree (S) and assume-unchanged (lowercase)
  # entries. Refuse the proof even if their visible contents look unchanged.
  _index_entries=$(git -C "$1" --work-tree="$1" --git-dir="$1/.git" ls-files -v 2>/dev/null) || return 1
  while IFS= read -r _index_entry; do
    case "$_index_entry" in
      [a-zS]' '*) return 1 ;;
    esac
  done <<EOF
$_index_entries
EOF
  return 0
}}
_rowless_content_safe() (
  # RV9 / CARD-15: read every HEAD blob; stat-cache equality is not salvage proof.
  # RV10: compare raw bytes; sandbox clean filters can hide edits from status.
  set -o pipefail
  _rowless_worktree_safe "$1" || exit 1
  cd "$1" || exit 1
  git --work-tree="$PWD" --git-dir="$PWD/.git" ls-tree -r -z HEAD | (
    while IFS= read -r -d '' _entry; do
      _meta="${{_entry%%$'\\t'*}}"
      _path="${{_entry#*$'\\t'}}"
      read -r _mode _type _expected <<< "$_meta"
      [ "$_type" = blob ] || exit 1
      case "$_mode" in
        100644|100755)
          [ -f "$_path" ] && [ ! -L "$_path" ] || exit 1
          _actual=$(git --work-tree="$PWD" --git-dir="$PWD/.git" hash-object --no-filters -- "$_path") || exit 1 ;;
        120000)
          [ -L "$_path" ] || exit 1
          _actual=$(readlink -n -- "$_path" | git --work-tree="$PWD" --git-dir="$PWD/.git" hash-object --stdin) || exit 1 ;;
        *) exit 1 ;;
      esac
      [ "$_actual" = "$_expected" ] || exit 1
    done
  )
)
"""


def _build_list_script(root: str) -> str:
    helpers = _remote_helpers(root)
    return f"""{_HEADER_LIST}
set -eu
{helpers}
for _sd in "$ROOT"/*/; do
  [ -d "$_sd" ] || continue
  _sd="${{_sd%/}}"
  if [ ! -f "$_sd/.workbay-lane-sandbox" ]; then
    continue
  fi
  _sk="${{_sd##*/}}"
  _tip=MISSING
  if _git_admin_shape_safe "$_sd" && _rev=$(git -C "$_sd" --work-tree="$_sd" --git-dir="$_sd/.git" rev-parse HEAD 2>/dev/null); then
    _tip="$_rev"
  fi
  _occupant=idle
  if _lane_occupant_live "$_sk" || _pid_in_sandbox "$_sd"; then
    _occupant=live
  fi
  _lock=free
  if _lane_lock_held "$_sk"; then
    _lock=held
  fi
  _marker_mtime=$(stat -c %Y "$_sd/.workbay-lane-sandbox" 2>/dev/null || printf '0')
  case "$_marker_mtime" in ''|*[!0-9]*) _marker_mtime=0 ;; esac
  _now=$(date +%s)
  _idle=$((_now - _marker_mtime))
  [ "$_idle" -ge 0 ] || _idle=0
  _bytes=$(du -sb "$_sd" 2>/dev/null | awk '{{print $1}}' || printf '0')
  case "$_bytes" in ''|*[!0-9]*) _bytes=0 ;; esac
  printf 'SANDBOX %s tip=%s occupant=%s lock=%s idle=%s bytes=%s\\n' \\
    "$_sk" "$_tip" "$_occupant" "$_lock" "$_idle" "$_bytes"
done
"""


def _build_rowless_probe_script(root: str, key: str, *, host_tip: str = "") -> str:
    quoted_key = _sh_single_quote(key)
    return f"""{_HEADER_PROBE_PREFIX}{key}
set -eu
{_remote_helpers(root)}
_sk={quoted_key}
_sd="$ROOT/$_sk"
exec 9>>"$ROOT/.lane-lock-$_sk" || {{ echo PROBE_FAILED; exit 0; }}
flock -n 9 || {{ echo PROBE_FAILED; exit 0; }}
if [ ! -f "$_sd/.workbay-lane-sandbox" ]; then
  echo PROBE_FAILED
  exit 0
fi
_generation=$(_sandbox_generation "$_sd") || {{ echo PROBE_FAILED; exit 0; }}
_rowless_worktree_safe "$_sd" || {{ echo "PROBE_FAILED $_worktree_reason"; exit 0; }}
_rowless_index_safe "$_sd" || {{ echo "DIRTY index_flags"; exit 0; }}
_rowless_content_safe "$_sd" || {{ echo "DIRTY raw_content"; exit 0; }}
_rowless_inventory_safe "$_sd" {_sh_single_quote(host_tip)} || {{ echo "DIRTY $_inventory_reason"; exit 0; }}
_tree=$(git -C "$_sd" --work-tree="$_sd" --git-dir="$_sd/.git" rev-parse 'HEAD^{{tree}}' 2>/dev/null) || {{ echo PROBE_FAILED; exit 0; }}
case "$_tree" in
  ''|*[!0-9a-f]*) echo PROBE_FAILED; exit 0 ;;
esac
printf 'CLEAN\nTREE %s\nGENERATION %s\n' "$_tree" "$_generation"
git -C "$_sd" --work-tree="$_sd" --git-dir="$_sd/.git" ls-tree -r --full-tree "$_tree"
"""


def _build_reap_script(
    root: str,
    keys: Sequence[str],
    *,
    rowless_proofs: Mapping[str, _RowlessProof] | None = None,
    idle_seconds: int = DEFAULT_IDLE_SECONDS,
) -> str:
    helpers = _remote_helpers(root)
    header = _HEADER_REAP_PREFIX + ",".join(keys)
    calls_list: list[str] = []
    for key in keys:
        proof = (rowless_proofs or {}).get(key)
        args = [key, proof.tree if proof else "", proof.generation if proof else "", proof.host_tip if proof else ""]
        calls_list.append("_reap_one " + " ".join(_sh_single_quote(arg) for arg in args))
    calls = "\n".join(calls_list)
    return f"""{header}
set -eu
{helpers}
exec 8>>"$ROOT/.reap.lock" || {{ echo LOCK_HELD; exit 0; }}
if ! flock -n 8; then
  echo LOCK_HELD
  exit 0
fi
_reap_one() {{
  _sk="$1"
  _proven_tree="$2"
  _proven_generation="$3"
  _proven_host_tip="$4"
  _sd="$ROOT/$_sk"
  _llf="$ROOT/.lane-lock-$_sk"
  (
    flock -n 9 || {{ echo "SKIPPED_LOCK $_sk"; exit 0; }}
    if [ ! -f "$_sd/.workbay-lane-sandbox" ]; then
      echo "SKIPPED_UNMARKED $_sk"
      exit 0
    fi
    if _lane_occupant_live "$_sk" || _pid_in_sandbox "$_sd"; then
      echo "SKIPPED_LIVE $_sk"
      exit 0
    fi
    if [ -n "$_proven_tree" ]; then
      # CARD-15: retain the dispatch lock across proof revalidation and removal.
      _rowless_worktree_safe "$_sd" || {{ echo "SKIPPED_CHANGED $_sk $_worktree_reason"; exit 0; }}
      _rowless_index_safe "$_sd" || {{ echo "SKIPPED_CHANGED $_sk index_flags"; exit 0; }}
      _rowless_content_safe "$_sd" || {{ echo "SKIPPED_CHANGED $_sk raw_content"; exit 0; }}
      _rowless_inventory_safe "$_sd" "$_proven_host_tip" || {{ echo "SKIPPED_CHANGED $_sk $_inventory_reason"; exit 0; }}
      _generation=$(_sandbox_generation "$_sd") || {{ echo "SKIPPED_CHANGED $_sk"; exit 0; }}
      _tree=$(git -C "$_sd" --work-tree="$_sd" --git-dir="$_sd/.git" rev-parse 'HEAD^{{tree}}' 2>/dev/null) || {{ echo "SKIPPED_CHANGED $_sk"; exit 0; }}
      _mtime=$(stat -c %Y "$_sd/.workbay-lane-sandbox" 2>/dev/null) || {{ echo "SKIPPED_CHANGED $_sk"; exit 0; }}
      case "$_mtime" in ''|*[!0-9]*) echo "SKIPPED_CHANGED $_sk"; exit 0 ;; esac
      _now=$(date +%s) || {{ echo "SKIPPED_CHANGED $_sk"; exit 0; }}
      if [ "$_generation" != "$_proven_generation" ] || [ "$_tree" != "$_proven_tree" ] || \\
         [ "$((_now - _mtime))" -lt {max(0, int(idle_seconds))} ]; then
        echo "SKIPPED_CHANGED $_sk"
        exit 0
      fi
    fi
    rm -rf "$_sd" "$ROOT/.venv-lane-$_sk" "$ROOT/.venv-sync-stamp-$_sk" 2>/dev/null || true
    if [ ! -e "$_sd" ]; then
      echo "REAPED $_sk"
    else
      echo "SKIPPED_GONE_FAILED $_sk"
    fi
  ) 9>>"$_llf" || echo "SKIPPED_LOCK $_sk"
}}
{calls}
"""


def _record_keyed_decision(
    *,
    task_ref: str,
    lane_id: str,
    tip_sha: str | None,
    actor: Any,
    sandbox_key: str,
    reason: str,
) -> None:
    from workbay_handoff_mcp import record_decision  # noqa: PLC0415

    sha = tip_sha or "unknown"
    decision = f"remote_sandbox_reap:{task_ref}:{lane_id}:{sha}"
    rationale = f"Reaped remote VM sandbox {sandbox_key} for lane {lane_id}: {reason}."
    try:
        record_decision(
            session=f"remote-sandbox-reap:{task_ref}",
            decision=decision,
            rationale=rationale,
            actor=actor,
            task_ref=task_ref,
            decision_origin="system",
            refresh_rationale_on_conflict=True,
        )
    except Exception:  # noqa: BLE001 — ledger failure must not undo a completed reap
        return
