"""Harvest-driven reaper for finished remote VM lane sandboxes.

This is the row-aware orchestrator complement to the standalone VM sweep in
``scripts/remote_agent.sh`` (RES-07). Marker-owned sandboxes with a row require
a terminal status (``closed`` / ``merged`` / ``closed_stale``). A sandbox
without a row is retired when its key maps to a branch identity pinned under
``refs/reclaimed``; other known branches still require a bounded probe proving
the clean, idle sandbox's exported blobs equal that host branch tip. Retired
sandboxes must also meet the idle floor. A non-terminal row is never harvested.
Callers also pass ``exclude_keys`` for the invoking pass.
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
import json
import os
import re
import subprocess
import tarfile
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    "KIND_SANDBOX_RETIRED_OURS",
    "KIND_SANDBOX_ROWLESS_OURS",
    "KIND_SANDBOX_UNHARVESTED",
    "KIND_SANDBOX_UNMAPPED",
    "LANE_OCCUPANT_LIVE_FUNCTION",
    "MergedMarkerWriteResult",
    "RemoteSandboxReapReport",
    "SSH_TIMEOUT_S",
    "SandboxVerdict",
    "TERMINAL_ROW_STATUSES",
    "release_remote_lane_lease",
    "plan_remote_sandboxes",
    "reap_owned_probe_process",
    "reap_remote_sandboxes",
    "write_merged_marker",
)

KIND_SANDBOX_HARVESTED = "sandbox_harvested"
KIND_SANDBOX_FOREIGN = "sandbox_foreign"
KIND_SANDBOX_LIVE = "sandbox_live"
KIND_SANDBOX_RETIRED_OURS = "sandbox_retired_ours"
KIND_SANDBOX_ROWLESS_OURS = "sandbox_rowless_ours"
KIND_SANDBOX_UNMAPPED = "sandbox_unmapped"
KIND_SANDBOX_UNHARVESTED = "sandbox_unharvested"
KIND_LOCK_HELD = "lock_held"
KIND_PROBE_FAILED = "probe_failed"

TERMINAL_ROW_STATUSES = _TERMINAL_LANE_STATUSES

EVIDENCE_RETENTION_SECONDS = 7 * 24 * 60 * 60
EVIDENCE_REAP_CANDIDATE_LIMIT = 32
_REMOTE_PRODUCT_HISTORY_LIMIT = 256
_EVIDENCE_KEY_RE = re.compile(r"^attempt-evidence-[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}-[0-9]+-[0-9a-f]{16}$")
_EVIDENCE_LISTING_RE = re.compile(r"^EVIDENCE (?P<key>\S+) age=(?P<age>[0-9]+)(?: bytes=(?P<bytes>[0-9]+))?$")
_EVIDENCE_YOUNG_RE = re.compile(r"^EVIDENCE_YOUNG count=(?P<count>[0-9]+)$")
_EVIDENCE_TRUNCATED_RE = re.compile(r"^EVIDENCE_TRUNCATED remaining=(?P<remaining>[0-9]+)$")

SSH_TIMEOUT_S = 30.0
GIT_TIMEOUT_S = 5.0

_DEFAULT_AGENT_ROOT = "workbay-sandbox"
_DEFAULT_AGENT_ROOTS = ("workbay-sandbox", "grok-sandbox")
_SANDBOX_KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{8}$")
_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")
_LISTING_RE = re.compile(
    r"^SANDBOX (?P<key>\S+) tip=(?P<tip>\S+) occupant=(?P<occupant>live|idle) "
    r"lock=(?P<lock>held|free)(?: tree=(?P<tree>\S+))?"
    r"(?: idle=(?P<idle>[0-9]+))?(?: bytes=(?P<bytes>[0-9]+))?$"
)
_REAP_LINE_RE = re.compile(
    r"^(?P<status>REAPED|SKIPPED_LIVE|SKIPPED_LOCK|SKIPPED_GONE_FAILED|SKIPPED_UNMARKED|SKIPPED_CHANGED) (?P<key>\S+)(?: (?P<detail>[a-z_]+))?$"
)
_HEADER_LIST = "# WORKBAY_REMOTE_SANDBOX_REAP op=list"
_HEADER_PROBE_PREFIX = "# WORKBAY_REMOTE_SANDBOX_REAP op=probe key="
_HEADER_REAP_PREFIX = "# WORKBAY_REMOTE_SANDBOX_REAP op=reap keys="
DEFAULT_IDLE_SECONDS = 21_600

LANE_OCCUPANT_LIVE_FUNCTION = """_lane_occupant_live() {
  _lk="${1:-}"
  [ -n "$_lk" ] || return 0
  _lf="$ROOT/.lane-live-$_lk"
  [ -f "$_lf" ] || return 1
  _expiry=
  _issued=
  _released=
  while IFS= read -r _lline || [ -n "$_lline" ]; do
    case "$_lline" in
      expiry=*) _expiry="${_lline#expiry=}" ;;
      issued=*) _issued="${_lline#issued=}" ;;
      released=*) _released="${_lline#released=}" ;;
    esac
  done < "$_lf" || return 0
  case "$_expiry" in
    ''|*[!0-9]*) return 0 ;;
  esac
  case "$_issued" in
    ''|*[!0-9]*) return 0 ;;
  esac
  case "$_released" in
    1) return 1 ;;
    ''|0) ;;
    *) return 0 ;;
  esac
  _now=$(date +%s)
  if [ "$_now" -ge "$_expiry" ]; then
    return 1
  fi
  return 0
}"""

SshRunner = Callable[..., Any]
RefLister = Callable[[Path], Sequence[str]]
TreeEntries = dict[str, tuple[str, str, str]]


@dataclass(frozen=True)
class _RowlessProof:
    tree: str
    generation: str
    host_tip: str = ""
    mapped_product: bool = False


@dataclass(frozen=True)
class SandboxVerdict:
    sandbox_key: str
    lane_id: str | None
    branch: str | None
    kind: str
    tip_sha: str | None
    reason: str


@dataclass(frozen=True)
class EvidenceVerdict:
    key: str
    status: str
    reason: str = ""


@dataclass(frozen=True)
class _ListedEvidence:
    key: str
    age_seconds: int
    bytes_used: int


@dataclass
class RemoteSandboxReapReport:
    evidence: list[EvidenceVerdict] = field(default_factory=list)
    evidence_skipped_young: int = 0
    evidence_truncated: bool = False
    evidence_candidates_remaining: int = 0
    verdicts: list[SandboxVerdict] = field(default_factory=list)
    reaped: list[str] = field(default_factory=list)
    dry_run: bool = True
    probe_error: str | None = None
    skipped: bool = False
    skip_reason: str | None = None
    gauge: dict[str, int | None] = field(
        default_factory=lambda: {
            "sandbox_count": 0,
            "sandbox_bytes": 0,
            "oldest_idle_seconds": 0,
            "foreign_sandbox_count": 0,
            "foreign_sandbox_bytes": 0,
            "retired_ours_count": 0,
            "retired_ours_bytes": 0,
            "rowless_ours_count": 0,
        }
    )


@dataclass(frozen=True)
class MergedMarkerWriteResult:
    """Typed result for the best-effort VM merged-marker write."""

    ok: bool
    written: tuple[str, ...] = ()
    error: str | None = None


@dataclass(frozen=True)
class _ListedSandbox:
    key: str
    tip_sha: str | None
    tree_sha: str | None
    occupant_live: bool
    lock_held: bool
    idle_seconds: int
    bytes_used: int | None


class _RemoteSandboxListing(list[_ListedSandbox | _ListedEvidence]):
    def __init__(self) -> None:
        super().__init__()
        self.evidence_skipped_young = 0
        self.evidence_truncated = False
        self.evidence_candidates_remaining = 0


@dataclass(frozen=True)
class _MappedRow:
    lane_id: str | None
    branch: str | None
    status: str | None
    branch_tip_sha: str | None = None
    landing_commit_sha: str | None = None


@dataclass(frozen=True)
class _AmbiguousRows:
    rows: tuple[_MappedRow, ...]


@dataclass(frozen=True)
class _ReclaimedPin:
    branch: str
    commit_sha: str | None
    error: str | None = None


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
    target_keys: Sequence[str] | None = None,
    target_tip_sha: str | None = None,
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
        target_keys=target_keys,
        target_tip_sha=target_tip_sha,
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
    target_keys: Sequence[str] | None = None,
    target_tip_sha: str | None = None,
) -> RemoteSandboxReapReport:
    """Reap harvested marker-gated remote VM sandboxes.

    Only ``sandbox_harvested`` and ``sandbox_retired_ours`` keys are sent to
    the remote delete script.
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
        target_keys=target_keys,
        target_tip_sha=target_tip_sha,
    )


def write_merged_marker(
    branches: Sequence[str | Mapping[str, Any] | Any] | str,
    *,
    repo_root: Path | str | None = None,
    runner: SshRunner | None = None,
) -> MergedMarkerWriteResult:
    """Atomically mark landed branches in the remote VM's ``.merged`` directory.

    ``branches`` may contain branch names or row/receipt objects with a
    ``branch`` field and optional ``receipt_sha`` / ``landed_at`` fields.
    ``landing_commit_sha`` and ``branch_tip_sha`` are accepted receipt-SHA
    aliases. One bounded SSH writes each marker to a uniquely created sibling
    temporary file and atomically renames it to
    ``$ROOT/.merged/<derived-key>.json``. The default SSH runner resolves its
    host from the explicit ``repo_root``; it never guesses from the caller's
    current directory. Inject ``runner`` at the SSH boundary in tests and
    callers; failures return a typed result and never undo a completed close.
    """
    try:
        items: Sequence[Any] = [branches] if isinstance(branches, str) else list(branches)
        if not items:
            return MergedMarkerWriteResult(ok=True)
        landed_at_default = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        markers: list[tuple[str, dict[str, str]]] = []
        seen: dict[str, str] = {}
        for item in items:
            branch = item if isinstance(item, str) else _row_text(item, "branch")
            if not isinstance(branch, str) or not branch.strip() or "\n" in branch or "\r" in branch:
                return MergedMarkerWriteResult(ok=False, error="branch is empty or malformed")
            branch = branch.strip()
            receipt_sha = ""
            landed_at = landed_at_default
            if not isinstance(item, str):
                receipt_sha = (
                    _row_text(item, "receipt_sha")
                    or _row_text(item, "landing_commit_sha")
                    or _row_text(item, "branch_tip_sha")
                    or ""
                )
                landed_at = _row_text(item, "landed_at") or landed_at_default
            if receipt_sha and _SHA_RE.fullmatch(receipt_sha) is None:
                return MergedMarkerWriteResult(ok=False, error=f"malformed receipt SHA for {branch!r}")
            key = _derive_lane_key(branch)
            previous_branch = seen.get(key)
            if previous_branch is not None:
                if previous_branch != branch:
                    return MergedMarkerWriteResult(ok=False, error="merged marker key collision")
                continue
            seen[key] = branch
            markers.append((key, {"branch": branch, "receipt_sha": receipt_sha, "landed_at": landed_at}))

        root = _resolve_sandbox_root(None)
        if root is None:
            return MergedMarkerWriteResult(ok=False, error="sandbox_root is empty or unsafe")
        workspace_root = Path(repo_root).expanduser() if repo_root is not None else None
        script_lines = [
            "# WORKBAY_REMOTE_MERGED_MARKER op=write",
            f"ROOT={_sh_single_quote(root)}",
            'case "$ROOT" in /*) ;; *) ROOT="$HOME/$ROOT" ;; esac',
            "set -eu",
            "umask 077",
            "_tmp=",
            'trap \'if [ -n "$_tmp" ]; then rm -f -- "$_tmp"; fi\' EXIT HUP INT TERM',
            'mkdir -p -- "$ROOT/.merged"',
        ]
        for index, (key, body) in enumerate(markers):
            delimiter = f"WORKBAY_MERGED_MARKER_JSON_{index}"
            script_lines.extend(
                (
                    f'_tmp=$(mktemp "$ROOT/.merged/{key}.json.tmp.XXXXXX")',
                    f'_marker="$ROOT/.merged/{key}.json"',
                    f"cat > \"$_tmp\" <<'{delimiter}'",
                    json.dumps(body, separators=(",", ":"), ensure_ascii=True),
                    delimiter,
                    'mv -f -- "$_tmp" "$_marker"',
                    f"printf 'WROTE %s\\n' '{key}'",
                )
            )
        script = "\n".join(script_lines)
        if runner is None:
            if workspace_root is None:
                return MergedMarkerWriteResult(ok=False, error="repo_root is required to resolve the remote gate host")
            if os.environ.get("PYTEST_CURRENT_TEST"):
                return MergedMarkerWriteResult(ok=False, error="default merged marker writer is forbidden under pytest")
            from workbay_orchestrator_mcp.orchestration.lane_census import (  # noqa: PLC0415
                _default_sandbox_ssh_runner,
            )

            raw = _default_sandbox_ssh_runner(script, timeout=SSH_TIMEOUT_S, workspace_root=workspace_root)
        else:
            raw = runner(script, timeout=SSH_TIMEOUT_S)
        stdout, stderr, returncode = _coerce_ssh_result(raw)
        written = tuple(
            line.removeprefix("WROTE ")
            for line in stdout.splitlines()
            if line.startswith("WROTE ") and line.removeprefix("WROTE ") in seen
        )
        if returncode not in (0, None):
            detail = (stderr or stdout or f"exit {returncode}").strip()
            return MergedMarkerWriteResult(
                ok=False,
                written=written,
                error=f"ssh exited {returncode}: {detail[:300]}",
            )
        expected = tuple(key for key, _body in markers)
        if written != expected:
            return MergedMarkerWriteResult(ok=False, written=written, error="merged marker write receipt mismatch")
        return MergedMarkerWriteResult(ok=True, written=written)
    except (TimeoutError, subprocess.TimeoutExpired) as exc:
        return MergedMarkerWriteResult(ok=False, error=f"ssh timed out: {exc}"[:300])
    except OSError as exc:
        return MergedMarkerWriteResult(ok=False, error=f"ssh failed: {exc}"[:300])
    except Exception as exc:  # noqa: BLE001 — lifecycle close must not be undone by the VM hook
        return MergedMarkerWriteResult(ok=False, error=f"{type(exc).__name__}: {exc}"[:300])


def reap_remote_evidence(
    *, ssh_runner: SshRunner, dry_run: bool = True, sandbox_root: str | None = None
) -> RemoteSandboxReapReport:
    """Evidence-only maintenance using the same listing and retention policy."""
    report = RemoteSandboxReapReport(dry_run=dry_run)
    root = _resolve_sandbox_root(sandbox_root)
    if root is None:
        return _fail(report, "sandbox_root is empty or unsafe")
    listed, error = _list_remote_sandboxes(ssh_runner, root, timeout=None)
    if error:
        return _fail(report, error)
    report.evidence_truncated = listed.evidence_truncated
    report.evidence_candidates_remaining = listed.evidence_candidates_remaining
    report.evidence_skipped_young = listed.evidence_skipped_young
    report.evidence = _purge_evidence(
        [item for item in listed if isinstance(item, _ListedEvidence)],
        ssh_runner,
        root,
        dry_run=dry_run,
        timeout=None,
    )
    return report


def _purge_evidence(
    evidence: Sequence[_ListedEvidence],
    ssh_runner: SshRunner,
    root: str,
    *,
    dry_run: bool,
    timeout: float | None = SSH_TIMEOUT_S,
) -> list[EvidenceVerdict]:
    verdicts: list[EvidenceVerdict] = []
    for item in evidence:
        if not _EVIDENCE_KEY_RE.fullmatch(item.key):
            verdict = EvidenceVerdict(item.key, "skipped_unsafe", "malformed evidence name")
        elif item.age_seconds < EVIDENCE_RETENTION_SECONDS:
            verdict = EvidenceVerdict(item.key, "skipped_young")
        elif dry_run:
            verdict = EvidenceVerdict(item.key, "would_reap")
        else:
            output, error = _run_ssh(ssh_runner, _build_evidence_reap_script(root, item.key), timeout=timeout)
            status = output.strip()
            if error or status not in {"reaped", "skipped_young", "skipped_unsafe"}:
                verdict = EvidenceVerdict(
                    item.key, "skipped_unsafe", error or f"unexpected evidence reap response: {output!r}"
                )
            else:
                verdict = EvidenceVerdict(item.key, status)
        verdicts.append(verdict)
    return verdicts


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
    target_keys: Sequence[str] | None,
    target_tip_sha: str | None,
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
    report.evidence_truncated = listed.evidence_truncated
    report.evidence_candidates_remaining = listed.evidence_candidates_remaining
    report.evidence_skipped_young = listed.evidence_skipped_young
    evidence = [item for item in listed if isinstance(item, _ListedEvidence)]
    listed = [item for item in listed if isinstance(item, _ListedSandbox)]
    scoped_keys = frozenset(str(key) for key in (target_keys or ()) if str(key))
    if target_keys is not None and any(not _SANDBOX_KEY_RE.fullmatch(key) for key in scoped_keys):
        return _fail(report, "refusing to reap unsafe target sandbox key")
    if target_keys is None:
        report.evidence = _purge_evidence(evidence, ssh_runner, root, dry_run=dry_run)
        actionable = listed
    else:
        # A landing cleanup must not reap unrelated attempt evidence or walk
        # sibling sandboxes as orphans. Full rows still participate in the
        # ownership map below; only matching keys are actionable.
        actionable = [item for item in listed if item.key in scoped_keys]
    if not actionable:
        return report
    mapped_rows = _index_rows(rows, task_ref=scoped)
    ref_branches: list[str] = []
    retired_pins: list[_ReclaimedPin] = []
    if any(_map_key(item.key, mapped_rows) is None for item in actionable):
        try:
            ref_branches = list((ref_lister or _list_repo_branches)(repo))
        except Exception as exc:  # noqa: BLE001 — ownership ambiguity must fail closed
            return _fail(report, f"known branch listing failed: {exc}")
        try:
            retired_pins = _list_repo_reclaimed_pins(repo)
        except Exception as exc:  # noqa: BLE001 — ownership ambiguity must fail closed
            return _fail(report, f"reclaimed branch listing failed: {exc}")
    known_keys = _known_branch_keys(repo, rows, ref_lister=lambda _repo: ref_branches)
    known_branches = _known_branches(rows, ref_branches)
    branches_by_key: dict[str, list[str]] = {}
    for branch in known_branches:
        branches_by_key.setdefault(_derive_lane_key(_branch_identity(branch)), []).append(branch)
    retired_branches_by_key: dict[str, list[str]] = {}
    retired_pins_by_branch: dict[str, _ReclaimedPin] = {}
    live_branch_identities = {_branch_identity(branch) for branch in ref_branches}
    for pin in retired_pins:
        branch = pin.branch
        if _branch_identity(branch) in live_branch_identities:
            continue
        retired_pins_by_branch.setdefault(branch, pin)
        retired_branches_by_key.setdefault(_derive_lane_key(branch), []).append(branch)

    foreign_keys: set[str] = set()
    rowless_ours_keys: set[str] = set()
    for item in actionable:
        if _map_key(item.key, mapped_rows) is not None:
            continue
        if item.key in retired_branches_by_key:
            continue
        if item.key in known_keys:
            rowless_ours_keys.add(item.key)
        else:
            foreign_keys.add(item.key)

    harvested_keys: list[str] = []
    rowless_proofs: dict[str, _RowlessProof] = {}
    for item in actionable:
        branch_candidates = branches_by_key.get(item.key, [])
        verdict = _classify(
            item,
            mapped_rows,
            repo,
            ssh_runner=ssh_runner,
            sandbox_root=root,
            rowless_branch=branch_candidates[0] if len(branch_candidates) == 1 else None,
            rowless_ours=item.key in known_keys,
            retired_branches=(
                retired_branches_by_key.get(item.key, ()) if _map_key(item.key, mapped_rows) is None else ()
            ),
            retired_pin=(
                retired_pins_by_branch.get(retired_branches_by_key[item.key][0])
                if _map_key(item.key, mapped_rows) is None and len(retired_branches_by_key.get(item.key, ())) == 1
                else None
            ),
            exclude_keys=excluded,
            idle_seconds=max(0, int(idle_seconds)),
            rowless_proofs=rowless_proofs,
            target_tip_sha=target_tip_sha,
        )
        report.verdicts.append(verdict)
        if verdict.kind in {KIND_SANDBOX_HARVESTED, KIND_SANDBOX_RETIRED_OURS}:
            harvested_keys.append(item.key)

    retired_key_set = {verdict.sandbox_key for verdict in report.verdicts if verdict.kind == KIND_SANDBOX_RETIRED_OURS}
    for item in listed:
        mapped = _map_key(item.key, mapped_rows)
        if isinstance(mapped, _MappedRow) and (mapped.status or "").strip().lower() in TERMINAL_ROW_STATUSES:
            retired_key_set.add(item.key)
    retired_ours_keys = frozenset(retired_key_set)
    report.gauge = _gauge_for(
        listed if target_keys is None else actionable,
        foreign_keys=frozenset(foreign_keys),
        retired_ours_keys=retired_ours_keys,
        rowless_ours_keys=frozenset(rowless_ours_keys),
    )

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
                kind=(
                    KIND_PROBE_FAILED
                    if verdict.kind in {KIND_SANDBOX_HARVESTED, KIND_SANDBOX_RETIRED_OURS}
                    else verdict.kind
                ),
                tip_sha=verdict.tip_sha,
                reason=(
                    reap_error
                    if verdict.kind in {KIND_SANDBOX_HARVESTED, KIND_SANDBOX_RETIRED_OURS}
                    else verdict.reason
                ),
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
                kind=(
                    KIND_LOCK_HELD
                    if verdict.kind in {KIND_SANDBOX_HARVESTED, KIND_SANDBOX_RETIRED_OURS}
                    else verdict.kind
                ),
                tip_sha=verdict.tip_sha,
                reason=(
                    "remote .reap.lock is held"
                    if verdict.kind in {KIND_SANDBOX_HARVESTED, KIND_SANDBOX_RETIRED_OURS}
                    else verdict.reason
                ),
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
        retired_ours_keys=frozenset(
            verdict.sandbox_key for verdict in report.verdicts if verdict.kind == KIND_SANDBOX_RETIRED_OURS
        ),
        rowless_ours_keys=frozenset(rowless_ours_keys),
    )
    return report


def _gauge_for(
    items: Sequence[_ListedSandbox],
    *,
    foreign_keys: frozenset[str] = frozenset(),
    retired_ours_keys: frozenset[str] = frozenset(),
    rowless_ours_keys: frozenset[str] = frozenset(),
) -> dict[str, int | None]:
    """Return repository-owned pressure separately from foreign VM usage."""
    foreign = [item for item in items if item.key in foreign_keys]
    retired = [item for item in items if item.key in retired_ours_keys]

    def _byte_total(values: Sequence[_ListedSandbox]) -> int | None:
        if any(item.bytes_used is None for item in values):
            return None
        return sum(item.bytes_used or 0 for item in values)

    gauge = {
        "sandbox_count": len(items),
        "sandbox_bytes": _byte_total([item for item in items if item.key not in foreign_keys]),
        "oldest_idle_seconds": max((item.idle_seconds for item in items), default=0),
        "foreign_sandbox_count": len(foreign),
        "foreign_sandbox_bytes": _byte_total(foreign),
        "rowless_ours_count": sum(item.key in rowless_ours_keys for item in items),
    }
    if retired:
        gauge["retired_ours_count"] = len(retired)
        gauge["retired_ours_bytes"] = _byte_total(retired)
    return gauge


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
            if verdict.kind in {KIND_SANDBOX_HARVESTED, KIND_SANDBOX_RETIRED_OURS}
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


def reap_remote_lane_sandbox(
    task_ref: str,
    *,
    lane_id: str,
    branch: str,
    expected_tip: str,
    primary_repo: Path | str,
    rows: Sequence[Mapping[str, Any] | Any],
    ssh_runner: SshRunner | None = None,
    sandbox_roots: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Reap one lane key, using every registry row to preserve ownership."""
    key = _derive_lane_key(_branch_identity(branch))
    if not _SANDBOX_KEY_RE.fullmatch(key):
        return {"ok": False, "status": "pending", "reason": "remote_lane_key_invalid", "sandbox_key": key}
    if sandbox_roots is None:
        configured = os.environ.get("WORKBAY_REMOTE_AGENT_ROOT")
        roots = list(
            dict.fromkeys(
                ([configured.strip()] if configured and configured.strip() else []) + list(_DEFAULT_AGENT_ROOTS)
            )
        )
    else:
        roots = list(dict.fromkeys(item.strip() for item in sandbox_roots if isinstance(item, str) and item.strip()))
    if not roots:
        roots = list(_DEFAULT_AGENT_ROOTS)
    run_remote = ssh_runner
    if run_remote is None:

        def run_remote(script: str, *, timeout: float) -> Any:
            from workbay_orchestrator_mcp.orchestration.lane_census import (  # noqa: PLC0415
                _default_sandbox_ssh_runner,
            )

            return _default_sandbox_ssh_runner(script, timeout=timeout, workspace_root=primary_repo)

    attempted: list[str] = []
    for root in roots:
        attempted.append(root)
        report = reap_remote_sandboxes(
            task_ref,
            ssh_runner=run_remote,
            primary_repo=primary_repo,
            rows=rows,
            sandbox_root=root,
            idle_seconds=0,
            target_keys=[key],
            target_tip_sha=expected_tip,
        )
        if report.probe_error:
            return {
                "ok": False,
                "status": "pending",
                "reason": "remote_probe_failed",
                "detail": report.probe_error,
                "sandbox_key": key,
                "sandbox_root": root,
                "attempted_roots": attempted,
            }
        verdict = next((item for item in report.verdicts if item.sandbox_key == key), None)
        if verdict is None:
            # A successful complete listing proves this root has no target.
            continue
        if key in report.reaped:
            continue
        return {
            "ok": False,
            "status": "pending",
            "reason": verdict.kind,
            "detail": verdict.reason,
            "sandbox_key": key,
            "sandbox_root": root,
            "attempted_roots": attempted,
        }
    return {
        "ok": True,
        "status": "complete",
        "reason": "reaped_or_absent",
        "sandbox_key": key,
        "attempted_roots": attempted,
    }


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
            branch_tip_sha=_row_text(row, "branch_tip_sha"),
            landing_commit_sha=_row_text(row, "landing_commit_sha"),
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


def _list_repo_reclaimed_pins(primary_repo: Path) -> list[_ReclaimedPin]:
    """Return branch identities and resolvable commits encoded by reclaimed refs.

    Landed feature branches use refs/reclaimed/<prefix>-<lane>; older pins may
    preserve the branch path directly as refs/reclaimed/feature/<name>. A
    lane-id/SHA pin does not encode its branch and is deliberately ignored.
    """
    try:
        proc = subprocess.run(
            [
                "git",
                "-C",
                str(primary_repo),
                "for-each-ref",
                "--format=%(refname)",
                "refs/reclaimed",
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

    identities: list[_ReclaimedPin] = []
    seen: set[str] = set()
    for line in proc.stdout.splitlines():
        ref = line.strip()
        prefix = "refs/reclaimed/"
        if not ref.startswith(prefix):
            continue
        pin_name = ref.removeprefix(prefix)
        if pin_name.startswith("feature/"):
            branch = pin_name
        elif "/" not in pin_name and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*-[A-Za-z0-9][A-Za-z0-9._-]*", pin_name):
            branch = f"feature/{pin_name}"
        else:
            continue
        if branch in seen:
            continue
        seen.add(branch)
        commit_output, commit_error = _run_local_git(
            primary_repo, "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"
        )
        commit_sha = commit_output.strip().lower() if commit_error is None else ""
        if re.fullmatch(r"[0-9a-f]{40}", commit_sha) is None:
            identities.append(
                _ReclaimedPin(
                    branch=branch,
                    commit_sha=None,
                    error=commit_error or "reclaimed ref did not resolve to a commit",
                )
            )
        else:
            identities.append(_ReclaimedPin(branch=branch, commit_sha=commit_sha))
    return identities


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
    retired_branches: Sequence[str] = (),
    retired_pin: _ReclaimedPin | None = None,
    exclude_keys: frozenset[str] = frozenset(),
    idle_seconds: int = DEFAULT_IDLE_SECONDS,
    rowless_proofs: dict[str, _RowlessProof] | None = None,
    target_tip_sha: str | None = None,
) -> SandboxVerdict:
    mapped = _map_key(item.key, indexed_rows)
    if mapped is None and not rowless_ours and not retired_branches:
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
        alias = _terminal_alias_product(mapped.rows, item, repo, target_tip_sha)
        if alias is not None:
            branch, tip = alias
            raw, error = _run_ssh(
                ssh_runner, _build_rowless_probe_script(sandbox_root, item.key, host_tip=tip, mapped_product=True)
            )
            detail = error
            if error is None:
                _entries, proof, clean, parse_error = _parse_rowless_probe(raw)
                detail = parse_error or ("remote product tree changed" if clean else raw.strip())
                if clean and parse_error is None and proof is not None and proof.tree == item.tree_sha:
                    if rowless_proofs is not None:
                        rowless_proofs[item.key] = _RowlessProof(proof.tree, proof.generation, tip, mapped_product=True)
                    return _rowless_verdict(
                        item, branch, KIND_SANDBOX_HARVESTED, "terminal aliases share preserved host landing ancestry"
                    )
            return _rowless_verdict(
                item,
                branch,
                KIND_SANDBOX_UNHARVESTED,
                f"terminal alias remote proof failed: {detail or 'missing proof'}",
            )
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
        if len(retired_branches) > 1:
            return SandboxVerdict(
                sandbox_key=item.key,
                lane_id=None,
                branch=None,
                kind=KIND_SANDBOX_UNHARVESTED,
                tip_sha=item.tip_sha,
                reason=f"ambiguous reclaimed branch identity across {len(retired_branches)} branches",
            )
        if retired_branches:
            branch = retired_branches[0]
            if item.idle_seconds < idle_seconds:
                return SandboxVerdict(
                    sandbox_key=item.key,
                    lane_id=None,
                    branch=branch,
                    kind=KIND_SANDBOX_UNHARVESTED,
                    tip_sha=item.tip_sha,
                    reason=f"retired sandbox idle {item.idle_seconds}s below threshold {idle_seconds}s",
                )
            if retired_pin is None:
                return SandboxVerdict(
                    sandbox_key=item.key,
                    lane_id=None,
                    branch=branch,
                    kind=KIND_SANDBOX_UNHARVESTED,
                    tip_sha=item.tip_sha,
                    reason="reclaimed pin proof unavailable",
                )
            if retired_pin.commit_sha is None:
                return SandboxVerdict(
                    sandbox_key=item.key,
                    lane_id=None,
                    branch=branch,
                    kind=KIND_SANDBOX_UNHARVESTED,
                    tip_sha=item.tip_sha,
                    reason=f"reclaimed pin is unresolvable: {retired_pin.error or 'commit lookup failed'}",
                )
            return _classify_reclaimed_pin(
                item,
                branch,
                retired_pin.commit_sha,
                repo,
                ssh_runner=ssh_runner,
                sandbox_root=sandbox_root,
                rowless_proofs=rowless_proofs,
            )
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
    if target_tip_sha is not None:
        preserved, reason = _remote_lane_product_preserved(
            item,
            repo,
            expected_tip=target_tip_sha,
            landing_commit=row.landing_commit_sha,
        )
        if not preserved:
            return SandboxVerdict(
                sandbox_key=item.key,
                lane_id=row.lane_id,
                branch=row.branch,
                kind=KIND_SANDBOX_UNHARVESTED,
                tip_sha=item.tip_sha,
                reason=reason,
            )
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


def _classify_reclaimed_pin(
    item: _ListedSandbox,
    branch: str,
    pin_sha: str,
    repo: Path,
    *,
    ssh_runner: SshRunner,
    sandbox_root: str,
    rowless_proofs: dict[str, _RowlessProof] | None = None,
) -> SandboxVerdict:
    if re.fullmatch(r"[0-9a-f]{40}", pin_sha) is None:
        return _rowless_verdict(
            item, branch, KIND_SANDBOX_UNHARVESTED, "reclaimed pin is unresolvable: malformed commit SHA"
        )
    try:
        raw, probe_error = _run_ssh(ssh_runner, _build_rowless_probe_script(sandbox_root, item.key, host_tip=pin_sha))
    except Exception as exc:  # noqa: BLE001 — failed content proof is never reap authority
        return _rowless_verdict(item, branch, KIND_SANDBOX_UNHARVESTED, f"reclaimed pin proof failed: {exc}")
    if probe_error is not None:
        return _rowless_verdict(item, branch, KIND_SANDBOX_UNHARVESTED, f"reclaimed pin proof failed: {probe_error}")
    _entries, proof, clean, parse_error = _parse_rowless_probe(raw)
    if parse_error is not None:
        return _rowless_verdict(item, branch, KIND_SANDBOX_UNHARVESTED, f"reclaimed pin proof failed: {parse_error}")
    if not clean:
        return _rowless_verdict(
            item,
            branch,
            KIND_SANDBOX_UNHARVESTED,
            f"reclaimed pin sandbox is dirty: {raw.strip() or 'clean proof missing'}",
        )
    if proof is None:
        return _rowless_verdict(item, branch, KIND_SANDBOX_UNHARVESTED, "reclaimed pin proof missing")
    if rowless_proofs is not None:
        rowless_proofs[item.key] = _RowlessProof(proof.tree, proof.generation, pin_sha)
    return SandboxVerdict(
        sandbox_key=item.key,
        lane_id=None,
        branch=branch,
        kind=KIND_SANDBOX_RETIRED_OURS,
        tip_sha=pin_sha,
        reason=f"reclaimed pin content proof passed at {pin_sha}",
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


def _terminal_alias_product(
    rows: Sequence[_MappedRow], item: _ListedSandbox, repo: Path, target_tip: str | None
) -> tuple[str, str] | None:
    """Require all aliases to belong to one landed history, without choosing a row."""
    branches = {row.branch for row in rows}
    if len(branches) != 1 or None in branches:
        return None
    if any((row.status or "").strip().lower() not in TERMINAL_ROW_STATUSES for row in rows):
        return None
    tips = {row.branch_tip_sha or "" for row in rows}
    if any(re.fullmatch(r"[0-9a-f]{40}", tip) is None for tip in tips):
        return None
    receipts = [row for row in rows if (row.status or "").strip().lower() == "merged"]
    if not receipts:
        return None
    # Every merged receipt must itself prove preservation; a good sibling receipt
    # cannot hide malformed or unlanded evidence on another alias.
    for row in receipts:
        if not _remote_lane_product_preserved(
            item, repo, expected_tip=row.branch_tip_sha or "", landing_commit=row.landing_commit_sha
        )[0]:
            return None
    products = {
        row.branch_tip_sha
        for row in receipts
        if all(_tip_is_ancestor(repo, tip, row.branch_tip_sha or "") for tip in tips)
        and (
            target_tip is None
            or _remote_lane_product_preserved(
                item, repo, expected_tip=target_tip, landing_commit=row.landing_commit_sha
            )[0]
        )
    }
    if len(products) != 1:
        return None
    return next(iter(branches)), next(iter(products))


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
    env = os.environ.copy()
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo), "merge-base", "--is-ancestor", tip_sha, branch],
            capture_output=True,
            text=True,
            check=False,
            timeout=GIT_TIMEOUT_S,
            stdin=subprocess.DEVNULL,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0


def _remote_lane_product_preserved(
    item: _ListedSandbox,
    repo: Path,
    *,
    expected_tip: str,
    landing_commit: str | None,
) -> tuple[bool, str]:
    """Prove a mapped remote sandbox's product is in the host landing history.

    History-stripped remote work can have a different commit SHA even when its
    complete tree is the same. The registered host lane tip must be an ancestor
    of the recorded landing commit, and the remote tree must match one of the
    bounded host-tip ancestry trees. A later feature integration may have a
    different final tree; that does not weaken the preserved-history proof.
    """
    if re.fullmatch(r"[0-9a-f]{40}", expected_tip) is None:
        return False, "registered host lane tip is malformed"
    if re.fullmatch(r"[0-9a-f]{40}", landing_commit or "") is None:
        return False, "host landing ancestry evidence is unavailable"
    if item.tip_sha is None:
        return False, "remote sandbox tip is unavailable"
    if not _tip_is_ancestor(repo, expected_tip, landing_commit or ""):
        return False, "registered host lane tip is not preserved in the landing history"
    if item.tip_sha == expected_tip:
        return True, "exact remote commit is preserved in host landing history"
    if item.tree_sha is None or re.fullmatch(r"[0-9a-f]{40}", item.tree_sha) is None:
        return False, "remapped remote commit has no verifiable tree"
    env = os.environ.copy()
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    try:
        host_history = subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "log",
                f"--max-count={_REMOTE_PRODUCT_HISTORY_LIMIT}",
                "--format=%T",
                "--end-of-options",
                expected_tip,
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=GIT_TIMEOUT_S,
            stdin=subprocess.DEVNULL,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False, "host lane ancestry tree probe failed"
    if host_history.returncode != 0:
        return False, "host lane ancestry trees are unavailable"
    preserved_trees = {
        tree.lower()
        for tree in host_history.stdout.splitlines()
        if re.fullmatch(r"[0-9a-f]{40}", tree.lower()) is not None
    }
    if item.tree_sha.lower() not in preserved_trees:
        return False, "remote sandbox tree is absent from preserved host lane history"
    return True, "remapped remote tree is preserved in host lane history"


def _list_remote_sandboxes(
    ssh_runner: SshRunner,
    root: str,
    *,
    timeout: float | None = SSH_TIMEOUT_S,
) -> tuple[_RemoteSandboxListing, str | None]:
    result, error = _run_ssh(ssh_runner, _build_list_script(root), timeout=timeout)
    if error is not None:
        return _RemoteSandboxListing(), error
    listed = _RemoteSandboxListing()
    for raw_line in result.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        evidence_match = _EVIDENCE_LISTING_RE.fullmatch(line)
        if evidence_match:
            listed.append(
                _ListedEvidence(
                    evidence_match["key"],
                    int(evidence_match["age"]),
                    int(evidence_match["bytes"] or 0),
                )
            )
            continue
        young_match = _EVIDENCE_YOUNG_RE.fullmatch(line)
        if young_match:
            listed.evidence_skipped_young = int(young_match["count"])
            continue
        truncated_match = _EVIDENCE_TRUNCATED_RE.fullmatch(line)
        if truncated_match:
            listed.evidence_truncated = True
            listed.evidence_candidates_remaining = int(truncated_match["remaining"])
            continue
        match = _LISTING_RE.match(line)
        if match is None:
            return [], f"unexpected listing line: {line!r}"
        key = match.group("key")
        if not _SANDBOX_KEY_RE.fullmatch(key):
            return [], f"unsafe sandbox key in listing: {key!r}"
        tip_raw = match.group("tip")
        tip_sha = tip_raw.lower() if _SHA_RE.fullmatch(tip_raw.lower()) else None
        tree_raw = match.group("tree")
        tree_sha = tree_raw.lower() if tree_raw and _SHA_RE.fullmatch(tree_raw.lower()) else None
        listed.append(
            _ListedSandbox(
                key=key,
                tip_sha=tip_sha,
                tree_sha=tree_sha,
                occupant_live=match.group("occupant") == "live",
                lock_held=match.group("lock") == "held",
                idle_seconds=int(match.group("idle") or 0),
                bytes_used=int(match.group("bytes")) if match.group("bytes") is not None else None,
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


def _run_ssh(ssh_runner: SshRunner, script: str, *, timeout: float | None = SSH_TIMEOUT_S) -> tuple[str, str | None]:
    try:
        raw = ssh_runner(script, timeout=timeout)
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
{LANE_OCCUPANT_LIVE_FUNCTION}
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
  _snapshot="${{2:-}}"
  if [ -n "$_snapshot" ] && [ -f "$_snapshot" ]; then
    while IFS= read -r _target || [ -n "$_target" ]; do
      case "$_target" in
        "$_sd"|"$_sd"/*) return 0 ;;
      esac
    done < "$_snapshot"
    return 1
  fi
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
  # Rowless inventory exempts only the dispatcher-owned marker.
  _inventory_status=$(_rowless_inventory_git status --porcelain=v2 --ignored=matching --untracked-files=all -- . ':(top,exclude,literal).workbay-lane-sandbox') || return 1
  if [ "${{3:-rowless}}" = mapped ]; then
    # remote_agent.sh provisions only this sibling venv symlink. Repository
    # ignores designate dependency/test caches; arbitrary ignored source is kept.
    while IFS= read -r _entry; do
      case "$_entry" in
        '') ;;
        '! .venv')
          [ -L "$1/.venv" ] &&
            [ "$(readlink -- "$1/.venv")" = "$ROOT/.venv-lane-$_sk" ] || return 1 ;;
        '! .pytest_cache/'|'! __pycache__/'|'! node_modules/') ;;
        '! '*/.pytest_cache/|'! '*/__pycache__/|'! '*/node_modules/) ;;
        *) return 1 ;;
      esac
    done <<< "$_inventory_status"
  else
    [ -z "$_inventory_status" ] || return 1
  fi
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
  if [ "${{3:-rowless}}" != mapped ]; then
    _host_tip="$2"
    case "$_host_tip" in ''|*[!0-9a-f]*) return 1 ;; esac
    _rowless_inventory_git cat-file -e "$_host_tip^{{commit}}" 2>/dev/null || return 1
    _additional=$(_rowless_inventory_git rev-list HEAD --not "$_host_tip" --) || return 1
    [ -z "$_additional" ] || return 1
  fi
  # Mapped products have already proved host landing ancestry by tree. The
  # translated host commit need not exist in the history-stripped remote repo.
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


_DISPATCH_NONCE_RE = re.compile(r"^[0-9]+-[0-9a-f]{16}$")


def _build_release_remote_lane_lease_script(root: str, lane_key: str, nonce: str) -> str:
    """Build a fenced, tombstoning remote lease-release probe."""
    if _SANDBOX_KEY_RE.fullmatch(lane_key) is None:
        raise ValueError("unsafe lane key")
    if _DISPATCH_NONCE_RE.fullmatch(nonce) is None:
        raise ValueError("unsafe dispatch nonce")
    resolved_root = _resolve_sandbox_root(root)
    if resolved_root is None:
        raise ValueError("unsafe sandbox root")
    quoted_root = _sh_single_quote(resolved_root)
    quoted_nonce = _sh_single_quote(nonce)
    return f"""set -eu
ROOT={quoted_root}
case \"$ROOT\" in
  /*) ;;
  *) ROOT=\"$HOME/$ROOT\" ;;
esac
_lf=\"$ROOT/.lane-live-{lane_key}\"
_emit_release() {{
  _status=\"$1\"
  _expiry=\"$2\"
  case \"$_expiry\" in
    ''|*[!0-9]*) _expiry=null ;;
  esac
  printf '{{\"status\":\"%s\",\"expiry_epoch\":%s}}\\n' \"$_status\" \"$_expiry\"
}}
if [ ! -f \"$_lf\" ]; then
  _emit_release not_found ''
  exit 0
fi
_expiry=$(sed -n 's/^expiry=//p' \"$_lf\" 2>/dev/null | head -n1)
_nonce=$(sed -n 's/^nonce=//p' \"$_lf\" 2>/dev/null | head -n1)
if [ \"$_nonce\" != {quoted_nonce} ]; then
  _emit_release fenced \"$_expiry\"
  exit 0
fi
_released=$(sed -n 's/^released=//p' \"$_lf\" 2>/dev/null | head -n1)
if [ \"$_released\" = 1 ]; then
  _emit_release already_released \"$_expiry\"
  exit 0
fi
_lock=\"$ROOT/.lane-lease-{lane_key}.lock\"
exec 8>>\"$_lock\"
flock -x 8
# Re-read the nonce after taking the lease mutation lock. A different nonce
# must never be replaced by this release operation.
_nonce=$(sed -n 's/^nonce=//p' \"$_lf\" 2>/dev/null | head -n1)
if [ \"$_nonce\" != {quoted_nonce} ]; then
  _emit_release fenced \"$_expiry\"
  exit 0
fi
_tmp=\"$_lf.release.$$\"
{{
  while IFS= read -r _line || [ -n \"$_line\" ]; do
    case \"$_line\" in
      released=*|released_at=*) ;;
      *) printf '%s\\n' \"$_line\" ;;
    esac
  done < \"$_lf\"
  printf 'released=1\\nreleased_at=%s\\n' \"$(date +%s)\"
}} > \"$_tmp\"
if ! mv -f \"$_tmp\" \"$_lf\"; then
  rm -f \"$_tmp\"
  _emit_release failed \"$_expiry\"
  exit 0
fi
_emit_release released \"$_expiry\"
"""


def release_remote_lane_lease(
    lane_key: str,
    nonce: str,
    *,
    host: str | None = None,
    sandbox_root: str | None = None,
    ssh_runner: SshRunner | None = None,
    timeout: float = SSH_TIMEOUT_S,
) -> dict[str, Any]:
    """Release one remote lane lease only when its nonce still owns the file.

    A release writes a ``released=1`` tombstone under the per-lease mutation
    lock. The tombstone keeps the original expiry for diagnosis while
    ``_lane_occupant_live`` treats it as non-occupying; a later dispatch may
    safely replace it after acquiring the lane lock.
    """
    if _SANDBOX_KEY_RE.fullmatch(str(lane_key)) is None:
        return {"ok": False, "status": "invalid_lane_key", "expiry_epoch": None}
    if _DISPATCH_NONCE_RE.fullmatch(str(nonce)) is None:
        return {"ok": False, "status": "invalid_nonce", "expiry_epoch": None}
    root = _resolve_sandbox_root(sandbox_root)
    if root is None:
        return {"ok": False, "status": "invalid_sandbox_root", "expiry_epoch": None}
    destination = str(host or os.environ.get("WORKBAY_REMOTE_GATE_HOST") or "").strip()
    if not destination or re.fullmatch(r"[A-Za-z0-9_.@:-]+", destination) is None:
        return {"ok": False, "status": "invalid_host", "expiry_epoch": None}
    if timeout <= 0:
        return {"ok": False, "status": "invalid_timeout", "expiry_epoch": None}
    script = _build_release_remote_lane_lease_script(root, lane_key, nonce)
    runner = ssh_runner
    try:
        if runner is None:
            completed = subprocess.run(
                [
                    "ssh",
                    "-T",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "StrictHostKeyChecking=yes",
                    "--",
                    destination,
                    script,
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
            )
            raw: Any = completed
        else:
            try:
                raw = runner(script, host=destination, timeout=timeout)
            except TypeError:
                raw = runner(script, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired, TimeoutError) as exc:
        return {"ok": False, "status": "transport_failed", "error": str(exc), "expiry_epoch": None}
    stdout, stderr, returncode = _coerce_ssh_result(raw)
    if returncode not in (0, None):
        detail = (stderr or stdout or f"exit {returncode}").strip()
        return {
            "ok": False,
            "status": "transport_failed",
            "error": detail[:300],
            "expiry_epoch": None,
        }
    try:
        payload = json.loads(stdout.strip().splitlines()[-1])
    except (IndexError, TypeError, ValueError, json.JSONDecodeError):
        return {"ok": False, "status": "invalid_response", "error": stdout[-300:], "expiry_epoch": None}
    if not isinstance(payload, dict) or not isinstance(payload.get("status"), str):
        return {"ok": False, "status": "invalid_response", "error": stdout[-300:], "expiry_epoch": None}
    expiry_raw = payload.get("expiry_epoch")
    expiry = expiry_raw if isinstance(expiry_raw, int) and not isinstance(expiry_raw, bool) else None
    status = payload["status"]
    return {
        "ok": status in {"released", "already_released", "not_found"},
        "status": status,
        "expiry_epoch": expiry,
        "lane_key": lane_key,
        "nonce": nonce,
    }


def _build_list_script(root: str) -> str:
    helpers = _remote_helpers(root)
    return f"""{_HEADER_LIST}
set -eu
{helpers}
_evidence_candidates=$(mktemp "${{TMPDIR:-/tmp}}/workbay-evidence-candidates.XXXXXX")
_evidence_sorted="$_evidence_candidates.sorted"
_process_snapshot=$(mktemp /tmp/workbay-process-cwds.XXXXXX)
trap 'rm -f "$_evidence_candidates" "$_evidence_sorted" "$_process_snapshot"' EXIT
for _cwd in /proc/[0-9]*/cwd; do
  [ -L "$_cwd" ] || continue
  _target=$(readlink "$_cwd" 2>/dev/null) || continue
  printf '%s\\n' "$_target" >> "$_process_snapshot"
done
_evidence_now=$(date +%s)
_evidence_young=0
for _sd in "$ROOT"/*/; do
  [ -d "$_sd" ] || continue
  _sd="${{_sd%/}}"
  _sk="${{_sd##*/}}"
  _em="$_sd/.workbay-attempt-evidence"
  if printf '%s\\n' "$_sk" | LC_ALL=C grep -Eq '{_EVIDENCE_KEY_RE.pattern}'; then
    [ -f "$_em" ] || _em="$_sd/.workbay-lane-sandbox"
  fi
  if [ -f "$_em" ]; then
    _mtime=$(stat -c %Y "$_em" 2>/dev/null || printf '0')
    _age=$(( _evidence_now - _mtime ))
    [ "$_age" -ge 0 ] || _age=0
    if [ "$_age" -ge {EVIDENCE_RETENTION_SECONDS} ]; then
      printf '%s\\t%s\\n' "$_age" "$_sk" >> "$_evidence_candidates"
    else
      _evidence_young=$((_evidence_young + 1))
    fi
    continue
  fi
  [ -f "$_sd/.workbay-lane-sandbox" ] || continue
  _tip=MISSING
  if _git_admin_shape_safe "$_sd" && _rev=$(git -C "$_sd" --work-tree="$_sd" --git-dir="$_sd/.git" rev-parse HEAD 2>/dev/null); then
    _tip="$_rev"
  fi
  _tree=MISSING
  if [ "$_tip" != MISSING ]; then
    _tree=$(git -C "$_sd" --work-tree="$_sd" --git-dir="$_sd/.git" rev-parse "$_tip^{{tree}}" 2>/dev/null) || _tree=MISSING
  fi
  _occupant=idle
  if _lane_occupant_live "$_sk" || _pid_in_sandbox "$_sd" "$_process_snapshot"; then
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
  printf 'SANDBOX %s tip=%s occupant=%s lock=%s tree=%s idle=%s\\n' \\
    "$_sk" "$_tip" "$_occupant" "$_lock" "$_tree" "$_idle"
done
printf 'EVIDENCE_YOUNG count=%s\\n' "$_evidence_young"
_evidence_count=$(awk 'END {{print NR + 0}}' "$_evidence_candidates")
if [ "$_evidence_count" -gt 0 ]; then
  _tab=$(printf '\\t')
  LC_ALL=C sort -t "$_tab" -k1,1nr -k2,2 "$_evidence_candidates" > "$_evidence_sorted"
  awk -F '\\t' -v limit={EVIDENCE_REAP_CANDIDATE_LIMIT} \
    'NR <= limit {{printf "EVIDENCE %s age=%s\\n", $2, $1}}' "$_evidence_sorted"
fi
_evidence_remaining=$((_evidence_count - {EVIDENCE_REAP_CANDIDATE_LIMIT}))
if [ "$_evidence_remaining" -gt 0 ]; then
  printf 'EVIDENCE_TRUNCATED remaining=%s\\n' "$_evidence_remaining"
fi
"""


def _build_evidence_reap_script(root: str, key: str) -> str:
    if not _EVIDENCE_KEY_RE.fullmatch(key):
        raise ValueError("unsafe evidence key")
    return f"""# WORKBAY_REMOTE_SANDBOX_REAP op=evidence key={key}
set -eu
{_remote_helpers(root)}
exec 8>"$ROOT/.reap.lock"
flock -n 8 || {{ echo skipped_unsafe; exit 0; }}
_sd="$ROOT/{key}"
[ -d "$_sd" ] && [ ! -L "$_sd" ] || {{ echo skipped_unsafe; exit 0; }}
_em="$_sd/.workbay-attempt-evidence"
[ -f "$_em" ] || _em="$_sd/.workbay-lane-sandbox"
[ -f "$_em" ] && [ ! -L "$_em" ] || {{ echo skipped_unsafe; exit 0; }}
_mtime=$(stat -c %Y "$_em")
_age=$(( $(date +%s) - _mtime ))
[ "$_age" -ge {EVIDENCE_RETENTION_SECONDS} ] || {{ echo skipped_young; exit 0; }}
rm -rf -- "$_sd"
[ ! -e "$_sd" ] && echo reaped || echo skipped_unsafe
"""


def _build_rowless_probe_script(root: str, key: str, *, host_tip: str = "", mapped_product: bool = False) -> str:
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
_rowless_inventory_safe "$_sd" {_sh_single_quote(host_tip)} {"mapped" if mapped_product else "rowless"} || {{ echo "DIRTY $_inventory_reason"; exit 0; }}
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
        args = [
            key,
            proof.tree if proof else "",
            proof.generation if proof else "",
            proof.host_tip if proof else "",
            "mapped" if proof and proof.mapped_product else "rowless",
        ]
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
  _proof_domain="$5"
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
      _rowless_inventory_safe "$_sd" "$_proven_host_tip" "$_proof_domain" || {{ echo "SKIPPED_CHANGED $_sk $_inventory_reason"; exit 0; }}
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


def _validate_probe_process_identity(receipt, observed) -> bool:
    """Principle 14: authority is the complete launch identity, never a directory."""
    from collections.abc import Mapping

    for identity in (receipt, observed):
        if not isinstance(identity, Mapping):
            return False
        for key in ("pid", "uid"):
            value = identity.get(key)
            if type(value) is not int or value < (1 if key == "pid" else 0):
                return False
        for key in ("host", "start_time", "command", "owner_nonce"):
            value = identity.get(key)
            if not isinstance(value, str) or not value.strip() or "\0" in value:
                return False
    return all(receipt[key] == observed[key] for key in ("host", "pid", "uid", "start_time", "command", "owner_nonce"))


def _owned_probe_remote_cleanup(receipt, budget, grace, attempt):
    """Self-contained Linux payload; pidfds close the inspection/signal reuse race."""
    import json
    import math
    import os
    import secrets
    import signal
    import time
    from pathlib import Path

    deadline = time.monotonic() + budget
    pid = receipt["pid"]
    fd = None

    def result(state, diagnostic):
        return {"state": state, "diagnostic": diagnostic, "cleanup_receipt": receipt}

    def expired(*args):
        raise TimeoutError("remote cleanup deadline")

    def read_identity():
        root = Path("/proc") / str(pid)
        stat = (root / "stat").read_text().rsplit(")", 1)[1].split()
        start = stat[19]
        status = (root / "status").read_text()
        uids = next(line.split()[1:] for line in status.splitlines() if line.startswith("Uid:"))
        # Refuse setuid/partial UID observations rather than infer the launch owner.
        uid = int(uids[0]) if len(uids) == 4 and len(set(uids)) == 1 else None
        command = (root / "cmdline").read_bytes().rstrip(b"\0").replace(b"\0", b" ").decode()
        nonces = [
            entry.split(b"=", 1)[1].decode()
            for entry in (root / "environ").read_bytes().split(b"\0")
            if entry.startswith(b"WORKBAY_PROBE_OWNER_NONCE=")
        ]
        # Host is bound by the explicit authenticated transport destination.
        observed = dict(
            host=receipt["host"],
            pid=pid,
            uid=uid,
            start_time=start,
            command=command,
            owner_nonce=nonces[0] if len(nonces) == 1 else None,
        )
        if not _validate_probe_process_identity(receipt, observed):
            raise ValueError("process identity missing or changed")

    def observe():
        try:
            read_identity()
        except FileNotFoundError:
            # Missing environ/status is unknown, not proof that the PID is gone.
            (Path("/proc") / str(pid) / "stat").read_text()
            raise ValueError("partial process observation") from None

    def pause(until):
        remaining = min(deadline, until) - time.monotonic()
        if remaining > 0:
            time.sleep(min(0.05, remaining))

    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, budget)
    try:
        # The bootstrap watchdog is armed before READY, including a no-EOF partition.
        started = time.monotonic()
        challenge = secrets.token_hex(32)
        print(json.dumps({"kind": "READY", "attempt": attempt, "challenge": challenge}), flush=True)
        grant = json.loads(input())
        if (
            not isinstance(grant, dict)
            or grant.get("kind") != "GRANT"
            or grant.get("attempt") != attempt
            or grant.get("challenge") != challenge
            or type(grant.get("remaining")) not in (int, float)
            or not math.isfinite(grant["remaining"])
        ):
            return result("blocked", "invalid cleanup grant")
        lease = min(budget, grant["remaining"] - (time.monotonic() - started))
        if lease <= 0:
            return result("pending", "cleanup grant expired")
        deadline = min(deadline, time.monotonic() + lease)
        signal.setitimer(signal.ITIMER_REAL, max(0.000001, deadline - time.monotonic()))
        if pid in {1, os.getpid(), os.getppid(), os.getpgrp(), os.getsid(0)}:
            return result("blocked", "caller identity excluded")
        observe()
        # No numeric-PID fallback: an unsupported kernel cannot safely authorize a signal.
        fd = os.pidfd_open(pid, 0)
        for signum in (signal.SIGTERM, signal.SIGKILL):
            observe()  # Fresh full comparison immediately before EACH individual signal.
            if time.monotonic() >= deadline:
                return result("pending", "cleanup budget exhausted")
            signal.pidfd_send_signal(fd, signum)
            until = min(deadline, time.monotonic() + grace) if signum == signal.SIGTERM else deadline
            while time.monotonic() < until:
                observe()
                pause(until)
        return result("pending", "process still present after KILL")
    except (FileNotFoundError, ProcessLookupError):
        return result("complete", "process disappeared")
    except TimeoutError:
        return result("pending", "cleanup budget exhausted")
    except (OSError, ValueError, IndexError, StopIteration, AttributeError, EOFError) as exc:
        return result("blocked", f"identity or signal unavailable: {type(exc).__name__}")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        if fd is not None:
            os.close(fd)


def run_owned_probe_cleanup_ssh(script, *, host, timeout, on_ready, authorized_host):
    """Authenticate the configured destination and reserve local SSH teardown time.

    Script delivery uses argv; stdin carries exactly one invocation-scoped grant.
    Completion remains conditional on timely scheduling and a healthy kernel.
    """
    import json
    import selectors
    import time

    if not authorized_host or host != authorized_host or not re.fullmatch(r"[A-Za-z0-9_.@:-]+", host):
        raise ValueError("cleanup host differs from authorized destination")
    if timeout <= 0.5:
        raise TimeoutError("insufficient cleanup transport budget")
    deadline = time.monotonic() + timeout
    proc = subprocess.Popen(
        ["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "--", host, script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    output = bytearray()
    granted = False
    selector = selectors.DefaultSelector()
    try:
        selector.register(proc.stdout, selectors.EVENT_READ)
        while True:
            remaining = deadline - time.monotonic() - 0.5
            if remaining <= 0 or not selector.select(remaining):
                raise TimeoutError("cleanup transport deadline")
            chunk = os.read(proc.stdout.fileno(), 65536)
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > 65536:
                raise ValueError("oversized cleanup response")
            if not granted and b"\n" in output:
                line, _, tail = output.partition(b"\n")
                grant = on_ready(json.loads(line))
                proc.stdin.write((json.dumps(grant) + "\n").encode())
                proc.stdin.flush()
                granted = True
                output = bytearray(tail)
        code = proc.wait(timeout=max(0.001, deadline - time.monotonic() - 0.5))
        if not granted:
            raise ValueError("cleanup transport closed before READY")
        return subprocess.CompletedProcess([], code, output.decode(), "")
    finally:
        selector.close()
        # Only the local SSH process is ours. Never signal its inherited group.
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=max(0.001, min(0.25, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=max(0.001, deadline - time.monotonic()))
        proc.stdin.close()
        proc.stdout.close()


def reap_owned_probe_process(
    cleanup_receipt: Mapping[str, Any],
    *,
    ssh_runner: SshRunner,
    timeout_s: float = 5.0,
    grace_s: float = 0.5,
) -> dict[str, Any]:
    """Return complete/pending/blocked with the original retained receipt.

    Runner contract: runner(script, host=receipt_host, timeout=remaining,
    on_ready=callback) authenticates the host, separates control from final output,
    parses one JSON READY line, and writes callback(ready) as one JSON GRANT line
    to payload stdin. It must bound all I/O and terminate/reap local SSH resources
    within timeout (reserving teardown time), including when callback raises.
    Timeout-only runners fail closed. No other stdin or grants are permitted.
    The callback is invocation-scoped and single-use; never cache a grant.
    Remote READY-to-GRANT elapsed is subtracted from the host's remaining duration.
    With compatible monotonic clock rates and bounded scheduling/signal latency,
    this conservatively bounds remote authority by the original host deadline.
    Unbounded partition/suspension cannot prove completion: retain pending/blocked.
    The remote bootstrap timer bounds a partition without EOF and no subprocess
    is spawned by this payload. Actual authenticated SSH integration is caller-owned.
    Launch must export WORKBAY_PROBE_OWNER_NONCE; start_time is Linux stat field
    22, command is the space-joined cmdline. Each child needs its own launch receipt.

    Canon https://github.com/darce/heuristics-canon: Principle 14 limits authority;
    RES-02 shares one deadline; TEST-15 is falsified by removing UID comparison.
    A matching command or shared PGID is a countercase to ownership, not proof.
    """
    import inspect
    import json
    import math
    import secrets
    import time

    deadline = time.monotonic() + timeout_s
    receipt = dict(cleanup_receipt)

    def result(state, diagnostic):
        return {"state": state, "diagnostic": diagnostic, "cleanup_receipt": receipt}

    if not _validate_probe_process_identity(receipt, receipt):
        return result("blocked", "incomplete launch receipt")
    if not math.isfinite(timeout_s) or not math.isfinite(grace_s) or timeout_s <= 0 or grace_s < 0:
        return result("blocked", "invalid cleanup budget")
    attempt = secrets.token_hex(32)
    granted = False

    def on_ready(ready):
        nonlocal granted
        if (
            granted
            or not isinstance(ready, dict)
            or ready.get("kind") != "READY"
            or ready.get("attempt") != attempt
            or not isinstance(ready.get("challenge"), str)
            or len(ready["challenge"]) != 64
        ):
            raise ValueError("invalid or replayed cleanup READY")
        granted = True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("cleanup deadline before grant")
        return dict(kind="GRANT", attempt=attempt, challenge=ready["challenge"], remaining=remaining)

    payload = "import json\n" + inspect.getsource(_validate_probe_process_identity)
    payload += "\n" + inspect.getsource(_owned_probe_remote_cleanup)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return result("pending", "cleanup budget exhausted")
    payload += (
        f"\nprint(json.dumps(_owned_probe_remote_cleanup({receipt!r}, {remaining!r}, {grace_s!r}, {attempt!r})))\n"
    )
    try:
        raw = ssh_runner(
            "python3 -c " + _sh_single_quote(payload), host=receipt["host"], timeout=remaining, on_ready=on_ready
        )
        if not granted or time.monotonic() >= deadline:
            return result("pending", "cleanup grant or timely confirmation unavailable")
        stdout, _, code = _coerce_ssh_result(raw)
        if code != 0:
            return result("blocked", "remote cleanup failed")
        response = json.loads(stdout)
        if (
            not isinstance(response, dict)
            or response.get("state") not in {"complete", "pending", "blocked"}
            or response.get("cleanup_receipt") != receipt
            or not isinstance(response.get("diagnostic"), str)
        ):
            return result("blocked", "invalid remote cleanup response")
        return result(response["state"], response["diagnostic"])
    except (TimeoutError, subprocess.TimeoutExpired):
        return result("pending", "remote cleanup timed out")
    except (OSError, ValueError, TypeError):
        return result("blocked", "remote cleanup unavailable")
