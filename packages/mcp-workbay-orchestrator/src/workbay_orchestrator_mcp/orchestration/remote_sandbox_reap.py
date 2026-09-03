"""Harvest-driven reaper for finished remote VM lane sandboxes.

The VM sweep in ``scripts/remote_agent.sh`` (RES-07) only reaps by marker
mtime age. This module is the orchestrator-side complement: a sandbox is a
candidate only when its marker file is present, and it is reaped only when
the lane row is terminal (``closed`` / ``merged`` / ``closed_stale``). A
non-terminal row is never harvested, even when the sandbox tip is already
reachable on the local lane branch — a live peer dispatch can share that
ancestry. Callers also pass ``exclude_keys`` for the invoking pass.
``review`` is openable and is not a status shortcut. No TTL/age heuristics.

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

Residual tech-debt (docstring-only; this lane does not edit
``scripts/remote_agent.sh``):
- F4: ``scripts/remote_agent.sh`` still does not flock ``$ROOT/.reap.lock``
  around occupancy write + materialize.
- F1/F5: listing still uses the self-releasing ``_lane_lock_held`` probe
  (``flock -n ... true``); only delete holds the per-lane lock across
  check and act.
- F7: production apply-path caller lives in ``lane_census.census_lanes``.
  ``TERMINAL_ROW_STATUSES`` is an alias of
  ``lane_worktree._TERMINAL_LANE_STATUSES``.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
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
    "KIND_SANDBOX_LIVE",
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
KIND_SANDBOX_LIVE = "sandbox_live"
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
    r"^SANDBOX (?P<key>\S+) tip=(?P<tip>\S+) occupant=(?P<occupant>live|idle) lock=(?P<lock>held|free)$"
)
_REAP_LINE_RE = re.compile(
    r"^(?P<status>REAPED|SKIPPED_LIVE|SKIPPED_LOCK|SKIPPED_GONE_FAILED|SKIPPED_UNMARKED) (?P<key>\S+)$"
)
_HEADER_LIST = "# WORKBAY_REMOTE_SANDBOX_REAP op=list"
_HEADER_REAP_PREFIX = "# WORKBAY_REMOTE_SANDBOX_REAP op=reap keys="

SshRunner = Callable[..., Any]


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


@dataclass(frozen=True)
class _ListedSandbox:
    key: str
    tip_sha: str | None
    occupant_live: bool
    lock_held: bool


@dataclass(frozen=True)
class _MappedRow:
    lane_id: str | None
    branch: str | None
    status: str | None


def plan_remote_sandboxes(
    task_ref: str,
    *,
    ssh_runner: SshRunner,
    primary_repo: Path | str,
    rows: Sequence[Mapping[str, Any] | Any],
    sandbox_root: str | None = None,
    actor: Any = None,
    exclude_keys: Sequence[str] | None = None,
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
    harvested_keys: list[str] = []
    for item in listed:
        verdict = _classify(item, mapped_rows, repo, exclude_keys=excluded)
        report.verdicts.append(verdict)
        if verdict.kind == KIND_SANDBOX_HARVESTED:
            harvested_keys.append(item.key)

    if dry_run or not harvested_keys:
        return report

    reap_lines, reap_error = _reap_remote_sandboxes(ssh_runner, root, harvested_keys)
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
    return report


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
) -> list[tuple[_MappedRow, str, str]]:
    indexed: list[tuple[_MappedRow, str, str]] = []
    for row in rows:
        row_task = _row_text(row, "task_ref")
        if row_task is not None and row_task != task_ref:
            continue
        branch = _row_text(row, "branch")
        mapped = _MappedRow(
            lane_id=_row_text(row, "lane_id"),
            branch=branch,
            status=_row_text(row, "status"),
        )
        derived = _derive_lane_key(branch) if branch else ""
        slug = _branch_slug(branch) if branch else ""
        indexed.append((mapped, derived, slug))
    return indexed


def _classify(
    item: _ListedSandbox,
    indexed_rows: Sequence[tuple[_MappedRow, str, str]],
    _repo: Path,
    *,
    exclude_keys: frozenset[str] = frozenset(),
) -> SandboxVerdict:
    if item.occupant_live:
        row = _map_key(item.key, indexed_rows)
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=row.lane_id if row else None,
            branch=row.branch if row else None,
            kind=KIND_SANDBOX_LIVE,
            tip_sha=item.tip_sha,
            reason="unexpired lease or live pid under sandbox",
        )
    if item.lock_held:
        row = _map_key(item.key, indexed_rows)
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=row.lane_id if row else None,
            branch=row.branch if row else None,
            kind=KIND_LOCK_HELD,
            tip_sha=item.tip_sha,
            reason="lane lock held",
        )
    if item.key in exclude_keys:
        row = _map_key(item.key, indexed_rows)
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=row.lane_id if row else None,
            branch=row.branch if row else None,
            kind=KIND_SANDBOX_UNHARVESTED,
            tip_sha=item.tip_sha,
            reason="excluded invoking-pass identity",
        )
    row = _map_key(item.key, indexed_rows)
    if row is None:
        return SandboxVerdict(
            sandbox_key=item.key,
            lane_id=None,
            branch=None,
            kind=KIND_SANDBOX_UNMAPPED,
            tip_sha=item.tip_sha,
            reason="marker present but no row/branch maps to the key",
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


def _map_key(key: str, indexed_rows: Sequence[tuple[_MappedRow, str, str]]) -> _MappedRow | None:
    exact = [row for row, derived, _slug in indexed_rows if derived and derived == key]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        return None
    slug_hits = [row for row, _derived, slug in indexed_rows if slug and _slug_matches(key, slug)]
    if len(slug_hits) == 1:
        return slug_hits[0]
    return None


def _slug_matches(key: str, slug: str) -> bool:
    prefix = f"{slug}-"
    if not key.startswith(prefix):
        return False
    return bool(re.fullmatch(r"[0-9a-f]{8}", key[len(prefix) :]))


def _branch_slug(branch: str) -> str:
    return branch.replace("/", "-")


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
            )
        )
    return listed, None


def _reap_remote_sandboxes(
    ssh_runner: SshRunner,
    root: str,
    keys: Sequence[str],
) -> tuple[list[str], str | None]:
    safe_keys = [key for key in keys if _SANDBOX_KEY_RE.fullmatch(key)]
    if len(safe_keys) != len(keys):
        return [], "refusing to reap unsafe sandbox key"
    result, error = _run_ssh(ssh_runner, _build_reap_script(root, safe_keys))
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
  if _rev=$(git -C "$_sd" rev-parse HEAD 2>/dev/null); then
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
  printf 'SANDBOX %s tip=%s occupant=%s lock=%s\\n' "$_sk" "$_tip" "$_occupant" "$_lock"
done
"""


def _build_reap_script(root: str, keys: Sequence[str]) -> str:
    helpers = _remote_helpers(root)
    header = _HEADER_REAP_PREFIX + ",".join(keys)
    calls = "\n".join(f"_reap_one {_sh_single_quote(key)}" for key in keys)
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
