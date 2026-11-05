"""Reap host-enumerated stale lane lease/live files under the VM lock protocol.

The lane-lock is retained: dispatch opens it before taking flock, so unlinking
it would let a paused dispatcher lock an orphaned inode. The lane-lock population
does not shrink here; vmreap-fence-01 owns that protocol change. Maintenance
writers in remote_agent.sh hold .reap.lock and dispatch writers hold the
lane-lock. The host lease-release probe takes only the lease-lock and rechecks
the nonce under it: holding that lock excludes its writes until the live file
is removed. A live file without a lease-lock is conservatively refused. We take
the mutex, lane-lock, then existing lease-lock, retaining these through journaling
and deletion, and recheck inode identities and occupancy before each unlink.

Dry runs may create an empty .reap.lock, never truncate it, and write nothing
else. A dry run without a lane-lock is advisory and cannot prove completion.
Remote status words are LOCK_ABSENT, LOCK_SKIPPED, LOCK_REAPED,
LOCK_WOULD_REAP, LOCK_WOULD_REAP_UNFENCED and LOCK_UNJOURNALED. Missing,
duplicate, malformed or unexpected output fails closed; remote calls never retry.
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from dataclasses import dataclass

from .remote_sandbox_reap import (
    _LANE_KEY_RE,
    _SANDBOX_KEY_RE,
    SSH_TIMEOUT_S,
    SshRunner,
    _remote_helpers,
    _resolve_sandbox_root,
    _run_ssh,
    _sh_single_quote,
)

LOCK_REAP_MIN_AGE_SECONDS = 3600
LOCK_REAP_KEY_LIMIT = 64
LOCK_REAP_ACTOR = "landing_lock_reap"
# Each census has a 10s TERM deadline, 2s KILL grace, and 3s of slack.
LOCK_REAP_TIMEOUT_PER_KEY_S = 15.0

_LOCK_LINE_RE = re.compile(
    r"^(?P<word>LOCK_ABSENT|LOCK_SKIPPED|LOCK_REAPED|LOCK_WOULD_REAP|"
    r"LOCK_WOULD_REAP_UNFENCED|LOCK_UNJOURNALED) (?P<key>\S+)(?: (?P<detail>\S+(?: \S+)*))?$"
)
_LIVE_REASONS = frozenset({"sandbox_present", "occupant_live", "scope_active", "held_lock"})
_UNSAFE_REASONS = frozenset(
    {
        "reap_lock_held",
        "inode_changed",
        "probe_failed",
        "recheck_failed",
        "journal_failed",
        "unlink_incomplete",
        "unsafe_path",
    }
)


@dataclass(frozen=True)
class LockVerdict:
    key: str
    status: str
    reason: str
    unlinked: tuple[str, ...] = ()


@dataclass
class LockReapReport:
    """Typed results; advisory_unfenced dry-run verdicts are never terminal."""

    dry_run: bool
    verdicts: list[LockVerdict]
    error: str | None = None

    @property
    def terminal(self) -> bool:
        return self.error is None and all(
            verdict.status in {"reaped", "absent", "dry_run"} and verdict.reason != "advisory_unfenced"
            for verdict in self.verdicts
        )


def _lock_basenames(key: str) -> tuple[str, ...]:
    """Removal order preserves the lease-lock until the live file is gone."""
    return (f".lane-live-{key}", f".lane-lease-{key}.lock")


def _safe_key(key: str) -> bool:
    return isinstance(key, str) and bool(_LANE_KEY_RE.fullmatch(key) and _SANDBOX_KEY_RE.fullmatch(key))


def _scope_units(key: str) -> tuple[str, ...]:
    # Mirror remote_agent.sh:_maintenance_lane_scope_live, including retry -sv twins.
    return tuple(
        f"{prefix}-{key}{retry}{sv}.scope"
        for prefix in ("workbay-lane", "grok-lane")
        for retry in ("", *(f"-r{i}" for i in range(1, 10)))
        for sv in ("", "-sv")
    )


def build_lock_reap_script(root: str, keys: Sequence[str], *, min_age_seconds: int, dry_run: bool) -> str:
    """Build an allowlist-only script; direct callers must supply valid inputs."""
    if len(keys) > LOCK_REAP_KEY_LIMIT or any(not _safe_key(key) for key in keys):
        raise ValueError("unsafe lock reap keys")
    if not isinstance(min_age_seconds, int) or min_age_seconds <= 0:
        raise ValueError("min_age_invalid")
    if _resolve_sandbox_root(root) is None:
        raise ValueError("sandbox_root is empty or unsafe")
    calls = "\n".join(
        f"_reap_locks_one {_sh_single_quote(key)} " + " ".join(map(_sh_single_quote, _scope_units(key)))
        for key in dict.fromkeys(keys)
    )
    if dry_run:
        tail = """
  if [ "$_fenced" -eq 1 ]; then
    printf 'LOCK_WOULD_REAP %s%s\n' "$_k" "$_present"
  else
    printf 'LOCK_WOULD_REAP_UNFENCED %s%s\n' "$_k" "$_present"
  fi
"""
        recheck = ""
    else:
        recheck = """
_recheck_fence() {
  _same_inode 9 "$ROOT/.lane-lock-$_k" || return 1
  if [ "$_lease_opened" -eq 1 ]; then
    _same_inode 6 "$ROOT/.lane-lease-$_k.lock" || return 1
  fi
  _regular_or_absent "$ROOT/.lane-live-$_k" || return 1
  _regular_or_absent "$ROOT/.lane-lease-$_k.lock" || return 1
  _lane_occupant_live "$_k" && return 1
  _path_present "$ROOT/$_k" && return 1
  return 0
}
"""
        tail = """
  _journal started || { _skip journal_failed; exit 0; }
  _removed=
  _outcome=reaped
  _reason=stale_lock
  for _basename in $_present; do
    if ! _recheck_fence; then
      _outcome=incomplete
      _reason=recheck_failed
      break
    fi
    if rm -f -- "$ROOT/$_basename" && ! _path_present "$ROOT/$_basename"; then
      _removed="$_removed $_basename"
    else
      _outcome=incomplete
      _reason=unlink_incomplete
      break
    fi
  done
  if ! _journal "$_outcome"; then
    printf 'LOCK_UNJOURNALED %s%s\n' "$_k" "$_removed"
    exit 0
  fi
  if [ "$_outcome" = reaped ]; then
    printf 'LOCK_REAPED %s%s\n' "$_k" "$_removed"
  else
    _skip "$_reason"
  fi
"""
    return (
        "# WORKBAY_REMOTE_LANE_LOCK_REAP\nset -u\n"
        + _remote_helpers(root)
        + """
_path_present() { [ -e "$1" ] || [ -L "$1" ]; }
_regular_or_absent() {
  [ ! -L "$1" ] && { [ ! -e "$1" ] || [ -f "$1" ]; }
}
_same_inode() {
  _regular_or_absent "$2" && [ -f "$2" ] || return 1
  case "$1" in
    9) _fd_identity=$(stat -L -c %d:%i /proc/self/fd/9 2>/dev/null) || return 1 ;;
    6) _fd_identity=$(stat -L -c %d:%i /proc/self/fd/6 2>/dev/null) || return 1 ;;
    8) _fd_identity=$(stat -L -c %d:%i /proc/self/fd/8 2>/dev/null) || return 1 ;;
    *) return 1 ;;
  esac
  _path_identity=$(stat -c %d:%i "$2" 2>/dev/null) || return 1
  [ -n "$_fd_identity" ] && [ "$_fd_identity" = "$_path_identity" ]
}
_outer_reason=
if ! _regular_or_absent "$ROOT/.reap.lock"; then
  _outer_reason=unsafe_path
elif ! exec 8>>"$ROOT/.reap.lock"; then
  _outer_reason=unsafe_path
elif ! flock -n 8; then
  _outer_reason=reap_lock_held
elif ! _same_inode 8 "$ROOT/.reap.lock"; then
  _outer_reason=inode_changed
fi
_skip() { printf 'LOCK_SKIPPED %s %s\n' "$_k" "$1"; }
"""
        + recheck
        + (
            """
_journal() {
  _regular_or_absent "$ROOT/.reap-outcomes.jsonl" || return 1
  _at=$(date +%s) || return 1
  printf '{"key":"%s","outcome":"%s","reason":"stale_lock","actor":"landing_lock_reap","at":%s}\n' \
    "$_k" "$1" "$_at" >>"$ROOT/.reap-outcomes.jsonl"
}
"""
            if not dry_run
            else ""
        )
        + """
_reap_locks_one() (
  _k="$1"
  shift
  [ -z "$_outer_reason" ] || { _skip "$_outer_reason"; exit 0; }
  if ! _path_present "$ROOT/.lane-lease-$_k.lock" && ! _path_present "$ROOT/.lane-live-$_k"; then
    printf 'LOCK_ABSENT %s\n' "$_k"
    exit 0
  fi
  for _path in "$ROOT/.lane-lock-$_k" "$ROOT/.lane-lease-$_k.lock" "$ROOT/.lane-live-$_k"; do
    _regular_or_absent "$_path" || { _skip unsafe_path; exit 0; }
  done
  # Snapshot the age source BEFORE any append open can create a new fence.
  _lock_existed=0
  if [ -f "$ROOT/.lane-lock-$_k" ]; then
    _lock_existed=1
    _age_source="$ROOT/.lane-lock-$_k"
  elif [ -f "$ROOT/.lane-lease-$_k.lock" ]; then
    _age_source="$ROOT/.lane-lease-$_k.lock"
  else
    _age_source="$ROOT/.lane-live-$_k"
  fi
  _mtime=$(stat -c %Y "$_age_source" 2>/dev/null) || { _skip inode_changed; exit 0; }
  case "$_mtime" in ''|*[!0-9]*) _skip inode_changed; exit 0 ;; esac
  _fenced=0
  _lease_opened=0
"""
        + ('  if [ "$_lock_existed" -eq 1 ]; then\n' if dry_run else "  if true; then\n")
        + """
    exec 9>>"$ROOT/.lane-lock-$_k" || { _skip unsafe_path; exit 0; }
    flock -n 9 || { _skip held_lock; exit 0; }
    _same_inode 9 "$ROOT/.lane-lock-$_k" || { _skip inode_changed; exit 0; }
    _fenced=1
    if [ -f "$ROOT/.lane-lease-$_k.lock" ]; then
      exec 6>>"$ROOT/.lane-lease-$_k.lock" || { _skip unsafe_path; exit 0; }
      flock -n 6 || { _skip held_lock; exit 0; }
      _same_inode 6 "$ROOT/.lane-lease-$_k.lock" || { _skip inode_changed; exit 0; }
      _lease_opened=1
    fi
  fi
  _path_present "$ROOT/$_k" && { _skip sandbox_present; exit 0; }
  _lane_occupant_live "$_k" && { _skip occupant_live; exit 0; }
  _scope_output=$(timeout -k 2 10 systemctl --user list-units --plain --no-legend --state=active "$@" 2>/dev/null) || {
    _skip probe_failed; exit 0;
  }
  [ -z "$_scope_output" ] || { _skip scope_active; exit 0; }
  # A dispatcher may have refreshed the existing fence after the first stat.
  # Re-read it under flock; retain the pre-open fallback for a newly made fence.
  if [ "$_lock_existed" -eq 1 ]; then
    _mtime=$(stat -c %Y "$ROOT/.lane-lock-$_k" 2>/dev/null) || { _skip inode_changed; exit 0; }
    case "$_mtime" in ''|*[!0-9]*) _skip inode_changed; exit 0 ;; esac
  fi
  _now=$(date +%s) || { _skip probe_failed; exit 0; }
  case "$_now" in ''|*[!0-9]*) _skip probe_failed; exit 0 ;; esac
"""
        + f'  [ "$((_now - _mtime))" -ge {min_age_seconds} ] || {{ _skip young; exit 0; }}\n'
        + """
  # The host lease-release probe does not take the lane-lock or reap mutex.
  # Do not remove its live file unless we also hold its existing mutation lock.
  if [ "$_fenced" -eq 1 ] && [ "$_lease_opened" -eq 0 ] && _path_present "$ROOT/.lane-live-$_k"; then
    _skip recheck_failed
    exit 0
  fi
  _present=
  for _basename in ".lane-live-$_k" ".lane-lease-$_k.lock"; do
    _regular_or_absent "$ROOT/$_basename" || { _skip unsafe_path; exit 0; }
    if _path_present "$ROOT/$_basename"; then
      _present="$_present $_basename"
    fi
  done
  [ -n "$_present" ] || { printf 'LOCK_ABSENT %s\n' "$_k"; exit 0; }
"""
        + tail
        + ")\n"
        + calls
        + "\n"
    )


def _parse_verdict(key: str, word: str, detail: str, *, dry_run: bool) -> LockVerdict:
    unsafe = LockVerdict(key, "skipped_unsafe", "unparsed_output")
    if word == "LOCK_ABSENT":
        return LockVerdict(key, "absent", "absent") if not detail else unsafe
    if word == "LOCK_SKIPPED":
        if detail == "young":
            return LockVerdict(key, "skipped_young", detail)
        if detail in _LIVE_REASONS:
            return LockVerdict(key, "skipped_live", detail)
        if detail in _UNSAFE_REASONS:
            return LockVerdict(key, "skipped_unsafe", detail)
        return unsafe
    names = tuple(detail.split())
    if len(set(names)) != len(names) or any(name not in _lock_basenames(key) for name in names):
        return unsafe
    if word == "LOCK_UNJOURNALED" and not dry_run:
        return LockVerdict(key, "skipped_unsafe", "journal_append_failed", names)
    if not names:
        return unsafe
    if word == "LOCK_REAPED" and not dry_run:
        return LockVerdict(key, "reaped", "stale_lock", names)
    if word == "LOCK_WOULD_REAP" and dry_run:
        return LockVerdict(key, "dry_run", "would_reap", names)
    if word == "LOCK_WOULD_REAP_UNFENCED" and dry_run:
        return LockVerdict(key, "dry_run", "advisory_unfenced", names)
    return unsafe


def reap_remote_lane_locks(
    keys: Sequence[str],
    *,
    ssh_runner: SshRunner,
    dry_run: bool = True,
    sandbox_root: str | None = None,
    min_age_seconds: int = LOCK_REAP_MIN_AGE_SECONDS,
    timeout: float | None = None,
) -> LockReapReport:
    """Reap only caller-enumerated keys, bounded by key count and SSH deadline."""
    report = LockReapReport(dry_run, [])
    if len(keys) > LOCK_REAP_KEY_LIMIT:
        report.error = "key_limit_exceeded"
        return report
    if not isinstance(min_age_seconds, int) or min_age_seconds <= 0:
        report.error = "min_age_invalid"
        return report
    root = _resolve_sandbox_root(sandbox_root)
    if root is None:
        report.error = "sandbox_root is empty or unsafe"
        return report
    safe_keys = list(dict.fromkeys(key for key in keys if _safe_key(key)))
    if not safe_keys:
        report.verdicts = [LockVerdict(key, "skipped_unsafe", "invalid_key") for key in keys]
        return report
    budget = SSH_TIMEOUT_S + LOCK_REAP_TIMEOUT_PER_KEY_S * len(safe_keys)
    if timeout is not None:
        if not math.isfinite(timeout) or timeout <= 0:
            report.error = "ssh timeout must be finite and positive"
            report.verdicts = [LockVerdict(key, "skipped_unsafe", "ssh_failed") for key in keys]
            return report
        budget = min(budget, timeout)
    script = build_lock_reap_script(root, safe_keys, min_age_seconds=min_age_seconds, dry_run=dry_run)
    try:
        stdout, error = _run_ssh(ssh_runner, script, timeout=budget)
    except Exception as exc:  # Remote runner adapters must degrade to a typed refusal.
        stdout, error = "", f"ssh failed: {type(exc).__name__}: {exc}"[:300]
    if error or not stdout.strip():
        report.error = error or "ssh returned empty stdout"
        report.verdicts = [LockVerdict(key, "skipped_unsafe", "ssh_failed") for key in keys]
        return report
    parsed: dict[str, list[LockVerdict]] = {key: [] for key in safe_keys}
    unexpected = False
    for line in stdout.splitlines():
        if not line.strip():
            continue
        match = _LOCK_LINE_RE.fullmatch(line)
        if match is None or match["key"] not in parsed:
            unexpected = True
            continue
        parsed[match["key"]].append(_parse_verdict(match["key"], match["word"], match["detail"] or "", dry_run=dry_run))
    for key in keys:
        if not _safe_key(key):
            verdict = LockVerdict(key, "skipped_unsafe", "invalid_key")
        elif unexpected or len(parsed[key]) != 1:
            verdict = LockVerdict(key, "skipped_unsafe", "unparsed_output")
        else:
            verdict = parsed[key][0]
        report.verdicts.append(verdict)
    return report
