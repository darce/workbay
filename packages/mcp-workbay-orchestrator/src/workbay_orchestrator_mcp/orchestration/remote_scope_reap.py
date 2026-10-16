"""Stop over-age, enumerated remote lane scopes during landing cleanup.

The host supplies one validated lane key and builds the complete finite unit
allowlist. The VM checks its live lease and flock state before stopping any
scope, so this action does not depend on ``RuntimeMaxUSec`` being finite.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from workbay_orchestrator_mcp.orchestration.remote_sandbox_reap import (
    _LANE_KEY_RE,
    _SANDBOX_KEY_RE,
    SSH_TIMEOUT_S,
    _remote_helpers,
    _resolve_sandbox_root,
    _run_ssh,
    _sh_single_quote,
)

__all__ = (
    "DEFAULT_LANDED_SCOPE_BOUND_SECONDS",
    "SCOPE_BOUND_MAX_SECONDS",
    "SCOPE_STOP_ACTOR",
    "SCOPE_STOP_MARGIN_SECONDS",
    "ScopeStopReport",
    "ScopeVerdict",
    "build_scope_stop_script",
    "scope_units_for_key",
    "stop_remote_lane_scope",
)

DEFAULT_LANDED_SCOPE_BOUND_SECONDS = 3600
SCOPE_STOP_MARGIN_SECONDS = 600
SCOPE_BOUND_MAX_SECONDS = 86400
SCOPE_STOP_ACTOR = "landing_scope_reap"

_MAX_RUNTIME_MAX_USEC = (1 << 64) - 1
_MAX_RUNTIME_SECONDS = _MAX_RUNTIME_MAX_USEC // 1_000_000
_MAX_SAFE_MARGIN_SECONDS = (1 << 63) - 1 - _MAX_RUNTIME_SECONDS - SCOPE_BOUND_MAX_SECONDS
_STATUS_VALUES = frozenset(
    {"stopped", "inactive", "skipped_young", "skipped_live", "skipped_unsafe", "stop_failed", "dry_run"}
)
_SIMPLE_OUTPUT_RE = re.compile(
    r"^SCOPE_SKIPPED (?P<unit>\S+) "
    r"(?P<detail>reap_lock_held|probe_failed|runtime_ceiling_malformed|occupant_live|held_lock|held_lease)$"
)
_INACTIVE_OUTPUT_RE = re.compile(r"^SCOPE_INACTIVE (?P<unit>\S+)$")
_AGED_OUTPUT_RE = re.compile(
    r"^SCOPE_(?P<kind>SKIPPED|WOULD_STOP|STOPPED|STOP_FAILED) (?P<unit>\S+)"
    r"(?: (?P<young>young))? age=(?P<age>-?[0-9]{1,19}) limit=(?P<limit>[0-9]{1,19}) "
    r"reason=(?P<reason>host_bound|unit_ceiling|cgroup_probe_failed|cgroup_path_missing|"
    r"cgroup_path_invalid|cgroup_procs_unreadable|cgroup_not_empty|cgroup_mount_unavailable)"
    r"(?: kill_status=(?P<kill_status>[0-9]{1,3}) stop_status=(?P<stop_status>[0-9]{1,3}))?$"
)


@dataclass(frozen=True)
class ScopeVerdict:
    unit: str
    status: str
    reason: str
    age_seconds: int | None = None
    limit_seconds: int | None = None
    kill_status: int | None = None
    stop_status: int | None = None

    def __post_init__(self) -> None:
        if self.status not in _STATUS_VALUES:
            raise ValueError(f"invalid scope verdict status: {self.status}")


@dataclass
class ScopeStopReport:
    key: str
    dry_run: bool
    bound_seconds: int
    verdicts: list[ScopeVerdict]
    error: str | None = None

    @property
    def terminal(self) -> bool:
        return self.error is None and not any(
            verdict.status in {"skipped_young", "skipped_live", "skipped_unsafe", "stop_failed"}
            for verdict in self.verdicts
        )


def _valid_key(key: object) -> bool:
    return (
        isinstance(key, str) and _LANE_KEY_RE.fullmatch(key) is not None and _SANDBOX_KEY_RE.fullmatch(key) is not None
    )


def scope_units_for_key(key: str) -> tuple[str, ...]:
    """Return the finite scope-unit allowlist used by the VM liveness probe."""
    if not _valid_key(key):
        raise ValueError("unsafe lane key")

    units = [
        f"workbay-lane-{key}.scope",
        f"workbay-lane-{key}-sv.scope",
        f"grok-lane-{key}.scope",
        f"grok-lane-{key}-sv.scope",
    ]
    for run in range(1, 10):
        units.extend(
            (
                f"workbay-lane-{key}-r{run}.scope",
                f"workbay-lane-{key}-r{run}-sv.scope",
                f"grok-lane-{key}-r{run}.scope",
                f"grok-lane-{key}-r{run}-sv.scope",
            )
        )
    return tuple(units)


def _valid_bound(bound_seconds: object) -> bool:
    return (
        isinstance(bound_seconds, int)
        and not isinstance(bound_seconds, bool)
        and 0 < bound_seconds <= SCOPE_BOUND_MAX_SECONDS
    )


def _valid_margin(margin_seconds: object) -> bool:
    return (
        isinstance(margin_seconds, int)
        and not isinstance(margin_seconds, bool)
        and 0 <= margin_seconds <= _MAX_SAFE_MARGIN_SECONDS
    )


def _resolve_scope_root(sandbox_root: object) -> str | None:
    # Validate runtime callers without making the typed public API's defensive
    # check statically unreachable.
    if sandbox_root is not None and not isinstance(sandbox_root, str):
        return None
    return _resolve_sandbox_root(sandbox_root)


def _unsafe_verdicts(units: tuple[str, ...], reason: str) -> list[ScopeVerdict]:
    return [ScopeVerdict(unit=unit, status="skipped_unsafe", reason=reason) for unit in units]


def build_scope_stop_script(
    root: str,
    key: str,
    *,
    bound_seconds: int,
    margin_seconds: int,
    dry_run: bool,
) -> str:
    """Build a lock-guarded script for only the units returned by this module."""
    if not isinstance(root, str):
        raise ValueError("unsafe sandbox root")
    if not _valid_key(key):
        raise ValueError("unsafe lane key")
    if not _valid_bound(bound_seconds):
        raise ValueError("invalid scope bound")
    if not _valid_margin(margin_seconds):
        raise ValueError("invalid scope margin")
    resolved_root = _resolve_sandbox_root(root)
    if resolved_root is None:
        raise ValueError("unsafe sandbox root")

    units = scope_units_for_key(key)
    quoted_units = "\n  ".join(_sh_single_quote(unit) for unit in units)
    quoted_key = _sh_single_quote(key)
    stop_branch = (
        'printf \'SCOPE_WOULD_STOP %s age=%s limit=%s reason=%s\\n\' "$_unit" "$_age" "$_limit" "$_limit_reason"'
        if dry_run
        else f"""\
  _kill_status=0
  _stop_status=0
  timeout 10 systemctl --user kill --kill-whom=all --signal=KILL "$_unit" >/dev/null 2>&1 || _kill_status=$?
  timeout 10 systemctl --user stop "$_unit" >/dev/null 2>&1 || _stop_status=$?
  _probe_status=0
  _after=$(timeout 10 systemctl --user show --no-pager -p ActiveState -p LoadState "$_unit" 2>/dev/null) || _probe_status=$?
  _after_state=
  _after_load_state=
  while IFS= read -r _after_line; do
    case "$_after_line" in
      ActiveState=*) _after_state=${{_after_line#ActiveState=}} ;;
      LoadState=*) _after_load_state=${{_after_line#LoadState=}} ;;
    esac
  done <<< "$_after"
  _confirmed_stopped=0
  _stop_reason=$_limit_reason
  case "$_after_state" in
    inactive) _confirmed_stopped=1 ;;
    failed)
      if _scope_cgroup_empty; then
        _confirmed_stopped=1
      else
        _stop_reason=$_cgroup_empty_reason
      fi
      ;;
    '') if [ "$_after_load_state" = not-found ]; then _confirmed_stopped=1; fi ;;
  esac
  if [ "$_probe_status" -ne 0 ] || [ "$_confirmed_stopped" -ne 1 ]; then
    _at=$(date +%s)
    printf '{{"unit":"%s","outcome":"scope_overage_stop_failed","reason":"%s","age":%s,"runtime_max_sec":%s,"scope_limit_sec":%s,"kill_status":%s,"stop_status":%s,"actor":"{SCOPE_STOP_ACTOR}","at":%s}}\\n' \\
      "$_unit" "$_stop_reason" "$_age" "$_unit_limit" "$_limit" "$_kill_status" "$_stop_status" "$_at" >>"$ROOT/.reap-outcomes.jsonl"
    printf 'SCOPE_STOP_FAILED %s age=%s limit=%s reason=%s kill_status=%s stop_status=%s\\n' \\
      "$_unit" "$_age" "$_limit" "$_stop_reason" "$_kill_status" "$_stop_status"
    return 0
  fi
  _at=$(date +%s)
  printf '{{"unit":"%s","outcome":"scope_overage_stopped","reason":"%s","age":%s,"scope_limit_sec":%s,"actor":"{SCOPE_STOP_ACTOR}","at":%s}}\\n' \\
    "$_unit" "$_limit_reason" "$_age" "$_limit" "$_at" >>"$ROOT/.reap-outcomes.jsonl"
  printf 'SCOPE_STOPPED %s age=%s limit=%s reason=%s\\n' "$_unit" "$_age" "$_limit" "$_limit_reason"
""".strip()
    )

    return f"""# WORKBAY_REMOTE_SCOPE_REAP key={key}
set -eu
export LC_ALL=C
{_remote_helpers(resolved_root)}
HOST_BOUND_SECONDS={bound_seconds}
MARGIN_SECONDS={margin_seconds}
SCOPE_UPTIME_PATH=${{SCOPE_UPTIME_PATH:-/proc/uptime}}
SCOPE_CGROUP_ROOT=${{SCOPE_CGROUP_ROOT:-/sys/fs/cgroup}}
exec 8>"$ROOT/.reap.lock"
_scope_units=(
  {quoted_units}
)
if ! flock -n 8; then
  for _unit in "${{_scope_units[@]}}"; do
    printf 'SCOPE_SKIPPED %s reap_lock_held\\n' "$_unit"
  done
  exit 0
fi

# A failed scope can still contain processes that survived SIGKILL. Its state
# alone is not evidence of quiescence. Bound both the property lookup and the
# filesystem probe; unavailable or malformed evidence stays retryable.
_scope_cgroup_empty() {{
  _cgroup_empty_reason=cgroup_probe_failed
  if ! _cgroup_show=$(timeout 10 systemctl --user show --no-pager -p ControlGroup "$_unit" 2>/dev/null); then
    return 1
  fi
  _cgroup_path=
  while IFS= read -r _cgroup_line; do
    case "$_cgroup_line" in
      ControlGroup=*) _cgroup_path=${{_cgroup_line#ControlGroup=}} ;;
    esac
  done <<< "$_cgroup_show"
  if [ -z "$_cgroup_path" ]; then
    _cgroup_empty_reason=cgroup_path_missing
    return 1
  fi
  case "$_cgroup_path" in
    /*) ;;
    *) _cgroup_empty_reason=cgroup_path_invalid; return 1 ;;
  esac
  case "$_cgroup_path/" in
    /|*'/../'*|*'/./'*|*'//'*) _cgroup_empty_reason=cgroup_path_invalid; return 1 ;;
  esac
  _cgroup_status=0
  timeout 10 bash -c '
    path=$1
    filesystem=$(stat -f -c %T "$path") || exit 4
    [ "$filesystem" = cgroup2fs ] || exit 4
    [ -d "$path" ] && [ -x "$path" ] || exit 2
    IFS=/ read -ra parts <<< "$2"
    for part in "${{parts[@]}}"; do
      [ -n "$part" ] || continue
      path=$path/$part
      [ ! -L "$path" ] || exit 2
      if [ ! -e "$path" ]; then exit 0; fi
      [ -d "$path" ] && [ -x "$path" ] || exit 2
    done
    [ -r "$path/cgroup.procs" ] || exit 2
    procs=$(cat "$path/cgroup.procs") || exit 2
    [ -z "$procs" ] || exit 3
  ' scope-cgroup "$SCOPE_CGROUP_ROOT" "$_cgroup_path" >/dev/null 2>&1 || _cgroup_status=$?
  case "$_cgroup_status" in
    0) return 0 ;;
    3) _cgroup_empty_reason=cgroup_not_empty ;;
    4) _cgroup_empty_reason=cgroup_mount_unavailable ;;
    *) _cgroup_empty_reason=cgroup_procs_unreadable ;;
  esac
  return 1
}}

# Accept integer microseconds and systemd's formatted time spans, including
# fractional seconds at microsecond precision. Components truncate to whole
# seconds, as do integer microseconds and usec/ms components. Normalize
# decimal values and bound every multiplication before shell arithmetic.
_scope_runtime_uint64() {{
  _runtime_value=$1
  while [[ ${{#_runtime_value}} -gt 1 && $_runtime_value == 0* ]]; do
    _runtime_value=${{_runtime_value:1}}
  done
  if [[ ${{#_runtime_value}} -gt 20 ]] || {{
    [[ ${{#_runtime_value}} -eq 20 ]] && [[ "$_runtime_value" > "18446744073709551615" ]]
  }}; then
    return 1
  fi
}}

_scope_parse_runtime() {{
  _runtime_raw=$1
  _unit_limit=0
  _limit_reason=unit_ceiling
  case "$_runtime_raw" in
    infinity|0|'') _unit_limit=$HOST_BOUND_SECONDS; _limit_reason=host_bound; return 0 ;;
  esac
  if [[ $_runtime_raw =~ ^[0-9]+$ ]]; then
    _scope_runtime_uint64 "$_runtime_raw" || return 1
    _runtime_digits=${{#_runtime_value}}
    if [[ $_runtime_digits -gt 6 ]]; then
      _unit_limit=${{_runtime_value:0:_runtime_digits-6}}
    fi
    return 0
  fi
  _runtime_rest=$_runtime_raw
  while [ -n "$_runtime_rest" ]; do
    if [[ "$_runtime_rest" =~ ^[[:space:]]*([0-9]+)(\\.[0-9]{{1,6}})?[[:space:]]*(usec|us|msec|ms|sec|s|min|m|h|d|w)(.*)$ ]]; then
      _runtime_component=${{BASH_REMATCH[1]}}
      _runtime_fraction=${{BASH_REMATCH[2]}}
      _runtime_unit=${{BASH_REMATCH[3]}}
      _runtime_rest=${{BASH_REMATCH[4]}}
      if [ -n "$_runtime_fraction" ] && [ "$_runtime_unit" != s ] && [ "$_runtime_unit" != sec ]; then
        return 1
      fi
      _scope_runtime_uint64 "$_runtime_component" || return 1
      case "$_runtime_unit" in
        usec|us|msec|ms) _runtime_factor=0 ;;
        sec|s) _runtime_factor=1 ;;
        min|m) _runtime_factor=60 ;;
        h) _runtime_factor=3600 ;;
        d) _runtime_factor=86400 ;;
        w) _runtime_factor=604800 ;;
      esac
      if [ "$_runtime_factor" -ne 0 ]; then
        _runtime_component_max=$((({_MAX_RUNTIME_SECONDS} - _unit_limit) / _runtime_factor))
        if [[ ${{#_runtime_value}} -gt ${{#_runtime_component_max}} ]] || {{
          [[ ${{#_runtime_value}} -eq ${{#_runtime_component_max}} ]] &&
            [[ "$_runtime_value" > "$_runtime_component_max" ]]
        }}; then
          return 1
        fi
        _unit_limit=$((_unit_limit + _runtime_value * _runtime_factor))
      fi
    else
      return 1
    fi
  done
}}

_scope_probe_age() {{
  if ! _show=$(timeout 10 systemctl --user show --no-pager -p ActiveState -p ActiveEnterTimestampMonotonic -p RuntimeMaxUSec "$_unit" 2>/dev/null); then
    printf 'SCOPE_SKIPPED %s probe_failed\\n' "$_unit"
    return 1
  fi
  _active_state=
  _active_enter=
  _runtime_max=
  _have_active=0
  _have_enter=0
  while IFS= read -r _line; do
    case "$_line" in
      ActiveState=*) _active_state=${{_line#ActiveState=}}; _have_active=1 ;;
      ActiveEnterTimestampMonotonic=*) _active_enter=${{_line#ActiveEnterTimestampMonotonic=}}; _have_enter=1 ;;
      RuntimeMaxUSec=*) _runtime_max=${{_line#RuntimeMaxUSec=}} ;;
    esac
  done <<< "$_show"
  if [ "$_have_active" -ne 1 ]; then
    printf 'SCOPE_SKIPPED %s probe_failed\\n' "$_unit"
    return 1
  fi
  case "$_active_state" in
    active) ;;
    inactive) printf 'SCOPE_INACTIVE %s\\n' "$_unit"; return 1 ;;
    failed)
      if _scope_cgroup_empty; then
        printf 'SCOPE_INACTIVE %s\\n' "$_unit"
        return 1
      fi
      ;;
    *) printf 'SCOPE_SKIPPED %s probe_failed\\n' "$_unit"; return 1 ;;
  esac
  if [ "$_have_enter" -ne 1 ] || [[ ! $_active_enter =~ ^[0-9]+$ ]]; then
    printf 'SCOPE_SKIPPED %s probe_failed\\n' "$_unit"
    return 1
  fi
  while [[ ${{#_active_enter}} -gt 1 && $_active_enter == 0* ]]; do _active_enter=${{_active_enter:1}}; done
  if [[ ${{#_active_enter}} -gt 18 ]]; then
    printf 'SCOPE_SKIPPED %s probe_failed\\n' "$_unit"
    return 1
  fi
  if ! IFS= read -r _uptime < "$SCOPE_UPTIME_PATH"; then
    printf 'SCOPE_SKIPPED %s probe_failed\\n' "$_unit"
    return 1
  fi
  _now=${{_uptime%%.*}}
  if [[ ! $_now =~ ^[0-9]+$ ]]; then
    printf 'SCOPE_SKIPPED %s probe_failed\\n' "$_unit"
    return 1
  fi
  while [[ ${{#_now}} -gt 1 && $_now == 0* ]]; do _now=${{_now:1}}; done
  _entered_seconds=$((_active_enter / 1000000))
  _age=$((_now - _entered_seconds))

  if ! _scope_parse_runtime "$_runtime_max"; then
    printf 'SCOPE_SKIPPED %s runtime_ceiling_malformed\\n' "$_unit"
    return 1
  fi
  _limit=$((_unit_limit + MARGIN_SECONDS))

  if [ "$_age" -lt "$_limit" ]; then
    printf 'SCOPE_SKIPPED %s young age=%s limit=%s reason=%s\\n' "$_unit" "$_age" "$_limit" "$_limit_reason"
    return 1
  fi
  return 0
}}

_scope_one() (
  _unit=$1
  _scope_probe_age || exit 0
  if _lane_occupant_live {quoted_key}; then
    printf 'SCOPE_SKIPPED %s occupant_live\\n' "$_unit"
    exit 0
  fi
  exec 9>>"$ROOT/.lane-lock-{key}"
  if ! flock -n 9; then
    printf 'SCOPE_SKIPPED %s held_lock\\n' "$_unit"
    exit 0
  fi
  exec 10>>"$ROOT/.lane-lease-{key}.lock"
  if ! flock -n 10; then
    printf 'SCOPE_SKIPPED %s held_lease\\n' "$_unit"
    exit 0
  fi
  if _lane_occupant_live {quoted_key}; then
    printf 'SCOPE_SKIPPED %s occupant_live\\n' "$_unit"
    exit 0
  fi
  # Dispatch creates/reuses these names under the lane lock. Recompute all
  # eligibility metadata while both locks are held so a replacement scope is
  # never stopped using the previous instance's age or runtime ceiling.
  _scope_probe_age || exit 0
  {stop_branch}
)

for _unit in "${{_scope_units[@]}}"; do
  _scope_one "$_unit"
done
"""


def _verdict_from_line(line: str, expected_units: frozenset[str]) -> ScopeVerdict | None:
    match = _INACTIVE_OUTPUT_RE.fullmatch(line)
    if match is not None:
        unit = match.group("unit")
        if unit not in expected_units:
            return None
        return ScopeVerdict(unit=unit, status="inactive", reason="inactive")

    match = _SIMPLE_OUTPUT_RE.fullmatch(line)
    if match is not None:
        unit = match.group("unit")
        if unit not in expected_units:
            return None
        detail = match.group("detail")
        if detail in {"occupant_live", "held_lock", "held_lease", "reap_lock_held"}:
            return ScopeVerdict(unit=unit, status="skipped_live", reason=detail)
        return ScopeVerdict(unit=unit, status="skipped_unsafe", reason=detail)

    match = _AGED_OUTPUT_RE.fullmatch(line)
    if match is None:
        return None
    unit = match.group("unit")
    if unit not in expected_units:
        return None
    kind = match.group("kind")
    if (kind == "SKIPPED") != (match.group("young") == "young"):
        return None
    reason = match.group("reason")
    if reason not in {"host_bound", "unit_ceiling"} and kind != "STOP_FAILED":
        return None
    kill_status = match.group("kill_status")
    stop_status = match.group("stop_status")
    if (kind == "STOP_FAILED") != (kill_status is not None):
        return None
    if kill_status is not None and (int(kill_status) > 255 or int(stop_status) > 255):
        return None
    try:
        age = int(match.group("age"))
        limit = int(match.group("limit"))
    except ValueError:
        return None
    max_shell_integer = (1 << 63) - 1
    if abs(age) > max_shell_integer or limit > max_shell_integer:
        return None
    if kind == "SKIPPED":
        return ScopeVerdict(unit=unit, status="skipped_young", reason=reason, age_seconds=age, limit_seconds=limit)
    if kind == "WOULD_STOP":
        return ScopeVerdict(unit=unit, status="dry_run", reason=reason, age_seconds=age, limit_seconds=limit)
    if kind == "STOPPED":
        return ScopeVerdict(unit=unit, status="stopped", reason=reason, age_seconds=age, limit_seconds=limit)
    return ScopeVerdict(
        unit=unit,
        status="stop_failed",
        reason=reason,
        age_seconds=age,
        limit_seconds=limit,
        kill_status=int(kill_status),
        stop_status=int(stop_status),
    )


def _parse_scope_output(units: tuple[str, ...], stdout: str, *, dry_run: bool) -> list[ScopeVerdict]:
    expected = frozenset(units)
    parsed: dict[str, ScopeVerdict] = {}
    malformed = False
    for line in stdout.splitlines():
        verdict = _verdict_from_line(line, expected)
        if verdict is None or verdict.unit in parsed:
            malformed = True
            break
        parsed[verdict.unit] = verdict
    if malformed:
        return _unsafe_verdicts(units, "unparsed_output")

    verdicts: list[ScopeVerdict] = []
    for unit in units:
        verdict = parsed.get(unit)
        if verdict is None:
            verdicts.append(ScopeVerdict(unit=unit, status="skipped_unsafe", reason="unparsed_output"))
        elif dry_run and verdict.status in {"stopped", "stop_failed"}:
            # A remote script mismatch must never report a mutation as a dry run.
            verdicts.append(ScopeVerdict(unit=unit, status="skipped_unsafe", reason="unparsed_output"))
        elif not dry_run and verdict.status == "dry_run":
            verdicts.append(ScopeVerdict(unit=unit, status="skipped_unsafe", reason="unparsed_output"))
        else:
            verdicts.append(verdict)
    return verdicts


def stop_remote_lane_scope(
    key: str,
    *,
    bound_seconds: int,
    ssh_runner: Callable[..., Any],
    dry_run: bool = True,
    sandbox_root: str | None = None,
    margin_seconds: int = SCOPE_STOP_MARGIN_SECONDS,
    timeout: float | None = None,
) -> ScopeStopReport:
    """Stop the over-age scope units for one validated lane key."""
    normalized_bound = bound_seconds if isinstance(bound_seconds, int) and not isinstance(bound_seconds, bool) else 0
    report = ScopeStopReport(key=key, dry_run=bool(dry_run), bound_seconds=normalized_bound, verdicts=[])

    if not _valid_key(key):
        report.error = "invalid_key"
        return report
    units = scope_units_for_key(key)
    if not _valid_bound(bound_seconds):
        report.error = "bound_invalid"
        report.verdicts = _unsafe_verdicts(units, "bound_invalid")
        return report
    if not _valid_margin(margin_seconds):
        report.error = "margin_invalid"
        report.verdicts = _unsafe_verdicts(units, "margin_invalid")
        return report
    if timeout is not None and (
        isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0
    ):
        report.error = "timeout_invalid"
        report.verdicts = _unsafe_verdicts(units, "timeout_invalid")
        return report
    resolved_root = _resolve_scope_root(sandbox_root)
    if resolved_root is None:
        report.error = "sandbox_root_invalid"
        report.verdicts = _unsafe_verdicts(units, "sandbox_root_invalid")
        return report

    try:
        script = build_scope_stop_script(
            resolved_root,
            key,
            bound_seconds=bound_seconds,
            margin_seconds=margin_seconds,
            dry_run=bool(dry_run),
        )

        def _timeout_required_runner(script: str, *, timeout: float | None) -> Any:
            result = ssh_runner(script, timeout=timeout)
            if isinstance(result, str):
                return result
            returncode = getattr(result, "returncode", None)
            if isinstance(returncode, bool) or not isinstance(returncode, int):
                raise ValueError("ssh runner returned an invalid exit status")
            return result

        output, ssh_error = _run_ssh(
            _timeout_required_runner,
            script,
            timeout=timeout if timeout is not None else SSH_TIMEOUT_S,
        )
    except Exception as exc:
        # Preserve the transport error in the report; callers must never mistake
        # an exception from the remote runner for a successful stop.
        report.error = f"ssh failed: {type(exc).__name__}: {exc}"
        report.verdicts = _unsafe_verdicts(units, "ssh_failed")
        return report

    if ssh_error is not None:
        report.error = ssh_error
        report.verdicts = _unsafe_verdicts(units, "ssh_failed")
        return report
    report.verdicts = _parse_scope_output(units, output, dry_run=bool(dry_run))
    return report
