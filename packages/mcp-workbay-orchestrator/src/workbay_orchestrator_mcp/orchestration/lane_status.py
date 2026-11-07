"""Bounded read-only lane status snapshots built from the lane census.

``census_verdict`` is the lane census classifier applied to this reader's own probe, not the verdict of a census run:
for an absent sandbox the census probe fails, while this probe observes the absence. ``verdict`` equals
``census_verdict`` except that ``no_sandbox`` means classifier ``orphaned_sandbox`` with the sandbox observed absent
from every root the remote reaper checks.
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
from collections.abc import Callable, Mapping, Sequence
from contextvars import copy_context
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration import lane_census, remote_sandbox_reap

DEFAULT_LIMIT = 20
MAX_LIMIT = 50
DEFAULT_BYTE_CAP = 24_576
MIN_BYTE_CAP = 1_024
MAX_BYTE_CAP = 65_536
CENSUS_CALL_TIMEOUT_S = 35.0
VERDICT_NO_SANDBOX = "no_sandbox"
_CENSUS_SLOT = threading.BoundedSemaphore(1)


@dataclass(frozen=True)
class _StatusProbe(lane_census._ProbeOk):
    """Census probe fields plus whether the sandbox directory was observed: True, False, or None when unobserved."""

    sandbox_dir: bool | None = None


def _probe_roots() -> list[str]:
    """Sandbox roots in reaper order: the configured root when set, then the reaper's default roots."""
    configured = str(os.environ.get("WORKBAY_REMOTE_AGENT_ROOT") or "").strip()
    roots: list[str] = []
    if configured:
        resolved = lane_census._resolve_sandbox_root(configured)
        if resolved is None:
            raise RuntimeError("remote_probe_unconfigured: WORKBAY_REMOTE_AGENT_ROOT is empty or unsafe")
        roots.append(resolved)
    roots.extend(remote_sandbox_reap._DEFAULT_AGENT_ROOTS)
    return list(dict.fromkeys(roots))


def _status_probe(lane: Mapping[str, Any]) -> _StatusProbe:
    """Use the census probe protocol; unreadable Git metadata is a probe failure, never zero or clean."""
    if os.environ.get("PYTEST_CURRENT_TEST"):
        raise RuntimeError("default remote probe is forbidden under pytest")
    host = str(os.environ.get("WORKBAY_REMOTE_GATE_HOST") or "").strip()
    if not host:
        raise RuntimeError("remote_probe_unconfigured: WORKBAY_REMOTE_GATE_HOST is unset")
    branch = str(lane.get("branch") or lane.get("lane_id") or "")
    slug = lane_census.sandbox_slug(branch)
    quoted_roots = " ".join(lane_census._sh_single_quote(root) for root in _probe_roots())
    remote = f"""
set -euo pipefail
SBX=
FIRST=
FIRST_ROOT=
for ROOT in {quoted_roots}; do
  case "$ROOT" in /*) ;; *) ROOT="$HOME/$ROOT" ;; esac
  cand="$ROOT/{slug}"
  [ -n "$FIRST" ] || {{ FIRST="$cand"; FIRST_ROOT="$ROOT"; }}
  d="$cand"
  while [ ! -e "$d" ] && [ ! -L "$d" ] && [ "$d" != "/" ]; do d=$(dirname "$d"); done
  if [ "$d" = "$cand" ]; then
    if {{ ! [ -L "$cand" ] && ! [ -d "$cand" ]; }} || {{ [ -d "$cand" ] && [ -x "$cand" ] && [ -r "$cand" ] && ! [ -e "$cand/.git" ] && ! [ -f "$cand/.workbay-lane-sandbox" ]; }}; then continue; fi
    SBX="$cand"; break
  fi
  if ! [ -x "$d" ]; then echo "sandbox_inaccessible: ${{d#"$HOME"/}}" >&2; exit 3; fi
done
[ -n "$SBX" ] || {{ SBX="$FIRST"; ROOT="$FIRST_ROOT"; }}
MARKER="$SBX/.workbay-lane-sandbox"
if [ -L "$SBX" ] && ! [ -d "$SBX" ]; then echo "sandbox_inaccessible: ${{SBX#"$HOME"/}}" >&2; exit 3; fi
if [ -d "$SBX" ] && ! {{ [ -x "$SBX" ] && [ -r "$SBX" ]; }}; then echo "sandbox_inaccessible: ${{SBX#"$HOME"/}}" >&2; exit 3; fi
if [ -f "$MARKER" ]; then echo "marker_mtime=$(stat -c %Y "$MARKER")"; else echo "marker_mtime="; fi
{lane_census.live_process_probe_script(slug)}
if [ -d "$SBX" ] && {{ [ -e "$SBX/.git" ] || [ -f "$MARKER" ]; }}; then echo "sandbox_dir=1"; else echo "sandbox_dir=0"; fi
if [ -e "$SBX/.git" ]; then
  # An unborn HEAD is a readable repository with no commits; any other failing Git read is unknown evidence.
  if git -C "$SBX" rev-parse --verify -q HEAD >/dev/null 2>&1; then
    roots=$(git -C "$SBX" rev-list --max-parents=0 HEAD 2>/dev/null) || {{ echo "git_metadata_failed: root enumeration failed" >&2; exit 3; }}
    base=$(printf '%s\\n' "$roots" | tail -n 1)
    [ -n "$base" ] || {{ echo "git_metadata_failed: root commit is missing" >&2; exit 3; }}
    commit_count=$(git -C "$SBX" rev-list --count "$base"..HEAD 2>/dev/null) || {{ echo "git_metadata_failed: commit count failed" >&2; exit 3; }}
  else
    head_status=$?
    [ "$head_status" -eq 1 ] || {{ echo "git_metadata_failed: HEAD verification failed (exit $head_status)" >&2; exit 3; }}
    bare=$(git -C "$SBX" rev-parse --is-bare-repository 2>/dev/null) || {{ echo "git_metadata_failed: repository is unreadable" >&2; exit 3; }}
    [ "$bare" = false ] || {{ echo "git_metadata_failed: repository is not non-bare" >&2; exit 3; }}
    head_ref=$(git -C "$SBX" symbolic-ref -q HEAD 2>/dev/null) || {{ echo "git_metadata_failed: HEAD is unreadable" >&2; exit 3; }}
    case "$head_ref" in refs/heads/*) ;; *) echo "git_metadata_failed: HEAD is not a branch" >&2; exit 3 ;; esac
    git -C "$SBX" check-ref-format "$head_ref" >/dev/null 2>&1 || {{ echo "git_metadata_failed: HEAD branch is malformed" >&2; exit 3; }}
    if git -C "$SBX" show-ref --verify --quiet "$head_ref" 2>/dev/null; then
      echo "git_metadata_failed: HEAD branch exists but HEAD did not resolve" >&2; exit 3
    else
      ref_status=$?
      [ "$ref_status" -eq 1 ] || {{ echo "git_metadata_failed: HEAD branch lookup failed (exit $ref_status)" >&2; exit 3; }}
    fi
    commit_count=0
  fi
  status=$(git -C "$SBX" status --porcelain 2>/dev/null) || {{ echo "git_metadata_failed: status failed" >&2; exit 3; }}
  if [ -n "$status" ]; then dirty=1; else dirty=0; fi
  echo "commit_count=$commit_count"
  echo "dirty=$dirty"
else
  echo "commit_count=0"
  echo "dirty=0"
fi
"""
    proc = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, remote],
        capture_output=True,
        text=True,
        timeout=lane_census.PROBE_TIMEOUT_S,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip() or f"ssh exit {proc.returncode}"
        raise RuntimeError(f"remote_probe_failed: {detail}")
    parsed: dict[str, str] = {}
    for line in (proc.stdout or "").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed[key.strip()] = value.strip()
    try:
        commit_count = int(parsed["commit_count"])
    except (KeyError, TypeError, ValueError):
        raise ValueError("probe commit_count is not an int") from None
    if commit_count < 0:
        raise ValueError("probe commit_count is not a non-negative int")
    if parsed.get("dirty") not in {"0", "1"}:
        raise ValueError("probe dirty is not 0 or 1")
    marker_mtime = lane_census._optional_epoch(parsed.get("marker_mtime") or None)
    live_process = parsed.get("live_process") == "1"
    dirty = parsed["dirty"] == "1"
    sandbox_dir = {"1": True, "0": False}.get(parsed.get("sandbox_dir", ""))
    return _StatusProbe(
        marker_mtime=marker_mtime,
        live_process=live_process,
        commit_count=commit_count,
        dirty=dirty,
        sandbox_dir=sandbox_dir,
    )


def _get(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _bounded_text(value: Any, max_chars: int) -> str:
    if value is None:
        return ""
    return "".join(char for char in str(value) if char.isprintable())[:max_chars]


def _typed_commit_count(evidence: Mapping[str, Any], verdict_kind: Any) -> dict[str, Any]:
    raw = evidence.get("commit_count")
    if verdict_kind == lane_census.VERDICT_PROBE_FAILED or isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        return {"state": "unknown"}
    return {"state": "known", "value": raw}


def _sandbox_present(evidence: Mapping[str, Any]) -> bool | None:
    """Tri-state sandbox presence: True when observed, False only when the directory was observed absent, else None."""
    if evidence.get("sandbox_present") is True or evidence.get("sandbox_dir") is True or evidence.get("dirty") is True:
        return True
    raw = evidence.get("commit_count")
    if isinstance(raw, int) and not isinstance(raw, bool) and raw > 0:
        return True
    if evidence.get("sandbox_dir") is False:
        return False
    return None


def _lane_row(verdict: Any) -> dict[str, Any]:
    evidence = _get(verdict, "evidence", {})
    if not isinstance(evidence, Mapping):
        evidence = {}
    kind = _get(verdict, "kind")
    sandbox_present = None if kind == lane_census.VERDICT_PROBE_FAILED else _sandbox_present(evidence)
    verdict_label = _bounded_text(kind, 48)
    if kind == lane_census.VERDICT_ORPHANED_SANDBOX and sandbox_present is False:
        verdict_label = VERDICT_NO_SANDBOX
    return {
        "lane_id": _bounded_text(_get(verdict, "lane_id"), 120),
        "dispatch_id": _bounded_text(_get(verdict, "dispatch_id"), 120),
        "status": _bounded_text(evidence.get("status"), 32),
        "verdict": verdict_label,
        "census_verdict": _bounded_text(evidence.get("census_verdict"), 48) or None,
        "repair": _bounded_text(_get(verdict, "repair"), 48) or None,
        "commit_count": _typed_commit_count(evidence, kind),
        "live_process": bool(evidence["live_process"]) if isinstance(evidence.get("live_process"), bool) else None,
        "dirty": bool(evidence["dirty"]) if isinstance(evidence.get("dirty"), bool) else None,
        "result_present": bool(evidence["result_present"])
        if isinstance(evidence.get("result_present"), bool)
        else None,
        "sandbox_present": sandbox_present,
    }


def _byte_size(payload: Mapping[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _clamp_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        value = default
    return min(maximum, max(minimum, value))


def _base_payload(task_ref: Any, limit: int, byte_cap: int) -> dict[str, Any]:
    return {
        "ok": True,
        "task_ref": _bounded_text(task_ref, 128) or None,
        "limit": limit,
        "byte_cap": byte_cap,
        "returned": 0,
        "has_more": False,
        "truncated": False,
        "lanes": [],
    }


def _newest_first_key(row: Mapping[str, Any]) -> tuple[float, str]:
    """Keep the bounded window on the newest rows; ``lane_id`` ascending breaks ties."""
    updated = lane_census._optional_epoch(row.get("updated_at"))
    return (-(updated or 0.0), str(row.get("lane_id") or ""))


def _sibling_population(resolved_task: str, ordered: list[dict[str, Any]]) -> list[dict[str, Any]] | None:
    """Attribution needs every task's rows, as census does; a failed all-task read is unknown, never empty."""
    if not resolved_task:
        return ordered
    try:
        return [dict(row) for row in lane_census._default_list_rows(None)]
    except Exception:  # noqa: BLE001 - fail closed on unavailable sibling evidence
        return None


def _recording_probe(
    observed_dirs: dict[lane_census.RowIdentity, bool], identity: lane_census.RowIdentity
) -> Callable[[Mapping[str, Any]], Any]:
    """Run the status probe and note, under this row's full identity, an explicit sandbox-directory observation."""

    def _probe(lane: Mapping[str, Any]) -> Any:
        probe = _status_probe(lane)
        sandbox_dir = getattr(probe, "sandbox_dir", None)
        if isinstance(sandbox_dir, bool):
            observed_dirs[identity] = sandbox_dir
        return probe

    return _probe


def _read_only_snapshot(task_ref: str | None, *, root: Path | str, limit: int) -> Any:
    """Build census-style verdicts from read-only row, artifact, and probe helpers."""
    resolved_task = str(task_ref or "").strip()
    rows = lane_census._default_list_rows(resolved_task or None)
    ordered = sorted(rows, key=_newest_first_key)
    sibling_rows = _sibling_population(resolved_task, ordered)
    prepared: list[tuple[Mapping[str, Any], dict[str, Any], Any]] = []
    observed_dirs: dict[lane_census.RowIdentity, bool] = {}
    workspace = Path(root)
    for row in ordered[:limit]:
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id:
            continue
        identity = lane_census._row_identity(row, resolved_task)
        artifacts = lane_census._default_list_dispatch_artifacts(
            workspace,
            lane_id,
            task_ref=lane_census._row_task_ref(row, resolved_task),
            dispatch_id=str(row.get("dispatch_id") or "").strip(),
            pass_id=str(row.get("pass_id") or "").strip(),
            branch=str(row.get("branch") or "").strip(),
            created_at=row.get("created_at"),
            sibling_rows=sibling_rows,
        )
        prepared.append((row, artifacts, lane_census._invoke_probe(_recording_probe(observed_dirs, identity), row)))

    now = lane_census._epoch(None)
    _running_ids, superseded_ids = lane_census._running_and_superseded(
        prepared,
        now=now,
        task_ref=resolved_task,
    )
    verdicts = []
    for row, artifacts, probe_result in prepared:
        identity = lane_census._row_identity(row, resolved_task)
        verdict = lane_census._classify(
            row=row,
            artifacts=artifacts,
            probe_result=probe_result,
            superseded=identity in superseded_ids,
            now=now,
        )
        evidence = dict(verdict.evidence)
        evidence["status"] = str(row.get("status") or "")
        evidence["task_ref"] = lane_census._row_task_ref(row, resolved_task)
        evidence["branch_tip_sha"] = row.get("branch_tip_sha")
        evidence["updated_at"] = row.get("updated_at")
        evidence["result_present"] = bool(artifacts.get("result_present"))
        evidence["turn_patch_size"] = lane_census._as_int(artifacts.get("turn_patch_size"), default=0)
        if "tool_call_count" not in evidence:
            evidence["tool_call_count"] = artifacts.get("tool_call_count")
        evidence["census_verdict"] = verdict.kind
        evidence["sandbox_dir"] = observed_dirs.get(identity)
        repair = verdict.repair
        if str(row.get("status") or "") in lane_census.CENSUS_SINK_STATUSES:
            repair = None
        verdicts.append(
            lane_census.LaneCensusVerdict(
                lane_id=verdict.lane_id,
                dispatch_id=verdict.dispatch_id,
                kind=verdict.kind,
                evidence=evidence,
                repair=repair,
                applied=False,
            )
        )
    return {
        "verdicts": verdicts,
        "truncated": len(ordered) > len(prepared),
        "attribution_unavailable": bool(resolved_task) and sibling_rows is None,
    }


def _error_payload(task_ref: Any, limit: int, byte_cap: int, error_code: str) -> dict[str, Any]:
    payload = _base_payload(task_ref, limit, byte_cap)
    payload.update({"ok": False, "error_code": error_code, "field": "census"})
    return payload


def lane_status(
    task_ref: str | None = None,
    *,
    root: Path | str,
    limit: int = DEFAULT_LIMIT,
    byte_cap: int = DEFAULT_BYTE_CAP,
) -> dict[str, Any]:
    """Return at most ``limit`` census-style rows under a clamped JSON byte budget.

    Read-only census helpers classify each row without calling the census
    runner, which advances its persistent observation window. A single daemon
    worker and semaphore bound timed-out remote probes.
    """
    bounded_limit = _clamp_int(limit, default=DEFAULT_LIMIT, minimum=1, maximum=MAX_LIMIT)
    bounded_byte_cap = _clamp_int(byte_cap, default=DEFAULT_BYTE_CAP, minimum=MIN_BYTE_CAP, maximum=MAX_BYTE_CAP)
    if not _CENSUS_SLOT.acquire(blocking=False):
        return _error_payload(task_ref, bounded_limit, bounded_byte_cap, "lane_census_busy")

    completed: dict[str, Any] = {}

    def _run_snapshot() -> None:
        try:
            completed["report"] = _read_only_snapshot(task_ref, root=root, limit=bounded_limit)
        except Exception as exc:  # noqa: BLE001 - keep internal details out of the MCP response
            completed["error"] = type(exc).__name__
        finally:
            _CENSUS_SLOT.release()

    worker = threading.Thread(
        target=copy_context().run,
        args=(_run_snapshot,),
        name="mcp-lane-status-snapshot",
        daemon=True,
    )
    worker.start()
    worker.join(CENSUS_CALL_TIMEOUT_S)
    if worker.is_alive():
        return _error_payload(task_ref, bounded_limit, bounded_byte_cap, "lane_census_timeout")
    if "error" in completed:
        return _error_payload(task_ref, bounded_limit, bounded_byte_cap, "lane_census_failed")

    report = completed.get("report")
    if _get(report, "list_error"):
        return _error_payload(task_ref, bounded_limit, bounded_byte_cap, "lane_census_failed")
    raw_verdicts = _get(report, "verdicts", ())
    if not isinstance(raw_verdicts, Sequence) or isinstance(raw_verdicts, (str, bytes)):
        return _error_payload(task_ref, bounded_limit, bounded_byte_cap, "lane_census_failed")

    rows = [_lane_row(verdict) for verdict in raw_verdicts]
    upstream_truncated = bool(_get(report, "truncated", False)) or len(rows) > bounded_limit
    payload = _base_payload(task_ref, bounded_limit, bounded_byte_cap)
    if _get(report, "attribution_unavailable") is True:
        # The all-task sibling read failed: untagged run evidence was left unattributed, not observed absent.
        payload["warnings"] = ["attribution_unavailable"]
    omitted_for_bytes = False
    for row in rows[:bounded_limit]:
        payload["lanes"].append(row)
        if _byte_size(payload) > bounded_byte_cap:
            payload["lanes"].pop()
            omitted_for_bytes = True
            break
    payload["returned"] = len(payload["lanes"])
    payload["has_more"] = upstream_truncated or omitted_for_bytes
    payload["truncated"] = payload["has_more"]
    while _byte_size(payload) > bounded_byte_cap and payload["lanes"]:
        payload["lanes"].pop()
        omitted_for_bytes = True
        payload["returned"] = len(payload["lanes"])
        payload["has_more"] = upstream_truncated or omitted_for_bytes
        payload["truncated"] = payload["has_more"]
    return payload
