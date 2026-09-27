"""Bounded read-only lane status snapshots built from the lane census."""

from __future__ import annotations

import json
import os
import subprocess
import threading
from collections.abc import Mapping, Sequence
from contextvars import copy_context
from pathlib import Path
from typing import Any

from workbay_orchestrator_mcp.orchestration import lane_census

DEFAULT_LIMIT = 20
MAX_LIMIT = 50
DEFAULT_BYTE_CAP = 24_576
MIN_BYTE_CAP = 1_024
MAX_BYTE_CAP = 65_536
CENSUS_CALL_TIMEOUT_S = 35.0
_CENSUS_SLOT = threading.BoundedSemaphore(1)


def _status_probe(lane: Mapping[str, Any]) -> tuple[float | None, bool, int, bool]:
    """Use the census probe protocol but fail closed on malformed counts."""
    if os.environ.get("PYTEST_CURRENT_TEST"):
        raise RuntimeError("default remote probe is forbidden under pytest")
    host = str(os.environ.get("WORKBAY_REMOTE_GATE_HOST") or "").strip()
    if not host:
        raise RuntimeError("remote_probe_unconfigured: WORKBAY_REMOTE_GATE_HOST is unset")
    branch = str(lane.get("branch") or lane.get("lane_id") or "")
    slug = lane_census.sandbox_slug(branch)
    agent_root = str(
        os.environ.get("WORKBAY_REMOTE_AGENT_ROOT") or os.environ.get("WORKBAY_REMOTE_GATE_DIR") or "src/.workbay-agent"
    )
    remote = f"""
set -euo pipefail
ROOT="$HOME/{agent_root}"
SBX="$ROOT/{slug}"
MARKER="$SBX/.workbay-lane-sandbox"
if [ -f "$MARKER" ]; then echo "marker_mtime=$(stat -c %Y "$MARKER")"; else echo "marker_mtime="; fi
{lane_census.live_process_probe_script(slug)}
if [ -d "$SBX/.git" ]; then
  base=$(git -C "$SBX" rev-list --max-parents=0 HEAD 2>/dev/null | tail -n 1 || true)
  if [ -n "$base" ]; then
    echo "commit_count=$(git -C "$SBX" rev-list --count "$base"..HEAD 2>/dev/null || echo 0)"
  else
    echo "commit_count=0"
  fi
  if git -C "$SBX" status --porcelain 2>/dev/null | grep -q .; then echo "dirty=1"; else echo "dirty=0"; fi
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
    marker_mtime = lane_census._optional_epoch(parsed.get("marker_mtime") or None)
    live_process = parsed.get("live_process") == "1"
    dirty = parsed.get("dirty") == "1"
    return marker_mtime, live_process, commit_count, dirty


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


def _lane_row(verdict: Any) -> dict[str, Any]:
    evidence = _get(verdict, "evidence", {})
    if not isinstance(evidence, Mapping):
        evidence = {}
    kind = _get(verdict, "kind")
    return {
        "lane_id": _bounded_text(_get(verdict, "lane_id"), 120),
        "dispatch_id": _bounded_text(_get(verdict, "dispatch_id"), 120),
        "status": _bounded_text(evidence.get("status"), 32),
        "verdict": _bounded_text(kind, 48),
        "repair": _bounded_text(_get(verdict, "repair"), 48) or None,
        "commit_count": _typed_commit_count(evidence, kind),
        "live_process": bool(evidence["live_process"]) if isinstance(evidence.get("live_process"), bool) else None,
        "dirty": bool(evidence["dirty"]) if isinstance(evidence.get("dirty"), bool) else None,
        "result_present": bool(evidence["result_present"])
        if isinstance(evidence.get("result_present"), bool)
        else None,
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


def _read_only_snapshot(task_ref: str | None, *, root: Path | str, limit: int) -> Any:
    """Build census-style verdicts from read-only row, artifact, and probe helpers."""
    resolved_task = str(task_ref or "").strip()
    rows = lane_census._default_list_rows(resolved_task or None)
    ordered = sorted(rows, key=lane_census._row_sort_key)
    prepared: list[tuple[Mapping[str, Any], dict[str, Any], Any]] = []
    workspace = Path(root)
    for row in ordered[:limit]:
        lane_id = str(row.get("lane_id") or "").strip()
        if not lane_id:
            continue
        artifacts = lane_census._default_list_dispatch_artifacts(workspace, lane_id)
        prepared.append((row, artifacts, lane_census._invoke_probe(_status_probe, row)))

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
