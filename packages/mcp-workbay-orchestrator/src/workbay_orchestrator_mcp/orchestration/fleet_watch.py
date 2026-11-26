"""Lightweight, stdlib-only live terminal dashboard for ``workbay fleet watch``.

Reads directly from ``fleet_snapshot.build_snapshot`` without maintaining a
second state store or writing cache files (Canon v0.26.2 [DATA-14] No dual
writes, lexicons/engineering.md#data-14, Designing Data-Intensive Applications,
ch. 11).
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, TextIO

from .fleet_snapshot import build_snapshot

ALT_SCREEN_ENTER = "\x1b[?1049h\x1b[?25l"
ALT_SCREEN_CLEAR = "\x1b[H\x1b[2J"
ALT_SCREEN_EXIT = "\x1b[?25h\x1b[?1049l"

DEFAULT_INTERVAL_S = 2.0
DEFAULT_WIDTH = 100
MIN_WIDTH = 40
MAX_LANE_ROWS = 40
MAX_BLOCKER_LINES = 10

_ACTIVE_PASS_STATUSES = frozenset({"running", "pending", "dispatching", "in_progress"})
_ACTIVE_LANE_STATUSES = frozenset({"active", "running", "in_progress", "blocked", "planned"})


def _positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a finite positive number") from exc
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return number


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def _finite_nonneg(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
        return float(value)
    return None


def _parse_timestamp(value: Any) -> float | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).timestamp()
        except (ValueError, OverflowError):
            return None
    return None


def _format_utc(value: Any) -> tuple[str, float]:
    if isinstance(value, str) and value.strip():
        parsed = _parse_timestamp(value)
        if parsed is not None:
            return value.strip(), parsed
        return value.strip(), time.time()
    epoch = _parse_timestamp(value)
    if epoch is None:
        epoch = time.time()
    iso = datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return iso, epoch


def _format_duration(seconds: float | None) -> str:
    if seconds is None or not math.isfinite(seconds) or seconds < 0:
        return "-"
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    minutes, sec = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}h"


def _format_bytes(num_bytes: float | None) -> str:
    if num_bytes is None or not math.isfinite(num_bytes) or num_bytes < 0:
        return "unavailable"
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(num_bytes)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)}B"
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{value:.1f}TiB"


def _truncate(text: str, width: int) -> str:
    clean = " ".join(str(text).replace("\r", " ").replace("\n", " ").split())
    if width <= 0:
        return ""
    if len(clean) <= width:
        return clean
    if width <= 3:
        return clean[:width]
    return clean[: width - 3] + "..."


def _fit_line(text: str, width: int) -> str:
    return _truncate(text, width)


def _pack_segments(segments: list[str], width: int, sep: str = " | ") -> list[str]:
    """Pack ``segments`` onto one or more lines, each bounded to ``width`` columns."""
    packed: list[str] = []
    current = ""
    for raw in segments:
        seg = " ".join(str(raw).replace("\r", " ").replace("\n", " ").split())
        if not seg:
            continue
        if not current:
            current = _truncate(seg, width)
        elif len(current) + len(sep) + len(seg) <= width:
            current = f"{current}{sep}{seg}"
        else:
            packed.append(current)
            current = _truncate(seg, width)
    if current:
        packed.append(current)
    return packed or [""]


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _collect_plan_blockers(snapshot: Mapping[str, Any]) -> tuple[dict[tuple[str, str], list[str]], list[str]]:
    by_lane: dict[tuple[str, str], list[str]] = {}
    all_blockers: list[str] = []
    plan = _as_dict(snapshot.get("plan"))
    for task in _as_list(plan.get("tasks")):
        task_dict = _as_dict(task)
        task_ref = str(task_dict.get("task_ref") or "unknown")
        for node in _as_list(task_dict.get("nodes")):
            node_dict = _as_dict(node)
            lane_id = str(node_dict.get("lane_id") or "")
            item_id = str(node_dict.get("plan_item_id") or "")
            reasons: list[str] = []
            for key in ("blockers", "inconsistency_reasons"):
                for item in _as_list(node_dict.get(key)):
                    if isinstance(item, str) and item.strip():
                        reasons.append(item.strip())
            if reasons:
                by_lane.setdefault((task_ref, lane_id), []).extend(reasons)
                prefix = f"{task_ref}/{lane_id or item_id or 'plan'}"
                for reason in reasons:
                    all_blockers.append(f"{prefix}: {reason}")
    return by_lane, all_blockers


def _collect_alerts(snapshot: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    """Return ``(hard_blockers, soft_alerts)`` deduplicated in stable order."""
    hard: list[str] = []
    soft: list[str] = []

    admission = _as_dict(snapshot.get("admission"))
    decision = str(admission.get("decision") or "").lower()
    if decision in ("refuse", "defer"):
        reason = admission.get("reason") or "unspecified"
        dim = admission.get("reason_code")
        suffix = f" ({dim})" if dim else ""
        hard.append(f"admission {decision}: {reason}{suffix}")

    host = _as_dict(snapshot.get("host"))
    for disk in _as_list(host.get("disk")):
        disk_dict = _as_dict(disk)
        policy = _as_dict(disk_dict.get("policy"))
        effect = str(policy.get("effect") or "none").lower()
        if effect in ("refusal", "warning"):
            kind = disk_dict.get("kind") or "disk"
            free_str = _format_bytes(_finite_nonneg(disk_dict.get("free_bytes")))
            floor = policy.get("threshold_gib")
            floor_str = f" floor={floor}GiB" if floor is not None else ""
            msg = f"host disk {kind} {effect}: free={free_str}{floor_str}"
            if effect == "refusal":
                hard.append(msg)
            else:
                soft.append(msg)

    vm = _as_dict(snapshot.get("vm"))
    reclaim = _as_dict(vm.get("reclaim"))
    for alarm in _as_list(reclaim.get("alarms")):
        if isinstance(alarm, str) and alarm.strip():
            hard.append(f"vm reclaim alarm: {alarm.strip()}")

    _, plan_blockers = _collect_plan_blockers(snapshot)
    hard.extend(plan_blockers)

    for lane in _as_list(snapshot.get("lanes")):
        lane_dict = _as_dict(lane)
        if str(lane_dict.get("status") or "").lower() == "blocked":
            t_ref = lane_dict.get("task_ref") or "unknown"
            l_id = lane_dict.get("lane_id") or "unknown"
            hard.append(f"lane {t_ref}/{l_id}: blocked")

    limits_hit = [str(item) for item in _as_list(snapshot.get("limits_hit")) if isinstance(item, str) and item.strip()]
    if limits_hit:
        soft.append("partial limits: " + ", ".join(limits_hit))
    elif snapshot.get("partial") is True:
        soft.append("partial snapshot: incomplete observations")

    def _dedup(items: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for item in items:
            if item not in seen:
                seen.add(item)
                out.append(item)
        return out

    return _dedup(hard), _dedup(soft)


def _extract_vm_metrics(
    snapshot: Mapping[str, Any],
    vm_stats: Mapping[str, Any] | None,
) -> dict[str, str]:
    vm = _as_dict(snapshot.get("vm"))
    override = _as_dict(vm_stats)

    vm_state = str(override.get("state") or vm.get("state") or "unknown").upper()

    # CPU load
    load = override.get("load") if "load" in override else override.get("cpu_load", vm.get("load"))
    cpu_text = "unavailable"
    if isinstance(load, (list, tuple)) and len(load) >= 1 and all(_finite_nonneg(x) is not None for x in load[:3]):
        vals = [float(x) for x in load[:3]]
        cpu_text = "/".join(f"{v:.2f}" for v in vals)
    elif _finite_nonneg(load) is not None:
        cpu_text = f"{float(load):.2f}"
    psi_cpu = _as_dict(_as_dict(vm.get("psi")).get("cpu")).get("some")
    psi_avg10 = _finite_nonneg(_as_dict(psi_cpu).get("avg10"))
    if psi_avg10 is not None:
        cpu_text = f"{cpu_text} (psi {psi_avg10:.1f}%)" if cpu_text != "unavailable" else f"psi {psi_avg10:.1f}%"

    # RAM available / total
    ram_avail = _finite_nonneg(
        override.get("ram_available_bytes", override.get("mem_available_bytes", vm.get("mem_available_bytes")))
    )
    ram_total = _finite_nonneg(
        override.get("ram_total_bytes", override.get("mem_total_bytes", vm.get("mem_total_bytes")))
    )
    if ram_avail is not None and ram_total is not None:
        ram_text = f"{_format_bytes(ram_avail)}/{_format_bytes(ram_total)}"
    elif ram_avail is not None:
        ram_text = f"{_format_bytes(ram_avail)} avail"
    else:
        ram_text = "unavailable"

    # Disk free / used
    disk_free = _finite_nonneg(override.get("disk_free_bytes"))
    disk_used = _finite_nonneg(override.get("disk_used_bytes"))
    if disk_free is None and disk_used is None:
        mounts = _as_list(vm.get("df"))
        primary = None
        for row in mounts:
            row_dict = _as_dict(row)
            if row_dict.get("mount") in ("/", "/home"):
                primary = row_dict
                if row_dict.get("mount") == "/":
                    break
        if primary is None and mounts:
            primary = _as_dict(mounts[0])
        if primary:
            disk_free = _finite_nonneg(primary.get("available_bytes"))
            disk_used = _finite_nonneg(primary.get("used_bytes"))
    if disk_free is not None and disk_used is not None:
        disk_text = f"{_format_bytes(disk_free)} free / {_format_bytes(disk_used)} used"
    elif disk_free is not None:
        disk_text = f"{_format_bytes(disk_free)} free"
    elif disk_used is not None:
        disk_text = f"{_format_bytes(disk_used)} used"
    else:
        disk_text = "unavailable"

    # Active workers
    active_workers_override = override.get("active_workers")
    if isinstance(active_workers_override, int) and not isinstance(active_workers_override, bool):
        workers_text = str(max(0, active_workers_override))
    else:
        active_passes = sum(
            1
            for p in _as_list(snapshot.get("passes"))
            if str(_as_dict(p).get("status") or "").lower() in _ACTIVE_PASS_STATUSES
        )
        vm_jobs_state = vm.get("jobs_state")
        vm_jobs = _as_list(vm.get("jobs"))
        vm_procs_state = vm.get("processes_state")
        vm_procs = _as_list(vm.get("processes"))
        if vm_jobs_state == "known":
            workers_text = f"{active_passes} active ({len(vm_jobs)} vm jobs)"
        elif vm_procs_state == "known":
            workers_text = f"{active_passes} active ({len(vm_procs)} vm procs)"
        else:
            workers_text = f"{active_passes} active"

    return {
        "state": vm_state,
        "cpu": cpu_text,
        "ram": ram_text,
        "disk": disk_text,
        "workers": workers_text,
    }


def _extract_host_line(snapshot: Mapping[str, Any]) -> str | None:
    admission = _as_dict(snapshot.get("admission"))
    adm_snap = _as_dict(admission.get("snapshot"))
    host = _as_dict(snapshot.get("host"))
    if not admission and not adm_snap and not host:
        return None

    decision = str(admission.get("decision") or "unknown").upper()
    avail = _finite_nonneg(adm_snap.get("available_ram"))
    total = _finite_nonneg(adm_snap.get("total_ram"))
    pressure = str(adm_snap.get("pressure") or "unknown")
    if avail is not None and total is not None:
        ram_str = f"{_format_bytes(avail)}/{_format_bytes(total)} ({pressure})"
    elif avail is not None:
        ram_str = f"{_format_bytes(avail)} ({pressure})"
    else:
        ram_str = "unavailable"

    repo_disk = None
    for row in _as_list(host.get("disk")):
        row_dict = _as_dict(row)
        if row_dict.get("kind") == "repository":
            repo_disk = row_dict
            break
    if repo_disk:
        free_b = _finite_nonneg(repo_disk.get("free_bytes"))
        total_b = _finite_nonneg(repo_disk.get("total_bytes"))
        if free_b is not None and total_b is not None:
            disk_str = f"{_format_bytes(free_b)}/{_format_bytes(total_b)}"
        elif free_b is not None:
            disk_str = f"{_format_bytes(free_b)} free"
        else:
            disk_str = "unavailable"
    else:
        disk_str = "unavailable"

    held = admission.get("held_slots")
    width_slots = admission.get("width")
    if isinstance(held, int) and isinstance(width_slots, int):
        slots_str = f"{held}/{width_slots}"
    elif isinstance(held, int):
        slots_str = str(held)
    else:
        slots_str = "unavailable"

    return f"Host [{decision}] | RAM: {ram_str} | Disk: {disk_str} | Slots: {slots_str}"


def _build_lane_rows(snapshot: Mapping[str, Any], now_epoch: float) -> list[dict[str, Any]]:
    plan_blockers_by_lane, _ = _collect_plan_blockers(snapshot)

    passes_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for raw_pass in _as_list(snapshot.get("passes")):
        p = _as_dict(raw_pass)
        key = (str(p.get("task_ref") or ""), str(p.get("lane_id") or ""))
        passes_by_key.setdefault(key, []).append(p)

    def _pass_sort_key(p: dict[str, Any]) -> float:
        return (
            _parse_timestamp(p.get("started_at"))
            or _parse_timestamp(p.get("observed_at"))
            or 0.0
        )

    def _backend_model(p: dict[str, Any] | None) -> str:
        if not p:
            return "-"
        backend = str(p.get("backend") or "").strip()
        routing = _as_dict(_as_dict(p.get("routing")).get("routing_effective"))
        model = str(routing.get("model") or p.get("model") or "").strip()
        if backend and model:
            return f"{backend}/{model}"
        return backend or model or "-"

    def _pass_age(p: dict[str, Any] | None, lane: dict[str, Any] | None) -> str:
        if lane:
            age = _finite_nonneg(lane.get("last_updated_age_seconds"))
            if age is not None:
                return _format_duration(age)
            updated = _parse_timestamp(lane.get("updated_at"))
            if updated is not None:
                return _format_duration(max(0.0, now_epoch - updated))
        if p:
            started = _parse_timestamp(p.get("started_at"))
            if started is not None:
                terminal = _parse_timestamp(p.get("terminal_at"))
                end = terminal if terminal is not None else now_epoch
                return _format_duration(max(0.0, end - started))
            phases = _as_dict(p.get("phases"))
            obs_totals = _as_dict(phases.get("observed_totals"))
            vals = [v for v in (_finite_nonneg(x) for x in obs_totals.values()) if v is not None]
            if vals:
                return _format_duration(sum(vals))
        return "-"

    rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()

    for raw_lane in _as_list(snapshot.get("lanes")):
        lane = _as_dict(raw_lane)
        task_ref = str(lane.get("task_ref") or "-")
        lane_id = str(lane.get("lane_id") or "-")
        key = (task_ref if task_ref != "-" else "", lane_id if lane_id != "-" else "")
        seen_keys.add(key)

        lane_passes = sorted(passes_by_key.get(key, []), key=_pass_sort_key)
        latest_pass = lane_passes[-1] if lane_passes else None

        lane_status = str(lane.get("status") or "unknown")
        pass_status = str(latest_pass.get("status") or "") if latest_pass else ""
        if pass_status and pass_status != lane_status:
            status_text = f"{lane_status}/{pass_status}"
        else:
            status_text = lane_status

        mode = str(latest_pass.get("remote_exec_mode") or "-") if latest_pass else "-"
        blockers = list(plan_blockers_by_lane.get((task_ref, lane_id), []))
        if lane_status.lower() == "blocked" and not blockers:
            blockers.append("blocked")
        if latest_pass and latest_pass.get("reason"):
            blockers.append(str(latest_pass["reason"]))

        priority = 0 if (lane_status.lower() in _ACTIVE_LANE_STATUSES or pass_status.lower() in _ACTIVE_PASS_STATUSES) else 1
        rows.append(
            {
                "priority": priority,
                "task_ref": task_ref,
                "lane_id": lane_id,
                "status": status_text,
                "mode": mode,
                "backend_model": _backend_model(latest_pass),
                "age": _pass_age(latest_pass, lane),
                "blockers": "; ".join(dict.fromkeys(blockers)) if blockers else "-",
            }
        )

    for key, plist in passes_by_key.items():
        if key in seen_keys:
            continue
        latest_pass = sorted(plist, key=_pass_sort_key)[-1]
        task_ref = str(latest_pass.get("task_ref") or "-")
        lane_id = str(latest_pass.get("lane_id") or latest_pass.get("pass_id") or "-")
        status_text = str(latest_pass.get("status") or "unknown")
        mode = str(latest_pass.get("remote_exec_mode") or "-")
        blockers = list(plan_blockers_by_lane.get((task_ref, lane_id), []))
        if latest_pass.get("reason"):
            blockers.append(str(latest_pass["reason"]))
        priority = 0 if status_text.lower() in _ACTIVE_PASS_STATUSES else 1
        rows.append(
            {
                "priority": priority,
                "task_ref": task_ref,
                "lane_id": lane_id,
                "status": status_text,
                "mode": mode,
                "backend_model": _backend_model(latest_pass),
                "age": _pass_age(latest_pass, None),
                "blockers": "; ".join(dict.fromkeys(blockers)) if blockers else "-",
            }
        )

    rows.sort(key=lambda r: (r["priority"], r["task_ref"], r["lane_id"]))
    return rows


def _column_widths(total_width: int) -> list[int]:
    """Allocate 7 column widths separated by single spaces summing to ``<= total_width``."""
    # Columns: TASK, LANE, STATUS, MODE, BACKEND/MODEL, AGE, BLOCKERS
    # 6 single-space separators -> available = total_width - 6
    avail = max(20, total_width - 6)
    weights = (0.14, 0.14, 0.14, 0.10, 0.20, 0.08, 0.20)
    mins = (6, 6, 6, 4, 8, 4, 6)
    widths = [max(m, int(avail * w)) for m, w in zip(mins, weights)]
    # Adjust to fit exactly within avail
    while sum(widths) > avail:
        idx = max(range(len(widths)), key=lambda i: widths[i] - mins[i])
        if widths[idx] <= mins[idx]:
            idx = max(range(len(widths)), key=lambda i: widths[i])
        if widths[idx] <= 1:
            break
        widths[idx] -= 1
    rem = avail - sum(widths)
    if rem > 0:
        widths[-1] += rem
    return widths


def _format_table_row(values: list[str], widths: list[int], total_width: int) -> str:
    cells = [_truncate(val, w).ljust(w) for val, w in zip(values, widths)]
    line = " ".join(cells).rstrip()
    return line[:total_width]


def render_watch_frame(
    snapshot: Mapping[str, Any] | None,
    *,
    width: int = DEFAULT_WIDTH,
    timestamp: str | float | int | None = None,
    interval_s: float = DEFAULT_INTERVAL_S,
    vm_stats: Mapping[str, Any] | None = None,
) -> str:
    """Render one complete, bounded terminal dashboard frame from a fleet snapshot.

    Pure function: performs no I/O and tolerates empty or partially degraded snapshots.
    """
    cols = max(MIN_WIDTH, int(width) if isinstance(width, int) and width > 0 else DEFAULT_WIDTH)
    snap = _as_dict(snapshot)
    ts_str, now_epoch = _format_utc(timestamp)

    root_raw = snap.get("root")
    workspace = Path(str(root_raw)).name if isinstance(root_raw, str) and root_raw.strip() else "unknown"
    if not workspace:
        workspace = str(root_raw) if root_raw else "unknown"

    snap_state = str(snap.get("state") or "unknown").lower()
    observed_epoch = _parse_timestamp(snap.get("observed_at"))
    age_str = _format_duration(max(0.0, now_epoch - observed_epoch)) if observed_epoch is not None else "unknown"

    hard_blockers, soft_alerts = _collect_alerts(snap)
    all_alerts = [*hard_blockers, *soft_alerts]
    if hard_blockers:
        health = "BLOCKED"
    elif snap.get("partial") is True or snap_state in ("unknown", "partial", "stale"):
        health = "DEGRADED"
    else:
        health = "HEALTHY"

    rule = "-" * cols
    lines: list[str] = []

    # 1. Header bar
    lines.extend(
        _pack_segments(
            [
                f"WORKBAY FLEET WATCH | {workspace}",
                ts_str,
                f"every {interval_s:g}s",
                f"snap={snap_state} ({age_str})",
                f"{health} (blockers={len(hard_blockers)})",
            ],
            cols,
        )
    )
    lines.append(rule)

    # 2. VM / Host Resource Strip
    vm_metrics = _extract_vm_metrics(snap, vm_stats)
    lines.extend(
        _pack_segments(
            [
                f"VM [{vm_metrics['state']}]",
                f"CPU: {vm_metrics['cpu']}",
                f"RAM: {vm_metrics['ram']}",
                f"Disk: {vm_metrics['disk']}",
                f"Workers: {vm_metrics['workers']}",
            ],
            cols,
        )
    )
    host_line = _extract_host_line(snap)
    if host_line:
        lines.extend(_pack_segments(host_line.split(" | "), cols))
    lines.append(rule)

    # 3. Active & Recent Lanes / Workers Table
    col_widths = _column_widths(cols)
    headers = ["TASK", "LANE", "STATUS", "MODE", "BACKEND/MODEL", "AGE", "BLOCKERS"]
    lines.append(_format_table_row(headers, col_widths, cols))
    rows = _build_lane_rows(snap, now_epoch)
    if not rows:
        lines.append(_fit_line("(no active or recent lanes)", cols))
    else:
        for row in rows[:MAX_LANE_ROWS]:
            lines.append(
                _format_table_row(
                    [
                        row["task_ref"],
                        row["lane_id"],
                        row["status"],
                        row["mode"],
                        row["backend_model"],
                        row["age"],
                        row["blockers"],
                    ],
                    col_widths,
                    cols,
                )
            )
        if len(rows) > MAX_LANE_ROWS:
            lines.append(_fit_line(f"... {len(rows) - MAX_LANE_ROWS} more lanes omitted", cols))
    lines.append(rule)

    # 4. Blockers / Alerts Footer
    if not all_alerts:
        lines.append(_fit_line("Blockers / Alerts: none", cols))
    else:
        lines.append(_fit_line(f"Blockers / Alerts ({len(all_alerts)}):", cols))
        for alert in all_alerts[:MAX_BLOCKER_LINES]:
            lines.append(_fit_line(f"  ! {alert}", cols))
        if len(all_alerts) > MAX_BLOCKER_LINES:
            lines.append(_fit_line(f"  ... and {len(all_alerts) - MAX_BLOCKER_LINES} more", cols))

    lines.append(_fit_line("Ctrl-C to exit | --once for single frame | --json for raw snapshot", cols))
    return "\n".join(lines)


def _detect_width() -> int:
    size = shutil.get_terminal_size(fallback=(DEFAULT_WIDTH, 24))
    return max(MIN_WIDTH, size.columns)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workbay fleet watch",
        description="Lightweight stdlib-only live terminal dashboard for WorkBay Fleet.",
    )
    parser.add_argument(
        "--interval",
        type=_positive_float,
        default=DEFAULT_INTERVAL_S,
        help="Poll interval in seconds (must be > 0; default: 2.0).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Render a single frame and exit 0 (ideal for pipes, tests, and CI).",
    )
    parser.add_argument(
        "--no-alt-screen",
        action="store_true",
        help="Render inline without ANSI alternate screen buffer.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Workspace root directory (default: current working directory).",
    )
    parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Emit raw snapshot JSON envelope (with --once, a single JSON object).",
    )
    parser.add_argument(
        "--width",
        type=_positive_int,
        default=None,
        help="Override terminal width in columns (default: auto-detect).",
    )
    parser.add_argument(
        "--vm",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Observe VM inventory (default on); --no-vm skips SSH.",
    )
    return parser


def run_watch(
    *,
    root: Path,
    interval_s: float = DEFAULT_INTERVAL_S,
    once: bool = False,
    no_alt_screen: bool = False,
    as_json: bool = False,
    width: int | None = None,
    vm: bool = True,
    stream: TextIO | None = None,
    snapshot_fn: Callable[..., dict[str, Any]] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    """Run the watch loop or single-shot render.

    Validates arguments before any snapshot reads (Canon v0.26.2 [AGT-21] Exit
    status is the outcome contract, lexicons/engineering.md#agt-21, The UNIX
    Programming Environment, ch. 5).
    """
    if not isinstance(interval_s, (int, float)) or isinstance(interval_s, bool) or not math.isfinite(interval_s) or interval_s <= 0:
        print("workbay fleet watch: --interval must be a finite positive number", file=sys.stderr)
        return 2
    if width is not None and (not isinstance(width, int) or isinstance(width, bool) or width <= 0):
        print("workbay fleet watch: --width must be a positive integer", file=sys.stderr)
        return 2

    out = sys.stdout if stream is None else stream
    fetcher = build_snapshot if snapshot_fn is None else snapshot_fn
    use_alt = (not no_alt_screen) and (not as_json) and (not once)

    if use_alt:
        out.write(ALT_SCREEN_ENTER)
        out.flush()
    try:
        while True:
            snapshot = fetcher(root, vm=vm)
            if as_json:
                out.write(json.dumps(snapshot, sort_keys=True, allow_nan=False) + "\n")
            else:
                cols = width if width is not None else _detect_width()
                frame = render_watch_frame(snapshot, width=cols, interval_s=float(interval_s))
                if use_alt:
                    out.write(ALT_SCREEN_CLEAR + frame + "\n")
                else:
                    out.write(frame + "\n")
            out.flush()
            if once:
                return 0
            sleep_fn(float(interval_s))
    except KeyboardInterrupt:
        return 0
    finally:
        if use_alt:
            out.write(ALT_SCREEN_EXIT)
            out.flush()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run_watch(
            root=args.root,
            interval_s=args.interval,
            once=args.once,
            no_alt_screen=args.no_alt_screen,
            as_json=args.as_json,
            width=args.width,
            vm=args.vm,
        )
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"workbay fleet watch: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
