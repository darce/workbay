"""The ``workbay fleet`` plugin: graphical observatory or explicit finite output."""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

from . import fleet_lineage
from .fleet_snapshot import _vm_unknown, build_lineage_tree, build_snapshot, read_snapshot_cache, write_snapshot_cache


def _positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return number


def _positive_integer(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def _tree_text(tree):
    """Render the bounded graph without implying an execution order or health.

    Quote producer fields as ASCII JSON to keep arbitrary IDs on one line.
    Reserve the final newline and, if needed, an explicit text truncation row.
    """
    lines = [
        f"Fleet tree {tree.get('state', 'unknown').upper()} partial={tree.get('partial', True)} "
        f"root_state={tree.get('root_state', 'unknown')} truncated={tree.get('truncated', False)} "
        f"limit={tree.get('limit', 'unknown')} byte_cap={tree['byte_cap']}",
        "cache=" + json.dumps(tree.get("cache", {}), sort_keys=True, allow_nan=False),
        "issues=" + json.dumps(tree.get("issues", []), sort_keys=True, allow_nan=False),
    ]
    lines.extend("node " + json.dumps(node, sort_keys=True, allow_nan=False) for node in tree["nodes"])
    for edge in tree["edges"]:
        lines.append(
            f"{json.dumps(edge['source'])} --{edge['relation']} inferred={edge['inferred']}--> "
            f"{json.dumps(edge['target'])}"
        )
    if len(("\n".join(lines) + "\n").encode()) <= tree["byte_cap"]:
        return "\n".join(lines)
    marker = "Text truncated=True (byte_cap)"
    retained, used = [], len(marker) + 1
    for line in lines:
        size = len(line.encode()) + 1
        if used + size > tree["byte_cap"]:
            break
        retained.append(line)
        used += size
    return "\n".join([*retained, marker])


def _tree_command(argv) -> int:
    parser = argparse.ArgumentParser(prog="workbay fleet tree", description="Bounded read-only lineage graph.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--json", action="store_true")
    mode.add_argument("--text", action="store_true", help="text graph (default)")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--root-id", help="observed node ID; follow outgoing containment and lineage edges")
    parser.add_argument("--limit", type=_positive_integer, default=200, help="node limit (clamped to 1–2000)")
    parser.add_argument(
        "--byte-cap", type=_positive_integer, default=65_536, help="output bytes (clamped to 1024–65536)"
    )
    args = parser.parse_args(argv)
    try:
        observed = fleet_lineage.read_lineage_snapshot(args.root)
        tree = fleet_lineage.bound_tree_bytes(
            build_lineage_tree(observed, root_id=args.root_id, limit=args.limit), args.byte_cap
        )
        # Compact JSON leaves room for print's newline within the producer's
        # bound, which counts the larger default-spaced ASCII JSON encoding.
        print(
            json.dumps(tree, sort_keys=True, allow_nan=False, separators=(",", ":")) if args.json else _tree_text(tree)
        )
        return 0  # A valid UNKNOWN/partial observation is a report, never authority.
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"workbay fleet tree: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


def _text(snapshot):
    admission = snapshot.get("admission", {})
    admission_reason = admission.get("reason") or "unknown"
    admission_dimension = admission.get("reason_code")
    lines = [
        f"Fleet {snapshot.get('state', 'unknown').upper()} partial={snapshot.get('partial', True)} "
        f"observed_at={snapshot.get('observed_at', 'UNKNOWN')}",
        f"Admission {admission.get('decision', 'unknown').upper()} "
        f"held={admission.get('held_slots')} stock={admission.get('stock', 'not_evaluated')} "
        f"reason={admission_reason}"
        + (f" dimension={admission_dimension}" if admission_dimension else ""),
        "TASK  LANE  PASS  STATUS  LINK  PHASES",
    ]
    seen = set()
    for row in snapshot.get("passes", []):
        seen.add((row.get("task_ref"), row.get("lane_id")))
        phase = row.get("phases", {})
        durations = phase.get("totals")
        rendered = "UNKNOWN" if phase.get("state") == "unknown" else json.dumps(durations, sort_keys=True)
        lines.append(
            "  ".join(str(row.get(k) or "UNKNOWN") for k in ("task_ref", "lane_id", "pass_id", "status", "link"))
            + "  "
            + rendered
        )
    for row in snapshot.get("lanes", []):
        if (row.get("task_ref"), row.get("lane_id")) not in seen:
            lines.append(f"{row['task_ref']}  {row['lane_id']}  UNKNOWN  {row['status']}  UNKNOWN  UNKNOWN")
    lines.append(f"Ledger {snapshot.get('ledger', {}).get('state', 'unknown').upper()}")
    spool = snapshot.get("spool", {})
    lines.append(f"Spool {spool.get('state', 'unknown').upper()} ({spool.get('quantity', 'lower_bound')})")
    for name, row in sorted(spool.get("families", {}).items()):
        lines.append(
            f"  {name}: entries={row['entries']} bytes={row['bytes']} {row['quantity']} "
            f"aged={row['aged_entries']} owner_unknown={row['ownership_unknown']}"
        )
    vm = snapshot.get("vm") or _vm_unknown()

    def measured(value):
        return "UNKNOWN" if value is None else json.dumps(value, sort_keys=True)

    lines.append(
        f"VM {vm.get('state', 'unknown').upper()} observed_at={vm.get('observed_at') or 'UNKNOWN'} "
        f"reason={vm.get('reason', 'none')} sandboxes={measured(vm.get('rows_emitted'))} "
        f"{vm.get('quantity', 'unknown')} truncated={vm.get('truncated', 'UNKNOWN')}"
    )
    psi = vm.get("psi") or {}
    axes = vm.get("axes") or {}
    for axis, label in (("cpu", "CPU"), ("memory", "Memory"), ("io", "IO")):
        source = axes.get(axis, {})
        extra = (
            " load=" + measured(vm.get("load"))
            if axis == "cpu"
            else " available_bytes=" + measured(vm.get("mem_available_bytes"))
            if axis == "memory"
            else ""
        )
        lines.append(
            f"  {label} {source.get('state', vm.get('state', 'unknown')).upper()} "
            f"PSI={measured(psi.get(axis))}{extra} source=/proc/pressure/{axis}"
        )
    lines.append(
        f"  Disk {axes.get('disk', {}).get('state', vm.get('state', 'unknown')).upper()} "
        f"mounts={measured(vm.get('df'))} source=df -P -B1"
    )
    processes = vm.get("processes", [])
    census = vm.get("process_census")
    process_count = (census.get("cwd_known") if census else
                     None if vm.get("processes_quantity", "unknown") == "unknown" else len(processes))
    process_line = f"  Processes={measured(process_count)} {vm.get('processes_quantity', 'unknown')}"
    if census:
        excluded = ",".join(f"{kind}:{census['excluded'][kind]}" for kind in (
            "user_manager", "ssh_session_daemon", "gpg_agent") if kind in census["excluded"])
        process_line += (f" scope=own_uid excluded={excluded or 'none'} "
                         f"exited={measured(census.get('exited_during_scan'))} "
                         f"foreign_uid_not_observed={measured(census.get('foreign_uid_tasks'))}")
    lines.append(process_line)
    reclaim = vm.get("reclaim") or {"state": "unknown", "reason": "not_reported"}
    if reclaim.get("state") != "known":
        lines.append(f"  Reclaimer UNKNOWN reason={reclaim.get('reason', 'UNKNOWN')}")
    else:
        age = reclaim.get("age_seconds")
        age_text = "UNKNOWN" if age is None else f"{int(age // 60)}m"
        counts = reclaim.get("counts") or {}
        held_counts = [count for kind, count in counts.items() if kind.startswith("hold_")]
        held = sum(held_counts) if held_counts and all(v is not None for v in held_counts) else None
        lines.append(
            f"  Reclaimer KNOWN outcome={reclaim.get('outcome') or 'UNKNOWN'} "
            f"finished_at={reclaim.get('finished_at') or 'UNKNOWN'} age={age_text} "
            f"freed={measured(reclaim.get('bytes_freed'))} reclaimed={measured(reclaim.get('reclaimed'))} "
            f"reclaimable={measured(counts.get('reclaimable'))} held={measured(held)} "
            f"alarms={','.join(reclaim.get('alarms') or []) or 'none'}"
        )
    for row in vm.get("sandboxes", []):
        lines.append(
            f"  {row['path']}: bytes={measured(row.get('bytes'))} "
            f"bytes_state={row.get('bytes_state', 'unknown')} "
            f"lock_holder_alive={row.get('lock_holder_alive', 'unknown')} "
            f"git_dirty={row.get('git_dirty', 'unknown')}"
        )
    if snapshot.get("limits_hit"):
        lines.append("Partial reasons: " + ", ".join(snapshot["limits_hit"]))
    return "\n".join(lines)


def _summarize_command(argv):
    from .fleet_snapshot import emit_summary_batch, import_summary_results

    parser = argparse.ArgumentParser(
        prog="workbay fleet summarize", description="Offline summary cache; no model calls."
    )
    parser.add_argument("--root", type=Path, default=Path.cwd())
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--emit-batch", type=Path)
    operation.add_argument("--import", dest="results", type=Path)
    args = parser.parse_args(argv)
    try:
        # Exact cached observation; do not refresh or infer missing work.
        snapshot = json.loads((args.root / ".task-state/fleet/snapshot.json").read_bytes())
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get("passes", []), list):
            raise ValueError("invalid snapshot")
        count = (
            emit_summary_batch(args.root, snapshot, args.emit_batch)
            if args.emit_batch
            else import_summary_results(args.root, snapshot, args.results)
        )
        print(json.dumps({"rows": count}))
        return 0
    except (OSError, ValueError, TypeError, KeyError) as exc:
        print(f"workbay fleet summarize: {exc}", file=sys.stderr)
        return 2


def _checklist_text(report):
    def quote(value):
        return json.dumps(value, ensure_ascii=True, allow_nan=False)

    lines = [
        f"Checklist {quote(report['subject'])} selection={report['selection']} "
        f"partial={report['partial']} pause_point={report['pause_point']}",
        "Observed checks",
    ]
    for row in report["rows"]:
        label = row["state"].upper()
        if row["freshness"] == "stale":
            label += " · STALE"
        lines.append(f"{row['row_id']} {label} reason={row['reason']}")
        if not row["checks"]:
            lines.append(
                f"  owner=validation_battery.run_battery evidence={quote('docs/contracts/harness-checklist-v1.md')}"
            )
        for check in row["checks"]:
            ev = check["evidence"]
            reference = "docs/contracts/harness-checklist-v1.md"
            binding = ""
            if ev:
                reference = ev.get("receipt_path") or (
                    f"test_result:{ev['verified_test_id']}"
                    if ev.get("verified_test_id")
                    else "workbay fleet checklist --task "
                    + quote(ev["scope"]["task_ref"])
                    + (" --lane " + quote(ev["scope"]["lane_id"]) if ev["scope"]["lane_id"] is not None else "")
                )
                if ev["binding"]["freshness"] == "stale":
                    binding = (
                        f" STALE measured_sha={ev['sha']} current_sha={ev['binding']['current_tip_sha']}"
                        f" measured_base={ev['base_sha']} current_base={ev['binding']['current_base_sha']}"
                    )
            lines.append(
                f"  {check['key']} {check['state'].upper()} owner={check['owner']} "
                f"reason={check['reason']} evidence={quote(reference)}{binding}"
            )
    lines.append("Claimed")
    for claim in report["claims"]:
        lines.append(
            f"  {claim['kind']} {quote(claim['text'])} source={quote(claim['source_id'])} "
            f"evidence={quote(claim['evidence_ref'])}"
        )
    lines.append("Judgment")
    for judgment in report["judgments"]:
        lines.append(
            f"  {judgment['kind']} decision={judgment['decision_id']} "
            f"exists={judgment['decision_exists']} pause=true evidence={quote(judgment['evidence_ref'])}"
        )
    if report["limits_hit"]:
        lines.append("Partial reasons: " + ", ".join(report["limits_hit"]))
    if report["diagnostics"]:
        lines.append("Diagnostics: " + ", ".join(report["diagnostics"]))
    return "\n".join(lines)


def _checklist_command(argv):
    from .harness_checklist_collect import collect_checklist, write_checklist_cache

    parser = argparse.ArgumentParser(
        prog="workbay fleet checklist", description="Read recorded checklist evidence on demand."
    )
    parser.add_argument("--task", required=True)
    parser.add_argument("--lane")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--require-observed", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args(argv)
    if not args.task or args.lane == "" or len(args.task) > 1024 or (args.lane is not None and len(args.lane) > 1024):
        parser.error("task and lane must be nonempty identities of at most 1024 characters")
    try:
        report = collect_checklist(args.root, args.task, args.lane, deadline=time.monotonic() + 30)
        if not isinstance(report, dict) or report.get("schema") != "harness-checklist-v1":
            raise ValueError("no checklist envelope")
        if not args.no_cache:
            write_checklist_cache(args.root, report)
        print(json.dumps(report, sort_keys=True, allow_nan=False) if args.json else _checklist_text(report))
        # Canon 0.26.2 **Exit status is the outcome contract** [AGT-21],
        # lexicons/engineering.md#agt-21, The UNIX Programming Environment ch. 5.
        if args.require_observed and (
            report["partial"]
            or report["selection"] != "selected"
            or not report["rows"]
            or any(row["state"] != "observed_pass" for row in report["rows"])
        ):
            return 1
        return 0
    except (OSError, ValueError, RuntimeError, TypeError, KeyError) as exc:
        print(f"workbay fleet checklist: {exc}", file=sys.stderr)
        return 2


def workbay_front_door(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["summarize"]:
        return _summarize_command(argv[1:])
    if argv[:1] == ["tree"]:
        return _tree_command(argv[1:])
    if argv[:1] == ["checklist"]:
        return _checklist_command(argv[1:])
    parser = argparse.ArgumentParser(
        prog="workbay fleet",
        description="Launch graphical Fleet with read-only VM inventory; use --json or --text for finite output.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--json", action="store_true")
    mode.add_argument("--text", action="store_true", help="finite text table")
    mode.add_argument("--serve", action="store_true", help="Refresh and launch Fleet (the default).")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--hours", type=_positive, default=24)
    parser.add_argument("--census-days", type=_positive, default=7)
    parser.add_argument("--no-cache", action="store_true", help="refresh without writing the snapshot cache")
    parser.add_argument("--cached", action="store_true", help="read existing cache without refreshing")
    parser.add_argument(
        "--vm",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="observe VM inventory (default on); --no-vm skips SSH",
    )
    parser.add_argument("--port", type=int, default=0, help="Web port (default: 0, a free port).")
    browser = parser.add_mutually_exclusive_group()
    browser.add_argument("--open", dest="open_browser", action="store_true")
    browser.add_argument("--no-open", dest="open_browser", action="store_false")
    parser.set_defaults(open_browser=None)
    args = parser.parse_args(argv)
    if args.serve and (args.cached or args.no_cache):
        parser.error("--serve requires a refreshed, written snapshot")
    serve = not (args.json or args.text or args.cached or args.no_cache)
    try:
        snapshot = (
            read_snapshot_cache(args.root)
            if args.cached
            else build_snapshot(args.root, hours=args.hours, census_days=args.census_days, vm=args.vm)
        )
        if snapshot is None:
            print("workbay fleet: snapshot cache missing", file=sys.stderr)
            return 2
        if not args.vm:
            snapshot = dict(snapshot, vm=_vm_unknown(), host_only=True)
        if not args.cached and not args.no_cache:
            write_snapshot_cache(args.root, snapshot)
        if serve:
            launch = ["--workspace-root", str(args.root), "--port", str(args.port)]
            if args.open_browser is not None:
                launch.append("--open" if args.open_browser else "--no-open")
            try:
                from workbay_canvas_mcp.cli import serve_fleet

                return serve_fleet(launch)
            except ImportError:
                print(
                    "workbay fleet --serve needs Studio; from a checkout run: bash scripts/install-workbay-studio.sh <checkout>",
                    file=sys.stderr,
                )
                return 2
        print(json.dumps(snapshot, sort_keys=True, allow_nan=False) if args.json else _text(snapshot))
        return 0  # An explicit UNKNOWN/partial observation is a valid report, not admission authorization.
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"workbay fleet: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(workbay_front_door())
