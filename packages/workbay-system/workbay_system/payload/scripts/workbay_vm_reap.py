#!/usr/bin/env python3
"""Host-side current-workspace release writer and installed VM reclaimer driver.

The writer owns idle/landed proof and release records. The installed VM service
owns deletion and its safety limits. This driver only delegates and measures.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

DF = "df -B1 --output=avail /"
STATUS = "python3 -I ~/.local/lib/workbay/workbay_vm_reclaim.py status --json"
START = "systemctl --user start workbay-vm-reclaim.service"


class StepFailure(Exception):
    def __init__(self, step, detail, *, next_command=None):
        self.step = step
        self.next_command = next_command
        super().__init__(f"{step}: {' '.join(str(detail).split())}")


def run_step(runner, step, argv, *, timeout=120, cwd=None, writer_receipt=False):
    try:
        result = runner(argv, capture_output=True, text=True, check=False, timeout=timeout, cwd=cwd)
    except (subprocess.TimeoutExpired, TimeoutError) as exc:
        raise StepFailure(step, "timeout") from exc
    except OSError as exc:
        raise StepFailure(step, str(exc)) from exc
    if writer_receipt and result.returncode == 1:
        # cli.py emits the complete receipt on an apply with no progress.
        # Nothing else may turn a failed command into a successful step.
        try:
            receipt = json.loads(result.stdout)
        except (ValueError, TypeError):
            receipt = None
        if isinstance(receipt, dict) and isinstance(receipt.get("summary"), dict):
            return result.stdout
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "no diagnostic").strip()
        raise StepFailure(step, f"exit {result.returncode}: {detail}")
    return result.stdout


def resolve_host(workspace, *, runner=subprocess.run):
    """Reuse remote_agent.sh's host precedence, common root and literal grammar.

    File values are never evaluated. As in _remote_gate_env_load, the last valid
    REMOTE_GATE_HOST assignment wins; WORKBAY_REMOTE_GATE_HOST is env-only.
    """
    host = os.environ.get("WORKBAY_REMOTE_GATE_HOST", "")
    if host:
        return host
    common = run_step(runner, "host-config", ["git", "rev-parse", "--path-format=absolute",
                                             "--git-common-dir"], timeout=10, cwd=workspace).strip()
    if not common or not Path(common).is_absolute():
        raise StepFailure("host-config", "git common directory is unavailable")
    config = Path(common).parent / ".workbay/remote-gate.env"
    try:
        lines = config.read_text(encoding="utf-8").splitlines() if config.is_file() else []
    except (OSError, UnicodeError) as exc:
        raise StepFailure("host-config", str(exc)) from exc
    for line in lines:
        line = line.lstrip(" \t")
        line = re.sub(r"^export[ \t]+", "", line)
        if not line.startswith("REMOTE_GATE_HOST="):
            continue
        raw = line.partition("=")[2]
        if raw.startswith("'") and raw.endswith("'") and len(raw) >= 2:
            value = raw[1:-1]
            if "'" in value:
                continue
        elif raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
            value = raw[1:-1]
            if any(char in value for char in "$`\\"):
                continue
        elif re.fullmatch(r"[A-Za-z0-9@._:/-]*", raw):
            value = raw
        else:
            continue
        host = value
    if not host:
        raise StepFailure("host-config", "host not configured: set WORKBAY_REMOTE_GATE_HOST or "
                          "REMOTE_GATE_HOST in .workbay/remote-gate.env")
    return host


def json_object(text, step):
    try:
        value = json.loads(text)
    except (ValueError, TypeError) as exc:
        raise StepFailure(step, "non-JSON reply") from exc
    if not isinstance(value, dict):
        raise StepFailure(step, "JSON reply must be an object")
    return value


def count(value, step):
    if isinstance(value, list):
        return len(value)
    if type(value) is not int or value < 0:
        raise StepFailure(step, "invalid nonnegative count")
    return value


def hold_rows(value, step):
    if not isinstance(value, list) or any(
        not isinstance(row, dict) or not isinstance(row.get("key"), str)
        or not isinstance(row.get("reason"), str) for row in value
    ):
        raise StepFailure(step, "invalid holds list")
    return value


def free_bytes(text, step):
    lines = text.split()
    if len(lines) != 2 or lines[0] != "Avail" or not lines[1].isdigit():
        raise StepFailure(step, "invalid df available-byte reply")
    return int(lines[1])


def human(value):
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if size < 1024 or unit == "PiB":
            return f"{int(size)}B" if unit == "B" else f"{size:.1f}{unit}"
        size /= 1024


def verdict(workspace, apply, runner, task_ref=None):
    started_at = datetime.now(timezone.utc)
    host = resolve_host(workspace, runner=runner)
    # Keep the configured host a single destination, never an SSH option.
    if host.startswith("-") or any(char.isspace() or ord(char) < 32 for char in host):
        raise StepFailure("host-config", "invalid SSH host")
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=30",
           "-o", "ServerAliveCountMax=4", host]
    argv = ["mcp-workbay-orchestrator", "--workspace-root", str(workspace), "remote-sandbox-reap", "--json"]
    if apply:
        argv.append("--apply")
    if task_ref is not None:
        argv.extend(["--task-ref", task_ref])
    writer = json_object(run_step(runner, "writer", argv, timeout=180, cwd=workspace,
                                  writer_receipt=True), "writer")
    if writer.get("ok") is not True:
        detail = str(writer.get("error") or "writer did not report ok")
        decision_error = str(writer.get("decision_error") or "")
        if decision_error:
            detail += f": {decision_error}"
        raise StepFailure("writer", detail)
    summary = writer.get("summary")
    if not isinstance(summary, dict):
        raise StepFailure("writer", "missing summary object")
    holds = list(hold_rows(summary.get("holds", []), "writer"))
    released = count(summary.get("released_to_reclaim", 0), "writer")
    writer_held = count(summary.get("hold_count", len(holds)), "writer")
    before = free_bytes(run_step(runner, "df-before", ssh + [DF]), "df-before")
    if apply:
        # start waits for the Type=oneshot service's job to finish by default.
        # The installed unit has TimeoutStartSec=50min; bound the SSH wait too.
        run_step(runner, "reclaimer-start", ssh + [START], timeout=3060)
    reclaimer = json_object(run_step(runner, "reclaimer-status", ssh + [STATUS]), "reclaimer-status")
    after = free_bytes(run_step(runner, "df-after", ssh + [DF]), "df-after")
    counts = reclaimer.get("counts")
    if not isinstance(counts, dict):
        raise StepFailure("reclaimer-status", "missing counts object")
    hold_counts = {key: count(value, "reclaimer-status") for key, value in counts.items()
                   if key.startswith("hold_") or key == "unclassified"}
    # Current status exposes aggregate counts, not all keys. Preserve that lower
    # bound without double-counting the writer's overlapping workspace holds.
    known = {row["key"] for row in holds}
    for row in hold_rows(reclaimer.get("holds", []), "reclaimer-status"):
        if row["key"] not in known:
            holds.append(row)
            known.add(row["key"])
    held = max(len(holds), writer_held, sum(hold_counts.values()))
    reasons = Counter(row["reason"] for row in holds) if holds else Counter(hold_counts)
    top_reasons = [reason for reason in sorted(reasons, key=lambda key: (-reasons[key], key)) if reasons[reason]][:3]
    last_run = reclaimer.get("last_run") or {}
    if not isinstance(last_run, dict):
        raise StepFailure("reclaimer-status", "stale_last_run" if apply else "invalid last_run object",
                          next_command=shlex.join(ssh + [STATUS]) if apply else None)
    if apply:
        try:
            receipt_start = datetime.fromisoformat(last_run["started_at"].replace("Z", "+00:00"))
            fresh = (receipt_start.tzinfo is not None and receipt_start >= started_at
                     and ("trigger" not in last_run or last_run["trigger"] == "timer"))
        except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
            fresh = False
        if not fresh:
            raise StepFailure("reclaimer-status", "stale_last_run", next_command=shlex.join(ssh + [STATUS]))
    reclaimed = count(last_run.get("reclaimed"), "reclaimer-status") if apply else 0
    freed = count(last_run.get("bytes_freed"), "reclaimer-status") if apply else 0
    starvation = last_run.get("starved")
    starved = ("yes" if reclaimer.get("health") == "starved" or isinstance(starvation, dict) and starvation
               else "no" if "health" in reclaimer else "unknown")
    return {
        "workspace_root": str(workspace), "mode": "apply" if apply else "dry-run",
        "freed_bytes": freed, "df_delta_bytes": after - before, "free_before_bytes": before, "free_after_bytes": after,
        "released": released, "reclaimed": reclaimed, "held": held, "holds": holds,
        "top_reasons": top_reasons, "starved": starved,
        "last_run_age_seconds": reclaimer.get("last_run_age_seconds") if not apply else None,
        "writer": writer, "reclaimer": reclaimer,
    }


def main(argv=None, *, runner=None):
    parser = argparse.ArgumentParser(description="Release idle, landed sandboxes for the current workspace "
                                     "and run the installed VM reclaimer. Cross-consumer discovery is not supported.")
    parser.add_argument("--workspace-root", type=Path, default=Path.cwd())
    parser.add_argument("--apply", action="store_true", help="write releases and start the VM service (default: dry run)")
    parser.add_argument("--task-ref", help="task that owns the writer decision receipt (make vm-reap TASK=<ref>)")
    parser.add_argument("--json", action="store_true", help="print one JSON object including the original step receipts")
    args = parser.parse_args(argv)
    apply = args.apply or os.environ.get("APPLY") == "1"
    try:
        result = verdict(args.workspace_root.resolve(), apply, runner or subprocess.run, args.task_ref)
    except StepFailure as exc:
        next_command = exc.next_command or f"make vm-reap TASK={shlex.quote(args.task_ref) if args.task_ref else '<task-ref>'}"
        if args.json:
            print(json.dumps({"ok": False, "step": exc.step, "error": str(exc), "next": next_command}))
        print(f"vm-reap failed step={exc} next={next_command}", file=sys.stderr)
        return 2
    if apply:
        rc = 0 if result["reclaimed"] >= 1 or result["freed_bytes"] > 0 else 3
    else:
        rc = 3 if result["starved"] == "yes" else 0
    if rc:
        # Dry-run writer output names the held keys, reasons and owners without
        # starting the service. Keep the selected workspace and task explicit.
        inspect = ["mcp-workbay-orchestrator", "--workspace-root", result["workspace_root"],
                   "remote-sandbox-reap", "--json"]
        if args.task_ref is not None:
            inspect.extend(["--task-ref", args.task_ref])
        result["next"] = shlex.join(inspect)
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        delta = result["df_delta_bytes"]
        age = f" last_run_age_seconds={result['last_run_age_seconds']}" if not apply else ""
        next_step = f" next={result['next']}" if rc else ""
        print(f"vm-reap freed={human(result['freed_bytes'])} "
              f"df_delta={'+' if delta >= 0 else '-'}{human(abs(delta))} free_before={human(result['free_before_bytes'])} "
              f"free_after={human(result['free_after_bytes'])} released={result['released']} "
              f"reclaimed={result['reclaimed']} held={result['held']}({','.join(result['top_reasons'])}) "
              f"starved={result['starved']} mode={result['mode']}{age}{next_step}")
        for row in result["holds"][:20]:
            print(f"held {row['key']} {row['reason']} owner={row.get('owner') or 'unknown'}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
