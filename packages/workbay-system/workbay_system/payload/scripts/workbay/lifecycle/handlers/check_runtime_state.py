"""Importable runtime-state safety gate for local check targets."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any


DiskUsage = Callable[[str], Any]


def runtime_state_errors(
    status_data: Mapping[str, Any],
    *,
    status_returncode: int = 0,
    disk_floor_mb: int = 0,
    disk_paths: Iterable[str] = (),
    disk_usage: DiskUsage = shutil.disk_usage,
) -> list[str]:
    errors: list[str] = []
    if status_returncode != 0:
        errors.append("lifecycle status failed")

    queue = status_data.get("projection_queue")
    q = queue if isinstance(queue, Mapping) else {}
    live = int(q.get("live_size_bytes") or 0)
    hard = int(q.get("hard_limit_bytes") or 0)
    if hard and live >= hard:
        errors.append(f"projection live spool {live} >= hard limit {hard}")

    if str(q.get("breaker_state") or "closed") == "open":
        errors.append("projection breaker is open")

    floor_bytes = max(0, disk_floor_mb) * 1024 * 1024
    if floor_bytes:
        for path in sorted(set(disk_paths)):
            free = int(disk_usage(path).free)
            if free < floor_bytes:
                errors.append(f"low disk headroom on {path}: {free} bytes < {floor_bytes}")
    return errors


def _status_command() -> list[str]:
    return [
        sys.executable,
        "packages/workbay-system/workbay_system/payload/scripts/workbay/lifecycle",
        "status",
        "--json",
    ]


def run(
    *,
    status_cmd: Sequence[str] | None = None,
    disk_floor_mb: int | None = None,
    disk_paths: Iterable[str] | None = None,
) -> int:
    cmd = list(status_cmd) if status_cmd is not None else _status_command()
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    sys.stderr.write(proc.stderr)
    try:
        status_data = json.loads(proc.stdout) if proc.stdout.strip() else {}
    except json.JSONDecodeError:
        status_data = {}
    floor = (
        int(os.environ.get("DISK_FLOOR_MB") or 0)
        if disk_floor_mb is None
        else disk_floor_mb
    )
    paths = set(disk_paths) if disk_paths is not None else {os.getcwd(), tempfile.gettempdir()}
    errors = runtime_state_errors(
        status_data,
        status_returncode=proc.returncode,
        disk_floor_mb=floor,
        disk_paths=paths,
    )
    for error in errors:
        print(f"check-runtime-state: FAIL - {error}", file=sys.stderr)
    if errors:
        return 1
    print("check-runtime-state: OK - projection runtime state is safe.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    _ = argv
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
