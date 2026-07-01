"""``task-reap`` lifecycle subcommand (internal).

Shells out to ``mcp-workbay-handoff archive --operation reap`` and prints
the closeable / active / ambiguous buckets. Dry-run by default; pass
``--apply`` to close+archive closeable rows only.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from . import _common


def _flatten_handoff_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("schema_version") == 2:
        data = payload.get("data")
        if isinstance(data, dict):
            flat = {**payload, **data}
            scope = payload.get("scope")
            if isinstance(scope, dict) and "task_ref" not in flat and scope.get("task_ref"):
                flat["task_ref"] = scope["task_ref"]
            return flat
    return payload


def _print_buckets(flat: dict[str, Any]) -> None:
    applied = flat.get("applied")
    if applied is not None:
        print(f"applied={applied}")
    reaped = flat.get("reaped") or []
    if reaped:
        print(f"reaped: {', '.join(reaped)}")
    failed = flat.get("failed") or []
    if failed:
        print(f"failed ({len(failed)}):")
        for entry in failed:
            if not isinstance(entry, dict):
                continue
            print(
                f"  - {entry.get('task_ref', '?')} "
                f"(stage={entry.get('stage', '?')}): {entry.get('error', '')}"
            )
    for bucket in ("closeable", "active", "ambiguous"):
        entries = flat.get(bucket) or []
        print(f"{bucket} ({len(entries)}):")
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            ref = entry.get("task_ref", "?")
            reason = entry.get("reason", "")
            branch = entry.get("target_branch")
            suffix = f" branch={branch}" if branch else ""
            print(f"  - {ref}{suffix}: {reason}")


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="lifecycle task-reap", add_help=True)
    parser.add_argument("--apply", action="store_true", default=False)
    parser.add_argument("--task", dest="task", default="")
    parser.add_argument("--json", dest="emit_json", action="store_true", default=False)
    args = parser.parse_args(argv)

    repo = _common.repo_root()
    if repo is None:
        _common.emit({"ok": False, "command": "task-reap", "error": "not_in_git_repo"})
        return 2

    handoff_argv = ["archive", "--operation", "reap"]
    if args.apply:
        handoff_argv.append("--apply")
    # NB: do NOT upper-case here. Unlike task-start/slice-start (which mint
    # canonical UPPER refs), task-reap *targets* pre-existing rows that may be
    # stored verbatim in lower-case (implementation note's motivating duplicates, e.g.
    # `ws-migdoctor-01-plan0043`). The classifier matches task_ref by exact,
    # case-sensitive equality, so upper-casing would make those rows
    # un-targetable by a scoped reap.
    task_ref = (args.task or "").strip()
    if task_ref:
        handoff_argv.extend(["--task-ref", task_ref])

    payload, warning = _common.run_handoff_json(repo, argv=handoff_argv, field="reap")
    if payload is None:
        receipt: dict[str, Any] = {
            "ok": False,
            "command": "task-reap",
            "error": "reap_unavailable",
        }
        if warning is not None:
            receipt["warning"] = warning.__dict__
        _common.emit(receipt)
        return 2

    flat = _flatten_handoff_payload(payload)
    if not flat.get("ok", True):
        _common.emit({"ok": False, "command": "task-reap", "handoff": flat})
        return 2

    receipt = {
        "ok": True,
        "command": "task-reap",
        "applied": flat.get("applied", False),
        "closeable": flat.get("closeable") or [],
        "active": flat.get("active") or [],
        "ambiguous": flat.get("ambiguous") or [],
        "reaped": flat.get("reaped") or [],
        "failed": flat.get("failed") or [],
    }
    if args.emit_json:
        _common.emit(receipt)
        return 0

    _print_buckets(flat)
    return 0
