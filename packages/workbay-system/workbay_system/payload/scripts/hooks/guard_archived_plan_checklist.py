#!/usr/bin/env python3
"""Block checklist glyph edits to archived task plans on protected branches."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

_CHECKBOX_LINE_RE = re.compile(r"^(\s*-\s*)\[( |x|X)\](\s+.*)$", re.MULTILINE)
_TASK_PLAN_MARKERS = ("/docs/tasks/", "/docs/epics/")


def _is_task_plan_path(path: str) -> bool:
    return any(marker in path for marker in _TASK_PLAN_MARKERS) and path.endswith(".md")


def _checklist_glyph_changes(before: str, after: str) -> bool:
    before_boxes = {m.group(0) for m in _CHECKBOX_LINE_RE.finditer(before)}
    after_boxes = {m.group(0) for m in _CHECKBOX_LINE_RE.finditer(after)}
    return before_boxes != after_boxes


def _task_ref_from_plan_path(path: str) -> str | None:
    name = Path(path).name
    if "-task-plan" not in name:
        return None
    # Plan filenames are "<TASK-REF>-<lowercase-descriptor-slug>-task-plan.md".
    # Task refs are UPPERCASE alnum/underscore/hyphen (grammar ^[A-Z][A-Z0-9_-]+$)
    # and the descriptor slug that follows is lowercase-kebab. Take the leading
    # run of non-lowercase segments as the full task ref. `split("-", 1)[0]`
    # collapsed "internal" to "WB", so the archive lookup
    # never matched and the guard was inert for every real hyphenated task ref.
    stem = name.split("-task-plan", 1)[0]
    ref_parts: list[str] = []
    for segment in stem.split("-"):
        if segment and not segment.islower():
            ref_parts.append(segment)
        else:
            break
    if not ref_parts:
        return None
    return "-".join(ref_parts).upper()


def _is_archived_task(repo: Path, task_ref: str) -> bool:
    proc = subprocess.run(
        ["mcp-workbay-handoff", "--workspace-root", str(repo), "archive", "--operation", "get", "--task-ref", task_ref],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0 and task_ref in proc.stdout


def scan_staged(repo: Path, branch: str) -> list[str]:
    if branch not in {"main", "master"}:
        return []
    proc = subprocess.run(
        ["git", "-C", str(repo), "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=False,
    )
    violations: list[str] = []
    for rel in proc.stdout.splitlines():
        if not _is_task_plan_path(rel):
            continue
        task_ref = _task_ref_from_plan_path(rel)
        if not task_ref or not _is_archived_task(repo, task_ref):
            continue
        show = subprocess.run(
            ["git", "-C", str(repo), "diff", "--cached", "--", rel],
            capture_output=True,
            text=True,
            check=False,
        )
        if not show.stdout:
            continue
        # crude split: use file at HEAD vs staged
        head = subprocess.run(
            ["git", "-C", str(repo), "show", f"HEAD:{rel}"],
            capture_output=True,
            text=True,
            check=False,
        )
        before = head.stdout if head.returncode == 0 else ""
        work = subprocess.run(
            ["git", "-C", str(repo), "show", f":{rel}"],
            capture_output=True,
            text=True,
            check=False,
        )
        after = work.stdout if work.returncode == 0 else ""
        if _checklist_glyph_changes(before, after):
            violations.append(rel)
    return violations


def _current_branch(repo: Path) -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


def main(argv: list[str] | None = None) -> int:
    repo = Path.cwd()
    branch = _current_branch(repo)
    hits = scan_staged(repo, branch)
    if hits:
        sys.stderr.write(
            "guard_archived_plan_checklist: refuse checklist edits to archived plan(s) on main: "
            + ", ".join(hits)
            + "\n"
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
