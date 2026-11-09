"""Freeze planning-review delivery before linting or invoking a reviewer.

Public function and ``python -m workbay_lifecycle.plan_review_freeze`` entry.
This helper creates a pending run; it does not judge or grant acceptance.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def freeze_plan_review(
    repo: Path,
    task_ref: str,
    plan_path: str,
    source_ref: str,
    review_run_id: str,
    session: str,
) -> dict:
    from workbay_handoff_mcp import api
    from workbay_handoff_mcp.config import RuntimeConfig
    from workbay_handoff_mcp.runtime import RuntimeNotConfiguredError, get_runtime_config

    repo = Path(repo).resolve()
    # Preserve the caller's explicit custody/storage configuration when it
    # already identifies this checkout. Standalone CLI resolves shared state.
    try:
        config = get_runtime_config()
    except RuntimeNotConfiguredError:
        config = None
    if config is None or config.workspace_root.resolve() != repo:
        api.configure_runtime(RuntimeConfig.for_repo(repo))
    logical = Path(plan_path)
    if logical.is_absolute():
        logical = logical.relative_to(repo)
    result = api.review_runs(
        review={
            "operation": "record",
            "task_ref": task_ref,
            "subject_path": logical.as_posix(),
            "subject_kind": "task_plan",
            "review_mode": "planning",
            "source_ref": source_ref,
            "review_run_id": review_run_id,
            "session": session,
        }
    )
    if not result.get("ok"):
        raise ValueError(result.get("data", {}).get("error", "plan_review_freeze_failed"))
    return result["data"]["review_subject"]


def plan_bytes(repo: Path, plan_path: str, source_ref: str | None = None) -> bytes:
    """Read exact worktree or immutable Git bytes, without text conversion."""
    if source_ref is None:
        target = repo.resolve() / plan_path
        if target.resolve(strict=True) != target or not target.is_file():
            raise ValueError("review_subject_path_redirected_or_not_file")
        target.relative_to(repo.resolve())
        return target.read_bytes()
    if source_ref.startswith("-"):
        raise ValueError("review_source_ref_invalid")
    proc = subprocess.run(
        ["git", "-C", str(repo), "show", f"{source_ref}:{plan_path}"],
        capture_output=True,
        timeout=10,
        check=False,
    )
    if proc.returncode != 0:
        raise ValueError("review_subject_source_unreadable")
    return proc.stdout


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", required=True)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--source-ref", required=True)
    parser.add_argument("--review-run-id", required=True)
    parser.add_argument("--session", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        root = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        ).stdout.strip()
        subject = freeze_plan_review(
            Path(root),
            args.task,
            args.plan,
            args.source_ref,
            args.review_run_id,
            args.session or os.environ.get("WORKBAY_SESSION_ID") or "plan-review:" + args.review_run_id,
        )
    except (ImportError, OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(subject, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
