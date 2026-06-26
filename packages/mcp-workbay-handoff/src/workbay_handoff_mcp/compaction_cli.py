"""``python -m workbay_handoff_mcp.compaction_cli`` — internal.

Single-action launcher behind ``Makefile.d/compaction.mk``'s
``compact-now`` target. Writes a ``session_compactions`` row for the
named task and prints ``compaction_id=<id>`` as the first stdout line so
the caller (operator or another tool) can chain on it. Additional stable
``key=value`` receipt lines expose the compression-value fields.

Mirrors :mod:`workbay_handoff_mcp.plan_cli` so a freshly bootstrapped
consumer can run ``make compact-now`` via the pinned
``uvx --from mcp-workbay-handoff`` launcher without an explicit
``pip install``.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path

from .compaction import COMPACTION_HARNESS_INPUT_CHOICES, compact_session, format_compaction_record_receipt_lines
from .config import RuntimeConfig
from .runtime import configure_runtime


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="workbay_handoff_mcp.compaction_cli",
        description="Manual compact-now launcher (internal).",
    )
    parser.add_argument("--workspace-root")
    parser.add_argument("--state-dir")
    parser.add_argument("--current-task-path")
    parser.add_argument("--dashboard-path")
    parser.add_argument(
        "--task-ref",
        required=True,
        help="Task ref (e.g. internal) to attribute the compaction to.",
    )
    parser.add_argument(
        "--transcript",
        default=None,
        help=(
            "Path to the harness transcript to compact. When omitted, "
            "the CLI falls back to the most recent file under "
            "~/.claude/projects/<workspace-slug>/."
        ),
    )
    parser.add_argument(
        "--harness",
        choices=COMPACTION_HARNESS_INPUT_CHOICES,
        default="manual",
        help="Harness label stored on the compaction row (default: manual).",
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help="Session id to record. Defaults to a fresh UUID.",
    )
    return parser


def _claude_projects_slug(workspace_root: Path) -> str:
    """Return the Claude Code projects directory slug for a workspace.

    Claude Code stores transcripts under ``~/.claude/projects/<slug>/``
    where the slug is the absolute workspace path with ``/`` replaced
    by ``-`` (and a leading ``-`` from the root slash).
    """
    return str(workspace_root.resolve()).replace(os.sep, "-")


def _resolve_transcript(workspace_root: Path) -> Path:
    home = Path(os.environ.get("HOME") or Path.home())
    projects_dir = home / ".claude" / "projects" / _claude_projects_slug(workspace_root)
    if not projects_dir.is_dir():
        raise FileNotFoundError(
            f"compact-now: --transcript not provided and no Claude Code "
            f"projects directory found at {projects_dir}. Pass "
            f"--transcript <path> explicitly."
        )
    candidates = [p for p in projects_dir.iterdir() if p.is_file()]
    if not candidates:
        raise FileNotFoundError(
            f"compact-now: --transcript not provided and {projects_dir} "
            f"contains no files. Pass --transcript <path> explicitly."
        )
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    runtime = RuntimeConfig.from_args(args)
    configure_runtime(runtime)

    if args.transcript is not None:
        transcript_path = Path(args.transcript)
    else:
        try:
            transcript_path = _resolve_transcript(runtime.workspace_root)
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            return 2

    session_id = args.session_id or str(uuid.uuid4())
    receipt = compact_session(
        transcript_path=transcript_path,
        task_ref=args.task_ref,
        harness=args.harness,
        session_id=session_id,
    )
    for line in format_compaction_record_receipt_lines(receipt):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
