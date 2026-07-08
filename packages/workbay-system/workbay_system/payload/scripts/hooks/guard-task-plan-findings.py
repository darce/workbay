#!/usr/bin/env python3
"""Guard hook: block task-plan drift into the mutable-execution territory.

Two violation shapes are detected and rejected, both scoped to the same
task-plan path filter:

1. **Pasted review-finding lists.** Review findings live in the
    WorkBay handoff database. They are recorded with
   ``review_findings(review={"operation":"record"|"batch_record", ...})`` and
   read back with ``review_findings(review={"operation":"list"|"get"})``.
   Pasting them inline into a task plan duplicates the source of truth,
   escapes the pre-merge gate, and silently rots when findings change status.

2. **Revision-history blocks.** Mutable execution history — status changes,
   slice-close rationale, review-pass results, run history — belongs in the
   handoff DB, not in the task-plan markdown. A ``Revision history:`` label
   or ``## Revision history`` heading inside a task plan turns the plan into
   an execution diary; record that history with ``set_handoff_state``,
   ``record_event``, ``close_slice``, ``review_findings``, and
   ``render_handoff`` instead.

The finding-list detector flags any block of **three or more consecutive
bulleted lines** where each bullet starts with a finding-like identifier
such as:

  - ``- internal: ...``
  - ``- **H-1**: ...``
  - ``- internal — ...``
  - ``- [M-2] ...``

Single mentions and cross-references are unaffected — only structured lists
of three or more in a row trip the heuristic, which is the shape an agent
produces when copy-pasting a ``review_findings(operation="list")`` result.

Residual risk and mitigation
----------------------------

The ≥3 consecutive-bullet threshold accepts a residual risk: a 1- or 2-bullet
finding paste is not caught by the run detector. A second heuristic
(``_detect_findings_under_heading``) closes this gap: any finding bullet that
appears under a section heading matching ``Findings`` (e.g. ``## Review
Findings``, ``### Open Findings``) is flagged regardless of bullet count. The
heading heuristic has no false positives on the calibration corpus.

Operating modes
---------------

1. **Claude Code hook** (default): reads the PreToolUse JSON payload from
   stdin and inspects ``tool_input.content`` (Write) or ``tool_input.new_string``
   (Edit). Exits 2 with an actionable reason on stderr to block the tool call.

2. ``--scan-staged``: enumerates ``git diff --cached --name-only`` filtered to
   the task-plan directories and scans each staged file blob. Suitable for a
   git ``pre-commit`` hook.

3. ``--scan-paths <path> [...]``: scans the on-disk content of each path
   (file or directory). Useful for ad-hoc sweeps of a known subtree.

4. ``--scan-repo``: enumerates ``git ls-files '*.md'`` from the repo root and
   filters by the same path scope used by the Claude Code hook mode. This is
   the mode wired into ``make lint-task-plans`` so CI sees every file the
   hook would block, not just a hard-coded subset of directories.

Path filter
-----------

Only files whose repo-relative path contains ``/docs/tasks/``, ``/docs/epics/``,
or matches ``*task-plan*.md`` are scanned. Other markdown documents are
exempt because they are allowed to discuss findings narratively. Top-level
numbered plans under ``docs/plans/**`` are historical planning drafts and
remain out of scope; the same path filter applies to both the finding-list
detector and the revision-history detector.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable

# Bullet line opens with `- ` or `* `, optional bold/bracket wrapping, then a
# finding-like identifier. Two flavors are accepted:
#   - severity shorthand: H-1, M-2, L-3
#   - task-prefixed:      internal, internal, DEMO-7-BR-01
# The full bullet does not need to be matched — we just need to recognize
# the *opening* of a finding-style entry.
_FINDING_BULLET_RE = re.compile(
    r"""
    ^\s*[-*]\s+               # bullet marker
    [*\[\(`]{0,2}             # optional opening markup (**, [, (, `)
    (?P<id>
        (?:[A-Z]{1,5}\d*-\d+(?:-[A-Z]+)?-\d+)   # task-prefixed: internal, DEMO-7-BR-01
        |
        (?:[HML]-\d+)                            # severity shorthand: H-1, M-2, L-3
    )
    [*\]\)`]{0,2}             # optional closing markup
    \s*[:\u2014\-]             # separator: ':', em-dash, or '-'
    """,
    re.VERBOSE,
)

_CONSECUTIVE_THRESHOLD = 3

# Heading pattern for the residual-risk heuristic.  Matches Markdown headings
# like "## Review Findings", "### Open Findings", "# Findings".
_FINDINGS_HEADING_RE = re.compile(
    r"^#{1,4}\s+(?:(?:open|review|closed|all)\s+)?findings\b",
    re.IGNORECASE,
)

_PATH_FILTER_SUBSTRINGS = ("/docs/tasks/", "/docs/epics/")
_PATH_FILTER_FILENAME_GLOBS = ("*task-plan*.md", "*-plan.md")
# Historical numbered plans under docs/plans/** are out of scope for both
# detectors. Without this exclusion the *-plan.md glob would pull in any
# legacy plan file that happens to end with -plan.md, contradicting the
# internal policy boundary.
_PATH_FILTER_EXCLUDE_SUBSTRINGS = ("/docs/plans/",)


def _path_should_be_scanned(rel_path: str) -> bool:
    """Return True if the given repo-relative path is in scope."""
    if not rel_path.endswith(".md"):
        return False
    normalized = "/" + rel_path.lstrip("/")
    if any(needle in normalized for needle in _PATH_FILTER_EXCLUDE_SUBSTRINGS):
        return False
    if any(needle in normalized for needle in _PATH_FILTER_SUBSTRINGS):
        return True
    name = Path(rel_path).name
    return any(_glob_match(name, pattern) for pattern in _PATH_FILTER_FILENAME_GLOBS)


def _glob_match(name: str, pattern: str) -> bool:
    # Tiny shim so we don't pull in fnmatch for one call.
    from fnmatch import fnmatchcase

    return fnmatchcase(name, pattern)


def _detect_finding_runs(text: str) -> list[tuple[int, list[str]]]:
    """Return list of (start_line_number, finding_ids) for runs >= threshold.

    Walks lines and counts consecutive bullet lines whose opener matches the
    finding pattern. A blank line, a non-bullet line, or a bullet line that
    does NOT match the finding pattern resets the run. Continuation lines
    (indented under a bullet) do not break the run, because pasted findings
    routinely include a continuation line for description.
    """
    runs: list[tuple[int, list[str]]] = []
    current_run: list[tuple[int, str]] = []
    in_bullet_continuation = False

    for idx, raw_line in enumerate(text.splitlines(), start=1):
        stripped = raw_line.lstrip()
        is_blank = stripped == ""
        is_bullet_open = stripped.startswith(("- ", "* "))

        if is_blank:
            # Blank line ends any run.
            if len(current_run) >= _CONSECUTIVE_THRESHOLD:
                runs.append((current_run[0][0], [item[1] for item in current_run]))
            current_run = []
            in_bullet_continuation = False
            continue

        if is_bullet_open:
            match = _FINDING_BULLET_RE.match(raw_line)
            if match:
                current_run.append((idx, match.group("id")))
                in_bullet_continuation = True
                continue
            # A non-finding bullet ends the run.
            if len(current_run) >= _CONSECUTIVE_THRESHOLD:
                runs.append((current_run[0][0], [item[1] for item in current_run]))
            current_run = []
            in_bullet_continuation = False
            continue

        # Non-bullet, non-blank line: treat as continuation if we are inside
        # a bullet run, otherwise as a hard reset.
        if in_bullet_continuation and (raw_line.startswith(" ") or raw_line.startswith("\t")):
            continue
        if len(current_run) >= _CONSECUTIVE_THRESHOLD:
            runs.append((current_run[0][0], [item[1] for item in current_run]))
        current_run = []
        in_bullet_continuation = False

    if len(current_run) >= _CONSECUTIVE_THRESHOLD:
        runs.append((current_run[0][0], [item[1] for item in current_run]))

    return runs


def _detect_findings_under_heading(text: str) -> list[tuple[int, list[str]]]:
    """Catch finding bullets placed under a Findings section heading (1-2 bullet gap).

    ``_detect_finding_runs`` requires ≥3 consecutive bullets, accepting the
    residual risk that a 1- or 2-bullet paste slips through.  This function
    closes that gap: once a heading matching ``_FINDINGS_HEADING_RE`` is seen,
    every finding bullet that follows (until the next heading) is collected and
    flagged regardless of count.  The start line in the returned tuple is the
    line of the first matching bullet, not the heading line.
    """
    runs: list[tuple[int, list[str]]] = []
    under_findings_heading = False
    group: list[tuple[int, str]] = []

    def _flush() -> None:
        nonlocal under_findings_heading, group
        if group:
            runs.append((group[0][0], [item[1] for item in group]))
        under_findings_heading = False
        group = []

    for idx, raw_line in enumerate(text.splitlines(), start=1):
        stripped = raw_line.lstrip()
        if stripped.startswith("#"):
            _flush()
            if _FINDINGS_HEADING_RE.match(stripped):
                under_findings_heading = True
            continue
        if under_findings_heading:
            match = _FINDING_BULLET_RE.match(raw_line)
            if match:
                group.append((idx, match.group("id")))

    _flush()
    return runs


def _detect_all_runs(text: str) -> list[tuple[int, list[str]]]:
    """Run both detection heuristics and return combined, deduplicated results."""
    seen: set[int] = set()
    combined: list[tuple[int, list[str]]] = []
    for run in _detect_finding_runs(text) + _detect_findings_under_heading(text):
        if run[0] not in seen:
            seen.add(run[0])
            combined.append(run)
    return combined


# ---------------------------------------------------------------------------
# Revision-history block detector
# ---------------------------------------------------------------------------

# Matches the two shapes a mutable execution diary takes inside task-plan
# markdown:
#   - ``Revision history:`` (or ``Revision History :``) as a bare label.
#   - ``## Revision history`` (any heading level 1-4) as a section heading.
# Either shape may sit inside a Markdown blockquote (one or more ``>``
# markers, optionally nested) — the legacy task plans cited as motivation
# for internal quote their metadata block, so a guard that ignores ``>``
# misses exactly the case it was created to catch (internal).
# Stable cross-references like "see the revision history in handoff" are
# narrative prose, not section openers, and do not trip the regex because
# the pattern anchors the inner match at a line opener (after the optional
# blockquote markers).
_REVISION_HISTORY_LINE_RE = re.compile(
    r"""
    ^\s*
    (?:>\s*)*                            # optional blockquote markers, possibly nested
    (?:
        revision\s+history\s*:           # label form: "Revision history:"
        |
        \#{1,4}\s*revision\s+history\b   # heading form: "## Revision history"
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _detect_revision_history_blocks(text: str) -> list[int]:
    """Return 1-based line numbers of revision-history block openers."""
    return [
        idx
        for idx, raw_line in enumerate(text.splitlines(), start=1)
        if _REVISION_HISTORY_LINE_RE.match(raw_line)
    ]


def _format_revision_history_reason(rel_path: str, lines: list[int]) -> str:
    out = [
        "BLOCKED: Revision-history block detected in a task plan.",
        "",
        f"  File: {rel_path}",
    ]
    for line_no in lines:
        out.append(f"    line {line_no}: revision-history heading or label")
    out.extend(
        [
            "",
            "Task plans carry stable planning prose. Mutable execution history --",
            "status changes, slice-close rationale, review-pass results, run history --",
            "lives in the handoff DB, not in the task-plan markdown.",
            "",
            "Record mutable task history through the MCP handoff tools:",
            "",
            "  set_handoff_state(...)         # task state, focus, target_branch",
            "  record_event(event={...})      # decision, test_result, blocker",
            "  close_slice(...)               # slice-complete decision + rationale",
            "  review_findings(review={...})  # review findings and dispositions",
            "  render_handoff(kind='dashboard'|'current_task')",
            "",
            "Legacy numbered plans under docs/plans/** are historical artifacts and",
            "remain out of scope for this guard. New task plans must keep their",
            "execution diary in handoff.",
            "",
            "See: docs/workbay/rules/branch-review-guide.md § Review Findings Placement",
        ]
    )
    return "\n".join(out)


def _collect_block_reasons(rel_path: str, text: str) -> list[str]:
    """Apply every detector and return the rejection messages they produce."""
    reasons: list[str] = []
    finding_runs = _detect_all_runs(text)
    if finding_runs:
        reasons.append(_format_block_reason(rel_path, finding_runs))
    revision_lines = _detect_revision_history_blocks(text)
    if revision_lines:
        reasons.append(_format_revision_history_reason(rel_path, revision_lines))
    return reasons


def _format_block_reason(rel_path: str, runs: list[tuple[int, list[str]]]) -> str:
    lines = [
        "BLOCKED: Pasted review-finding list detected in a task plan.",
        "",
        f"  File: {rel_path}",
    ]
    for start_line, ids in runs:
        preview = ", ".join(ids[:6]) + (", ..." if len(ids) > 6 else "")
        lines.append(f"    line {start_line}: {len(ids)} consecutive finding bullets ({preview})")
    lines.extend(
        [
            "",
            "Review findings live in WorkBay handoff, not in task plans.",
            "Record them with the MCP tools so they are gated by handoff_close_check:",
            "",
            '  review_findings(review={"operation":"record",       ...})  # single finding',
            '  review_findings(review={"operation":"batch_record", ...})  # 3+ findings',
            "",
            "List existing findings with:",
            '  review_findings(review={"operation":"list", "task_ref": "<task>", "status":"open"})',
            "",
            "If you need to reference findings in the task plan, link to them by ID",
            "(e.g. 'see internal in handoff') instead of duplicating their bodies.",
            "",
            "See: docs/workbay/rules/branch-review-guide.md § Review Findings Placement",
        ]
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Claude Code hook mode
# ---------------------------------------------------------------------------


def _extract_claude_payload(stdin_data: dict) -> tuple[str, str] | None:
    """Pull (file_path, content_to_scan) from a Claude Code PreToolUse payload.

    For Write tools, scan the full ``content`` being written.

    For Edit tools, simulate the replacement against the current file content
    and scan the result. The hook must catch incremental edits whose
    ``new_string`` alone contains fewer than three finding bullets but whose
    post-edit document does — for example, an Edit that adds the third bullet
    to a file that already had two. Scanning ``new_string`` in isolation
    misses that case (internal).

    If the target file does not exist or cannot be read, fall back to scanning
    ``new_string`` so an obviously bad payload still trips the guard.

    Returns None when the tool input is not a markdown write/edit we care
    about.
    """
    tool_input = stdin_data.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        return None

    file_path = tool_input.get("file_path") or ""
    if not isinstance(file_path, str) or not file_path:
        return None

    # Write tool: scan the full content being written.
    if "content" in tool_input and isinstance(tool_input["content"], str):
        return file_path, tool_input["content"]

    # Edit tool: simulate the replacement so the scan sees the post-edit
    # document, not just the inserted fragment.
    new_string = tool_input.get("new_string")
    if not isinstance(new_string, str):
        return None

    raw_old = tool_input.get("old_string")
    old_string = raw_old if isinstance(raw_old, str) else ""
    replace_all = bool(tool_input.get("replace_all", False))

    try:
        existing = Path(file_path).read_text(encoding="utf-8")
    except OSError:
        # File missing or unreadable — Edit would fail anyway. Fall back to
        # scanning new_string alone so a self-contained bad payload still
        # blocks.
        return file_path, new_string

    if old_string and old_string in existing:
        simulated = (
            existing.replace(old_string, new_string)
            if replace_all
            else existing.replace(old_string, new_string, 1)
        )
    else:
        # old_string not found — the actual Edit will fail. Scan new_string
        # alone so a payload that pastes a finding list still trips the guard.
        simulated = new_string

    return file_path, simulated


def _to_repo_relative(path: str, repo_root: str) -> str:
    if not repo_root:
        return path
    if path.startswith(repo_root + "/"):
        return path[len(repo_root) + 1 :]
    return path


def _git_repo_root() -> str:
    from resolve_handoff_src import resolve_harness_workspace_root

    return resolve_harness_workspace_root()


def _run_claude_hook() -> int:
    try:
        data = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if not isinstance(data, dict):
        return 0

    try:
        from _protocol import validate_event  # type: ignore[import-not-found]

        validate_event(data, expected="PreToolUse")
    except ImportError:
        pass

    extracted = _extract_claude_payload(data)
    if extracted is None:
        return 0
    file_path, content = extracted

    repo_root = _git_repo_root()
    rel_path = _to_repo_relative(file_path, repo_root)
    if not _path_should_be_scanned(rel_path):
        return 0

    reasons = _collect_block_reasons(rel_path, content)
    if not reasons:
        return 0

    sys.stderr.write("\n\n".join(reasons) + "\n")
    return 2


# ---------------------------------------------------------------------------
# --scan-staged mode (git pre-commit)
# ---------------------------------------------------------------------------


def _staged_markdown_paths() -> list[str]:
    proc = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    if proc.returncode != 0:
        return []
    out: list[str] = []
    for line in proc.stdout.splitlines():
        candidate = line.strip()
        if candidate and _path_should_be_scanned(candidate):
            out.append(candidate)
    return out


def _staged_blob(rel_path: str) -> str | None:
    proc = subprocess.run(
        ["git", "show", f":{rel_path}"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _run_scan_staged() -> int:
    paths = _staged_markdown_paths()
    failures: list[str] = []
    for rel_path in paths:
        blob = _staged_blob(rel_path)
        if blob is None:
            continue
        failures.extend(_collect_block_reasons(rel_path, blob))
    if failures:
        sys.stderr.write("\n\n".join(failures) + "\n")
        return 1
    return 0


# ---------------------------------------------------------------------------
# --scan-paths mode (make lint-task-plans)
# ---------------------------------------------------------------------------


def _iter_scan_targets(targets: Iterable[str]) -> Iterable[Path]:
    for raw in targets:
        path = Path(raw)
        if not path.exists():
            continue
        if path.is_file():
            yield path
            continue
        for child in path.rglob("*.md"):
            yield child


def _run_scan_paths(targets: list[str]) -> int:
    repo_root = _git_repo_root() or "."
    failures: list[str] = []
    for path in _iter_scan_targets(targets):
        try:
            rel_path = str(path.resolve().relative_to(Path(repo_root).resolve()))
        except ValueError:
            rel_path = str(path)
        if not _path_should_be_scanned(rel_path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        failures.extend(_collect_block_reasons(rel_path, text))
    if failures:
        sys.stderr.write("\n\n".join(failures) + "\n")
        return 1
    return 0


# ---------------------------------------------------------------------------
# --scan-repo mode (make lint-task-plans)
# ---------------------------------------------------------------------------


def _run_scan_repo() -> int:
    """Sweep every tracked .md file in the repo through the path filter.

    The Claude Code hook mode treats any path containing ``/docs/tasks/`` or
    ``/docs/epics/``, plus any filename matching ``*task-plan*.md`` or
    ``*-plan.md``, as in scope. CI must mirror that scope or a bypassed local
    hook can land forbidden finding lists in files outside the hard-coded
    subtree (internal). Using ``git ls-files`` keeps the discovery
    grounded in tracked files and lets the same ``_path_should_be_scanned``
    helper that gates the hook also gate the sweep.
    """
    repo_root = _git_repo_root()
    if not repo_root:
        sys.stderr.write("guard-task-plan-findings: not inside a git repo\n")
        return 1
    proc = subprocess.run(
        ["git", "-C", repo_root, "ls-files", "-z", "*.md"],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    if proc.returncode != 0:
        sys.stderr.write(
            "guard-task-plan-findings: git ls-files failed: " + proc.stderr
        )
        return 1
    failures: list[str] = []
    repo_root_path = Path(repo_root)
    for entry in proc.stdout.split("\0"):
        rel_path = entry.strip()
        if not rel_path or not _path_should_be_scanned(rel_path):
            continue
        try:
            text = (repo_root_path / rel_path).read_text(encoding="utf-8")
        except OSError:
            continue
        failures.extend(_collect_block_reasons(rel_path, text))
    if failures:
        sys.stderr.write("\n\n".join(failures) + "\n")
        return 1
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scan-staged",
        action="store_true",
        help="Scan files currently staged for commit (git pre-commit mode).",
    )
    parser.add_argument(
        "--scan-paths",
        nargs="+",
        metavar="PATH",
        help="Scan the given files or directories on disk.",
    )
    parser.add_argument(
        "--scan-repo",
        action="store_true",
        help="Sweep every tracked .md file in the repo through the path filter.",
    )
    args = parser.parse_args(argv)

    selected = sum(bool(x) for x in (args.scan_staged, args.scan_paths, args.scan_repo))
    if selected > 1:
        parser.error(
            "--scan-staged, --scan-paths, and --scan-repo are mutually exclusive"
        )

    if args.scan_staged:
        return _run_scan_staged()
    if args.scan_paths:
        return _run_scan_paths(args.scan_paths)
    if args.scan_repo:
        return _run_scan_repo()
    return _run_claude_hook()


if __name__ == "__main__":
    sys.exit(main())
