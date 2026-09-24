#!/usr/bin/env python3
"""Lint guard: ban multi-line `python -c '...'` heredocs in shell scripts.

internal / Layer 3 of the heredoc-eradication bug class fix. Three reasons
the multi-line `python -c '...'` pattern is fragile and should never recur:

1. **Apostrophes inside Python comments close the bash single-quoted
   string prematurely.** internal hit this on a comment that read
   "the active row's revision" — bash terminated the heredoc at `row`,
   the next character was `(`, and the script aborted with
   `syntax error near unexpected token '('`.
2. **Backslash escapes interact unpredictably with bash heredoc parsing.**
   Even when the heredoc parses, escape semantics differ between bash
   single-quoted strings, bash double-quoted strings, and Python source.
3. **Test coverage is gated on shell-execution smoke tests, which most
   pre-commit/CI flows skip.** The package test suite cannot reach the
   bash wrapper layer because it imports the Python directly. The bug
   only manifests at the next `make` invocation, often days after the
   change landed.

The structural fix is to promote inline Python to a standalone module
invoked via ``python <path>`` instead of ``python -c '<heredoc>'``. The
canonical example is ``scripts/_task_start_inline.py`` (called from
``scripts/task-start.sh``) and ``scripts/_task_finish_inline.py``
(called from ``scripts/task-finish.sh``).

This script walks ``scripts/**/*.sh`` and fails on any multi-line
``python -c '...'`` invocation. Single-line ``python -c "import x"``
patterns are allowed because they cannot embed multi-line content and
the apostrophe risk is minimal.

Usage:
    python3 scripts/hooks/lint-no-inline-python-heredoc.py [--paths PATH ...]

Exit code 0 on success, 1 on any violation. Wired into ``make lint-scripts``
in the root Makefile.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Iterable

# The canonical project root: this file lives at scripts/hooks/, so the
# root is two levels up.
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATTERNS = ("scripts/**/*.sh",)

# Match `python` or `python3` (optionally with a path prefix) followed by
# `-c '<body>'`. The body extends until the matching unescaped closing
# single quote. Bash single-quoted strings cannot contain ANY single
# quote (escaped or otherwise) — once a `'` appears the string ends —
# so we treat any `'` as the terminator.
HEREDOC_RE = re.compile(
    r"""
    (?:^|[\s\\;|&])              # word boundary or start of line
    (?P<invocation>
        (?:[^\s'"]*/)?           # optional path prefix
        python3?                  # python or python3
    )
    (?:[^\n]*?)                  # optional flags / args before -c
    \s-c\s+                      # the -c flag itself
    '(?P<body>[^']*)'            # single-quoted argument body
    """,
    re.MULTILINE | re.VERBOSE,
)


def _iter_shell_files(patterns: Iterable[str]) -> Iterable[Path]:
    """Yield matching .sh files for each glob pattern.

    Patterns may be either repo-relative (resolved against REPO_ROOT) or
    absolute (resolved against the filesystem root). Path.glob refuses
    absolute patterns, so the absolute branch parses the pattern via
    Path.parent.glob(Path.name).
    """
    seen: set[Path] = set()
    for pattern in patterns:
        candidate_iter: Iterable[Path]
        pattern_path = Path(pattern)
        if pattern_path.is_absolute():
            anchor = pattern_path.parent
            tail = pattern_path.name
            # Walk up until the anchor is glob-free; collect parts that contain
            # `*` so they re-attach as the pattern.
            while any(ch in anchor.name for ch in "*?["):
                tail = f"{anchor.name}/{tail}"
                anchor = anchor.parent
            try:
                candidate_iter = anchor.glob(tail)
            except (NotImplementedError, OSError):
                continue
        else:
            candidate_iter = REPO_ROOT.glob(pattern)
        for path in candidate_iter:
            if not path.is_file():
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            yield resolved


def _line_number(text: str, offset: int) -> int:
    return text[:offset].count("\n") + 1


def find_violations(paths: Iterable[Path]) -> list[tuple[Path, int, int]]:
    """Return a list of (path, line, body_lines) violations.

    A violation is any ``python -c '...'`` heredoc whose body contains
    a literal newline (i.e. is multi-line). Single-line `-c` invocations
    are allowed.
    """
    violations: list[tuple[Path, int, int]] = []
    for path in paths:
        try:
            text = path.read_text()
        except OSError:
            continue
        for match in HEREDOC_RE.finditer(text):
            body = match.group("body")
            if "\n" not in body:
                continue
            line = _line_number(text, match.start("invocation"))
            body_lines = body.count("\n") + 1
            violations.append((path, line, body_lines))
    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "--paths",
        nargs="+",
        default=list(DEFAULT_PATTERNS),
        help="Glob patterns relative to the repo root (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    violations = find_violations(_iter_shell_files(args.paths))
    if not violations:
        return 0

    print(
        "\u274c lint-no-inline-python-heredoc: "
        f"{len(violations)} multi-line `python -c '...'` heredoc(s) detected.",
        file=sys.stderr,
    )
    for path, line, body_lines in violations:
        rel = path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path
        print(
            f"  {rel}:{line}: multi-line `python -c '...'` heredoc "
            f"({body_lines} lines)",
            file=sys.stderr,
        )
    print(
        "\n  Multi-line `python -c '...'` heredocs are banned because:\n"
        "    1. Apostrophes in Python comments close the bash string prematurely.\n"
        "    2. Backslash escapes interact unpredictably with bash heredoc parsing.\n"
        "    3. Shell-execution test coverage is rare; bugs surface in production.\n"
        "\n"
        "  Promote the inline Python to a standalone .py file:\n"
        "    \"${PYENV_ROOT}/versions/${PYENV_VERSION}/bin/python\" \\\n"
        "      \"${REPO_ROOT}/scripts/_my_inline_script.py\"\n"
        "\n"
        "  See scripts/_task_start_inline.py and scripts/_task_finish_inline.py\n"
        "  for the canonical pattern. internal / heredoc bug class eradication.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
