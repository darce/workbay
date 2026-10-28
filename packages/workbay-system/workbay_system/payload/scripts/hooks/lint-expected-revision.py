#!/usr/bin/env python3
"""Lint guard: scripts/_*.py callers must pass expected_revision explicitly.

internal / Layer 3 of the expected_revision bug class eradication. The
MCP boundary already rejects missing ``expected_revision`` for existing
rows, but the error only fires at runtime — long after the code is
committed. This static check catches the bug at lint time.

Walks ``scripts/_*.py`` (the standalone inline-Python modules promoted
from shell heredocs by internal) and fails when any ``set_handoff_state``
or ``update_task_status`` call appears without ``expected_revision`` in
the same call expression.

Usage:
    python3 scripts/hooks/lint-expected-revision.py [--paths PATTERN ...]

Exit code 0 on success, 1 on any violation. Wired into ``make lint-scripts``
in the root Makefile.
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PATTERNS = ("scripts/_*.py",)

# Function names whose calls must include `expected_revision` as a keyword argument.
GUARDED_FUNCTIONS = frozenset({"set_handoff_state", "update_task_status"})


def _iter_python_files(patterns: Iterable[str]) -> Iterable[Path]:
    seen: set[Path] = set()
    for pattern in patterns:
        pattern_path = Path(pattern)
        if pattern_path.is_absolute():
            anchor = pattern_path.parent
            tail = pattern_path.name
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
            if not path.is_file() or not path.name.endswith(".py"):
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            yield resolved


def _collect_import_aliases(tree: ast.Module) -> dict[str, str]:
    """Map local alias names to their canonical imported names.

    Handles ``from X import set_handoff_state as write_state`` by recording
    ``{"write_state": "set_handoff_state"}``. Bare ``import`` statements and
    ``from X import Y`` without alias produce identity mappings (Y -> Y).
    This closes internal: without the alias map, a renamed import
    bypasses the guard entirely because the AST walker only sees the local
    alias name in the call node, not the canonical function name.
    """
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                canonical = alias.name
                local = alias.asname or alias.name
                if canonical in GUARDED_FUNCTIONS:
                    aliases[local] = canonical
    return aliases


def find_violations(paths: Iterable[Path]) -> list[tuple[Path, int, str]]:
    """Return (path, line, description) for each violating call or parse error."""
    violations: list[tuple[Path, int, str]] = []
    for path in paths:
        try:
            source = path.read_text()
        except OSError:
            continue
        # internal: surface parse failures as violations instead of
        # silently skipping. A syntactically broken scripts/_*.py file means
        # the guard cannot verify it, which is worse than a false positive.
        try:
            tree = ast.parse(source, filename=str(path))
        except SyntaxError as exc:
            violations.append((
                path,
                exc.lineno or 0,
                f"SyntaxError: {exc.msg} (file cannot be parsed; lint guard cannot verify it)",
            ))
            continue

        # Build an alias map so `from X import set_handoff_state as Y`
        # is caught when `Y(...)` is called without expected_revision.
        aliases = _collect_import_aliases(tree)

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            # Resolve the function name from either a bare name or an
            # attribute (e.g. `mcp_server.set_handoff_state(...)`).
            call_name: str | None = None
            if isinstance(node.func, ast.Name):
                call_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                call_name = node.func.attr
            if call_name is None:
                continue
            # Resolve through the alias map. If the call name is a known
            # alias for a guarded function, use the canonical name.
            canonical = aliases.get(call_name, call_name)
            if canonical not in GUARDED_FUNCTIONS:
                continue
            # Check whether `expected_revision` is present as a keyword argument.
            keyword_names = {kw.arg for kw in node.keywords if kw.arg is not None}
            if "expected_revision" not in keyword_names:
                display_name = (
                    f"{call_name} (alias for {canonical})" if call_name != canonical else call_name
                )
                violations.append((path, node.lineno, f"{display_name}(...) has no `expected_revision` kwarg"))
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

    violations = find_violations(_iter_python_files(args.paths))
    if not violations:
        return 0

    print(
        f"\u274c lint-expected-revision: "
        f"{len(violations)} call(s) missing `expected_revision`.",
        file=sys.stderr,
    )
    for path, line, description in violations:
        rel = path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path
        print(f"  {rel}:{line}: {description}", file=sys.stderr)
    print(
        "\n  Every call to set_handoff_state or update_task_status that touches\n"
        "  an existing handoff_state row requires expected_revision. The canonical\n"
        "  pattern:\n"
        "\n"
        "    identity = get_handoff_state(sections='identity')\n"
        "    active = identity.get('data', {}).get('active')\n"
        "    expected_revision = active.get('revision') if active else None\n"
        "    set_handoff_state(..., expected_revision=expected_revision)\n"
        "\n"
        "  See scripts/_task_start_inline.py for the canonical example.\n"
        "  internal / expected_revision bug class eradication.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
