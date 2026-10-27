#!/usr/bin/env python3
"""Post-merge probes for the two silent failure modes of a stale-branch landing.

Usage::

    python scripts/merge_landing_probe.py <merge-rev>            # both probes
    python scripts/merge_landing_probe.py --probe dead <merge>   # one probe

Both classes were observed while landing the in-flight worktree backlog, and
neither is caught by a test suite, because in both cases the suite stays green:

MERGEDEAD
    The branch's NON-conflicting hunks import a parallel, earlier implementation
    of a feature main already solved under different symbol names. Nothing
    references the imported names, so the merged file carries two implementations
    of one contract. Reported per changed Python file as a name the merge added
    that is either defined twice in the same scope or never referenced.

MERGEREVERT
    A stale branch's NON-conflicting hunks DELETE a fix main landed after the
    merge base. Conflict resolution never sees those hunks, and the tests that
    covered the fix are often deleted in the same merge. Reported as a deleted
    line that a main-side commit after the merge base had added.

Both probes are advisory evidence, not a verdict: an intentional removal and a
silent revert look identical to Git, which is exactly why a human has to read
the report. Exit code 1 means "something to read", not "the merge is wrong".

Style findings (ruff/mypy) never appear here and never block a landing; they are
tracked as style debt and fixed in a later wave. A red TEST blocks a landing; a
red LINTER does not.
"""

from __future__ import annotations

import argparse
import ast
import collections
import re
import subprocess
import sys


def _git(*args: str) -> str:
    """Run git and return stdout, treating a non-zero exit as empty output."""
    proc = subprocess.run(["git", *args], capture_output=True, text=True)
    return proc.stdout if proc.returncode == 0 else ""


def _blob(rev: str, path: str) -> str | None:
    proc = subprocess.run(["git", "show", f"{rev}:{path}"], capture_output=True, text=True)
    return proc.stdout if proc.returncode == 0 else None


def _scoped_defs(src: str) -> dict[tuple[str, str], list[int]] | None:
    """Map ``(scope_path, name) -> [lineno]`` so sibling scopes never collide.

    Two methods with the same name in different classes are distinct scopes, not
    a redefinition, and must not be reported.
    """
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    out: dict[tuple[str, str], list[int]] = {}

    def walk(node: ast.AST, scope: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                out.setdefault((scope, child.name), []).append(child.lineno)
                walk(child, f"{scope}.{child.name}")
            else:
                walk(child, scope)

    walk(tree, "")
    return out


_DUNDER_RE = re.compile(r"^__\w+__$")


def _word_re(name: str) -> re.Pattern[str]:
    return re.compile(rf"\b{re.escape(name)}\b")


def _referenced_in_other_file(merge: str, name: str, path: str) -> bool:
    """True when some other Python file at ``merge`` mentions ``name``.

    A cross-file reference is the common case for a module's public entry
    points, and a same-file-only scan reports every one of them as dead. Names
    imported inside a function body are invisible to a module-level import scan,
    so this greps the merged tree instead of parsing imports.
    """
    out = _git("grep", "--fixed-strings", "--word-regexp", "-l", "-e", name, merge, "--", "*.py")
    for line in out.splitlines():
        hit = line.split(":", 1)[-1]
        if hit and hit != path:
            return True
    return False


def probe_dead_code(first_parent: str, merge: str) -> list[str]:
    """Names the merge added that nothing references, or that shadow a sibling."""
    changed = _git("diff", "--name-only", first_parent, merge).split()
    problems: list[str] = []
    for path in (f for f in changed if f.endswith(".py")):
        new = _blob(merge, path)
        if new is None:
            continue
        new_defs = _scoped_defs(new)
        if new_defs is None:
            continue
        old = _blob(first_parent, path)
        old_defs = _scoped_defs(old) if old else {}
        if old_defs is None:
            old_defs = {}
        lines = new.splitlines()
        for (scope, name), locs in new_defs.items():
            if len(locs) > 1:
                where = scope or "<module>"
                problems.append(f"{path}: {name!r} defined {len(locs)}x in scope {where!r} at {locs}")
            if (scope, name) in old_defs or name.startswith("test_"):
                # A pre-existing name is not merge-introduced, and pytest
                # collects test functions by name, so unreferenced is normal.
                continue
            if _DUNDER_RE.match(name):
                # __post_init__ and friends are called by the interpreter, never
                # by name; reporting them is noise that trains readers to ignore
                # the probe, which is worse than having no probe at all.
                continue
            pattern = _word_re(name)
            refs = sum(len(pattern.findall(line)) for line in lines)
            if refs > len(locs):
                continue
            if _referenced_in_other_file(merge, name, path):
                continue
            problems.append(f"{path}:{locs[0]} {name!r} added by merge but never referenced")
    return problems


def probe_silent_revert(merge: str) -> list[str]:
    """Lines the merge deletes that main added after the merge base."""
    parents = _git("rev-list", "--parents", "-n", "1", merge).split()
    if len(parents) < 3:
        return []
    first, second = parents[1], parents[2]
    base = _git("merge-base", first, second).strip()
    if not base:
        return []
    diff = _git("diff", "--unified=0", first, merge)
    path: str | None = None
    deleted: dict[str, list[str]] = collections.defaultdict(list)
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            path = line[6:]
        elif path and line.startswith("-") and not line.startswith("---"):
            body = line[1:].strip()
            if body:
                deleted[path].append(body)

    problems: list[str] = []
    for path, lines in sorted(deleted.items()):
        main_side = _git("log", "--format=%H", f"{base}..{first}", "--", path).split()
        if not main_side:
            continue
        added_on_main: set[str] = set()
        for sha in main_side:
            for raw in _git("show", sha, "--", path).splitlines():
                if raw.startswith("+") and not raw.startswith("+++") and len(raw) > 1:
                    added_on_main.add(raw[1:].strip())
        hits = [line for line in lines if line in added_on_main]
        if hits:
            problems.append(
                f"{path}: merge deletes {len(hits)} line(s) main added after {base[:9]}"
            )
            problems.extend(f"    - {hit[:150]}" for hit in hits[:6])
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("merge", help="the merge commit to probe")
    parser.add_argument(
        "--probe",
        choices=("dead", "revert", "both"),
        default="both",
        help="which probe to run (default: both)",
    )
    args = parser.parse_args(argv)

    parents = _git("rev-list", "--parents", "-n", "1", args.merge).split()
    if len(parents) < 3:
        print(f"{args.merge} is not a merge commit; nothing to probe")
        return 0
    first_parent = parents[1]

    found = 0
    if args.probe in ("dead", "both"):
        problems = probe_dead_code(first_parent, args.merge)
        for line in problems:
            print(f"MERGEDEAD: {line}" if not line.startswith("    ") else line)
        print(f"MERGEDEAD: {len(problems)} problem(s)")
        found += len(problems)
    if args.probe in ("revert", "both"):
        problems = probe_silent_revert(args.merge)
        for line in problems:
            print(f"MERGEREVERT: {line}" if not line.startswith("    ") else line)
        reverted = sum(1 for line in problems if not line.startswith("    "))
        print(f"MERGEREVERT: {reverted} path(s) with reverted lines")
        found += reverted
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
