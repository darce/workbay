#!/usr/bin/env python3
"""Report what a stale branch still adds that the integration ref does not have.

The expensive failure in a long landing crank is spending a full review, a
conflict resolution and a gate run on a branch whose work main already
absorbed under different symbol names or in a different module. That branch
merges cleanly enough to look productive and contributes nothing, and its
residual hunks can even revert the later correction main landed on top.

This probe answers one narrow, checkable question per branch:

    which named surface does the branch add that the integration tree does
    not already define anywhere?

It deliberately does NOT answer "is this branch superseded". Name presence is
not semantic equivalence, and a pure refactor adds no names at all. The output
is evidence for that judgment, not the judgment:

* ``new_surface``   -- ``def``/``class`` names added by the branch that the
  integration tree defines nowhere. A non-empty list means the branch has
  work to land; read it.
* ``absorbed``      -- names the branch adds that the integration tree already
  defines, in this file or any other. Relocation into a new module counts as
  absorbed, which is the common shape.
* ``contradicted``  -- names both sides define where the bodies differ. This is
  the dangerous class: the same test name asserting the opposite outcome means
  merging the branch reverts a later correction. Always read these.
* ``touched_only``  -- the branch changes existing symbols without adding
  names. The probe has nothing to say about these; they need a real diff read.

Exit 0 always: this is an instrument, not a gate. A gate that guesses at
supersession would eventually delete work.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

_DEF_RE = re.compile(r"^\s*(?:async\s+)?(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)")
#: Dunders are interpreter-called and are defined in nearly every tree, so a
#: presence check on them says nothing about supersession either way.
_DUNDER_RE = re.compile(r"^__\w+__$")


def _git(*args: str) -> str:
    proc = subprocess.run(
        ["git", *args], capture_output=True, text=True, check=False, timeout=180
    )
    if proc.returncode not in (0, 1):
        raise SystemExit(f"git {chr(32).join(args)} failed: {proc.stderr.strip()[:400]}")
    return proc.stdout


def _changed_python_files(base: str, branch: str) -> list[str]:
    out = _git("diff", "--name-only", "--diff-filter=d", f"{base}..{branch}", "--", "*.py")
    return [line for line in out.splitlines() if line.strip()]


def _defined_names(rev: str, path: str) -> dict[str, str]:
    """Map every ``def``/``class`` name defined at ``rev:path`` to its body text."""
    proc = subprocess.run(
        ["git", "show", f"{rev}:{path}"], capture_output=True, text=True, check=False, timeout=60
    )
    if proc.returncode != 0:
        return {}
    names: dict[str, str] = {}
    lines = proc.stdout.splitlines()
    starts: list[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        m = _DEF_RE.match(line)
        if m and not _DUNDER_RE.match(m.group(1)):
            starts.append((idx, m.group(1)))
    for pos, (idx, name) in enumerate(starts):
        end = starts[pos + 1][0] if pos + 1 < len(starts) else len(lines)
        # Last definition wins, which mirrors Python own binding rule: a
        # shadowed earlier body is not the one that runs.
        names[name] = "\n".join(line.rstrip() for line in lines[idx:end]).strip()
    return names


def _defined_anywhere(rev: str, name: str) -> bool:
    """True when ``rev`` defines ``name`` in any tracked Python file.

    Relocating a helper into a new module is the common way an integration ref
    absorbs a branch, so a same-file check reports absorbed work as new.
    """
    out = _git(
        "grep",
        "--extended-regexp",
        "-l",
        "-e",
        # POSIX ERE, which is what git grep speaks: \s and \b are Python-only
        # and match nothing here. A relocated definition missed this way is
        # reported as new surface, which is the expensive direction to be wrong.
        rf"^[[:space:]]*(async +)?(def|class) +{re.escape(name)}([[:space:]]|\(|:)",
        rev,
        "--",
        "*.py",
    )
    return bool(out.strip())


def probe(branch: str, integration: str = "main") -> dict:
    base = _git("merge-base", integration, branch).strip()
    if not base:
        raise SystemExit(f"no merge base between {integration} and {branch}")
    new_surface: list[str] = []
    absorbed: list[str] = []
    contradicted: list[dict] = []
    touched_only: list[str] = []
    for path in _changed_python_files(base, branch):
        base_names = _defined_names(base, path)
        branch_names = _defined_names(branch, path)
        head_names = _defined_names(integration, path)
        added = sorted(set(branch_names) - set(base_names))
        if not added:
            touched_only.append(path)
            continue
        for name in added:
            if name in head_names:
                if head_names[name] != branch_names[name]:
                    contradicted.append({"file": path, "name": name})
                else:
                    absorbed.append(f"{path}::{name}")
                continue
            if _defined_anywhere(integration, name):
                absorbed.append(f"{path}::{name} (relocated)")
                continue
            new_surface.append(f"{path}::{name}")
    return {
        "branch": branch,
        "integration": integration,
        "merge_base": base,
        "new_surface": new_surface,
        "absorbed": absorbed,
        "contradicted": contradicted,
        "touched_only": touched_only,
        "note": (
            "Evidence, not a verdict. An empty new_surface with a non-empty "
            "contradicted or touched_only list still needs a diff read."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("branch")
    parser.add_argument("--integration", default="main")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--repo", default=".")
    args = parser.parse_args(argv)

    os.chdir(Path(args.repo).resolve())
    report = probe(args.branch, args.integration)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    print(f"branch       {report['branch']}  (base {report['merge_base'][:9]})")
    print(f"new surface  {len(report['new_surface'])}")
    for item in report["new_surface"]:
        print(f"  + {item}")
    print(f"absorbed     {len(report['absorbed'])}")
    print(f"contradicted {len(report['contradicted'])}")
    for item in report["contradicted"]:
        print(f"  ! {item['file']}::{item['name']} differs from {report['integration']}")
    print(f"touched-only files {len(report['touched_only'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
