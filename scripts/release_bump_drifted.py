#!/usr/bin/env python3
"""One-shot coordinated bump of every drifted git-mirror package.

The v0.1.37 release exposed how manual the coordinated bump is: discover which
packages drifted (`check_release_version_drift`), bump each in dependency order
(`release_prepare` per package), then separately re-sync the front-door
`workbay` anchor's exact `==` member pins (`stack_pins.py sync`) — six
ordering-sensitive steps the operator drives by hand, with `--allow-dirty`
plumbing after the first bump.

This script collapses that into one command (`make release-bump-drifted`):

  1. Read the drift set from `check_release_version_drift`.
  2. Bump each drifted *member* (patch) in dependency order — reusing
     `release_prepare`, so its uvx-pin gate and `>=` downstream-floor cascade
     still run.
  3. Whenever the anchor is prepared, `stack_pins.py sync` rewrites the `workbay` anchor's
     exact `==` pins, and the anchor is bumped too (its shipped `[project]`
     table changed, so it would otherwise drift on the next check).

It leaves the working tree dirty for the operator to review, fill in the CHANGELOG
stubs (the `changelog-ready` preflight gate enforces this), and commit. Env-sync
stays off (release_prepare default) so the coordinated bump never stalls.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

# Siblings in scripts/ (sys.path[0] when run as a script).
import check_release_version_drift as drift_mod
import release_prepare as prepare_mod

REPO_ROOT = Path(__file__).resolve().parents[1]
RELEASE_PREPARE = REPO_ROOT / "scripts" / "release_prepare.py"
STACK_PINS = REPO_ROOT / "scripts" / "stack_pins.py"

# The front-door runtime version anchor: it pins every member with an exact
# `==` (not a `>=` floor), so `release_prepare`'s floor cascade does NOT touch
# it — `stack_pins.py sync` is its only writer. Mirrors stack_pins.py's own
# single-anchor assumption (workbay-stack was retired; `workbay` is the anchor).
ANCHOR = "workbay"


def git_mirror_order(repo_root: Path) -> list[str]:
    """git-mirror package names in manifest (dependency) order."""
    return [str(p["name"]) for p in drift_mod._publishable_packages(repo_root)]


def plan_bumps(drifted: list[str], manifest_order: list[str], anchor: str = ANCHOR) -> tuple[list[str], bool]:
    """Decide which members to bump and whether the anchor must follow.

    Returns ``(member_bumps_in_order, bump_anchor)``. A member bump rewrites the
    anchor's exact pins, so the anchor is bumped whenever any member bumped, or
    when the anchor's own payload drifted.
    """
    drifted_set = set(drifted)
    members = [p for p in manifest_order if p in drifted_set and p != anchor]
    bump_anchor = (anchor in drifted_set) or bool(members)
    return members, bump_anchor


def _run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, cwd=REPO_ROOT)
    if result.returncode != 0:
        raise SystemExit(f"release-bump-drifted: step failed ({' '.join(cmd)}) exited {result.returncode}")


class ReleasePlanningError(ValueError):
    """The floor graph cannot produce a safe release plan."""


def cascade_bumps(repo_root: Path, drifted: list[str]) -> list[str]:
    """Order the floor-rewrite closure upstream first, including private packages.

    Graph edges match release_prepare's floor writer. Manifest order breaks
    ties between ready packages; publication flags do not affect versioning.
    """
    manifest = json.loads((repo_root / "config/release/packages.json").read_text(encoding="utf-8"))["packages"]
    sources = {
        p["name"]: prepare_mod.read_pyproject_version(repo_root / p["path"] / "pyproject.toml") for p in manifest
    }
    edges = {name: [] for name in sources}
    rewrites = {name: [] for name in sources}
    for package in manifest:
        name = package["name"]
        version = prepare_mod.compute_version(sources[name][0], "patch")
        pattern = re.compile(rf'({re.escape(package["distribution"])}>=)(\d+\.\d+\.\d+)(,[^"]+")')
        for dependent, (_, source) in sources.items():
            if dependent != name and pattern.search(source):
                edges[name].append(dependent)
                if pattern.sub(rf"\g<1>{version}\g<3>", source) != source:
                    rewrites[name].append(dependent)

    # Diagnose an actual cycle before computing even an empty bump plan.
    visited = set()
    active = []

    def visit(name):
        if name in active:
            cycle = active[active.index(name):] + [name]
            raise ReleasePlanningError("floor dependency cycle: " + " -> ".join(cycle))
        if name in visited:
            return
        active.append(name)
        for dependent in edges[name]:
            visit(dependent)
        active.pop()
        visited.add(name)

    for name in sources:
        visit(name)

    incoming = {name: 0 for name in sources}
    for dependents in edges.values():
        for dependent in dependents:
            incoming[dependent] += 1
    order = []
    while incoming:
        name = next(name for name, count in incoming.items() if count == 0)
        order.append(name)
        del incoming[name]
        for dependent in edges[name]:
            incoming[dependent] -= 1

    planned = set(drifted)
    for name in order:
        if name in planned:
            planned.update(rewrites[name])
    return [name for name in order if name in planned]


def ensure_clean_tree(repo_root: Path, allow_dirty: bool) -> None:
    if allow_dirty or not (repo_root / ".git").exists():
        return

    result = subprocess.run(
        ["git", "status", "--short"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or "git status failed")
    if result.stdout.strip():
        raise SystemExit("working tree is dirty; commit/stash unrelated changes or pass --allow-dirty to override")


def ensure_drift_checkable(repo_root: Path) -> None:
    shallow = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "--is-shallow-repository"],
        capture_output=True,
        text=True,
        check=False,
    )
    if shallow.returncode == 0 and shallow.stdout.strip() == "true":
        raise SystemExit(
            "release-bump-drifted: refusing to run on a shallow clone; "
            "git history is required to plan version-drift bumps"
        )


def drifted_names_or_fail(drifts: list[drift_mod.Drift]) -> list[str]:
    hard_failures = [d for d in drifts if d.version_set_commit == "(none)"]
    if hard_failures:
        details = ", ".join(f"{d.name} @ {d.version}" for d in hard_failures)
        raise SystemExit(
            "release-bump-drifted: drift check found version state that cannot "
            f"be auto-bumped safely ({details}); commit or fix the version bump "
            "first, then re-run"
        )
    return [d.name for d in drifts]


def _prepare(pkg: str, release_date: str) -> None:
    # --allow-dirty: the coordinated bump mutates several pyprojects in one pass,
    # so every call after the first sees a dirty tree. --no-env-sync: never stall
    # the release on the installed-metadata re-derive (it is off by default now,
    # but pass it explicitly so this stays correct if that default ever changes).
    _run(
        [
            sys.executable,
            str(RELEASE_PREPARE),
            pkg,
            "patch",
            "--allow-dirty",
            "--no-env-sync",
            "--date",
            release_date,
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="release-bump-drifted")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the coordinated bump plan without writing anything.",
    )
    parser.add_argument(
        "--allow-dirty",
        action="store_true",
        help=(
            "Allow starting from a dirty working tree. By default the command "
            "fails fast before planning/writing so uncommitted release drift "
            "cannot be mixed with the coordinated bump."
        ),
    )
    parser.add_argument("--date", dest="release_date", default=date.today().isoformat())
    args = parser.parse_args(argv)

    ensure_clean_tree(REPO_ROOT, args.allow_dirty)
    ensure_drift_checkable(REPO_ROOT)

    drifts = drift_mod.check(REPO_ROOT)
    drifted_names = drifted_names_or_fail(drifts)
    try:
        cascaded_names = cascade_bumps(REPO_ROOT, drifted_names)
    except ReleasePlanningError as exc:
        print(f"release-bump-drifted: {exc}", file=sys.stderr)
        return 1
    members, bump_anchor = plan_bumps(cascaded_names, cascaded_names)

    if not members and not bump_anchor:
        print("release-bump-drifted: no git-mirror package has drifted — nothing to bump.")
        return 0

    plan_line = ", ".join(members) if members else "(no members)"
    print(f"release-bump-drifted: drifted members to bump (in order): {plan_line}")
    if bump_anchor:
        why = "drifted" if ANCHOR in set(drifted_names) else "member pins changed"
        print(f"release-bump-drifted: will also bump anchor {ANCHOR!r} ({why}) + sync pins")

    if args.dry_run:
        print("release-bump-drifted: dry-run — no files written.")
        return 0

    for pkg in members:
        _prepare(pkg, args.release_date)

    if bump_anchor:
        # Rewrite the anchor's exact `==` pins to the freshly-bumped members.
        _run([sys.executable, str(STACK_PINS), "sync"])

    if bump_anchor:
        _prepare(ANCHOR, args.release_date)

    print(
        "release-bump-drifted: done. Review the diff, fill in each CHANGELOG entry "
        "(replace the TODO stub — the changelog-ready gate enforces this), then "
        "commit and run `make release-public`."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
