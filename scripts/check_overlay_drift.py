#!/usr/bin/env python3
"""Fail when a git-carried root overlay twin drifts from its payload canon.

``packages/workbay-system/workbay_system/payload/docs/workbay/`` is the single
source of truth. Some of its files are also materialized into the root
``docs/workbay/`` tree so that the public git export ships them: ``export_public.py``
selects with ``git ls-files`` and copies the on-disk bytes of each tracked path
(``shutil.copy2``). A root twin that goes stale therefore reaches consumers even
though the payload is correct. That is the drift this gate exists to catch.

Scope: **tracked paths only.**
-----------------------------
``docs/workbay/{contracts,rules,templates}`` are gitignored (``.gitignore``
lines 52-54). What lives there on any given machine is install state, not repo
state, and it is not the same tree twice:

* on a self-hosting checkout ``make dogfood DOGFOOD_SOURCE=worktree``
  materializes payload -> root, and ``docs/workbay/rules/_sync_from_canon.py``
  additionally mirrors an external heuristics canon into the *same* directory;
* on a fresh worktree ``docs/workbay/rules`` is a symlink straight back into the
  payload, so an on-disk tree walk compares the canon against itself.

An on-disk (``rglob``) comparison of those directories therefore has two failure
modes and no success mode: against the dogfooded tree it reports the second
writer's files as drift forever (a permanently red gate signals nothing about
*new* drift, and trains readers to ignore it), and against the symlinked tree it
passes vacuously. Enumerating with ``git ls-files`` instead removes both: the
canon mirror is untracked, so it is structurally out of scope rather than
excluded by a hand-maintained denylist, and the symlink contributes no tracked
root paths at all.

Only the intersection is asserted — a root-tracked file must have a
payload-tracked twin and identical bytes. A payload file with no root twin is
*not* drift: root materialization is per-install and optional, and the payload
ships to consumers on its own via the package. Requiring the reverse direction
is what made the previous gate un-greenable.

A root-tracked file under a materialized subtree with *no* payload twin is
reported as an ``orphan``: these subtrees are declared install-materialized
surfaces, so a root-only member there has no source of truth and would ship
unreviewed.

A tracked pair that is a symlink, or that resolves to the same file (hardlink
or symlink-to-self), is not a comparable twin: following the link compares the
payload to itself and reads as a false match. Both this gate and the syncer
refuse those identities.

Fail-closed floor
-----------------
An empty comparison set is this gate's strongest false pass: ``git rm`` the last
tracked root twin, or mistype a prefix, and a purely relational check finds
nothing to compare and reports success. ``REQUIRED_TWINS`` and
``MIN_COMPARED_FILES`` make "nothing was compared" a failure instead of a pass,
and the success line prints the coverage count so the gate's reach stays visible
rather than implicit.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

ROOT_DOCS_PREFIX = "docs/workbay"
PAYLOAD_DOCS_PREFIX = "packages/workbay-system/workbay_system/payload/docs/workbay"

# The install-materialized surfaces under docs/workbay/ (the three directories
# gitignored at .gitignore:52-54). Listing a subtree here costs nothing while it
# has no tracked root twins; it means that the moment one is tracked, it is
# covered.
MATERIALIZED_SUBTREES = ("contracts", "rules", "templates")

# Fail-closed floor. Every entry must be present in the compared set on every
# run; losing one is a regression in the public export surface, not a reason to
# compare less. Paths are relative to the subtree root, e.g.
# "contracts/harness-protocol.yaml" -> docs/workbay/contracts/harness-protocol.yaml.
REQUIRED_TWINS = frozenset({"contracts/harness-protocol.yaml"})

# Secondary floor, independent of the named set above: if a future edit empties
# REQUIRED_TWINS, this still refuses a zero-coverage pass.
MIN_COMPARED_FILES = 1

# Repository-redirecting git env vars. A parent GIT_DIR would make
# ``git ls-files`` enumerate the wrong index even when cwd is REPO_ROOT.
_GIT_REPO_ENV = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_COMMON_DIR",
    "GIT_PREFIX",
)

_SYNC_PAIR_RECIPE = (
    "content/missing-on-disk: `make sync-overlay-canon` then commit the result "
    "(repairs already-tracked pairs only)"
)
_ORPHAN_RECIPE = (
    "orphan: untrack the root path (`git rm --cached docs/workbay/<path>`) "
    "or add the matching payload twin; `make sync-overlay-canon` cannot "
    "create a payload counterpart"
)
_FLOOR_RECIPE = (
    "floor/required-twin-missing: force-track the payload bytes at the root "
    "path (`git add -f docs/workbay/<twin>`), then `make sync-overlay-canon`"
)
_IDENTITY_RECIPE = (
    "symlink/same-file: replace the root twin with an independent regular file "
    "(not a symlink or the same resolved file as payload)"
)


@dataclass(frozen=True)
class TwinEnumeration:
    """The tracked root twins and root-only orphans found by the gate."""

    twins: tuple[str, ...]
    orphans: tuple[str, ...]


def decode_captured_stream(blob: str | bytes | None) -> str:
    """Return subprocess capture output as stripped text."""
    if blob is None:
        return ""
    if isinstance(blob, bytes):
        return blob.decode("utf-8", "replace").strip()
    return blob.strip()


def format_subprocess_error(exc: subprocess.CalledProcessError) -> str:
    """Keep the CalledProcessError summary and forward captured streams."""
    chunks = [str(exc)]
    stderr = decode_captured_stream(exc.stderr)
    stdout = decode_captured_stream(exc.stdout)
    if stderr:
        chunks.append(f"stderr: {stderr}")
    if stdout:
        chunks.append(f"stdout: {stdout}")
    return " — ".join(chunks)


def _git_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in _GIT_REPO_ENV:
        env.pop(key, None)
    return env


def _tracked_files() -> set[str]:
    """Return every path in this worktree's index, as posix-relative strings.

    Pin ``--git-dir`` to ``REPO_ROOT/.git`` so a nested directory inside another
    checkout cannot walk up and enumerate the parent index (a false listing).
    """
    proc = subprocess.run(
        [
            "git",
            "--git-dir",
            str(REPO_ROOT / ".git"),
            "--work-tree",
            str(REPO_ROOT),
            "ls-files",
            "-z",
        ],
        check=True,
        capture_output=True,
        env=_git_env(),
    )
    return {item.decode() for item in proc.stdout.split(b"\0") if item}


def _subtree_members(tracked: set[str], prefix: str, subtree: str) -> set[str]:
    base = f"{prefix}/{subtree}/"
    return {path[len(base) :] for path in tracked if path.startswith(base)}


def enumerate_twins(tracked: set[str] | None = None) -> TwinEnumeration:
    """Resolve the gate's twin set once for both checking and synchronization."""
    if tracked is None:
        tracked = _tracked_files()

    twins: list[str] = []
    orphans: list[str] = []
    for subtree in MATERIALIZED_SUBTREES:
        root_members = _subtree_members(tracked, ROOT_DOCS_PREFIX, subtree)
        payload_members = _subtree_members(tracked, PAYLOAD_DOCS_PREFIX, subtree)
        for rel in sorted(root_members):
            twin = f"{subtree}/{rel}"
            if rel in payload_members:
                twins.append(twin)
            else:
                orphans.append(twin)

    return TwinEnumeration(tuple(twins), tuple(orphans))


def independent_twin_violation(repo_root: Path, twin: str) -> str | None:
    """Return a finding when the pair is a symlink or the same resolved file."""
    root_path = repo_root / ROOT_DOCS_PREFIX / twin
    payload_path = repo_root / PAYLOAD_DOCS_PREFIX / twin
    symlink_sides = [
        side
        for side, path in (("root", root_path), ("payload", payload_path))
        if path.is_symlink()
    ]
    if symlink_sides:
        joined = " and ".join(symlink_sides)
        return (
            f"symlink: {twin} — {joined} path is a symlink (following it "
            "compares a path to itself and reads as a false match)"
        )
    if root_path.exists() and payload_path.exists():
        try:
            if os.path.samefile(root_path, payload_path):
                return (
                    f"same-file: {twin} — root and payload resolve to the same file"
                )
        except OSError:
            pass
    return None


def repair_recipes_for(
    findings: list[str],
    *,
    required_missing_from_index: bool = False,
) -> tuple[str, ...]:
    """Map observed finding classes to repairs the matching tool can actually do."""
    classes = {line.split(":", 1)[0] for line in findings}
    recipes: list[str] = []
    if classes & {"content", "missing-on-disk"}:
        recipes.append(_SYNC_PAIR_RECIPE)
    if "orphan" in classes:
        recipes.append(_ORPHAN_RECIPE)
    if required_missing_from_index:
        recipes.append(_FLOOR_RECIPE)
    if classes & {"symlink", "same-file"}:
        recipes.append(_IDENTITY_RECIPE)
    return tuple(recipes)


def main() -> int:
    findings: list[str] = []

    try:
        enumeration = enumerate_twins()
    except subprocess.CalledProcessError as exc:
        # An enumeration failure must never read as "no drift" (OBS-08).
        print(
            "overlay drift gate — cannot enumerate tracked files: "
            f"{format_subprocess_error(exc)}",
            file=sys.stderr,
        )
        return 1
    except OSError as exc:
        print(
            f"overlay drift gate — cannot enumerate tracked files: {exc}",
            file=sys.stderr,
        )
        return 1

    for twin in enumeration.orphans:
        findings.append(
            f"orphan: {twin} — tracked at root with no tracked payload twin"
        )

    compared: list[str] = []
    for twin in enumeration.twins:
        identity = independent_twin_violation(REPO_ROOT, twin)
        if identity:
            findings.append(identity)
            continue

        root_path = REPO_ROOT / ROOT_DOCS_PREFIX / twin
        payload_path = REPO_ROOT / PAYLOAD_DOCS_PREFIX / twin
        if not root_path.is_file():
            findings.append(
                f"missing-on-disk: {twin} — tracked at root but absent from the working tree"
            )
            continue
        if not payload_path.is_file():
            findings.append(
                f"missing-on-disk: payload/{twin} — tracked but absent from the working tree"
            )
            continue

        compared.append(twin)
        if root_path.read_bytes() != payload_path.read_bytes():
            findings.append(
                f"content: {twin} — root export copy differs from payload canon"
            )

    # --- fail-closed floor ---------------------------------------------------
    enumerated = set(enumeration.twins)
    required_missing_from_index = bool(REQUIRED_TWINS - enumerated)
    for twin in sorted(REQUIRED_TWINS - enumerated):
        findings.append(
            f"floor: {twin} — required twin was not compared "
            "(untracked at root or untracked in payload)"
        )
    if MIN_COMPARED_FILES < 1:
        findings.append(
            "floor: MIN_COMPARED_FILES must be >= 1 — a zero floor lets an "
            "empty comparison set pass as success"
        )
    if len(compared) < MIN_COMPARED_FILES:
        findings.append(
            f"floor: compared {len(compared)} twin(s), below the required "
            f"minimum of {MIN_COMPARED_FILES}"
        )

    if findings:
        print(
            "overlay drift gate — git-carried root twins diverged from payload canon:",
            file=sys.stderr,
        )
        for line in findings:
            print(f"  {line}", file=sys.stderr)
        print(
            "Repair by finding class (payload is the source of truth):",
            file=sys.stderr,
        )
        for recipe in repair_recipes_for(
            findings,
            required_missing_from_index=required_missing_from_index,
        ):
            print(f"  {recipe}", file=sys.stderr)
        return 1

    print(
        f"ok: {len(compared)} tracked root twin(s) match payload canon "
        f"across {', '.join(MATERIALIZED_SUBTREES)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
