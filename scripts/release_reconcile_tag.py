#!/usr/bin/env python3
"""Provenance-verified tag reconciliation for the ``pypi_without_tag`` state.

implementation note S4c. A package can be *published on PyPI but carry no git tag*
(``pypi_without_tag``) — e.g. ``workbay-protocol 0.1.7`` / ``mcp-workbay-
orchestrator 0.5.1`` shipped tag-less this cycle. Creating the missing tag is
bookkeeping, but tagging the *wrong* commit is exactly the 0019 hazard: ``HEAD``
may already carry unreleased changes under the same version, so blindly tagging
``HEAD`` as ``<pkg>-v<version>`` would anchor the tag to source that was never
the published artifact.

This path REFUSES to tag unless the candidate commit's package source is
byte-identical to the published PyPI sdist. It downloads the published sdist for
``version``, compares every shipped source file against the commit's blobs, and
only emits (or, under ``--execute``, runs) the ``git tag`` when the parity is
perfect; any mismatched or missing file aborts with a non-zero exit.

Usage:
    python scripts/release_reconcile_tag.py \
        --package workbay-protocol --version 0.1.7 \
        [--commit <ref>] [--pkg-path packages/workbay-protocol] \
        [--repo <root>] [--execute]

``--execute`` creates the local tag ``<package>-v<version>`` at the verified
commit; pushing it remains a separate, explicit operator step.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Files an sdist carries that are build-generated, not git-tracked source. They
# have no commit counterpart and must not count as a parity discrepancy.
_GENERATED_BASENAMES = {"PKG-INFO"}


def sdist_source_files(sdist_bytes: bytes) -> dict[str, bytes]:
    """Map ``relpath -> bytes`` for the source files inside a ``.tar.gz`` sdist.

    The leading ``<name>-<version>/`` directory is stripped, and build-generated
    metadata (``PKG-INFO``, ``*.egg-info/*``) is excluded so only git-tracked
    source remains for the byte-parity comparison.
    """
    files: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(sdist_bytes), mode="r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            parts = member.name.split("/", 1)
            if len(parts) != 2:
                # A bare top-level file with no <name>-<version>/ prefix; skip.
                continue
            relpath = parts[1]
            basename = relpath.rsplit("/", 1)[-1]
            if basename in _GENERATED_BASENAMES:
                continue
            if any(seg.endswith(".egg-info") for seg in relpath.split("/")):
                continue
            extracted = tar.extractfile(member)
            if extracted is None:
                continue
            files[relpath] = extracted.read()
    return files


def _git(repo_root: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo_root), *args],
        capture_output=True,
        check=False,
    )


def commit_blob(repo_root: Path, commit: str, relpath: str) -> bytes | None:
    """Return the bytes of ``relpath`` at ``commit``, or None if absent there."""
    proc = _git(repo_root, "show", f"{commit}:{relpath}")
    if proc.returncode != 0:
        return None
    return proc.stdout


@dataclass
class ParityReport:
    """Outcome of comparing published sdist source against a commit's blobs."""

    commit: str
    matched: list[str] = field(default_factory=list)
    mismatched: list[str] = field(default_factory=list)
    missing_in_commit: list[str] = field(default_factory=list)
    # Shipped-source files present in the commit but absent from the published
    # sdist — i.e. the commit carries unreleased additions under the same
    # version (the 0019 hazard); a non-empty list must block tagging.
    extra_in_commit: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            not self.mismatched
            and not self.missing_in_commit
            and not self.extra_in_commit
        )


def _glob_to_regex(pattern: str) -> str:
    """Translate a hatchling-style exclude glob to an anchored regex.

    Supports ``**`` (any path segments, including none), ``*`` (within a
    segment), and a trailing ``/`` (a directory prefix, e.g. ``tests/``).
    """
    if pattern.endswith("/"):
        return re.escape(pattern.rstrip("/")) + r"(?:/.*)?\Z"
    out: list[str] = []
    i, n = 0, len(pattern)
    while i < n:
        char = pattern[i]
        if char == "*":
            if i + 1 < n and pattern[i + 1] == "*":
                out.append(".*")
                i += 2
                if i < n and pattern[i] == "/":
                    out.append("/?")
                    i += 1
                continue
            out.append("[^/]*")
            i += 1
            continue
        out.append(re.escape(char))
        i += 1
    return "".join(out) + r"\Z"


def _excluded(relpath: str, patterns: list[str]) -> bool:
    return any(re.match(_glob_to_regex(p), relpath) for p in patterns)


def _sdist_exclude_patterns(repo_root: Path, commit: str, pkg_path: str) -> list[str]:
    """The package's declared sdist ``exclude`` globs at ``commit`` (or [])."""
    blob = commit_blob(repo_root, commit, f"{pkg_path}/pyproject.toml")
    if blob is None:
        return []
    try:
        pyproject = tomllib.loads(blob.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError):
        return []
    sdist = (
        pyproject.get("tool", {})
        .get("hatch", {})
        .get("build", {})
        .get("targets", {})
        .get("sdist", {})
    )
    return [str(p) for p in sdist.get("exclude", [])]


def _commit_tracked_files(repo_root: Path, commit: str, pkg_path: str) -> list[str]:
    """Repo-relative-to-``pkg_path`` tracked file paths under ``pkg_path`` at ``commit``."""
    proc = _git(repo_root, "ls-tree", "-r", "--name-only", commit, "--", pkg_path)
    if proc.returncode != 0:
        return []
    prefix = f"{pkg_path}/"
    files = []
    for line in proc.stdout.decode("utf-8", "replace").splitlines():
        if line.startswith(prefix):
            files.append(line[len(prefix) :])
    return files


def verify_parity(
    sdist_files: dict[str, bytes],
    repo_root: Path,
    commit: str,
    pkg_path: str,
) -> ParityReport:
    """Compare each published source file against ``<pkg_path>/<relpath>`` at ``commit``.

    A perfect report (``ok``) means the commit's package source is byte-identical
    to what PyPI shipped; any mismatch or missing file means the commit is NOT a
    safe anchor for the version tag.
    """
    report = ParityReport(commit=commit)
    for relpath in sorted(sdist_files):
        commit_path = f"{pkg_path}/{relpath}" if pkg_path else relpath
        blob = commit_blob(repo_root, commit, commit_path)
        if blob is None:
            report.missing_in_commit.append(relpath)
        elif blob == sdist_files[relpath]:
            report.matched.append(relpath)
        else:
            report.mismatched.append(relpath)

    # Reverse direction: catch shipped-source files the COMMIT carries that the
    # published sdist lacks (unreleased additions under the same version). Scope
    # to the top-level roots the sdist actually ships from, and drop files the
    # package's own sdist `exclude` globs remove, so non-shipped trees (tests/,
    # evals/) do not cause spurious refusals. Any residual ambiguity errs toward
    # refusing — the safe direction for a release-provenance gate.
    shipped_roots = {relpath.split("/", 1)[0] for relpath in sdist_files}
    exclude_patterns = _sdist_exclude_patterns(repo_root, commit, pkg_path)
    sdist_relpaths = set(sdist_files)
    for relpath in sorted(_commit_tracked_files(repo_root, commit, pkg_path)):
        if relpath in sdist_relpaths:
            continue
        if relpath.split("/", 1)[0] not in shipped_roots:
            continue
        if _excluded(relpath, exclude_patterns):
            continue
        report.extra_in_commit.append(relpath)
    return report


def fetch_sdist_bytes(distribution: str, version: str) -> bytes:
    """Download the published sdist for ``distribution==version`` from PyPI.

    Hermetic test seam: ``RELEASE_RECONCILE_FAKE_SDIST`` (a filesystem path) is
    read instead of touching the network.
    """
    fake = os.environ.get("RELEASE_RECONCILE_FAKE_SDIST")
    if fake:
        return Path(fake).read_bytes()

    import urllib.request

    url = f"https://pypi.org/pypi/{distribution}/{version}/json"
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        payload = json.loads(response.read().decode("utf-8"))
    sdist_url = None
    for entry in payload.get("urls", []):
        if entry.get("packagetype") == "sdist":
            sdist_url = entry.get("url")
            break
    if sdist_url is None:
        raise SystemExit(
            f"{distribution} {version}: no sdist found on PyPI (cannot verify "
            f"byte-parity for a tag-less reconciliation)"
        )
    with urllib.request.urlopen(sdist_url, timeout=60) as response:  # noqa: S310
        return response.read()


def reconcile(
    *,
    repo_root: Path,
    pkg_path: str,
    distribution: str,
    version: str,
    commit: str,
    execute: bool,
) -> int:
    """Verify byte-parity then (under ``execute``) tag the commit. Returns exit code."""
    sdist_bytes = fetch_sdist_bytes(distribution, version)
    sdist_files = sdist_source_files(sdist_bytes)
    if not sdist_files:
        print(
            f"[reconcile] {distribution} {version}: published sdist contained no "
            f"comparable source files — refusing to tag.",
            file=sys.stderr,
        )
        return 1

    report = verify_parity(sdist_files, repo_root, commit, pkg_path)
    tag = f"{distribution}-v{version}"

    if not report.ok:
        print(
            f"[reconcile] REFUSING to tag {tag} at {commit}: package source is "
            f"NOT byte-identical to the published PyPI sdist.",
            file=sys.stderr,
        )
        for relpath in report.mismatched:
            print(f"  mismatch: {relpath}", file=sys.stderr)
        for relpath in report.missing_in_commit:
            print(f"  missing in commit: {relpath}", file=sys.stderr)
        for relpath in report.extra_in_commit:
            print(f"  unreleased addition in commit: {relpath}", file=sys.stderr)
        print(
            "  pick the commit that produced the published artifact (or do not "
            "tag); tagging drifted source would misattribute the release.",
            file=sys.stderr,
        )
        return 1

    print(
        f"[reconcile] OK: {len(report.matched)} source files byte-identical to "
        f"the published {distribution} {version} sdist at {commit}."
    )
    if execute:
        proc = _git(repo_root, "tag", tag, commit)
        if proc.returncode != 0:
            sys.stderr.write(proc.stderr.decode("utf-8", "replace"))
            return proc.returncode
        print(f"[reconcile] created tag {tag} at {commit} (push it explicitly).")
    else:
        print(f"[reconcile] dry-run — would create tag: git tag {tag} {commit}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="release-reconcile-tag")
    parser.add_argument("--package", required=True, help="distribution name")
    parser.add_argument("--version", required=True, help="already-published version")
    parser.add_argument("--commit", default="HEAD", help="candidate commit to tag")
    parser.add_argument(
        "--pkg-path",
        default=None,
        help="repo-relative package dir (default: packages/<package>)",
    )
    parser.add_argument("--repo", default=str(REPO_ROOT), help="repository root")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="create the local tag when parity holds (default: dry-run)",
    )
    args = parser.parse_args(argv)

    pkg_path = args.pkg_path or f"packages/{args.package}"
    return reconcile(
        repo_root=Path(args.repo),
        pkg_path=pkg_path,
        distribution=args.package,
        version=args.version,
        commit=args.commit,
        execute=bool(args.execute),
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
