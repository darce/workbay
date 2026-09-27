#!/usr/bin/env python3
"""Guard that a publishable package's shipped payload cannot change without a version bump.

implementation note S1. The 0.1.x release shipped two whole feature arcs (implementation note +
0019) of ``workbay-bootstrap`` / ``workbay-system`` changes with **no
version bump** — their PyPI versions silently fell behind ``HEAD`` because the
shipped payload lives in dirs (``scripts/``, ``Makefile.d/``, ``.github/``,
``skills/``, ``docs/``) that do not *look* like package source, and the missing
tags hid it. This gate makes that impossible to ship silently.

For each git-mirror package (``config/release/packages.json`` ``git_mirror:true``),
it finds the commit that *set* the current ``pyproject`` version and fails if
any file under the package's **shipped payload** changed *after* that commit. A
clean bump (no payload change since the version was set) passes.

The version-set commit is found by parsing each ``pyproject.toml`` revision with
``tomllib`` (NOT a brittle source-text pickaxe), so any valid TOML spelling of
the version is handled. If the current version cannot be located in git history
at all (e.g. an uncommitted bump), that is a *hard failure*, not a silent skip.

"Shipped payload" is the UNION of every surface the wheel ships, derived from
the build config:
  * ``[tool.hatch.build.targets.wheel.force-include]`` source keys (the overlay
    surfaces ``workbay-system`` ships), AND
  * ``only-include`` / ``packages`` roots (the importable namespace), AND
  * a fallback of ``<pkg>/src`` for an ordinary src-layout package, AND
  * the ``[project]`` table itself (minus ``version``) — wheel METADATA such as
    dependency pins is shipped payload, the whole payload for a meta-package
    like ``workbay``.

Run standalone (`make check-release-version-drift`), via ``release.sh``
preflight / the single-package path, or the git-mirror release plan step.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11 — tomllib is stdlib only on 3.11+
    sys.stderr.write(f"check-release-version-drift requires Python 3.11+ (tomllib); got {sys.version.split()[0]}\n")
    raise SystemExit(2) from None

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Drift:
    name: str
    version: str
    version_set_commit: str
    changed_files: list[str] = field(default_factory=list)
    reason: str = "shipped payload changed since the version was set"


def _publishable_packages(repo_root: Path) -> list[dict]:
    manifest = json.loads((repo_root / "config" / "release" / "packages.json").read_text(encoding="utf-8"))
    return [p for p in manifest.get("packages", []) if p.get("git_mirror")]


def _wheel_build_config(pyproject: dict) -> dict:
    return pyproject.get("tool", {}).get("hatch", {}).get("build", {}).get("targets", {}).get("wheel", {})


def _package_version(repo_root: Path, pkg_path: str) -> str:
    pyproject = tomllib.loads((repo_root / pkg_path / "pyproject.toml").read_text(encoding="utf-8"))
    return str(pyproject["project"]["version"])


def payload_paths(repo_root: Path, pkg_entry: dict) -> list[str]:
    """Repo-relative paths constituting the package's shipped payload.

    The UNION of all shipped surfaces so nothing that lands in the wheel is
    invisible to the gate: ``force-include`` keys + ``only-include``/``packages``
    roots (hatchling), falling back to ``<pkg>/src`` for a src-layout package
    with no explicit wheel surface.
    """
    pkg_path = pkg_entry["path"]
    pyproject = tomllib.loads((repo_root / pkg_path / "pyproject.toml").read_text(encoding="utf-8"))
    wheel = _wheel_build_config(pyproject)
    paths: set[str] = set()
    paths.update(f"{pkg_path}/{key}" for key in wheel.get("force-include", {}))
    # Union both — hatchling allows only-include AND packages together; a `or`
    # short-circuit would drop `packages` roots whenever only-include is set,
    # under-covering the payload (a false-PASS direction for this gate).
    for inc in [*wheel.get("only-include", []), *wheel.get("packages", [])]:
        # Resolve the wheel-include name to its on-disk source so a src-layout
        # package is not silently missed: `only-include=["workbay_bootstrap"]`
        # with the code under `src/workbay_bootstrap` ships from `src/<name>`.
        # Adding only `<pkg>/<name>` (which does not exist) left `git diff`
        # blind to `src/` payload changes (false-PASS). Add every candidate that
        # exists on disk (a nonexistent path is inert in `git diff`); fall back
        # to the literal only when neither resolves.
        candidates = [f"{pkg_path}/{inc}", f"{pkg_path}/src/{inc}"]
        existing = [c for c in candidates if (repo_root / c).exists()]
        paths.update(existing or [f"{pkg_path}/{inc}"])
    if not paths:
        paths.add(f"{pkg_path}/src")
    return sorted(paths)


def _git(repo_root: Path, *args: str, check: bool = False) -> subprocess.CompletedProcess:
    # Choice: perform every drift-relevant Git read under --no-replace-objects
    # instead of refusing on replace refs. A local refs/replace/* can substitute
    # a drifting commit with a clean tree, so the gate must always inspect the
    # canonical object graph; unrelated local replace refs can remain inert.
    return subprocess.run(
        ["git", "-C", str(repo_root), "--no-replace-objects", *args],
        capture_output=True,
        text=True,
        check=check,
    )


def _version_at(repo_root: Path, commit: str, rel_pyproject: str) -> str | None:
    """The ``project.version`` recorded in ``rel_pyproject`` at ``commit``, or None
    when the file/version is absent there (e.g. the package did not yet exist)."""
    proc = _git(repo_root, "show", f"{commit}:{rel_pyproject}")
    if proc.returncode != 0:
        return None
    try:
        return str(tomllib.loads(proc.stdout)["project"]["version"])
    except (tomllib.TOMLDecodeError, KeyError, TypeError):
        return None


def _project_table_at(repo_root: Path, commit: str, rel_pyproject: str) -> dict | None:
    """The parsed ``[project]`` table at ``commit``, minus ``version``.

    The wheel's METADATA (name, dependencies / ``Requires-Dist`` pins,
    entry points, …) is shipped payload too — for a meta-package like
    ``workbay`` it is the *entire* payload — so it gets the same
    drift treatment as file payloads. ``version`` is excluded because the
    version-set commit changes it by definition.
    """
    proc = _git(repo_root, "show", f"{commit}:{rel_pyproject}")
    if proc.returncode != 0:
        return None
    try:
        table = tomllib.loads(proc.stdout).get("project")
    except tomllib.TOMLDecodeError:
        return None
    if not isinstance(table, dict):
        return None
    return {key: value for key, value in table.items() if key != "version"}


def _version_set_commit(repo_root: Path, pkg_path: str, version: str) -> str | None:
    """The most recent commit that *transitioned* the package version INTO ``version``.

    Spelling-robust: parses each ``pyproject.toml`` revision with tomllib rather
    than pickaxing the source text, so ``version="x"`` / single-quoted / spacing
    variants are all handled. Returns None only when ``version`` never appears in
    history (an uncommitted bump) — the caller treats that as a hard failure.
    """
    rel = f"{pkg_path}/pyproject.toml"
    # --follow traverses across a rename of pyproject.toml (valid with a single
    # pathspec) so a pure-rename-after-set commit is not mis-read as the
    # version-set commit, which would shift the drift baseline forward.
    log = _git(repo_root, "log", "--follow", "--format=%H", "--", rel)
    if log.returncode != 0:
        return None
    for commit in [c for c in log.stdout.split() if c]:  # newest first
        if _version_at(repo_root, commit, rel) != version:
            continue
        # `commit` carries the current version; it *set* it iff its parent did not.
        if _version_at(repo_root, f"{commit}^", rel) != version:
            return commit
    return None


def _changed_since(repo_root: Path, commit: str, paths: list[str]) -> list[str]:
    proc = _git(repo_root, "diff", "--name-only", f"{commit}..HEAD", "--", *paths, check=True)
    return [line for line in proc.stdout.splitlines() if line.strip()]


_SHALLOW_OBJECT_ID_RE = re.compile(r"\A(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")


def _commit_parents(repo_root: Path, revision: str) -> list[str] | None:
    """Return the stored parents for a commit, including parents hidden by a shallow boundary."""
    proc = _git(repo_root, "cat-file", "commit", revision)
    if proc.returncode != 0:
        return None
    return [line.split(maxsplit=1)[1] for line in proc.stdout.splitlines() if line.startswith("parent ")]


def _shallow_history_issue(repo_root: Path) -> tuple[str | None, int | None]:
    """Check shallow state only against the ``HEAD`` ref used by this gate.

    Git's repository-wide shallow status cannot distinguish an inert salvage
    boundary from one that truncates the history this gate walks. A boundary
    is relevant exactly when it is an ancestor of ``HEAD``. The second tuple
    item is the number of unrelated boundaries seen; ``None`` means there was
    no shallow file, so callers can keep the clean non-shallow path silent.
    """
    try:
        common_proc = _git(repo_root, "rev-parse", "--git-common-dir")
    except (OSError, UnicodeError) as exc:
        return f"cannot locate the git common directory while checking shallow history: {exc}", None
    if common_proc.returncode != 0 or not common_proc.stdout.strip():
        return "cannot locate the git common directory while checking shallow history", None

    common = Path(common_proc.stdout.strip())
    if not common.is_absolute():
        common = repo_root / common
    try:
        if not common.is_dir():
            return f"git common directory {common} is not readable", None
        # Opening the directory distinguishes an unreadable common dir from a
        # valid, empty one. The yielded entry is intentionally not inspected.
        next(common.iterdir(), None)
    except (OSError, UnicodeError) as exc:
        return f"cannot read git common directory {common}: {exc}", None

    grafts = common / "info" / "grafts"
    try:
        grafts.lstat()
    except FileNotFoundError:
        pass
    except (OSError, UnicodeError) as exc:
        return f"cannot inspect grafts file {grafts}: {exc}", None
    else:
        return f"info/grafts is present at {grafts}", None

    shallow = common / "shallow"
    try:
        shallow.lstat()
    except FileNotFoundError:
        return None, None
    except (OSError, UnicodeError) as exc:
        return f"cannot inspect shallow-boundary file {shallow}: {exc}", None

    try:
        if not shallow.is_file():
            return f"shallow-boundary file {shallow} is not a readable regular file", None
        raw_lines = shallow.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        return f"cannot read shallow-boundary file {shallow}: {exc}", None

    boundaries: list[str] = []
    for raw_line in raw_lines:
        boundary = raw_line.strip()
        if not boundary:
            continue
        if _SHALLOW_OBJECT_ID_RE.fullmatch(boundary) is None:
            return f"shallow boundary {boundary!r} is not a valid commit object id", None
        boundaries.append(boundary)

    for boundary in boundaries:
        try:
            ancestor_proc = _git(
                repo_root,
                "--no-replace-objects",
                "merge-base",
                "--is-ancestor",
                boundary,
                "HEAD",
            )
        except (OSError, UnicodeError) as exc:
            return f"cannot determine whether shallow boundary {boundary} is an ancestor of HEAD: {exc}", None
        if ancestor_proc.returncode == 0:
            parent_detail = ""
            try:
                parents = _commit_parents(repo_root, boundary)
            except (OSError, UnicodeError):
                parents = None
            if parents:
                parent_detail = f"; stored parent(s): {', '.join(parents)}"
            return (
                f"shallow boundary {boundary} is an ancestor of HEAD; the required "
                f"comparison history is truncated{parent_detail}"
            ), None
        if ancestor_proc.returncode == 1:
            continue
        detail = (ancestor_proc.stderr or ancestor_proc.stdout).strip()
        detail = detail or f"git merge-base exited {ancestor_proc.returncode}"
        return f"cannot determine whether shallow boundary {boundary} is an ancestor of HEAD: {detail}", None

    return None, len(boundaries)


def check(repo_root: Path, only: str | None = None) -> list[Drift]:
    """Return a Drift per publishable package that changed its payload without a bump,
    or whose current version cannot be located in git history (hard failure).

    When ``only`` is given, restrict the check to that single publishable package.
    The git-mirror release syncs only the dist tag it is about to cut, so an
    unrelated package's drift cannot block a decoupled single-package release."""
    drifts: list[Drift] = []
    packages = _publishable_packages(repo_root)
    if only is not None:
        packages = [p for p in packages if p["name"] == only]
    for pkg in packages:
        version = _package_version(repo_root, pkg["path"])
        commit = _version_set_commit(repo_root, pkg["path"], version)
        if commit is None:
            drifts.append(
                Drift(
                    name=pkg["name"],
                    version=version,
                    version_set_commit="(none)",
                    reason=(
                        f"version {version} is not present in git history for "
                        f"{pkg['path']}/pyproject.toml — commit the bump (or it is "
                        f"an undetectable spelling) before releasing"
                    ),
                )
            )
            continue
        changed = _changed_since(repo_root, commit, payload_paths(repo_root, pkg))
        # Wheel metadata drift: the [project] table (dependency pins, entry
        # points, …) ships in METADATA, so a pin rewrite without a bump —
        # e.g. stack-pins-sync touching only workbay's dependencies —
        # is payload drift even though no payload *file* changed.
        rel_pyproject = f"{pkg['path']}/pyproject.toml"
        if _project_table_at(repo_root, "HEAD", rel_pyproject) != _project_table_at(repo_root, commit, rel_pyproject):
            changed = sorted({*changed, rel_pyproject})
        if changed:
            drifts.append(
                Drift(
                    name=pkg["name"],
                    version=version,
                    version_set_commit=commit[:12],
                    changed_files=changed,
                )
            )
    return drifts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="check-release-version-drift")
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repo root to check (default: the git toplevel of cwd).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help=(
            'Emit a machine-readable {"drifted": [names]} object instead of the '
            "human report. Used by release.sh to mark drifted packages in the "
            "release plan/status. Exit code is unchanged (1 when drift exists)."
        ),
    )
    parser.add_argument(
        "--package",
        default=None,
        help=(
            "Only check this publishable package (default: every git_mirror:true "
            "package). The git-mirror release syncs only the dist tag it is about "
            "to cut so an unrelated package's drift cannot block a decoupled "
            "single-package release."
        ),
    )
    args = parser.parse_args(argv)

    if args.repo_root:
        repo_root = Path(args.repo_root).resolve()
    else:
        top = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True)
        repo_root = Path(top.stdout.strip() or ".").resolve()

    packages = _publishable_packages(repo_root)
    if args.package is not None:
        names = {p["name"] for p in packages}
        if args.package not in names:
            sys.stderr.write(
                f"check-release-version-drift: --package {args.package!r} is not a "
                "git_mirror:true package in config/release/packages.json\n"
            )
            return 2

    history_issue, unrelated_boundaries = _shallow_history_issue(repo_root)
    if history_issue is not None:
        sys.stderr.write(
            "check-release-version-drift: refusing to run on a shallow clone or grafted history: "
            f"{history_issue}. Recover with a full clone (e.g. actions/checkout "
            "with fetch-depth: 0).\n"
        )
        return 2
    if unrelated_boundaries is not None:
        sys.stderr.write(
            "check-release-version-drift: shallow repository; "
            f"shallow boundaries seen: {unrelated_boundaries}, all unrelated to HEAD; continuing.\n"
        )

    drifts = check(repo_root, only=args.package)

    if args.json:
        # Machine-readable projection for release.sh: the drifted names only.
        # Exit code still signals drift (1) so callers that care can branch on
        # it, but release.sh reads the name list from stdout regardless.
        print(json.dumps({"drifted": [d.name for d in drifts]}))
        return 1 if drifts else 0

    if not drifts:
        print("ok: no git-mirror package has shipped-payload drift since its version was set")
        return 0

    print("release version drift — these git-mirror packages are not safe to release as-is:")
    for d in drifts:
        print(f"  - {d.name} @ {d.version} (version set at {d.version_set_commit})")
        print(f"      {d.reason}")
        for f in d.changed_files[:20]:
            print(f"        {f}")
        if len(d.changed_files) > 20:
            print(f"        … and {len(d.changed_files) - 20} more")
    print("Bump each package's version (+ CHANGELOG) so the shipped artifact matches HEAD.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
