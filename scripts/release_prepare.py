#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import date
from pathlib import Path

# Sibling module (scripts/ is on sys.path[0] when run as a script). Guards
# managed MCP-server uvx pins against silent drift — see its docstring and
# check_release_bump() below.
from check_mcp_pin_drift import check_release_bump


REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "config" / "release" / "packages.json"
VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
PYPROJECT_VERSION_RE = re.compile(r'(?m)^version\s*=\s*"([^"]+)"\s*$')


def load_manifest() -> list[dict[str, object]]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    packages = manifest.get("packages")
    if not isinstance(packages, list):
        raise SystemExit(f"manifest must contain a 'packages' list: {MANIFEST_PATH}")
    return packages


def load_package(package_name: str) -> dict[str, object]:
    for package in load_manifest():
        if package.get("name") == package_name:
            return package
    raise SystemExit(f"unknown package in release manifest: {package_name}")


def ensure_clean_tree(allow_dirty: bool) -> None:
    if allow_dirty or not (REPO_ROOT / ".git").exists():
        return

    result = subprocess.run(
        ["git", "status", "--short"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or "git status failed")
    if result.stdout.strip():
        raise SystemExit("working tree is dirty; pass --allow-dirty to override")


def compute_version(current_version: str, bump: str) -> str:
    match = VERSION_RE.fullmatch(current_version)
    if not match:
        raise SystemExit(
            f"unsupported current version (expected X.Y.Z): {current_version}"
        )

    major, minor, patch = (int(part) for part in match.groups())
    if bump == "patch":
        return f"{major}.{minor}.{patch + 1}"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    if bump == "major":
        return f"{major + 1}.0.0"
    if VERSION_RE.fullmatch(bump):
        return bump
    raise SystemExit(f"unsupported bump value: {bump}")


def read_pyproject_version(pyproject_path: Path) -> tuple[str, str]:
    source = pyproject_path.read_text(encoding="utf-8")
    match = PYPROJECT_VERSION_RE.search(source)
    if match is None:
        raise SystemExit(f"could not find project version in {pyproject_path}")
    return match.group(1), source


def rewrite_pyproject_version(source: str, new_version: str) -> str:
    return PYPROJECT_VERSION_RE.sub(f'version = "{new_version}"', source, count=1)


def update_changelog(changelog_path: Path, new_version: str, release_date: str) -> str:
    source = changelog_path.read_text(encoding="utf-8")
    entry_heading = f"## [{new_version}] — {release_date}"
    if entry_heading in source:
        return source
    marker = "## Unreleased"
    if marker not in source:
        raise SystemExit(f"could not find '{marker}' in {changelog_path}")
    insertion = (
        f"{marker}\n\n"
        f"{entry_heading}\n\n"
        "### Changed\n\n"
        "- TODO: summarize this release.\n"
    )
    return source.replace(marker, insertion, 1)


def update_dependency_floors(
    package_name: str, distribution: str, new_version: str
) -> list[tuple[Path, str]]:
    updated_files: list[tuple[Path, str]] = []
    dependency_re = re.compile(
        rf'({re.escape(distribution)}>=)(\d+\.\d+\.\d+)(,[^"]+")'
    )

    for package in load_manifest():
        if package["name"] == package_name:
            continue
        pyproject_path = REPO_ROOT / str(package["path"]) / "pyproject.toml"
        source = pyproject_path.read_text(encoding="utf-8")
        updated = dependency_re.sub(rf"\g<1>{new_version}\g<3>", source)
        if updated == source:
            continue
        updated_files.append((pyproject_path, updated))

    return updated_files


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare a package for release")
    parser.add_argument("package")
    parser.add_argument("bump")
    parser.add_argument("--allow-dirty", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    # Env-sync is OPT-IN as of internal. The post-bump
    # re-derive shells out to `make sync-version-drift-*` which can stall for
    # minutes (it reinstalls dist metadata), and it stalled the v0.1.37 release
    # when run per-package. The SSOT is the pyproject the bump just wrote; the
    # installed-env re-derive is a local-dev convenience, so callers opt in with
    # --env-sync when they want `__version__`/importlib.metadata to match HEAD.
    parser.add_argument(
        "--env-sync",
        action="store_true",
        help=(
            "After the bump, re-derive installed dist metadata (root .venv + the "
            "description-service pyenv) so importlib.metadata matches the new "
            "pyproject version. Off by default; slow. A genuine sync failure is a "
            "hard error."
        ),
    )
    parser.add_argument(
        "--no-env-sync",
        action="store_true",
        help=(
            "Back-compat no-op: env-sync is already off by default. Retained so "
            "existing callers/CI that pass --no-env-sync keep working; it also "
            "wins over --env-sync if both are passed."
        ),
    )
    parser.add_argument("--date", dest="release_date", default=date.today().isoformat())
    return parser


def rederive_install_state() -> None:
    """Re-derive installed dist metadata to match the just-written pyproject
    SSOT (internal).

    ``release_prepare`` rewrites every package's ``[project].version`` but the
    installed dists (root ``.venv`` and the ``description-service`` pyenv) keep
    the pre-bump version; ``version_of`` prefers ``importlib.metadata`` over
    pyproject, so ``__version__`` reports stale and the first post-bump
    ``make check-all`` trips ``check-version-drift-pyenv``. Invoke the existing
    idempotent write-side targets so the bump leaves derived install state
    coherent with no manual sync step.

    Both targets self-skip when their env is absent (fresh clone / CI), so this
    is a no-op there; a genuine sync failure is a HARD error — leaving the SSOT
    and installs silently split-brain is the defect this removes.

    Called only on the real write path, below ``main``'s ``--dry-run`` early
    return, so ``--dry-run`` never mutates installed envs.
    """
    commands = (
        ["make", "sync-version-drift-venv"],
        ["make", "-C", "packages/mcp-workbay-handoff", "sync-version-drift-pyenv"],
    )
    for command in commands:
        result = subprocess.run(command, cwd=REPO_ROOT)
        if result.returncode != 0:
            raise SystemExit(
                "release-prepare: post-bump env re-derive failed: "
                f"{' '.join(command)} exited {result.returncode}. Fix the sync "
                "and re-run, or pass --no-env-sync (CI/dispatch only)."
            )


def main() -> int:
    args = build_parser().parse_args()
    ensure_clean_tree(args.allow_dirty)

    package = load_package(args.package)
    package_path = REPO_ROOT / str(package["path"])
    pyproject_path = package_path / "pyproject.toml"
    # `changelog` is optional in the manifest schema (the public mirror drops
    # it), so validate presence here instead of crashing with a bare KeyError.
    if "changelog" not in package:
        raise SystemExit(
            f"package {args.package!r} declares no changelog in config/release/packages.json; "
            "release-prepare requires one"
        )
    changelog_path = REPO_ROOT / str(package["changelog"])

    current_version, pyproject_source = read_pyproject_version(pyproject_path)
    new_version = compute_version(current_version, args.bump)

    # Gate: a managed MCP server (mcp-workbay-orchestrator / -handoff) must
    # not be bumped while its uvx pins still point at the old version.
    # update_dependency_floors below only touches pyproject floors, not the
    # canonical mcp_servers.yaml pin or its generated copy (`make
    # mcp-pins-sync`), so without this the bump would ship the same drift that
    # the v0.1.22 release did. Runs before any write (dry-run included) so the
    # operator updates the manifest, regenerates, and fixes the coupled docs
    # deliberately.
    pin_ok, pin_messages = check_release_bump(REPO_ROOT, args.package, new_version)
    if not pin_ok:
        raise SystemExit("\n".join(pin_messages))

    updated_pyproject = rewrite_pyproject_version(pyproject_source, new_version)
    updated_changelog = update_changelog(changelog_path, new_version, args.release_date)
    updated_dependency_files = update_dependency_floors(
        args.package,
        str(package["distribution"]),
        new_version,
    )

    relative_pyproject = pyproject_path.relative_to(REPO_ROOT)
    relative_changelog = changelog_path.relative_to(REPO_ROOT)
    if args.dry_run:
        print(f"Would bump {args.package}: {current_version} -> {new_version}")
        print(f"Would update {relative_pyproject}")
        print(f"Would update {relative_changelog}")
        for dependency_path, _ in updated_dependency_files:
            print(f"Would update {dependency_path.relative_to(REPO_ROOT)}")
        return 0

    pyproject_path.write_text(updated_pyproject, encoding="utf-8")
    changelog_path.write_text(updated_changelog, encoding="utf-8")
    for dependency_path, updated_source in updated_dependency_files:
        dependency_path.write_text(updated_source, encoding="utf-8")

    print(f"Prepared {args.package}: {current_version} -> {new_version}")
    print(f"Updated {relative_pyproject}")
    print(f"Updated {relative_changelog}")
    for dependency_path, _ in updated_dependency_files:
        print(f"Updated {dependency_path.relative_to(REPO_ROOT)}")

    # Re-derive installed dist metadata so the bump is transactional over its
    # derived state — OPT-IN (--env-sync), off by default. Gated below the
    # --dry-run early return, so a dry run never mutates installed envs.
    # --no-env-sync remains an explicit "definitely skip" override.
    if args.env_sync and not args.no_env_sync:
        rederive_install_state()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
