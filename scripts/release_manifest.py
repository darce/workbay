#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "config" / "release" / "packages.json"
PACKAGES_DIR = REPO_ROOT / "packages"
REQUIRED_FIELDS = (
    "name",
    "path",
    "distribution",
    "artifact_prefix",
    "publish",
    "test_command",
)
# `changelog` is optional: the private monorepo declares + validates it, but the
# public export strips both the CHANGELOG files and the manifest field (verbose
# internal changelogs are not shipped). Validate existence only when present.


def load_manifest() -> list[dict[str, object]]:
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    packages = data.get("packages")
    if not isinstance(packages, list):
        raise SystemExit(f"manifest must contain a 'packages' list: {MANIFEST_PATH}")
    return packages


def validate_packages(packages: list[dict[str, object]]) -> None:
    seen_names: set[str] = set()
    for package in packages:
        missing = [field for field in REQUIRED_FIELDS if field not in package]
        if missing:
            raise SystemExit(f"package entry missing fields {missing}: {package!r}")

        name = package["name"]
        if not isinstance(name, str) or not name:
            raise SystemExit(f"package name must be a non-empty string: {package!r}")
        if name in seen_names:
            raise SystemExit(f"duplicate package name in manifest: {name}")
        seen_names.add(name)

        package_path = REPO_ROOT / str(package["path"])
        if not package_path.is_dir():
            raise SystemExit(f"package path does not exist: {package_path}")
        if "changelog" in package:
            changelog_path = REPO_ROOT / str(package["changelog"])
            if not changelog_path.is_file():
                raise SystemExit(f"changelog path does not exist: {changelog_path}")
        if not isinstance(package["publish"], bool):
            raise SystemExit(f"publish must be a boolean for {name}")
        if "git_mirror" in package and not isinstance(package["git_mirror"], bool):
            raise SystemExit(f"git_mirror must be a boolean for {name}")

    check_pyproject_drift(packages)


def check_pyproject_drift(packages: list[dict[str, object]]) -> None:
    if not PACKAGES_DIR.is_dir():
        return
    manifest_paths = {str(package["path"]) for package in packages}
    drift: list[str] = []
    for child in sorted(PACKAGES_DIR.iterdir()):
        if not child.is_dir():
            continue
        if not (child / "pyproject.toml").is_file():
            continue
        relative = f"packages/{child.name}"
        if relative not in manifest_paths:
            drift.append(relative)
    if drift:
        raise SystemExit(
            "package directories with pyproject.toml are missing from the manifest: "
            + ", ".join(drift)
            + " (add them with publish:false if they should not ship to PyPI)"
        )



def iter_packages(release_only: bool) -> list[dict[str, object]]:
    packages = load_manifest()
    validate_packages(packages)
    if release_only:
        return [package for package in packages if package.get("git_mirror")]
    return packages


def cmd_list(args: argparse.Namespace) -> int:
    packages = iter_packages(args.release_only)
    if args.format == "json":
        json.dump(packages, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    for package in packages:
        value = package[args.field]
        if isinstance(value, bool):
            sys.stdout.write(("true" if value else "false") + "\n")
        else:
            sys.stdout.write(f"{value}\n")
    return 0


def cmd_get(args: argparse.Namespace) -> int:
    packages = iter_packages(False)
    for package in packages:
        if package["name"] != args.package:
            continue
        value = package[args.field]
        if isinstance(value, bool):
            sys.stdout.write(("true" if value else "false") + "\n")
        else:
            sys.stdout.write(f"{value}\n")
        return 0
    raise SystemExit(f"unknown package in release manifest: {args.package}")


def cmd_validate(_: argparse.Namespace) -> int:
    validate_packages(load_manifest())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect the release package manifest")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List manifest packages")
    list_parser.add_argument("--field", default="name", choices=REQUIRED_FIELDS)
    list_parser.add_argument("--format", default="lines", choices=("lines", "json"))
    list_parser.add_argument("--release-only", action="store_true")
    list_parser.set_defaults(func=cmd_list)

    get_parser = subparsers.add_parser("get", help="Read one field for a named package")
    get_parser.add_argument("package")
    get_parser.add_argument("field", choices=REQUIRED_FIELDS)
    get_parser.set_defaults(func=cmd_get)

    validate_parser = subparsers.add_parser("validate", help="Validate manifest structure and paths")
    validate_parser.set_defaults(func=cmd_validate)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())