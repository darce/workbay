from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

_REQUIRED_FIXTURE_SOURCES = (
    "mcp-workbay-handoff/src",
    "workbay-protocol/src",
)


def _resolve_package_root() -> Path:
    here = Path(__file__).resolve()
    direct = here.parents[2]
    if (direct / "workbay_system" / "payload").is_dir() and (
        direct / "pyproject.toml"
    ).is_file():
        return direct

    for start in (Path.cwd(), *Path.cwd().parents):
        for candidate in (start / "packages" / "workbay-system", start):
            if (
                candidate.is_dir()
                and (candidate / "pyproject.toml").is_file()
                and (candidate / "workbay_system" / "payload").is_dir()
            ):
                return candidate.resolve()

    return direct


PACKAGE_ROOT = _resolve_package_root()
PACKAGES_ROOT = PACKAGE_ROOT.parent


@dataclass(frozen=True, slots=True)
class ResolutionContext:
    target_root: Path
    packages_root: Path


def resolve_context(
    target: Path | None = None,
    packages_root: Path | None = None,
    *,
    require_fixture_sources: bool = True,
) -> ResolutionContext:
    resolved_target = (target or _resolve_package_root()).resolve()
    resolved_packages = (packages_root or PACKAGE_ROOT.parent).resolve()

    if not resolved_target.is_dir():
        raise ValueError(
            f"--target must be an existing directory; got {resolved_target!s}"
        )

    # Only the fixture-building check (check_harness_sync) copies real package
    # sources out of packages_root; lint-hoisted-paths and check-skills never
    # do, so they must not fail-fast on a packages_root that lacks them.
    if require_fixture_sources:
        for relative in _REQUIRED_FIXTURE_SOURCES:
            fixture_source = resolved_packages / relative
            if not fixture_source.is_dir():
                raise ValueError(
                    f"--packages-root {resolved_packages!s} is missing required fixture source "
                    f"{relative!s}; provide --packages-root pointing at a tree containing "
                    "mcp-workbay-handoff/src and workbay-protocol/src"
                )

    return ResolutionContext(
        target_root=resolved_target,
        packages_root=resolved_packages,
    )


def parse_overlay_tooling_argv(
    argv: list[str], *, require_fixture_sources: bool = True
) -> tuple[ResolutionContext, list[str]]:
    """Parse uniform --target/--packages-root flags; return context + remaining argv."""
    import argparse

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--target")
    parser.add_argument("--packages-root")
    parser.add_argument("--repo-root", help="deprecated alias for --target")
    known, remaining = parser.parse_known_args(argv)
    target_raw = known.target or known.repo_root
    packages_raw = known.packages_root
    ctx = resolve_context(
        target=Path(target_raw) if target_raw else None,
        packages_root=Path(packages_raw) if packages_raw else None,
        require_fixture_sources=require_fixture_sources,
    )
    return ctx, remaining
