"""Lightweight direct-path CLI for the harness ``errors-record`` command."""

from __future__ import annotations

import argparse
import json
from importlib import metadata as importlib_metadata
from typing import Sequence

from .agent_errors import record_agent_error_direct


def _resolve_installed_package_version(package_name: str) -> str | None:
    try:
        try:
            return importlib_metadata.version(package_name)
        except importlib_metadata.PackageNotFoundError:
            distributions = importlib_metadata.packages_distributions().get(package_name)
            if distributions:
                return importlib_metadata.version(distributions[0])
    except Exception:  # noqa: BLE001 -- provenance is best-effort for telemetry
        pass
    return None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Record a direct-path workbay agent error.")
    parser.add_argument("--error-class", required=True)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--detail")
    parser.add_argument("--tool-name")
    parser.add_argument("--command-preview")
    parser.add_argument("--package-name")
    parser.add_argument("--package-version")
    parser.add_argument("--workbay-release")
    parser.add_argument("--harness", default="hook")
    parser.add_argument("--task-ref")
    return parser


def record_from_args(args: argparse.Namespace) -> dict:
    package_version = args.package_version
    if package_version is None and args.package_name:
        package_version = _resolve_installed_package_version(args.package_name)
    return record_agent_error_direct(
        error_class=args.error_class,
        summary=args.summary,
        detail=args.detail,
        tool_name=args.tool_name,
        command_preview=args.command_preview,
        package_name=args.package_name,
        package_version=package_version,
        workbay_release=args.workbay_release,
        harness=args.harness,
        task_ref=args.task_ref,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    result = record_from_args(args)
    print(json.dumps(result, indent=2, sort_keys=True))
    if isinstance(result, dict) and result.get("ok") is False:
        if not str(result.get("error") or "").startswith("spool write failed:"):
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
