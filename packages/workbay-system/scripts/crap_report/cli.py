#!/usr/bin/env python3
"""CLI for CRAP reports — stdout is data, diagnostics on stderr ([AGT-15]/[AGT-21])."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from pathlib import Path
from typing import Sequence

# Allow `python path/to/cli.py` without installing the package.
_SCRIPTS = Path(__file__).resolve().parents[1]
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from crap_report.complexity import RadonUnavailableError, collect_complexity  # noqa: E402
from crap_report.coverage_load import load_coverage_json, normalize_repo_path  # noqa: E402
from crap_report.join import score_methods  # noqa: E402
from crap_report.render import to_json_dict, to_markdown_table  # noqa: E402


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="crap-report",
        description="Rank Python methods by CRAP score (complexity × coverage).",
    )
    p.add_argument(
        "--coverage",
        required=True,
        type=Path,
        help="Path to coverage.py JSON report (coverage.json)",
    )
    p.add_argument(
        "--path",
        action="append",
        dest="paths",
        type=Path,
        help="Source path (file or dir); repeatable. "
        "Default: files listed in the coverage report (not the whole repo).",
    )
    p.add_argument("--threshold", type=float, default=30.0)
    p.add_argument("--top", type=int, default=50, help="Markdown top-N (default 50)")
    p.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Write JSON to file (creates parent dirs; exit 8 on write failure)",
    )
    p.add_argument(
        "--md-out",
        type=Path,
        default=None,
        help="Write Markdown to file (creates parent dirs; exit 8 on write failure)",
    )
    p.add_argument(
        "--allow-empty",
        action="store_true",
        help="Exit 0 when no methods match after filters (default exit 6). "
        "Does not suppress exit 7 (coverage report has no on-disk files).",
    )
    p.add_argument(
        "--include-tests",
        action="store_true",
        help="Include test_*/tests paths (venv/generated still excluded)",
    )
    p.add_argument(
        "--include-unmeasured",
        action="store_true",
        help="Score methods missing from coverage as cov=0 (classic CRAP; noisy). "
        "Default: rank only measured methods; list unmeasured high-CC separately.",
    )
    p.add_argument(
        "--scan-all-paths",
        action="store_true",
        help="When --path is omitted, scan repo root instead of coverage file set.",
    )
    p.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Repo root for path normalization (default: cwd)",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def _paths_from_coverage(index_files: dict, repo_root: Path) -> list[Path]:
    """Prefer scanning only files present in the coverage report."""
    out: list[Path] = []
    for key in index_files:
        rel = normalize_repo_path(key, repo_root=repo_root)
        p = repo_root / rel
        if p.is_file():
            out.append(p)
    return out


def _tool_versions() -> dict[str, str]:
    """Resolved installed versions for report provenance (never raises)."""
    versions: dict[str, str] = {
        "python": f"{sys.version_info.major}.{sys.version_info.minor}."
        f"{sys.version_info.micro}",
    }
    for dist in ("radon", "coverage"):
        try:
            versions[dist] = _pkg_version(dist)
        except PackageNotFoundError:
            versions[dist] = "unknown"
        except Exception:
            # Defensive: never let metadata lookup abort the report run.
            versions[dist] = "unknown"
    return versions


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
    except SystemExit as exc:
        code = exc.code
        return int(code) if isinstance(code, int) else 2

    coverage_path: Path = args.coverage
    if not coverage_path.is_file():
        print(f"crap-report: coverage file not found: {coverage_path}", file=sys.stderr)
        return 3

    repo_root = (args.repo_root or Path.cwd()).resolve()
    try:
        # Same root for load + collect + join so absolute coverage keys map.
        index = load_coverage_json(coverage_path, repo_root=repo_root)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"crap-report: invalid coverage JSON: {exc}", file=sys.stderr)
        return 4

    if args.paths:
        paths = list(args.paths)
        path_mode = "explicit"
    elif args.scan_all_paths:
        paths = [repo_root]
        path_mode = "repo_root"
    else:
        paths = _paths_from_coverage(index.files, repo_root)
        path_mode = "coverage_files"
        if not paths:
            print(
                "crap-report: coverage report has no on-disk files under repo root "
                "(exit 7); pass --path or --scan-all-paths",
                file=sys.stderr,
            )
            return 7

    try:
        units = collect_complexity(paths, repo_root=repo_root)
    except RadonUnavailableError as exc:
        print(f"crap-report: {exc}", file=sys.stderr)
        return 5
    except OSError as exc:
        print(f"crap-report: scan failed: {exc}", file=sys.stderr)
        return 5

    measured_only = not args.include_unmeasured
    report = score_methods(
        units,
        index,
        threshold=args.threshold,
        apply_default_excludes=True,
        include_tests=args.include_tests,
        measured_only=measured_only,
        repo_root=repo_root,
        provenance={
            "coverage_path": str(coverage_path),
            "coverage_mtime": datetime.fromtimestamp(
                coverage_path.stat().st_mtime, tz=timezone.utc
            ).isoformat(),
            "cc_tool": "radon",
            "tool_versions": _tool_versions(),
            "paths": [str(p) for p in paths],
            "path_mode": path_mode,
            "measured_only": measured_only,
            "repo_root": str(repo_root),
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        },
    )

    # Diagnostics on stderr ([AGT-15])
    s = report.summary
    print(
        f"crap-report: path_mode={path_mode} units={len(units)} "
        f"ranked={s['methods']} crappy={s['crappy']} "
        f"unmeasured_omitted={s.get('unmeasured_omitted', 0)}",
        file=sys.stderr,
    )
    if s.get("unmeasured_omitted", 0) and measured_only:
        print(
            "crap-report: tip: unmeasured high-CC listed in MD/JSON separately; "
            "use --include-unmeasured for classic cov=0 scoring",
            file=sys.stderr,
        )

    if not report.methods and not args.allow_empty:
        print(
            "crap-report: no methods after filters "
            "(check --path / excludes; use --allow-empty to silence)",
            file=sys.stderr,
        )
        return 6

    payload = to_json_dict(report)
    text = json.dumps(payload, indent=2, sort_keys=False) + "\n"
    if args.json_out:
        try:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(text, encoding="utf-8")
        except OSError as exc:
            print(
                f"crap-report: failed to write --json-out {args.json_out}: {exc}",
                file=sys.stderr,
            )
            return 8
    else:
        sys.stdout.write(text)

    if args.md_out:
        try:
            args.md_out.parent.mkdir(parents=True, exist_ok=True)
            args.md_out.write_text(
                to_markdown_table(report, top_n=args.top), encoding="utf-8"
            )
        except OSError as exc:
            print(
                f"crap-report: failed to write --md-out {args.md_out}: {exc}",
                file=sys.stderr,
            )
            return 8

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
