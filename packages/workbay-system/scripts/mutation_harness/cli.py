#!/usr/bin/env python3
"""CLI for the parallel mutation-guard harness.

Stdout is the JSON SweepVerdict (data). Progress events are line-oriented
JSON objects on stderr so a poller can consume them without parsing the
verdict. Exit 0 only when every non-allowed mutant is killed AND the
node-ID baseline reconciles.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable, Sequence, TextIO

# Allow `python path/to/cli.py` without installing the package.
_SCRIPTS = Path(__file__).resolve().parents[1]
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from mutation_harness.baseline import (  # noqa: E402
    BaselineError,
    load_and_reconcile,
    load_node_ids,
    reconcile_baseline,
)
from mutation_harness.manifest import ManifestError, load_manifest  # noqa: E402
from mutation_harness.models import (  # noqa: E402
    BaselineReport,
    SweepVerdict,
    compute_exit_code,
)
from mutation_harness.runner import make_default_runner  # noqa: E402
from mutation_harness.scheduler import run_sweep  # noqa: E402


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="mutation-harness",
        description=(
            "Parallel mutation-guard sweep: per-mutant sandboxes, LPT list "
            "scheduling over an edgeless conflict graph (not optimal), "
            "JSON verdict + progress stream."
        ),
    )
    p.add_argument(
        "--manifest",
        required=True,
        type=Path,
        help="Path to mutant manifest JSON",
    )
    p.add_argument(
        "--source-root",
        type=Path,
        default=None,
        help="Tree to copy per mutant (default: cwd)",
    )
    p.add_argument(
        "--jobs",
        type=int,
        default=None,
        help="Worker pool size (default: min(cores-1, N))",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Default per-mutant wall-clock timeout in seconds (default: 60)",
    )
    p.add_argument(
        "--full",
        action="store_true",
        help="Run full suite per mutant (ignore per-mutant test lists)",
    )
    p.add_argument(
        "--baseline-expected",
        type=Path,
        default=None,
        help="Expected test node-ID baseline file",
    )
    p.add_argument(
        "--baseline-observed",
        type=Path,
        default=None,
        help="Observed test node-ID file to reconcile against expected",
    )
    p.add_argument(
        "--baseline-floor",
        type=int,
        default=None,
        help="Absolute minimum observed node-ID count",
    )
    p.add_argument(
        "--baseline-bootstrap",
        action="store_true",
        help=(
            "Allow empty expected baseline (first-run only). "
            "Absolute floor still applies."
        ),
    )
    p.add_argument(
        "--json-out",
        type=Path,
        default=None,
        help="Write verdict JSON to file (also printed to stdout if omitted)",
    )
    p.add_argument(
        "--duration-hints",
        type=Path,
        default=None,
        help="JSON object map mutant_id -> seconds for LPT ordering",
    )
    p.add_argument(
        "--no-progress",
        action="store_true",
        help="Suppress line-oriented progress JSON on stderr",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def _progress_writer(stream: TextIO) -> Callable[[dict], None]:
    def _emit(event: dict) -> None:
        stream.write(json.dumps(event, sort_keys=True) + "\n")
        stream.flush()

    return _emit


def resolve_cli_baseline(
    *,
    baseline_expected: Path | None,
    baseline_observed: Path | None = None,
    baseline_floor: int | None = None,
    bootstrap: bool = False,
) -> BaselineReport | None:
    """Adjudicate baseline flags for the production CLI entry point.

    Always routes through :func:`reconcile_baseline` when an expected file is
    given — never fabricates ``ok=True``. An empty expected baseline hard-fails
    unless ``bootstrap=True`` (and even then the absolute floor still applies).
    """
    if baseline_expected is None:
        return None
    if baseline_observed is not None:
        return load_and_reconcile(
            baseline_expected,
            baseline_observed,
            absolute_floor=baseline_floor,
            bootstrap=bootstrap,
        )
    # No observed file: still reconcile so empty expected cannot exit 0.
    # Self-observe the expected set (suite growth / self-check); empty fails.
    expected = load_node_ids(baseline_expected)
    return reconcile_baseline(
        expected,
        expected,
        absolute_floor=baseline_floor,
        bootstrap=bootstrap,
        expected_source=str(baseline_expected),
    )


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parse_args(argv)
    except SystemExit as exc:
        code = exc.code
        return int(code) if isinstance(code, int) else 2

    try:
        mutants = load_manifest(args.manifest)
    except ManifestError as exc:
        print(f"mutation-harness: {exc}", file=sys.stderr)
        return 2

    source_root = (args.source_root or Path.cwd()).resolve()
    if not source_root.is_dir():
        print(
            f"mutation-harness: source root not a directory: {source_root}",
            file=sys.stderr,
        )
        return 2

    duration_hints: dict[str, float] | None = None
    if args.duration_hints is not None:
        try:
            raw = json.loads(Path(args.duration_hints).read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("duration hints must be a JSON object")
            duration_hints = {str(k): float(v) for k, v in raw.items()}
        except (OSError, ValueError, json.JSONDecodeError, TypeError) as exc:
            print(f"mutation-harness: duration hints: {exc}", file=sys.stderr)
            return 2

    progress = None if args.no_progress else _progress_writer(sys.stderr)
    runner = make_default_runner(
        source_root,
        default_timeout=float(args.timeout),
        full_suite=bool(args.full),
        progress=progress,
    )

    results, schedule_mode, jobs_used = run_sweep(
        mutants,
        runner=runner,
        jobs=args.jobs,
        duration_hints=duration_hints,
        progress=progress,
    )

    baseline: BaselineReport | None = None
    if args.baseline_expected is not None:
        try:
            baseline = resolve_cli_baseline(
                baseline_expected=args.baseline_expected,
                baseline_observed=args.baseline_observed,
                baseline_floor=args.baseline_floor,
                bootstrap=bool(args.baseline_bootstrap),
            )
        except BaselineError as exc:
            print(f"mutation-harness: baseline: {exc}", file=sys.stderr)
            baseline = BaselineReport(ok=False, message=str(exc))

    exit_code = compute_exit_code(results, mutants, baseline)
    verdict = SweepVerdict(
        results=results,
        baseline=baseline,
        jobs=jobs_used,
        default_timeout=float(args.timeout),
        full_suite=bool(args.full),
        schedule_mode=schedule_mode,
        exit_code=exit_code,
    )

    text = json.dumps(verdict.to_dict(), indent=2, sort_keys=False) + "\n"
    if args.json_out:
        try:
            args.json_out.parent.mkdir(parents=True, exist_ok=True)
            args.json_out.write_text(text, encoding="utf-8")
        except OSError as exc:
            print(
                f"mutation-harness: failed to write --json-out {args.json_out}: {exc}",
                file=sys.stderr,
            )
            return 8
    else:
        sys.stdout.write(text)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
