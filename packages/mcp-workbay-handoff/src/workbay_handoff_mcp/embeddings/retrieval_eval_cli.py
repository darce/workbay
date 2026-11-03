"""CLI entry for ``make retrieval-eval`` (implementation note S2).

Deterministic offline batch runner. Writes a markdown report. Does **not**
produce an S3 verdict when the fixture is synthetic smoke data. The
``snapshot`` fixture evaluates an out-of-tree handoff.db and prints a real
computed verdict (finding 7681).

``--gate MIN_RECALL`` turns the run into a fail-closed process gate (EVALGATE-H-02):

* evaluated and semantic mean_recall_at_10 >= bound -> exit 0
* evaluated and below the bound -> non-zero
* could not evaluate (missing snapshot, provider, or ground truth) -> non-zero,
  with a message naming the missing prerequisite. Never degrades to smoke.

Smoke + ``--gate`` still asserts a real recall bound over the synthetic corpus
and states plainly that it certifies machinery, not retrieval quality.

Usage (via Make)::

    make retrieval-eval
    make retrieval-eval OUT=tmp/retrieval-eval.md FIXTURE=smoke
    make retrieval-eval FIXTURE=snapshot  # requires WORKBAY_RETRIEVAL_EVAL_SNAPSHOT

Environment:

    WORKBAY_RETRIEVAL_EVAL_SNAPSHOT  — out-of-tree handoff.db path for
                                       ``--fixture snapshot`` (never committed)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


def _default_out_path() -> Path:
    return Path("tmp") / "retrieval-eval-report.md"


def _ensure_src_importable() -> None:
    """Allow file-path invocation without a PYTHONPATH prefix on the command."""
    src_dir = str(Path(__file__).resolve().parents[2])
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def _load_operator_ground_truth_cases() -> Any:
    """Import S1 cases from the sibling tests/ dir (no PYTHONPATH prefix)."""
    tests_dir = str(Path(__file__).resolve().parents[3] / "tests")
    if tests_dir not in sys.path:
        sys.path.insert(0, tests_dir)
    from retrieval_eval_ground_truth import load_ground_truth_cases  # noqa: PLC0415

    return load_ground_truth_cases()


def _apply_recall_gate(report: Any, *, min_recall: float, fixture: str) -> int:
    """Exit 0 iff semantic mean_recall_at_10 meets ``min_recall``; else non-zero."""
    if fixture == "smoke":
        print(
            "gate: smoke fixture certifies machinery, not retrieval quality",
            file=sys.stderr,
        )
    semantic = report.pooled.get("semantic") if report.pooled else None
    if semantic is None:
        print(
            "error: cannot evaluate: semantic arm metrics missing from report",
            file=sys.stderr,
        )
        return 2
    recall = float(semantic.mean_recall_at_10)
    print(f"gate: semantic mean_recall_at_10={recall:.4f} min={min_recall}")
    if recall < min_recall:
        print(
            f"error: semantic mean_recall_at_10={recall:.4f} is below gate {min_recall}",
            file=sys.stderr,
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="implementation note retrieval-eval harness (S0+S2)")
    parser.add_argument(
        "--fixture",
        default="smoke",
        choices=("smoke", "snapshot"),
        help=(
            "Fixture set to evaluate. 'smoke' is synthetic/non-authoritative; "
            "'snapshot' evaluates WORKBAY_RETRIEVAL_EVAL_SNAPSHOT with operator ground truth."
        ),
    )
    parser.add_argument(
        "--out",
        default=str(_default_out_path()),
        help="Markdown report output path (default: tmp/retrieval-eval-report.md)",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional JSON dump path for the structured report",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Ranking depth for arm selection (default 10)",
    )
    parser.add_argument(
        "--gate",
        type=float,
        default=None,
        metavar="MIN_RECALL",
        help=(
            "Fail closed: exit non-zero if semantic mean_recall_at_10 is below this "
            "bound, or if the run cannot evaluate. Does not fall back to smoke."
        ),
    )
    args = parser.parse_args(argv)

    # Snapshot cannot-evaluate must name the missing env var even when the
    # embeddings extra is absent — check before importing eval_retrieval.
    snapshot_path = None
    if args.fixture == "snapshot":
        snapshot_path = os.environ.get("WORKBAY_RETRIEVAL_EVAL_SNAPSHOT")
        if not snapshot_path:
            print(
                "error: WORKBAY_RETRIEVAL_EVAL_SNAPSHOT is unset or empty; "
                "required for --fixture snapshot (path to out-of-tree handoff.db)",
                file=sys.stderr,
            )
            return 2

    _ensure_src_importable()

    # Lazy imports so `--help` works without embedding extras.
    from workbay_handoff_mcp.embeddings.eval_retrieval import (  # noqa: PLC0415
        render_retrieval_eval_markdown,
        run_retrieval_eval,
        run_snapshot_eval,
    )

    if args.fixture == "smoke":
        # Import from tests fixture module when available; fall back to inlined
        # synthetic path via the package-local smoke builder for `make` runs
        # that do not put tests/ on PYTHONPATH.
        cases, provider, open_conn, fixture_label, cleanup = _load_smoke()
        try:
            with open_conn() as conn:
                report = run_retrieval_eval(
                    conn,
                    provider,
                    cases,
                    top_k=args.top_k,
                    fixture_label=fixture_label,
                )
        finally:
            cleanup()
    elif args.fixture == "snapshot":
        from workbay_handoff_mcp.embeddings.provider import EmbeddingProvider  # noqa: PLC0415

        provider = EmbeddingProvider.from_env()
        if provider is None:
            print(
                "error: EmbeddingProvider.from_env() returned None "
                "(embeddings not provisioned or ONNX artifact absent)",
                file=sys.stderr,
            )
            return 2

        try:
            cases = _load_operator_ground_truth_cases()
        except ImportError as exc:
            print(
                "error: cannot import operator ground truth "
                f"(retrieval_eval_ground_truth missing under package tests/): {exc}",
                file=sys.stderr,
            )
            return 2

        assert snapshot_path is not None  # checked above
        try:
            report = run_snapshot_eval(
                snapshot_path,
                provider=provider,
                cases=cases,
                top_k=args.top_k,
            )
        except FileNotFoundError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    else:  # pragma: no cover - argparse choices guard this
        print(f"unknown fixture: {args.fixture}", file=sys.stderr)
        return 2

    md = render_retrieval_eval_markdown(report)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"wrote {out_path}")

    if args.json_out:
        json_path = Path(args.json_out)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(report.as_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {json_path}")

    # Smoke keeps the deferred sentinel; snapshot prints the real computed verdict.
    if args.fixture == "snapshot":
        print(f"verdict: {report.verdict}")
    elif report.verdict is None:
        print("verdict: deferred (no S1 ground truth; smoke-only machinery proof)")

    if args.gate is not None:
        return _apply_recall_gate(report, min_recall=args.gate, fixture=args.fixture)
    return 0


def _load_smoke():
    """Build an isolated synthetic smoke DB + cases for machinery proof."""
    import tempfile  # noqa: PLC0415
    from pathlib import Path as _Path  # noqa: PLC0415

    from workbay_handoff_mcp.embeddings._smoke_corpus import (  # noqa: PLC0415
        SMOKE_FIXTURE_LABEL,
        bound_smoke_cases,
        build_smoke_runtime_corpus,
    )
    from workbay_handoff_mcp.shared_schema import _get_db_connection  # noqa: PLC0415

    tmp = tempfile.TemporaryDirectory(prefix="retrieval-eval-smoke-")
    root = _Path(tmp.name)
    provider, _task_ref = build_smoke_runtime_corpus(root)
    cases = bound_smoke_cases()

    def open_conn():
        return _get_db_connection()

    def cleanup() -> None:
        tmp.cleanup()

    return cases, provider, open_conn, SMOKE_FIXTURE_LABEL, cleanup


if __name__ == "__main__":
    raise SystemExit(main())
