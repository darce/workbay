#!/usr/bin/env python3
"""Token budget checker for guideline files.

Warns (exit 1) when any scanned file exceeds the per-file token budget.
Token count: len(text) // 4  (GPT-4 approximation; no external dependency).

Usage:
    python .github/hooks/token_budget.py [--budget N] [paths ...]

Can also be used as a pre-commit hook:
    entry: python .github/hooks/token_budget.py
    args: [--budget, "2000"]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _approx_tokens(text: str) -> int:
    """Approximate GPT-4 token count: 4 characters per token on average."""
    return max(1, len(text) // 4)


def check(paths: list[Path], budget: int) -> list[tuple[Path, int]]:
    """Return (path, token_count) pairs for files that exceed the budget."""
    over: list[tuple[Path, int]] = []
    for p in paths:
        if not p.exists() or not p.is_file():
            continue
        tokens = _approx_tokens(p.read_text(encoding="utf-8", errors="replace"))
        if tokens > budget:
            over.append((p, tokens))
    return over


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Token budget checker for guideline files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("paths", nargs="*", help="Files to check")
    parser.add_argument(
        "--budget",
        type=int,
        default=2000,
        help="Maximum tokens per file (default: 2000)",
    )
    args = parser.parse_args(argv)

    paths = [Path(p) for p in args.paths]
    over = check(paths, args.budget)

    if over:
        print(
            f"[token_budget] WARNING: {len(over)} file(s) exceed the "
            f"{args.budget}-token guideline budget:"
        )
        for p, tokens in over:
            print(f"  {p}: ~{tokens} tokens  (budget: {args.budget})")
        print(
            "[token_budget] Trim generic/upstream reference content or "
            "split into a ctx7-linked stub before merging."
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
