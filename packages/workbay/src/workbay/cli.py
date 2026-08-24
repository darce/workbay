"""``workbay`` console-script entry point.

The front door is a thin wrapper over ``workbay_bootstrap.cli.main``. Its one
piece of behaviour beyond pass-through: when the user runs ``workbay install``
without naming an overlay source, default ``--source package`` so the install
materializes from the published ``workbay-system`` distribution payload — the
tree the front door actually ships. ``workbay embeddings`` is handled locally
as the canonical operator toggle (implicit ``Path.cwd()``); every other
invocation reaches the bootstrap parser verbatim — a blanket inject would break
``workbay doctor`` / ``status`` with "unrecognized arguments: --source package"
(``--source`` is an ``install``-subparser argument only).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _has_explicit_source(argv: list[str]) -> bool:
    """True when the user already passed ``--source`` (space or ``=`` form)."""
    return any(a == "--source" or a.startswith("--source=") for a in argv)


def _install_argv(argv: list[str]) -> list[str]:
    """Default the ``install`` overlay source to ``package``.

    Inject ``--source package`` only when the first token is exactly
    ``install`` and the user did not pass ``--source`` themselves. All other
    argument vectors pass through untouched.
    """
    if argv[:1] == ["install"] and not _has_explicit_source(argv):
        return ["install", "--source", "package", *argv[1:]]
    return list(argv)


def _embeddings_parser() -> argparse.ArgumentParser:
    """Build the ``workbay embeddings`` parser (implicit cwd; no ``--target``)."""
    parser = argparse.ArgumentParser(prog="workbay embeddings")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--status",
        action="store_true",
        help="Print .workbay/embedding.env file-gate status as JSON (not process-env).",
    )
    mode.add_argument(
        "--enable",
        action="store_true",
        help="Enable embeddings via .workbay/embedding.env file gate (does not clear process-env).",
    )
    mode.add_argument(
        "--disable",
        action="store_true",
        help="Disable embeddings via .workbay/embedding.env file gate.",
    )
    return parser


def _run_embeddings(argv: list[str]) -> int:
    """Handle ``workbay embeddings`` via the single ``embedding_provision`` runner."""
    from workbay_bootstrap.embedding_provision import run_embeddings_cli

    args = _embeddings_parser().parse_args(argv[1:])
    return run_embeddings_cli(
        Path.cwd().resolve(),
        status=args.status,
        enable=args.enable,
        disable=args.disable,
    )


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if argv[:1] == ["embeddings"]:
        return _run_embeddings(argv)

    # Lazy import so ``import workbay.cli`` does not hard-require the bootstrap
    # distribution at module load (keeps the pure argv transform importable on
    # its own and testable without the full dependency tree).
    from workbay_bootstrap.cli import main as bootstrap_main

    return bootstrap_main(_install_argv(argv))


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    raise SystemExit(main())
