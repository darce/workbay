"""``workbay`` console-script entry point.

The front door is a thin wrapper over ``workbay_bootstrap.cli.main``. Its one
piece of behaviour beyond pass-through: when the user runs ``workbay install``
without naming an overlay source, default ``--source package`` so the install
materializes from the published ``workbay-system`` distribution payload — the
tree the front door actually ships. Every other invocation reaches the
bootstrap parser verbatim; a blanket inject would break ``workbay doctor`` /
``status`` with "unrecognized arguments: --source package" (``--source`` is an
``install``-subparser argument only).
"""

from __future__ import annotations

import sys


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


def main(argv: list[str] | None = None) -> int:
    # Lazy import so ``import workbay.cli`` does not hard-require the bootstrap
    # distribution at module load (keeps the pure argv transform importable on
    # its own and testable without the full dependency tree).
    from workbay_bootstrap.cli import main as bootstrap_main

    if argv is None:
        argv = sys.argv[1:]
    return bootstrap_main(_install_argv(argv))


if __name__ == "__main__":  # pragma: no cover - exercised via the console script
    raise SystemExit(main())
