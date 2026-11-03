"""Compatibility shim — implementation in workbay_system.overlay_tooling.lint_hoisted_paths."""
from workbay_system.overlay_tooling import lint_hoisted_paths as _impl


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__():
    return dir(_impl)


if __name__ == "__main__":
    import sys

    raise SystemExit(_impl.main(sys.argv[1:]))
