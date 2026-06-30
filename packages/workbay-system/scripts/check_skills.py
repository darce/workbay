"""Compatibility shim — implementation in workbay_system.overlay_tooling.check_skills."""
from workbay_system.overlay_tooling import check_skills as _impl


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__():
    return dir(_impl)


if __name__ == "__main__":
    raise SystemExit(_impl.main())
