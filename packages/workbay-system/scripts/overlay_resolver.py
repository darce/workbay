"""Compatibility shim — implementation in workbay_system.overlay_tooling.overlay_resolver."""
from workbay_system.overlay_tooling import overlay_resolver as _impl


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__():
    return dir(_impl)
