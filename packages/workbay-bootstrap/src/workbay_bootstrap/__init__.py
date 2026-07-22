"""workbay-bootstrap: hoist the shared workbay-system surface into consumer repos."""

from workbay_protocol.version import version_of

from workbay_bootstrap.install import install

__all__ = ["install"]
__version__ = version_of("workbay-bootstrap", anchor=__file__)
