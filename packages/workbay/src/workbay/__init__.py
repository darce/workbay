"""``workbay`` — the WorkBay front door.

A single command, ``workbay install``, hoists the WorkBay agentic-workflow
surface into any repository. The front door carries no member code of its own:
it is the runtime version anchor — pinning every published WorkBay runtime
member — and delegates to ``workbay-bootstrap`` (defaulting the install overlay
source to the published
distribution payload). See ``workbay.cli`` for the wrapper.
"""

from workbay_protocol.version import version_of

__version__ = version_of("workbay", anchor=__file__)
