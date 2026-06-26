"""``workbay`` — the WorkBay front door.

A single command, ``workbay install``, hoists the WorkBay agentic-workflow
surface into any repository. The front door carries no member code of its own:
it pins the published ``workbay-stack`` runtime and delegates to
``workbay-bootstrap`` (defaulting the install overlay source to the published
distribution payload). See ``workbay.cli`` for the wrapper.
"""

__version__ = "0.2.0"
