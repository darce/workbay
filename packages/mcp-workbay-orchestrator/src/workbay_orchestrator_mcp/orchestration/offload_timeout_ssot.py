"""Single-source wall-clock ceilings for adapter-timeout offload profiles."""

from __future__ import annotations

import os

CODEX_TIMEOUT_CAP = 3600
GROK_TIMEOUT_CAP = 1800
CURSOR_TIMEOUT_CAP_DEFAULT = 900


def resolve_cursor_timeout_cap() -> int:
    """Read a positive cursor override, containing malformed values at the seam."""
    raw = (os.environ.get("WORKBAY_CURSOR_TIMEOUT") or "").strip()
    if not raw:
        return CURSOR_TIMEOUT_CAP_DEFAULT
    try:
        value = int(raw)
    except ValueError:
        return CURSOR_TIMEOUT_CAP_DEFAULT
    return value if value > 0 else CURSOR_TIMEOUT_CAP_DEFAULT


CURSOR_TIMEOUT_CAP = resolve_cursor_timeout_cap()
