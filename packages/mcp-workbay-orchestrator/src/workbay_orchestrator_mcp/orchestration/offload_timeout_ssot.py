"""Single-source wall-clock ceilings for adapter-timeout offload profiles.

Ceilings that intentionally stay outside this module, owned by lane fbr19
in ``worker_daemon.py``:

* ``_LANE_CHECK_TIMEOUT_SECONDS = 1800`` (env ``WORKBAY_LANE_CHECK_TIMEOUT``)
* ``CURSOR_TIMEOUT_CAP`` import/use on the execute-path max-turns arm
* adapter derivation via ``remote_lane_timeout_ceiling_s`` /
  ``derive_adapter_timeout_bounds`` / ``resolve_remote_lane_timeout_s``
* grok derivation via ``derive_grok_single_cycle_bounds``
* ``_SELF_VERIFY_TIMEOUT_SECONDS = 1800``
"""

from __future__ import annotations

import os

CODEX_TIMEOUT_CAP = 3600
GROK_TIMEOUT_CAP = 1800
CURSOR_TIMEOUT_CAP_DEFAULT = 900
OPENROUTER_REMOTE_LANE_TIMEOUT_S = 300
OPENROUTER_TIMEOUT_CAP = 1500
LANE_TIMEOUT_MARGIN_SECONDS = 15
DEFAULT_LANE_TIMEOUT_SECONDS = GROK_TIMEOUT_CAP - LANE_TIMEOUT_MARGIN_SECONDS


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
