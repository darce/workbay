"""Resolve the host's parent directory for remote agent sandboxes."""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from pathlib import PurePosixPath, PureWindowsPath

logger = logging.getLogger(__name__)

# Keep the old root in the discovery set through migration. A caller still has
# to apply its normal occupancy and marker checks before any destructive action.
LEGACY_REMOTE_AGENT_ROOTS = ("grok-sandbox",)
DEFAULT_REMOTE_AGENT_ROOT = "workbay-sandbox"


def resolve_remote_agent_root(env: Mapping[str, str] | None = None) -> str:
    """Return the validated remote agent root from the environment or default."""
    settings = os.environ if env is None else env
    configured_root = settings.get("WORKBAY_REMOTE_AGENT_ROOT")
    root = DEFAULT_REMOTE_AGENT_ROOT if configured_root is None else configured_root.strip()

    posix_path = PurePosixPath(root)
    windows_path = PureWindowsPath(root)
    if not root:
        reason = "WORKBAY_REMOTE_AGENT_ROOT must not be empty"
        logger.warning("remote_agent_root_refused", extra={"event": "remote_agent_root_refused", "reason": reason})
        raise ValueError(reason)
    if re.fullmatch(r"[A-Za-z0-9/_.-]+", root) is None:
        reason = "WORKBAY_REMOTE_AGENT_ROOT contains characters rejected by remote_agent.sh"
        logger.warning("remote_agent_root_refused", extra={"event": "remote_agent_root_refused", "reason": reason})
        raise ValueError(reason)
    if ".." in posix_path.parts or ".." in windows_path.parts:
        reason = "WORKBAY_REMOTE_AGENT_ROOT must not contain '..'"
        logger.warning("remote_agent_root_refused", extra={"event": "remote_agent_root_refused", "reason": reason})
        raise ValueError(reason)
    return root


def known_remote_agent_roots(env: Mapping[str, str] | None = None) -> tuple[str, ...]:
    """Return the configured root and legacy discovery roots, without duplicates.

    This is an enumeration contract, not permission to move or delete a legacy
    root; cleanup callers must continue to prove per-resource idleness.
    """
    return tuple(dict.fromkeys((resolve_remote_agent_root(env), *LEGACY_REMOTE_AGENT_ROOTS)))
