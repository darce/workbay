"""One strict remote execution authority and pass-scoped selection custody."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from workbay_protocol.bootstrap import load_remote_exec_mode

_MODE_ENV = "WORKBAY_REMOTE_EXEC_MODE"


class RemoteExecModeError(Exception):
    """A remote mode or repository policy cannot authorize dispatch."""

    code = "invalid_remote_exec_mode"

    def __init__(self, message: str, *, value: object = None) -> None:
        super().__init__(message)
        self.value = message if value is None else value


@dataclass(frozen=True, slots=True)
class RemoteExecSelection:
    mode: Literal["attached", "detached"]
    backend_id: str
    source: Literal["environment", "per_backend", "config_default", "builtin_default"]
    repo_root: Path
    config_identity: str | None

    def to_mapping(self) -> dict[str, str | None]:
        return {
            "mode": self.mode,
            "backend_id": self.backend_id,
            "source": self.source,
            "repo_root": str(self.repo_root),
            "config_identity": self.config_identity,
        }


# The engine owns this scope and resets it in finally. ContextVar keeps parallel
# passes isolated while the synchronous worker/lane-exec call chain carries the
# exact immutable selection without adding an environment carrier.
ACTIVE_REMOTE_EXEC_SELECTION: ContextVar[RemoteExecSelection | None] = ContextVar(
    "active_remote_exec_selection", default=None
)


def current_remote_exec_selection(backend_id: str, *, repo_root: Path | str) -> RemoteExecSelection | None:
    selection = ACTIVE_REMOTE_EXEC_SELECTION.get()
    if selection is not None and (
        selection.backend_id != backend_id or selection.repo_root != Path(repo_root).resolve()
    ):
        raise RemoteExecModeError("remote execution selection differs from the pass repository/backend")
    return selection


def resolve_remote_exec_mode(
    backend_id: str,
    *,
    repo_root: Path | str,
    effective_env: Mapping[str, str] | None = None,
) -> RemoteExecSelection:
    """Resolve strict environment > backend policy > default > attached.

    Explicit environments, including empty mappings, are authoritative. Validate
    every policy entry before selecting a winner so an override cannot mask a
    broken repository policy. No read is cached across separate invocations.
    """
    root = Path(repo_root).resolve()
    try:
        policy = load_remote_exec_mode(root)
    except ValueError as exc:
        raise RemoteExecModeError(str(exc)) from exc
    identity = None
    if policy is not None:
        encoded = json.dumps(policy.model_dump(exclude_unset=True), sort_keys=True, separators=(",", ":"))
        identity = "sha256:" + hashlib.sha256(encoded.encode()).hexdigest()
    environment = os.environ if effective_env is None else effective_env
    if _MODE_ENV in environment:
        mode = environment[_MODE_ENV]
        if mode not in ("attached", "detached"):
            raise RemoteExecModeError(
                f"WORKBAY_REMOTE_EXEC_MODE value {mode!r} is invalid; expected attached or detached.", value=mode
            )
        source = "environment"
    elif policy is not None and backend_id in policy.per_backend:
        mode, source = policy.per_backend[backend_id], "per_backend"
    elif policy is not None and policy.default is not None:
        mode, source = policy.default, "config_default"
    else:
        mode, source = "attached", "builtin_default"
    return RemoteExecSelection(mode, backend_id, source, root, identity)
