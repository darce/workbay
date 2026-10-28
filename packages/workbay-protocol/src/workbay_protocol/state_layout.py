"""Resolve the current WorkBay layout without touching the filesystem.

State directory precedence is explicit ``state_dir``, then nonblank
``WORKBAY_HANDOFF_STATE_DIR`` via :func:`resolve_env_alias`, then
``<workspace_root>/.task-state``. Relative overrides anchor at the workspace,
not the process cwd; a relative workspace anchors at the cwd. Paths expand
``~``.

The layout is lexical: no git discovery, no file probes (symlinks are not
followed), no writes, and no normalization (``..`` is kept, because collapsing
it changes the result when ``..`` follows a symlinked component). A consumer
that needs physical identity, for example a containment guard, calls
``resolve()`` on the returned path itself. Callers choose the workspace
(including primary-worktree discovery) and enforce any consumer-specific
containment policy.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .env_aliases import resolve_env_alias
from .paths import runtime_root_path


# These subsystem-specific overrides are documented aliases only. They do not
# participate in global state precedence; their existing callers retain their
# own resolution semantics until migrated. In particular, the dev-temp scan
# root is distinct from the layout's state-local tmp_dir.
SUBSYSTEM_STATE_OVERRIDES = {
    "WORKBAY_EVALS_STATE_DIR": "evals state directory",
    "WORKBAY_DEV_TEMP_ROOT": "dev-temp scan root (default /tmp)",
}


@dataclass(frozen=True)
class StateLayout:
    workspace_root: Path
    install_root: Path
    state_dir: Path
    cache_root: Path
    tmp_dir: Path

    def offload_pass_record(self, pass_id: str) -> Path:
        """Return the existing offload-pass record filename under state_dir."""
        return self.state_dir / f"offload-pass-{pass_id}.json"


def resolve_state_layout(
    workspace_root: str | Path,
    *,
    state_dir: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> StateLayout:
    """Return today's layout; ``environ=None`` reads the current environment.

    An explicitly supplied environment mapping replaces the ambient environment,
    including when empty. Only ``None`` means an explicit state_dir was omitted.
    """
    root = Path(workspace_root).expanduser()
    if not root.is_absolute():
        root = Path.cwd() / root
    if state_dir is None:
        state_dir = resolve_env_alias("WORKBAY_HANDOFF_STATE_DIR", env=environ)
    candidate = Path(state_dir).expanduser() if state_dir is not None else root / ".task-state"
    if not candidate.is_absolute():
        candidate = root / candidate
    return StateLayout(
        workspace_root=root,
        install_root=runtime_root_path(root),
        state_dir=candidate,
        cache_root=root / ".workbay-cache",
        tmp_dir=candidate / "tmp",
    )
