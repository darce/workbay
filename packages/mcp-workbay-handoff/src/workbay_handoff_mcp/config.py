from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

from workbay_protocol import resolve_env_alias

_DEFAULT_CLOSE_CHECK_COMMAND_TIMEOUT_SECONDS = 600

_DEFAULT_TOOL_PROFILE = "all"
_VALID_TOOL_PROFILES = ("all",)
_GIT_SUBPROCESS_TIMEOUT_SECONDS = 5


class ConsumerRootResolutionError(RuntimeError):
    """Raised when packaged consumer startup cannot infer a git-backed root."""


def _resolve_runtime_path(path_value: str | Path, *, workspace_root: Path) -> Path:
    """Resolve config paths relative to the runtime workspace root.

    Explicit relative paths from harness config should anchor at the
    declared workspace root, not the process cwd. This keeps consumer
    configs like ".", ".task-state", and "CURRENT_TASK.json" stable even
    when the server is launched from a different directory.
    """
    candidate = Path(path_value).expanduser()
    if not candidate.is_absolute():
        candidate = workspace_root / candidate
    return candidate.resolve()


def _resolve_primary_worktree_root(start_dir: Path) -> Path | None:
    """Resolve the primary git worktree root from a starting directory.

    Uses ``git rev-parse --git-common-dir`` to find the shared ``.git``
    location across all linked worktrees of the same physical repository,
    then walks one level up to the primary worktree root.

    Returns ``None`` when git is not available, when the start directory is
    not inside a git repository, or when the resolved common dir does not
    point at a recognisable ``.git`` directory.

    The output of ``git rev-parse --git-common-dir`` is documented to be
    relative to the cwd of the git invocation when the call is made from the
    primary worktree (typically ``.git``) and an absolute path when called
    from a linked worktree (the absolute path of the primary's ``.git``
    directory). The two cases are normalised here so callers always receive
    the primary worktree's root directory.
    """
    if not start_dir.exists():
        return None
    try:
        proc = subprocess.run(
            ["git", "-C", str(start_dir), "rev-parse", "--git-common-dir"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_GIT_SUBPROCESS_TIMEOUT_SECONDS,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    raw = proc.stdout.strip()
    if not raw:
        return None
    common_path = Path(raw)
    if not common_path.is_absolute():
        # Relative paths are relative to the cwd of the git invocation, which
        # is start_dir.
        common_path = (start_dir / common_path).resolve()
    else:
        common_path = common_path.resolve()
    if common_path.name == ".git":
        return common_path.parent
    # Defensive: a bare repo or unusual layout returned a non-".git" path.
    # Use it as-is rather than guessing at a parent that may not be a
    # checkout root.
    return common_path


@dataclass(frozen=True)
class RuntimeConfig:
    workspace_root: Path
    state_dir: Path
    db_path: Path
    current_task_path: Path
    dashboard_path: Path
    exports_dir: Path
    artifact_db_path: Path
    artifact_index_min_bytes: int = 4096
    artifact_index_min_lines: int = 80
    tool_profile: str = _DEFAULT_TOOL_PROFILE
    # When False (the default), routine MCP write paths skip
    # auto-regenerating CURRENT_TASK.json. Explicit
    # render_handoff(kind="current_task") calls and the import_export
    # round-trips are unaffected — they always render. The
    # always-current operator surface is DASHBOARD.txt;
    # CURRENT_TASK.json is only an on-demand task-scoped export.
    #
    # Consumers that still rely on the file being kept current by every MCP
    # write can opt back in by passing
    # ``current_task_auto_regen=True`` to ``RuntimeConfig.for_workspace``
    # or by setting ``WORKBAY_HANDOFF_CURRENT_TASK_AUTO_REGEN=1`` in the
    # environment.
    current_task_auto_regen: bool = False
    # internal/5c: feature flag for the two-state finding lifecycle
    # (``open -> resolved_on_branch -> integrated``). Defaulted to True in
    # implementation note now that the backfill script has landed and consumers have
    # absorbed the new ``status`` values. Override to ``False`` via
    # ``finding_lifecycle_states_enabled=False`` kwarg or
    # ``WORKBAY_HANDOFF_FINDING_LIFECYCLE_STATES=0`` to fall back to the legacy
    # one-state ``fixed`` close behavior during regressions.
    finding_lifecycle_states_enabled: bool = True
    # implementation note D2: when True (default), ``task-finish`` runs a bounded
    # ``archive --operation reap`` + ``reap_done`` sweep after close.
    # ``task_finish`` reads ``WORKBAY_HANDOFF_TASK_FINISH_AUTO_REAP`` directly;
    # this field mirrors that env for MCP doctor/runtime surfaces.
    task_finish_auto_reap_enabled: bool = True
    # implementation note D5: dashboard policy knobs (override via env or for_workspace kwargs).
    dashboard_stale_threshold_hours: int = 24
    dashboard_needs_attention_cap: int = 25
    dashboard_attention_severities: tuple[str, ...] = ("high", "medium")
    dashboard_integrity_budget_ms: float = 3000.0
    dashboard_representative_render_budget_ms: float = 10_000.0
    # E7 close-check verification gate. ``close_check_required_commands`` are shell
    # commands run fail-closed during ``handoff_close_check(enforce=True)``; any
    # non-zero / un-launchable / timed-out command blocks the close. Override via
    # ``WORKBAY_HANDOFF_CLOSE_CHECK_REQUIRED_COMMANDS`` as either a JSON array
    # (``["make check-all"]``) or a newline-separated list (NOT comma-separated).
    # Default empty = today's evidence-only behavior (no execution). The per-command
    # timeout (seconds) overrides via
    # ``WORKBAY_HANDOFF_CLOSE_CHECK_COMMAND_TIMEOUT_SECONDS``. See
    # docs/guides/close-check-verification-commands.md.
    close_check_required_commands: tuple[str, ...] = ()
    close_check_command_timeout_seconds: int = _DEFAULT_CLOSE_CHECK_COMMAND_TIMEOUT_SECONDS
    state_workspace_root: Path | None = None
    git_workspace_root: Path | None = None

    def __post_init__(self) -> None:
        if self.tool_profile not in _VALID_TOOL_PROFILES:
            raise ValueError(f"Invalid tool_profile: {self.tool_profile!r}")
        if self.state_workspace_root is None:
            object.__setattr__(self, "state_workspace_root", self.workspace_root)
        if self.git_workspace_root is None:
            object.__setattr__(self, "git_workspace_root", self.workspace_root)

    @property
    def per_task_projection_dir(self) -> Path:
        """Directory holding per-task projection files written by
        ``_write_per_task_projection``. Used by internal's derive-on-read
        workspace summary as the source of truth for active tasks.
        """
        return self.state_dir / "current"

    @property
    def compaction_config_root(self) -> Path:
        """Root used for compaction contracts and overlay settings.

        ``workspace_root`` intentionally collapses linked worktrees to the
        primary checkout so all worktrees share one handoff DB. Compaction
        threshold overlays are source/config files, so they must resolve
        from the calling git worktree when one is known.
        """
        return self.git_workspace_root or self.workspace_root

    @classmethod
    def for_workspace(
        cls,
        workspace_root: str | Path,
        *,
        state_dir: str | Path | None = None,
        current_task_path: str | Path | None = None,
        dashboard_path: str | Path | None = None,
        exports_dir: str | Path | None = None,
        tool_profile: str | None = None,
        current_task_auto_regen: bool | None = None,
        finding_lifecycle_states_enabled: bool | None = None,
        task_finish_auto_reap_enabled: bool | None = None,
        dashboard_stale_threshold_hours: int | None = None,
        dashboard_needs_attention_cap: int | None = None,
        dashboard_attention_severities: tuple[str, ...] | None = None,
        dashboard_integrity_budget_ms: float | None = None,
        dashboard_representative_render_budget_ms: float | None = None,
        close_check_required_commands: tuple[str, ...] | None = None,
        close_check_command_timeout_seconds: int | None = None,
        git_workspace_root: str | Path | None = None,
    ) -> RuntimeConfig:
        resolved_workspace_root = Path(workspace_root).expanduser().resolve()
        resolved_git_workspace_root = (
            Path(git_workspace_root).expanduser().resolve()
            if git_workspace_root is not None
            else resolved_workspace_root
        )
        resolved_state_dir = (
            _resolve_runtime_path(state_dir, workspace_root=resolved_workspace_root)
            if state_dir is not None
            else resolved_workspace_root / ".task-state"
        )
        resolved_current_task_path = (
            _resolve_runtime_path(current_task_path, workspace_root=resolved_workspace_root)
            if current_task_path is not None
            else resolved_workspace_root / "CURRENT_TASK.json"
        )
        resolved_dashboard_path = (
            _resolve_runtime_path(dashboard_path, workspace_root=resolved_workspace_root)
            if dashboard_path is not None
            else resolved_workspace_root / "DASHBOARD.txt"
        )
        resolved_exports_dir = (
            _resolve_runtime_path(exports_dir, workspace_root=resolved_workspace_root)
            if exports_dir is not None
            else resolved_state_dir / "exports"
        )
        if current_task_auto_regen is None:
            env_value = resolve_env_alias("WORKBAY_HANDOFF_CURRENT_TASK_AUTO_REGEN")
            if env_value is not None:
                resolved_auto_regen = env_value.strip().lower() not in {"0", "false", "off", "no"}
            else:
                resolved_auto_regen = False
        else:
            resolved_auto_regen = current_task_auto_regen
        if finding_lifecycle_states_enabled is None:
            flag_env = resolve_env_alias("WORKBAY_HANDOFF_FINDING_LIFECYCLE_STATES")
            if flag_env is not None:
                resolved_lifecycle_flag = flag_env.strip().lower() not in {"0", "false", "off", "no"}
            else:
                resolved_lifecycle_flag = True
        else:
            resolved_lifecycle_flag = finding_lifecycle_states_enabled
        if task_finish_auto_reap_enabled is None:
            reap_env = resolve_env_alias("WORKBAY_HANDOFF_TASK_FINISH_AUTO_REAP")
            if reap_env is not None:
                resolved_auto_reap = reap_env.strip().lower() not in {"0", "false", "off", "no"}
            else:
                resolved_auto_reap = True
        else:
            resolved_auto_reap = task_finish_auto_reap_enabled

        def _parse_positive_int(env_name: str, default: int, override: int | None) -> int:
            if override is not None:
                return override
            raw = resolve_env_alias(env_name)
            if raw is None:
                return default
            try:
                return max(1, int(raw.strip()))
            except ValueError:
                return default

        def _parse_positive_float(env_name: str, default: float, override: float | None) -> float:
            if override is not None:
                return override
            raw = resolve_env_alias(env_name)
            if raw is None:
                return default
            try:
                return max(1.0, float(raw.strip()))
            except ValueError:
                return default

        resolved_stale_hours = _parse_positive_int(
            "WORKBAY_HANDOFF_DASHBOARD_STALE_THRESHOLD_HOURS",
            24,
            dashboard_stale_threshold_hours,
        )
        resolved_attention_cap = _parse_positive_int(
            "WORKBAY_HANDOFF_DASHBOARD_NEEDS_ATTENTION_CAP",
            25,
            dashboard_needs_attention_cap,
        )
        resolved_integrity_budget = _parse_positive_float(
            "WORKBAY_HANDOFF_DASHBOARD_INTEGRITY_BUDGET_MS",
            3000.0,
            dashboard_integrity_budget_ms,
        )
        resolved_render_budget = _parse_positive_float(
            "WORKBAY_HANDOFF_DASHBOARD_REPRESENTATIVE_RENDER_BUDGET_MS",
            10_000.0,
            dashboard_representative_render_budget_ms,
        )
        if dashboard_attention_severities is not None:
            resolved_attention_severities = dashboard_attention_severities
        else:
            raw_sev = resolve_env_alias("WORKBAY_HANDOFF_DASHBOARD_ATTENTION_SEVERITIES")
            if raw_sev:
                resolved_attention_severities = tuple(
                    part.strip().lower() for part in raw_sev.split(",") if part.strip()
                ) or ("high", "medium")
            else:
                resolved_attention_severities = ("high", "medium")

        resolved_required_commands = _parse_close_check_required_commands(
            close_check_required_commands,
            env_name="WORKBAY_HANDOFF_CLOSE_CHECK_REQUIRED_COMMANDS",
        )
        resolved_command_timeout = _parse_positive_int(
            "WORKBAY_HANDOFF_CLOSE_CHECK_COMMAND_TIMEOUT_SECONDS",
            _DEFAULT_CLOSE_CHECK_COMMAND_TIMEOUT_SECONDS,
            close_check_command_timeout_seconds,
        )

        return cls(
            workspace_root=resolved_workspace_root,
            state_dir=resolved_state_dir,
            db_path=resolved_state_dir / "handoff.db",
            current_task_path=resolved_current_task_path,
            dashboard_path=resolved_dashboard_path,
            exports_dir=resolved_exports_dir,
            artifact_db_path=resolved_state_dir / "mcp-artifacts.db",
            tool_profile=tool_profile or _DEFAULT_TOOL_PROFILE,
            current_task_auto_regen=resolved_auto_regen,
            finding_lifecycle_states_enabled=resolved_lifecycle_flag,
            task_finish_auto_reap_enabled=resolved_auto_reap,
            dashboard_stale_threshold_hours=resolved_stale_hours,
            dashboard_needs_attention_cap=resolved_attention_cap,
            dashboard_attention_severities=resolved_attention_severities,
            dashboard_integrity_budget_ms=resolved_integrity_budget,
            dashboard_representative_render_budget_ms=resolved_render_budget,
            close_check_required_commands=resolved_required_commands,
            close_check_command_timeout_seconds=resolved_command_timeout,
            state_workspace_root=resolved_workspace_root,
            git_workspace_root=resolved_git_workspace_root,
        )

    @classmethod
    def for_repo(
        cls,
        start_dir: str | Path | None = None,
        *,
        state_dir: str | Path | None = None,
        current_task_path: str | Path | None = None,
        dashboard_path: str | Path | None = None,
        exports_dir: str | Path | None = None,
        tool_profile: str | None = None,
        current_task_auto_regen: bool | None = None,
        finding_lifecycle_states_enabled: bool | None = None,
    ) -> RuntimeConfig:
        """Build a RuntimeConfig anchored at the primary git worktree.

        Resolves the workspace root by walking from ``start_dir`` (or the
        current working directory if omitted) to the primary git worktree
        via ``git rev-parse --git-common-dir``. Every linked worktree of the
        same physical repository will therefore resolve to the same
        ``.task-state/handoff.db``, eliminating the per-worktree DB
        divergence that breaks ``make context`` when run from a linked
        worktree while the MCP server reads the primary worktree's DB.

        When git is not available or ``start_dir`` is not inside a git
        repository, this falls back to ``RuntimeConfig.for_workspace``
        anchored at ``start_dir`` (or the current working directory). This
        keeps non-git contexts (tests, ad-hoc tmpdir setups) working
        unchanged.

        Explicit ``state_dir`` / ``current_task_path`` / ``exports_dir``
        arguments are passed through and override the resolved defaults
        unchanged. This means a caller can still anchor the DB at an
        arbitrary path if it has a reason to bypass the primary-worktree
        resolution (e.g. running against a snapshotted state directory in
        a fixture).
        """
        start = Path(start_dir).expanduser().resolve() if start_dir is not None else Path.cwd().resolve()
        primary_root = _resolve_primary_worktree_root(start)
        workspace_root = primary_root if primary_root is not None else start
        return cls.for_workspace(
            workspace_root,
            state_dir=state_dir,
            current_task_path=current_task_path,
            dashboard_path=dashboard_path,
            exports_dir=exports_dir,
            tool_profile=tool_profile,
            current_task_auto_regen=current_task_auto_regen,
            finding_lifecycle_states_enabled=finding_lifecycle_states_enabled,
            git_workspace_root=start,
        )

    @classmethod
    def from_args(cls, args: object) -> RuntimeConfig:
        """Build a RuntimeConfig from CLI args / env vars.

        internal: ``from_args`` routes the resolved ``workspace_root``
        through ``for_repo`` so an MCP server (or any other CLI entry point)
        launched with ``--workspace-root`` pointing at a *linked* git
        worktree silently collapses to the primary worktree's
        ``.task-state/handoff.db``. Without this redirection the MCP server
        and the lifecycle scripts (which already use ``for_repo`` after the
        internal base slice) end up writing to two different per-worktree
        DBs, defeating the divergence-loop closure the slice claims.

        Explicit ``--state-dir`` / ``--current-task-path`` / ``--exports-dir``
        overrides remain authoritative and are passed through unchanged.
        Callers that genuinely need a per-worktree state directory (e.g.
        a snapshot fixture or a per-worker isolation test) keep that
        escape hatch. The fix only affects the default-resolution path
        where the harness invokes the server with just
        ``--workspace-root``.
        """
        workspace_root = getattr(args, "workspace_root", None) or resolve_env_alias("WORKBAY_HANDOFF_WORKSPACE_ROOT")
        if not workspace_root:
            cwd = Path.cwd().resolve()
            if _resolve_primary_worktree_root(cwd) is not None:
                workspace_root = str(cwd)
            else:
                raise RuntimeError("WORKBAY_HANDOFF_WORKSPACE_ROOT must be set or passed via --workspace-root")

        state_dir = getattr(args, "state_dir", None) or resolve_env_alias("WORKBAY_HANDOFF_STATE_DIR")
        current_task_path = getattr(args, "current_task_path", None) or resolve_env_alias(
            "WORKBAY_HANDOFF_CURRENT_TASK_PATH"
        )
        dashboard_path = getattr(args, "dashboard_path", None) or resolve_env_alias("WORKBAY_HANDOFF_DASHBOARD_PATH")
        exports_dir = getattr(args, "exports_dir", None) or resolve_env_alias("WORKBAY_HANDOFF_EXPORTS_DIR")
        start = Path(workspace_root).expanduser().resolve()

        if (
            _resolve_primary_worktree_root(start) is None
            and state_dir is None
            and current_task_path is None
            and dashboard_path is None
            and exports_dir is None
        ):
            raise ConsumerRootResolutionError(
                "mcp-workbay-handoff could not resolve <consumer-root> "
                f"- caller cwd {start} is not inside a git repository. "
                "Set WORKBAY_HANDOFF_WORKSPACE_ROOT and, if needed, "
                "WORKBAY_HANDOFF_STATE_DIR / WORKBAY_HANDOFF_DASHBOARD_PATH / "
                "WORKBAY_HANDOFF_CURRENT_TASK_PATH explicitly, or call "
                "RuntimeConfig.for_workspace(...) for a non-git fixture."
            )

        return cls.for_repo(
            workspace_root,
            state_dir=state_dir,
            current_task_path=current_task_path,
            dashboard_path=dashboard_path,
            exports_dir=exports_dir,
            tool_profile=getattr(args, "tool_profile", None) or resolve_env_alias("WORKBAY_HANDOFF_TOOL_PROFILE"),
        )


logger = logging.getLogger(__name__)


def _parse_close_check_required_commands(
    override: tuple[str, ...] | None,
    *,
    env_name: str,
) -> tuple[str, ...]:
    if override is not None:
        return tuple(cmd.strip() for cmd in override if cmd.strip())
    raw = resolve_env_alias(env_name)
    if raw is None:
        return ()
    stripped = raw.strip()
    if not stripped:
        return ()
    if stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            logger.warning(
                "%s is JSON-shaped but not valid JSON; close-check verification "
                "commands are DISABLED for this run. Fix the value (a JSON array of "
                "command strings) so the fail-closed gate runs.",
                env_name,
            )
            return ()
        if not isinstance(parsed, list):
            logger.warning(
                "%s parsed as %s, not a JSON list; close-check verification commands "
                "are DISABLED for this run. Provide a JSON array of command strings.",
                env_name,
                type(parsed).__name__,
            )
            return ()
        return tuple(str(item).strip() for item in parsed if str(item).strip())
    return tuple(part.strip() for part in stripped.splitlines() if part.strip())
