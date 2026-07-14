"""Typed receipt models for lifecycle read-only commands."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class ReceiptWarning:
    field: str
    reason: str
    exception_type: str | None = None


@dataclass(frozen=True)
class DirtySummary:
    staged: int
    unstaged: int
    untracked: int
    total: int


@dataclass(frozen=True)
class DaemonStatus:
    enabled: bool
    source: str


@dataclass(frozen=True)
class WorkflowFile:
    present: bool
    path: str | None


@dataclass(frozen=True)
class PlanVisibility:
    path: str | None
    exists: bool
    title: str | None
    task_ref_matches_branch: bool | None
    stale_reason: str | None
    read_branch: str | None = None
    read_command: str | None = None
    read_receipt: str | None = None


@dataclass(frozen=True)
class LastTestSummary:
    command: str | None
    commit_sha: str | None
    passed: bool | None
    verified_at: str | None


@dataclass(frozen=True)
class ReviewState:
    open_findings_count: int | None
    blockers_count: int | None
    last_test_summary: LastTestSummary | None
    ready_state: str | None


@dataclass(frozen=True)
class PlanBaselineSummary:
    """internal: read-only projection of the plan-baseline state.

    Mirrors the ``PlanBaselineStatus.baseline_status`` vocabulary
    (``accepted``/``missing``/``unknown``) and surfaces the structured
    reason + recovery command so first-line orientation receipts can
    show the gap without re-querying MCP.
    """

    status: str
    reason: str | None
    task_plan_path: str | None
    target_branch: str | None
    next_command: str | None
    acceptance_ready: bool = False
    plan_untracked_on_main: bool = False


@dataclass(frozen=True)
class HandoffProjection:
    task_ref: str | None
    status: str | None
    target_branch: str | None
    target_worktree_path: str | None
    task_plan_path: str | None


@dataclass(frozen=True)
class NextCommand:
    command: str
    reason: str


@dataclass(frozen=True)
class TaskEntry:
    task_ref: str | None
    status: str | None
    target_branch: str | None
    target_worktree_path: str | None
    task_plan_path: str | None
    task_plan_exists: bool
    cwd_matches_target: bool
    updated_at: str | None


@dataclass(frozen=True)
class TasksReceipt:
    ok: bool
    command: str
    branch: str
    worktree_path: str
    head: str
    repo_root: str
    cwd: str
    tasks: list[TaskEntry]
    active_count: int
    stale_done_count: int
    handoff_available: bool
    truncated: bool
    limit: int
    warnings: list[ReceiptWarning] = field(default_factory=list)
    # internal: workspace_role classifies the current checkout
    # as the control plane (root main / master) or implementation plane
    # (a conforming feature branch). "unknown" covers non-conforming
    # branches and any future buckets.
    workspace_role: str = "unknown"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class DoctorEnv:
    findings: list[dict[str, object]] = field(default_factory=list)
    available: bool = False


@dataclass(frozen=True)
class DoctorMcp:
    handoff_reachable: bool
    orchestrator_reachable: bool
    latencies_ms: dict[str, float] = field(default_factory=dict)
    # internal/2: tri-state surface for cold-start tolerance. Both
    # ``handoff_reachable`` and ``orchestrator_reachable`` are derived
    # back-compat views: each is True iff the matching ``*_status`` field
    # is "reachable" or "warming" (the probe ultimately succeeded either
    # way). ``mcp_status`` covers the workbay-handoff endpoint;
    # ``orchestrator_status`` covers the workbay-orchestrator endpoint.
    mcp_status: str = "unreachable"
    orchestrator_status: str = "unreachable"


@dataclass(frozen=True)
class DoctorBranch:
    name: str
    head: str
    ahead_of_main: int | None
    dirty: int
    protected_paths_dirty: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DoctorLifecycle:
    status_handler_ok: bool
    tasks_handler_ok: bool
    expected_stubs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DoctorDashboard:
    exists: bool
    fresh: bool
    fragments_present: bool
    last_regen_at: str | None


@dataclass(frozen=True)
class DoctorHooks:
    """Hook-wiring visibility facet.

    internal: ``expected``/``actual``/``drift`` now describe the
    repo's hoisted git-hook scripts reached through ``core.hooksPath`` in the
    inspected checkout (``expected`` is empty when ``core.hooksPath`` is unset,
    so an opt-out repo never drifts). The ``stop_adapters_*`` fields describe
    the compact-session harness Stop adapters declared in
    ``portable_commands.json``: an adapter is *drifted* when bootstrap reports
    a managed adapter finding, *installed* when its target file carries a
    ``_managed_by: workbay-bootstrap`` Stop entry, otherwise it is
    *optional-not-installed* and reported without failing the receipt.

    internal (additive): ``hook_adapters`` reports EVERY manifest
    ``hooks[]`` family keyed by hook_id, each with the same
    ``available`` / ``installed`` / ``drifted`` / ``optional_not_installed``
    lists. The legacy ``stop_adapters_*`` keys keep their compact-session-only
    meaning for backward compatibility.
    """

    expected: list[str] = field(default_factory=list)
    actual: list[str] = field(default_factory=list)
    drift: list[str] = field(default_factory=list)
    stop_adapters_available: list[str] = field(default_factory=list)
    stop_adapters_installed: list[str] = field(default_factory=list)
    stop_adapters_drifted: list[str] = field(default_factory=list)
    stop_adapters_optional_not_installed: list[str] = field(default_factory=list)
    git_hooks_path: str | None = None
    git_hooks_hoisted: bool = False
    remediation: list[str] = field(default_factory=list)
    hook_adapters: dict[str, dict[str, list[str]]] = field(default_factory=dict)


@dataclass(frozen=True)
class DoctorPlanBaseline:
    """internal: cross-task plan-baseline drift facet.

    Aggregates the per-task baseline state across every live handoff
    row. ``available=False`` signals the doctor probe could not reach
    MCP and ``baselines`` is empty; callers render that as
    ``baseline=unknown`` rather than silently passing.
    """

    available: bool = False
    counts: dict[str, int] = field(default_factory=dict)
    baselines: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class DoctorDirtyMain:
    """internal: ownership-aware dirty-main facet.

    ``mode_recommended`` mirrors the ``check_main_clean.py --mode`` axis:
    ``warn`` (no protected paths dirty), ``doctor`` (dirty paths exist
    and operator should triage), or ``block`` (caller is at a publish
    boundary; treat as hard fail). ``ownership_hint`` is populated when
    internal attribution data is reachable; otherwise it stays ``None``
    and the operator gets path-only diagnostics.
    """

    branch: str
    protected_paths_dirty: list[str] = field(default_factory=list)
    mode_recommended: str = "warn"
    remediation: list[str] = field(default_factory=list)
    ownership_hint: str | None = None


@dataclass(frozen=True)
class DoctorVenv:
    """internal follow-up: worktree-root ``.venv`` / pytest-resolution facet.

    Reports whether the inspected worktree carries the root
    ``.venv/bin/pytest`` that ``task-start`` provisions, and whether an
    ambient ``pytest`` on ``PATH`` resolves *outside* that worktree — the
    pyenv-shim trap the root venv exists to prevent.
    ``ambient_pytest_outside_worktree`` is the actionable signal: ``True``
    means a bare ``pytest`` would load the wrong environment; ``None`` means
    no ambient ``pytest`` was found at all (unknown, not a confirmed risk).
    """

    root_venv_present: bool = False
    root_venv_pytest_present: bool = False
    ambient_pytest_path: str | None = None
    ambient_pytest_outside_worktree: bool | None = None
    root_venv_handoff_present: bool = False
    resolved_handoff_cli_path: str | None = None
    ambient_handoff_cli_outside_venv: bool | None = None
    projection_spool_depth: int = 0
    projection_queue: dict[str, object] = field(default_factory=dict)
    remediation: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DoctorHostMemory:
    """internal host-memory admission facet.

    Computed by a stdlib-only mini-probe (this package must NOT import
    ``workbay_orchestrator_mcp``); it mirrors the orchestrator D1 parses. The
    duplication is deliberate and bounded, like the doctor's other direct
    git/filesystem reads. ``available`` is ``False`` when the probe could not
    read the host (unknown platform / read error), in which case the facet is
    informational only. ``would_refuse_heavy`` + ``severity=error`` name the
    act-now condition: a heavy dispatch would currently be refused.
    """

    available: bool = False
    platform: str = ""
    available_ram_gib: float = 0.0
    pressure: str = "unknown"
    derived_width: int = 0
    held_slots: int = 0
    swapfile_count: int = 0
    swap_volume_free_gib: float = 0.0
    would_refuse_heavy: bool = False
    severity: str = "ok"  # ok | warning | error
    note: str = ""


@dataclass(frozen=True)
class DoctorReceipt:
    ok: bool
    command: str
    env: DoctorEnv
    mcp: DoctorMcp
    branch: DoctorBranch
    lifecycle: DoctorLifecycle
    dashboard: DoctorDashboard
    hooks: DoctorHooks
    next_command: NextCommand
    warnings: list[ReceiptWarning] = field(default_factory=list)
    dirty_main: DoctorDirtyMain = field(
        default_factory=lambda: DoctorDirtyMain(branch="")
    )
    plan_baseline: DoctorPlanBaseline = field(
        default_factory=lambda: DoctorPlanBaseline()
    )
    venv: DoctorVenv = field(default_factory=lambda: DoctorVenv())
    host_memory: DoctorHostMemory = field(
        default_factory=lambda: DoctorHostMemory()
    )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class StatusReceipt:
    ok: bool
    command: str
    task_ref: str | None
    branch: str
    worktree_path: str
    head: str
    handoff_projection: str
    repo_root: str
    cwd: str
    cwd_matches_target: bool
    target_worktree_path: str
    target_branch: str
    merge_base_available: bool
    dirty_summary: DirtySummary
    daemon_status: DaemonStatus
    workflow_file: WorkflowFile
    plan: PlanVisibility
    handoff_available: bool
    handoff: HandoffProjection | None
    review: ReviewState | None
    warnings: list[ReceiptWarning] = field(default_factory=list)
    next_command: NextCommand = field(
        default_factory=lambda: NextCommand(command="", reason="")
    )
    # internal: workspace_role / canonical_worktree_path teach
    # `status` to point at the implementation plane from root `main`
    # without grepping `git worktree list` or DASHBOARD.txt. See
    # docs/workbay/rules/development-workflow.md "Canonical Workflow
    # Loop" for the operator-facing rule.
    workspace_role: str = "unknown"
    canonical_worktree_path: str | None = None
    plan_baseline: PlanBaselineSummary | None = None
    projection_spool_depth: int = 0
    projection_queue: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
