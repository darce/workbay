"""Explicit inputs, state ownership and side-effect order for one retire-sweep pass.

``lane_retire_sweep._sweep_locked`` coordinates one pass under the sweep lock.
The units in :mod:`lane_retire_sweep_steps` each read :class:`SweepInputs` and
read or write named fields of :class:`SweepState`. Nothing in this module does
I/O; it only states the contract the steps are checked against.

Dependency injection
    ``SweepInputs.deps`` is the ``lane_retire_sweep`` module object itself.
    Steps resolve every collaborator and tunable constant through it at call
    time (``deps._git_status(...)``, ``deps._GIT_STATUS_TIMEOUT_S``), so a
    monkeypatch on ``lane_retire_sweep`` reaches extracted code exactly as it
    reached the inline body. ``terminal_statuses``, ``session_live``,
    ``lock_is_held`` and ``worktree_tombstone`` are bound once per pass by the
    coordinator, as the inline body bound them with function-local imports.

Side-effect order of one pass (unchanged by the extraction)
    1. scan: lane rows, tombstone drain (apply) or pending count (dry run),
       linked-worktree inventory, branch inventory; any budget stop returns a
       typed ``budget_exhausted`` summary;
    2. selection: first retry-state read (fairness order), identity filter
       probes, one scan-cost write, grace expiry, second retry-state read and
       candidate ordering;
    3. admission and disposition, per candidate in order: limits, grace, retry
       backoff, Git status and torn-removal restore, liveness, admission fence
       probe and persisted attempt (apply), review close, no-work release,
       work-complete close, absorbed close, retire actuator under the fence and
       result accounting;
    4. reporting: retry deferrals, persisted cursor (apply), active-but-merged
       probe, starvation verdict and the summary.

State ownership
    Scan units own ``rows`` through ``branches`` and ``fence_release``.
    Selection units own the
    candidate maps, identity-filter flags and ``next_grace_expiry_at``.
    Admission/disposition units own the retry entries, cursor, counters and
    per-lane timings. Shared refusal sinks (``could_not_act``, ``skip_counts``,
    ``instrument_errors``) are append-only for every unit. Reporting reads
    everything and writes only ``retry_state_pruned``, ``has_more`` and
    ``remaining_count``.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Literal

Candidate = tuple[dict[str, Any], str, bool, "str | None"]
LaneKey = tuple[str, str]

# Per-candidate control returned by an admission/disposition unit. ``None``
# means "fall through to the next unit"; the coordinator maps the other two
# onto the loop's ``continue`` and ``break``.
CONTINUE: Literal["continue"] = "continue"
BREAK: Literal["break"] = "break"
Control = Literal["continue", "break"] | None


@dataclass(frozen=True)
class SweepInputs:
    """Read-only inputs of one pass. Steps never mutate these."""

    deps: ModuleType
    terminal_statuses: Any
    session_live: Callable[..., Any]
    lock_is_held: Callable[..., Any]
    worktree_tombstone: ModuleType
    repo_root: Path
    apply: bool
    max_lanes: int
    budget_deadline: float
    grace_s: float
    task_ref: str | None
    started: float
    budget_ceiling_reason: str | None
    scan_progress: dict[str, Any] | None
    scan_ceiling_limited: bool
    allowance_s: float
    scan_reservation_enabled: bool
    scan_deadline: float
    scan_exhausted_outcome: str = "budget_exhausted"


@dataclass
class SweepState:
    """Mutable state of one pass, grouped by the unit family that writes it."""

    # Shared refusal and error sinks (append-only for every unit).
    instrument_errors: list[str] = field(default_factory=list)
    phase_timings: dict[str, float] = field(default_factory=dict)
    could_not_act: list[dict[str, Any]] = field(default_factory=list)
    skip_counts: Counter[str] = field(default_factory=Counter)
    # Read-through caches shared by selection and disposition.
    manifest_cache: dict[str, dict[str, Any] | None] = field(default_factory=dict)
    integration_refs_cache: dict[str, tuple[list[str], str | None]] = field(default_factory=dict)
    timeout_receipt_keys: Any = None

    # Scan units.
    lane_progress: dict[str, Any] = field(
        default_factory=lambda: {"pages_fetched": 0, "rows_fetched": 0, "total_matching": None}
    )
    lane_rows_failed: bool = False
    rows: list[dict[str, Any]] = field(default_factory=list)
    tombstones_drained: int = 0
    tombstones_pending: int = 0
    tombstone_drain_budget_exhausted: bool = False
    # The tombstone drain's typed ``fence_release`` records, verbatim plus their step.
    fence_release: list[dict[str, Any]] = field(default_factory=list)
    linked: dict[str, Any] = field(default_factory=dict)
    worktree_inventory_failed: bool = False
    branches: set[str] | None = None

    # Selection units.
    candidates: list[Candidate] = field(default_factory=list)
    candidate_states: dict[LaneKey, dict[str, Any]] = field(default_factory=dict)
    work_complete_candidates: dict[LaneKey, dict[str, Any]] = field(default_factory=dict)
    no_work_candidates: dict[LaneKey, dict[str, Any]] = field(default_factory=dict)
    absorbed: dict[LaneKey, str] = field(default_factory=dict)
    root_identity: str | None = None
    root_branch: str = ""
    nonterminal: set[LaneKey] = field(default_factory=set)
    missing_worktree_and_branch: int = 0
    filter_rows: list[dict[str, Any]] = field(default_factory=list)
    retained_retry_keys: set[LaneKey] = field(default_factory=set)
    global_identity_read: dict[str, Any] = field(default_factory=dict)
    scan_retry_entries: dict[LaneKey, dict[str, Any]] = field(default_factory=dict)
    pass_lanes_by_task: dict[str, set[str]] = field(default_factory=dict)
    pass_records_reader: Any | None = None
    identity_filter_stopped: bool = False
    identity_filter_unvisited: int = 0
    # Unknown identity evidence (failed, timed out or unreadable) is not inspected absence.
    identity_evidence_unknown: bool = False
    filter_overrun_probe_used: bool = False
    identity_filter_timing: dict[str, Any] = field(default_factory=dict)
    scan_cost_unreadable: Any = None
    now_epoch: float = 0.0
    next_grace_expiry_at: float | None = None

    # Admission and disposition units.
    cursor: LaneKey | None = None
    cursor_priority: Any = None
    retry_entries: dict[LaneKey, dict[str, Any]] = field(default_factory=dict)
    retry_state_pruned: int = 0
    candidate_index_by_key: dict[LaneKey, int] = field(default_factory=dict)
    outcome_counts: Counter[str] = field(default_factory=Counter)
    phase_instrument_counts: Counter[str] = field(default_factory=Counter)
    retired_lane_ids: list[str] = field(default_factory=list)
    lane_timings: list[dict[str, Any]] = field(default_factory=list)
    # Published before retirement so a crash cannot erase the replacement-review signal.
    events: list[dict[str, Any]] = field(default_factory=list)
    attempted: int = 0
    admission_blocked: bool = False
    has_more: bool = False
    remaining_count: int = 0
    process_snapshot: tuple[list[str], list[str]] | None = None
    process_probed: bool = False
    retire_seconds: float = 0.0
    deferred_indices: set[int] = field(default_factory=set)
    admitted_keys: set[LaneKey] = field(default_factory=set)
    cursor_advanced: bool = False
    absorbed_retired: int = 0


@dataclass
class CandidateTurn:
    """One candidate's admission/disposition turn.

    ``status``, ``special`` and ``landing`` start from the selection snapshot
    and are narrowed by the review and close units exactly as the inline loop
    rebound its locals.
    """

    index: int
    row: dict[str, Any]
    worktree_path: str
    path_exists: bool
    landing: str | None
    lane_id: str
    row_task: str
    key: LaneKey
    cursor_before_candidate: LaneKey | None
    candidate_phase_timings: dict[str, float]
    status: Any
    special: dict[str, Any] | None
    no_work: dict[str, Any] | None
    candidate_state: dict[str, Any] = field(default_factory=dict)
