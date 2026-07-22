"""Coordinator-side wave dispatch primitives (implementation note S3).

Holds ``LaneSpec``, ``build_ready_facts``, wave-width resolution, and the
slot-coordinator (lock + semaphore + owned-handoff claim). The public MCP tool
``dispatch_wave`` lives in ``api.py`` and wires these helpers to
``_run_offload_pass_impl`` + ``await_offload_passes``.

This module must NOT import ``api`` at module scope (avoids a circular import).
"""

from __future__ import annotations

import math
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Collection, Mapping, Sequence

from workbay_orchestrator_mcp.orchestration.backend_registry import cost_class_for_backend
from workbay_orchestrator_mcp.orchestration.host_resources import (
    COST_REMOTE,
    _GATED_COST_CLASSES,
    _release_heavy_slot,
    acquire_heavy_slot,
    locks_root,
    resolve_live_admission,
)
from workbay_orchestrator_mcp.orchestration.lane_ready_set import compute_ready_set

# Terminal worktree_lanes statuses treated as "completed" for ready-set
# exclusion. Deliberately broader than CLOSEABLE_LANE_STATUSES (which omits
# closed_stale — implementation note rev5-b-02).
_COMPLETED_LANE_STATUSES = frozenset({"merged", "closed", "closed_stale"})

# Default env cap when WORKBAY_REMOTE_AGENT_MAX_LANES is unset (matches
# scripts/remote_agent.sh MAX_LANES default).
_DEFAULT_ENV_CAP = 3

# Turn-budget profiles filled by LaneSpec.for_kind before positivity validation.
# In-slice defaults; measured verify-twin budgets land as overrides.
_KIND_PROFILES: dict[str, dict[str, Any]] = {
    "implement": {
        "token_budget": 200_000,
        "timeout_seconds": 900.0,
        "model": "",
        "effort": "high",
        "brief": "",
        "backend": "grok-remote",
    },
    "review": {
        "token_budget": 100_000,
        "timeout_seconds": 600.0,
        "model": "",
        "effort": "high",
        "brief": "",
        "backend": "grok-remote",
    },
}

# Typed deferral / refusal reasons (stable for tests + decision rows).
REASON_WAVE_WIDTH_ZERO = "wave_max_width_zero"
REASON_NOT_REMOTE_WAVE = "not_remote_wave_member"
REASON_NOT_READY = "not_ready"
REASON_ADMISSION_DEFERRED = "admission_deferred"
REASON_ADMISSION_REFUSED = "admission_refused"
REASON_SLOT_UNAVAILABLE = "heavy_slot_unavailable"
REASON_EMPTY_RESULT = "empty_result"
REASON_LANE_SPEC_INVALID = "lane_spec_invalid"
REASON_WORKER_TIMEOUT = "worker_timeout"

# Slack added on top of batch-aware (ceil(n/width) * max timeout) join budget so
# scheduler/setup jitter does not false-timeout a still-finishing tail batch.
WAVE_JOIN_SLACK_SECONDS = 30.0


class LaneSpecError(ValueError):
    """Fail-closed LaneSpec construction / factory refusal."""


@dataclass(frozen=True, slots=True)
class LaneSpec:
    """One wave-member lane: backend-derived cost class, fail-closed budgets.

    ``cost_class`` is always derived via ``cost_class_for_backend(backend)``.
    An explicit disagreeing override is refused at the factory.
    """

    lane_id: str
    backend: str
    token_budget: int
    timeout_seconds: float
    model: str = ""
    effort: str = "high"
    brief: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.lane_id, str) or not self.lane_id.strip():
            raise LaneSpecError("LaneSpec.lane_id must be a non-empty string")
        if not isinstance(self.backend, str) or not self.backend.strip():
            raise LaneSpecError("LaneSpec.backend must be a non-empty string")
        if isinstance(self.token_budget, bool) or not isinstance(self.token_budget, int) or self.token_budget <= 0:
            raise LaneSpecError(
                "LaneSpec requires a positive token_budget (mandatory, fail-closed)."
            )
        try:
            timeout = float(self.timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise LaneSpecError(
                "LaneSpec requires a positive timeout_seconds (mandatory, fail-closed)."
            ) from exc
        if timeout <= 0:
            raise LaneSpecError(
                "LaneSpec requires a positive timeout_seconds (mandatory, fail-closed)."
            )

    @property
    def cost_class(self) -> str:
        return cost_class_for_backend(self.backend)

    @classmethod
    def for_kind(cls, lane_kind: str, **overrides: Any) -> LaneSpec:
        """Fill the turn-budget profile for *lane_kind*, then validate positivity.

        Explicit ``cost_class`` that disagrees with the backend-derived class is
        refused. Missing / ``<=0`` ``token_budget`` or ``timeout_seconds`` after
        merge is refused (row 20).
        """
        kind = str(lane_kind or "").strip()
        if kind not in _KIND_PROFILES:
            raise LaneSpecError(
                f"LaneSpec.for_kind: unknown lane_kind {lane_kind!r}; "
                f"valid: {sorted(_KIND_PROFILES)}"
            )
        profile = dict(_KIND_PROFILES[kind])
        # Pull optional cost_class check before merge (not a LaneSpec field).
        explicit_cost = overrides.pop("cost_class", None)
        if "lane_id" not in overrides and "lane_id" not in profile:
            raise LaneSpecError("LaneSpec.for_kind requires lane_id in overrides")
        merged = {**profile, **overrides}
        backend = str(merged.get("backend") or "").strip()
        derived = cost_class_for_backend(backend)
        if explicit_cost is not None and str(explicit_cost).strip() != derived:
            raise LaneSpecError(
                f"LaneSpec cost_class override {explicit_cost!r} disagrees with "
                f"backend-derived {derived!r} for backend {backend!r}"
            )
        return cls(
            lane_id=str(merged["lane_id"]),
            backend=backend,
            token_budget=merged["token_budget"],  # type: ignore[arg-type]
            timeout_seconds=float(merged["timeout_seconds"]),
            model=str(merged.get("model") or ""),
            effort=str(merged.get("effort") or "high"),
            brief=str(merged.get("brief") or ""),
        )


def _extract_listed_lanes(listed: object) -> list[dict[str, Any]]:
    if not isinstance(listed, dict):
        return []
    data = listed.get("data") if isinstance(listed.get("data"), dict) else listed
    if not isinstance(data, dict):
        return []
    lanes = data.get("lanes")
    if not isinstance(lanes, list):
        return []
    return [row for row in lanes if isinstance(row, dict)]


def build_ready_facts(task_ref: str, root: Path) -> tuple[set[str], set[str]]:
    """Return ``(satisfied, completed)`` for :func:`compute_ready_set`.

    * ``completed`` — worktree_lanes whose status is in
      ``{merged, closed, closed_stale}`` (via ``manage_worktree_lane`` list;
      deliberately not ``CLOSEABLE_LANE_STATUSES``).
    * ``satisfied`` — discharged-prereq set: ``U ∈ satisfied`` iff **for every
      dependent** ``D`` with ``U ∈ depends_on[D]``,
      ``lane_dependency_satisfied(U, D)`` holds. Under-counting is safe (only
      lowers wave width); over-counting would oversubscribe. No second oracle —
      reuses the existing predicate only (rev5-b-02).
    """
    from workbay_orchestrator_mcp.lanes import manage_worktree_lane  # noqa: PLC0415
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
        load_manifest,
    )
    from workbay_orchestrator_mcp.orchestration.orchestrator_lanes import (  # noqa: PLC0415
        _depends_on_map,
        lane_dependency_satisfied,
    )

    root_path = Path(root)
    listed = manage_worktree_lane(
        operation="list",
        task_ref=task_ref,
        status="all",
        limit=10_000,
    )
    completed: set[str] = set()
    for row in _extract_listed_lanes(listed):
        lid = str(row.get("lane_id") or "").strip()
        status = str(row.get("status") or "").strip()
        if lid and status in _COMPLETED_LANE_STATUSES:
            completed.add(lid)

    try:
        manifest = load_manifest(task_ref, orchestrator_root=str(root_path))
    except (FileNotFoundError, OSError, RuntimeError, ValueError, TypeError):
        return set(), completed

    depends = _depends_on_map(manifest if isinstance(manifest, dict) else {})
    # dependents_of[U] = every D that lists U as a direct prereq
    dependents_of: dict[str, list[str]] = {}
    for dependent, prereqs in depends.items():
        if not isinstance(prereqs, list):
            continue
        for u in prereqs:
            if isinstance(u, str) and u.strip():
                dependents_of.setdefault(u.strip(), []).append(dependent)

    satisfied: set[str] = set()
    for upstream, dependents in dependents_of.items():
        if not dependents:
            # Vacuous: no dependents → treated as discharged (rare; U was a prereq).
            satisfied.add(upstream)
            continue
        ok_for_all = True
        for dependent in dependents:
            try:
                ok, _reason = lane_dependency_satisfied(
                    root_path, task_ref, upstream, dependent
                )
            except Exception:  # noqa: BLE001 — predicate fault → not satisfied
                ok = False
            if not ok:
                ok_for_all = False
                break
        if ok_for_all:
            satisfied.add(upstream)

    return satisfied, completed


def env_wave_cap() -> int:
    """``WORKBAY_REMOTE_AGENT_MAX_LANES`` integer (default 3). Garbage → 0 (fail closed)."""
    raw = os.environ.get("WORKBAY_REMOTE_AGENT_MAX_LANES")
    if raw is None or str(raw).strip() == "":
        return _DEFAULT_ENV_CAP
    try:
        return int(str(raw).strip())
    except ValueError:
        return 0


def compute_wave_ready_ids(
    *,
    task_ref: str,
    root: Path,
    satisfied: Collection[str] | None = None,
    completed: Collection[str] | None = None,
) -> set[str]:
    """Lane ids in the current ready frontier (same oracle as wave-width sizing).

    Used by both ``resolve_wave_max_width`` (sizing) and ``coordinate_wave``
    (submit filter) so non-ready wave members are never submitted (GRPH-05).
    Fail-closed: any fact/manifest fault yields an empty set (no submits).
    """
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
        load_manifest,
    )

    try:
        if satisfied is None or completed is None:
            sat, comp = build_ready_facts(task_ref, root)
            if satisfied is None:
                satisfied = sat
            if completed is None:
                completed = comp
        manifest = load_manifest(task_ref, orchestrator_root=str(root))
    except (FileNotFoundError, OSError, RuntimeError, ValueError, TypeError):
        return set()
    result = compute_ready_set(manifest, satisfied=satisfied, completed=completed)
    return {
        str(e.get("lane_id"))
        for e in (result.get("ready") or [])
        if isinstance(e, dict) and e.get("lane_id")
    }


def resolve_wave_max_width(
    *,
    task_ref: str,
    root: Path,
    wave_lane_ids: Collection[str],
    wave_max_width: int | None = None,
    satisfied: Collection[str] | None = None,
    completed: Collection[str] | None = None,
) -> int:
    """``min(|ready ∩ wave_lane_ids|, env_cap)``; optional explicit override clamps further."""
    ready_ids = compute_wave_ready_ids(
        task_ref=task_ref,
        root=root,
        satisfied=satisfied,
        completed=completed,
    )
    wave_ids = {str(x) for x in wave_lane_ids if x}
    frontier = len(ready_ids & wave_ids)
    env_cap = env_wave_cap()
    width = min(frontier, env_cap) if env_cap >= 0 else 0
    if wave_max_width is not None:
        try:
            explicit = int(wave_max_width)
        except (TypeError, ValueError):
            explicit = 0
        width = min(width, max(0, explicit))
    return max(0, width)


def _is_remote_wave_member(spec: LaneSpec) -> bool:
    """Wave membership: only COST_REMOTE (grok-remote) is wave-routable (row 26)."""
    return spec.cost_class == COST_REMOTE


def coordinate_wave(
    lane_specs: Sequence[LaneSpec | Mapping[str, Any]],
    *,
    task_ref: str,
    workspace_root: Path,
    run_pass: Callable[..., Any],
    await_passes: Callable[..., Any] | None = None,
    wave_max_width: int | None = None,
    wait_seconds: float = 0.0,
) -> dict[str, Any]:
    """Run a blocking-join wave: claim/admit, submit via *run_pass*, optional join.

    Parameters
    ----------
    run_pass
        Callable matching ``_run_offload_pass_impl`` kwargs (must accept
        ``reserved_slot_idx`` for gated lanes).
    await_passes
        Optional join over pass_ids (``await_offload_passes``). When ``None``
        or ``wait_seconds<=0``, results come from the thread-pool futures only.
    """
    root = Path(workspace_root)
    specs: list[LaneSpec] = []
    refused: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    dispatched: list[dict[str, Any]] = []

    for raw in lane_specs:
        try:
            if isinstance(raw, LaneSpec):
                spec = raw
            elif isinstance(raw, Mapping):
                # Mapping path: require budgets explicitly (fail-closed, no silent defaults).
                if "token_budget" not in raw or "timeout_seconds" not in raw:
                    raise LaneSpecError(
                        "LaneSpec requires token_budget and timeout_seconds "
                        "(mandatory, fail-closed)."
                    )
                # Refuse an explicit free-form cost_class that disagrees with the
                # backend-derived class (parity with LaneSpec.for_kind; row 18).
                _explicit_cc = raw.get("cost_class")
                if _explicit_cc is not None and str(_explicit_cc).strip() != cost_class_for_backend(
                    str(raw["backend"])
                ):
                    raise LaneSpecError(
                        f"cost_class {str(_explicit_cc)!r} disagrees with backend-derived "
                        f"{cost_class_for_backend(str(raw['backend']))!r} "
                        f"for backend {str(raw['backend'])!r}"
                    )
                spec = LaneSpec(
                    lane_id=str(raw["lane_id"]),
                    backend=str(raw["backend"]),
                    token_budget=raw["token_budget"],  # type: ignore[arg-type]
                    timeout_seconds=float(raw["timeout_seconds"]),  # type: ignore[arg-type]
                    model=str(raw.get("model") or ""),
                    effort=str(raw.get("effort") or "high"),
                    brief=str(raw.get("brief") or ""),
                )
            else:
                raise LaneSpecError(f"unsupported lane_spec type: {type(raw)!r}")
        except (LaneSpecError, TypeError, ValueError, KeyError) as exc:
            refused.append(
                {
                    "lane_id": str(getattr(raw, "lane_id", None) or (raw.get("lane_id") if isinstance(raw, Mapping) else "") or ""),
                    "reason": REASON_LANE_SPEC_INVALID,
                    "error": str(exc),
                }
            )
            continue
        if not _is_remote_wave_member(spec):
            refused.append(
                {
                    "lane_id": spec.lane_id,
                    "backend": spec.backend,
                    "cost_class": spec.cost_class,
                    "reason": REASON_NOT_REMOTE_WAVE,
                    "error": (
                        f"dispatch_wave routes only COST_REMOTE/grok-remote; "
                        f"lane {spec.lane_id!r} cost_class={spec.cost_class!r} "
                        f"(daemon owns non-remote twins)"
                    ),
                }
            )
            continue
        specs.append(spec)

    # Generate pass_ids up front for every remote member (including later deferred).
    pass_ids: dict[str, str] = {s.lane_id: str(uuid.uuid4()) for s in specs}
    first_pass_prefix = ""
    if pass_ids:
        first = next(iter(pass_ids.values()))
        first_pass_prefix = first.split("-")[0] if first else first[:8]
    wave_id = f"wave-{task_ref}-{first_pass_prefix or 'empty'}"

    if not specs:
        return {
            "ok": True,
            "wave_id": wave_id,
            "dispatched": dispatched,
            "deferred": deferred,
            "refused": refused,
            "wave_max_width": 0,
        }

    # GRPH-05: only submit ready ∩ wave. Width sizing already used this frontier;
    # the submit path previously ignored it and ran every remote member.
    ready_ids = compute_wave_ready_ids(task_ref=task_ref, root=root)
    ready_specs: list[LaneSpec] = []
    for s in specs:
        if s.lane_id in ready_ids:
            ready_specs.append(s)
        else:
            deferred.append(
                {
                    "lane_id": s.lane_id,
                    "pass_id": pass_ids[s.lane_id],
                    "status": "deferred",
                    "reason": REASON_NOT_READY,
                    "error": (
                        f"lane {s.lane_id!r} is not in the ready frontier "
                        f"(unsatisfied depends_on or already completed)"
                    ),
                }
            )
    specs = ready_specs
    wave_lane_ids = [s.lane_id for s in specs]

    if not specs:
        return {
            "ok": True,
            "wave_id": wave_id,
            "dispatched": dispatched,
            "deferred": deferred,
            "refused": refused,
            "wave_max_width": 0,
        }

    width = resolve_wave_max_width(
        task_ref=task_ref,
        root=root,
        wave_lane_ids=wave_lane_ids,
        wave_max_width=wave_max_width,
    )
    # Semaphore concurrency = filtered (ready) set size, clamped by env/explicit.
    width = min(width, len(specs))
    if width == 0:
        # Fail closed — never construct threading.Semaphore(0) (row 19).
        for s in specs:
            deferred.append(
                {
                    "lane_id": s.lane_id,
                    "pass_id": pass_ids[s.lane_id],
                    "reason": REASON_WAVE_WIDTH_ZERO,
                    "error": "wave_max_width is 0 (empty ready frontier or env_cap=0)",
                }
            )
        return {
            "ok": True,
            "wave_id": wave_id,
            "dispatched": dispatched,
            "deferred": deferred,
            "refused": refused,
            "wave_max_width": 0,
        }

    admit_lock = threading.Lock()
    pool_sem = threading.Semaphore(width)

    # Batch-aware join deadline: excess lanes run in serial batches of `width`,
    # so the budget is ceil(n/width) * max(timeout) + slack — not a single-batch
    # max(timeout)+slack, which false-timeouts the still-running tail batch.
    _batches = math.ceil(len(specs) / max(1, width))
    deadline_seconds = (
        _batches * max((float(s.timeout_seconds) for s in specs), default=0.0)
        + WAVE_JOIN_SLACK_SECONDS
    )

    def _run_one(spec: LaneSpec) -> dict[str, Any]:
        pass_id = pass_ids[spec.lane_id]
        cost = spec.cost_class
        reserved_idx: int | None = None
        slot_fd: int | None = None
        owned = False
        acquired = pool_sem.acquire(timeout=deadline_seconds)
        if not acquired:
            return {
                "lane_id": spec.lane_id,
                "pass_id": pass_id,
                "status": "refused",
                "reason": REASON_WORKER_TIMEOUT,
                "error": "semaphore acquire timed out",
            }
        try:
            if cost == COST_REMOTE:
                # Short-circuit: no heavy-slot claim (decision:4156 / row 18).
                reserved_idx = None
                slot_fd = None
            elif cost in _GATED_COST_CLASSES:
                with admit_lock:
                    decision = resolve_live_admission(root, cost)
                    if decision.decision != "allow":
                        reason = (
                            REASON_ADMISSION_REFUSED
                            if decision.decision == "refuse"
                            else REASON_ADMISSION_DEFERRED
                        )
                        return {
                            "lane_id": spec.lane_id,
                            "pass_id": pass_id,
                            "status": "deferred",
                            "reason": reason,
                            "error": f"host memory admission {decision.decision}: {decision.reason}",
                            "admission": decision.to_dict(),
                        }
                    claimed = acquire_heavy_slot(locks_root(root), decision.derived_width)
                    if claimed is None:
                        return {
                            "lane_id": spec.lane_id,
                            "pass_id": pass_id,
                            "status": "deferred",
                            "reason": REASON_SLOT_UNAVAILABLE,
                            "error": "heavy slot unavailable after admission allow",
                        }
                    reserved_idx, slot_fd = claimed
            else:
                # light / unknown: treat as not wave-routable (should not reach here)
                return {
                    "lane_id": spec.lane_id,
                    "pass_id": pass_id,
                    "status": "refused",
                    "reason": REASON_NOT_REMOTE_WAVE,
                    "error": f"unsupported cost_class for wave: {cost}",
                }

            try:
                result = run_pass(
                    lane_id=spec.lane_id,
                    task_ref=task_ref,
                    backend=spec.backend,
                    model=spec.model or None,
                    reasoning_effort=spec.effort,
                    token_budget=spec.token_budget,
                    timeout_seconds=spec.timeout_seconds,
                    pass_id=pass_id,
                    reserved_slot_idx=reserved_idx,
                )
                owned = True  # worker (or this thread) owns the fd until release below
            except Exception as exc:  # noqa: BLE001
                return {
                    "lane_id": spec.lane_id,
                    "pass_id": pass_id,
                    "status": "refused",
                    "reason": "run_pass_error",
                    "error": str(exc),
                }
            finally:
                # Ownership handoff (row 19): if submission failed before ownership,
                # close + de-register here. Once owned, release after the call.
                if slot_fd is not None and not owned and reserved_idx is not None:
                    _release_heavy_slot(reserved_idx, slot_fd)
                    slot_fd = None
                    reserved_idx = None

            if not isinstance(result, dict) or not result:
                entry = {
                    "lane_id": spec.lane_id,
                    "pass_id": pass_id,
                    "status": "refused",
                    "reason": REASON_EMPTY_RESULT,
                    "error": "empty_result from run_offload_pass",
                    "result": result,
                }
            else:
                outcome = str(result.get("outcome") or "")
                ok = bool(result.get("ok", True))
                error_kind = str(result.get("error_kind") or outcome or "")
                if error_kind in ("admission_deferred", "admission_refused") or outcome in (
                    "admission_deferred",
                    "admission_refused",
                ):
                    entry = {
                        "lane_id": spec.lane_id,
                        "pass_id": pass_id,
                        "status": "deferred",
                        "reason": error_kind or outcome,
                        "result": result,
                    }
                elif not ok and outcome != "still_running":
                    # Any ok:False that is not an in-progress marker is a REFUSAL —
                    # including failures with no outcome/error_kind (e.g. "Lane
                    # worktree does not exist"). Never let an empty outcome fall
                    # through to the dispatched branch.
                    entry = {
                        "lane_id": spec.lane_id,
                        "pass_id": pass_id,
                        "status": "refused",
                        "reason": outcome or error_kind or "error",
                        "result": result,
                    }
                else:
                    entry = {
                        "lane_id": spec.lane_id,
                        "pass_id": pass_id,
                        "status": "dispatched",
                        "result": result,
                    }
            return entry
        finally:
            if slot_fd is not None and reserved_idx is not None and owned:
                _release_heavy_slot(reserved_idx, slot_fd)
            # Only release if we acquired (timeout path returns before this try).
            if acquired:
                pool_sem.release()

    # Submit all remote members under the wave semaphore bound.
    # Manual pool (not `with`) so context-manager __exit__ shutdown(wait=True)
    # cannot re-hang the coordinator past the batch-aware join deadline.
    results: list[dict[str, Any]] = []
    pool = ThreadPoolExecutor(max_workers=max(1, width))
    try:
        futures = {pool.submit(_run_one, s): s for s in specs}
        try:
            # Track futures the as_completed loop actually yielded. A future may
            # complete in the window between the last yield and the deadline
            # re-check (done() but never yielded) — harvest those in the timeout
            # handler so dispatched passes are not dropped/orphaned.
            harvested: set = set()
            for fut in as_completed(futures, timeout=deadline_seconds):
                harvested.add(fut)
                try:
                    results.append(fut.result())
                except Exception as exc:  # noqa: BLE001
                    spec = futures[fut]
                    results.append(
                        {
                            "lane_id": spec.lane_id,
                            "pass_id": pass_ids[spec.lane_id],
                            "status": "refused",
                            "reason": "worker_exception",
                            "error": str(exc),
                        }
                    )
        except FuturesTimeoutError:
            for fut, spec in futures.items():
                if fut in harvested:
                    continue
                if fut.done():
                    try:
                        results.append(fut.result())
                    except Exception as exc:  # noqa: BLE001 — mirror the main-loop exception branch
                        results.append(
                            {
                                "lane_id": spec.lane_id,
                                "pass_id": pass_ids[spec.lane_id],
                                "status": "refused",
                                "reason": "worker_exception",
                                "error": str(exc),
                            }
                        )
                else:
                    results.append(
                        {
                            "lane_id": spec.lane_id,
                            "pass_id": pass_ids[spec.lane_id],
                            "status": "refused",
                            "reason": REASON_WORKER_TIMEOUT,
                            "error": "wave join deadline exceeded",
                        }
                    )
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    # Secondary join over ONLY the dispatched passes (when requested). A lane
    # that was ready-filtered out (not_ready) or admission-deferred/refused was
    # never handed to run_pass and has no persisted pass state; joining its
    # pre-minted pass_id would mis-report it as unknown_pass=failed. Deferred
    # lanes keep their pass_id in their own deferred[] entry for correlation.
    # (GRPH-05 join follow-through — hardening two-agent merge gate.)
    dispatched_pass_ids = [
        e["pass_id"]
        for e in results
        if e.get("status") == "dispatched" and e.get("pass_id")
    ]
    join_payload: dict[str, Any] | None = None
    if await_passes is not None and wait_seconds > 0 and dispatched_pass_ids:
        try:
            join_payload = await_passes(
                dispatched_pass_ids,
                wait_seconds=wait_seconds,
                mode="all_complete",
                task_ref=task_ref,
            )
        except Exception as exc:  # noqa: BLE001
            join_payload = {"ok": False, "error": str(exc)}

    for entry in results:
        status = entry.get("status")
        if status == "dispatched":
            dispatched.append(entry)
        elif status == "deferred":
            deferred.append(entry)
        else:
            refused.append(entry)

    return {
        "ok": True,
        "wave_id": wave_id,
        "dispatched": dispatched,
        "deferred": deferred,
        "refused": refused,
        "wave_max_width": width,
        "pass_ids": dispatched_pass_ids,
        "join": join_payload,
    }


def claim_gated_slot(
    workspace_root: Path,
    cost_class: str,
) -> tuple[int, int] | None:
    """Serialise-friendly helper: resolve admission then claim a heavy slot.

    Returns ``(idx, fd)`` on success, ``None`` when deferred/refused/full.
    Caller owns the fd and must ``_release_heavy_slot`` (or hand off).
    """
    if cost_class not in _GATED_COST_CLASSES:
        return None
    root = Path(workspace_root)
    decision = resolve_live_admission(root, cost_class)
    if decision.decision != "allow":
        return None
    return acquire_heavy_slot(locks_root(root), decision.derived_width)
