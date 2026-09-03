"""Coordinator-side wave dispatch primitives (implementation note S3).

Holds ``LaneSpec``, ``build_ready_facts``, wave-width resolution, and the
slot-coordinator (lock + semaphore + owned-handoff claim). The public MCP tool
``dispatch_wave`` lives in ``api.py`` and wires these helpers to
``_run_offload_pass_impl`` + ``await_offload_passes``.

This module must NOT import ``api`` at module scope (avoids a circular import).
"""

from __future__ import annotations

import contextvars
import json
import logging
import math
import os
import statistics
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Callable, Collection, Mapping, Sequence

from workbay_orchestrator_mcp.orchestration import learned_wave_cap
from workbay_orchestrator_mcp.orchestration.backend_registry import cost_class_for_backend, validate_backend
from workbay_orchestrator_mcp.orchestration.backend_spec import default_effort_for_model
from workbay_orchestrator_mcp.orchestration.host_resources import (
    _GATED_COST_CLASSES,
    COST_REMOTE,
    AdmissionDecision,
    _release_heavy_slot,
    acquire_heavy_slot,
    derive_stock_lane_kind,
    format_admission_gate_error,
    locks_root,
    resolve_live_admission,
)
from workbay_orchestrator_mcp.orchestration.lane_ready_set import compute_ready_set, dispatch_order
from workbay_orchestrator_mcp.orchestration.offload_profiles import (
    BOUND_GROK_DERIVED,
    OFFLOAD_AGENT_PROFILES,
)
from workbay_orchestrator_mcp.orchestration.offload_timeout_ssot import GROK_TIMEOUT_CAP

# Terminal worktree_lanes statuses treated as "completed" for ready-set
# exclusion. Deliberately broader than CLOSEABLE_LANE_STATUSES (which omits
# closed_stale — implementation note rev5-b-02).
_COMPLETED_LANE_STATUSES = frozenset({"merged", "closed", "closed_stale"})

# Default env cap when WORKBAY_REMOTE_AGENT_MAX_LANES is unset (matches
# scripts/remote_agent.sh MAX_LANES default; the two MUST move together, or the
# coordinator admits a width the VM then defers with exit 75).
_DEFAULT_ENV_CAP = 20

# Conservative fixed admission width, below the width at which the remote VM
# was observed to crash. The observed width is evidence, not a cap term.
SEED_WAVE_WIDTH = 12

logger = logging.getLogger(__name__)

# Seconds between pool.submit calls when admitting a multi-lane wave. Spreads
# sandbox materialization (clone + uv sync) so width-20 is not a thundering
# herd. Pacing is ON by default; operators may set the env to 0 to opt out.
_DEFAULT_DISPATCH_STAGGER_SECONDS = 0.5
# Per-submit ceiling: the paced submit loop runs before any deadline clock
# starts, so an unbounded value (unit slip, inf) would hang the coordinator
# with the wave audit row already open. Clamp rather than refuse so pacing
# survives a typo instead of being deleted by it.
_MAX_DISPATCH_STAGGER_SECONDS = 30.0
# Whole-wave submission-phase budget. The per-gap ceiling alone still allows
# ceiling x (n-1) coordinator stall before any deadline clock starts; at
# width 20 that is 9.5 minutes with no lane running. Cap the sum of sleeps
# across the wave, and derive the effective gap from wave size at submit.
_TOTAL_DISPATCH_STAGGER_BUDGET_SECONDS = 30.0
# Positive floor for a nonzero stagger. Subnormals (1e-300) pass `stagger > 0`
# and report pacing ON while pacing nothing — raise them to a real interval.
# Explicit 0 remains the only opt-out and is not raised.
_MIN_DISPATCH_STAGGER_SECONDS = 0.05

# Turn-budget profiles filled by LaneSpec.for_kind before positivity validation.
# In-slice defaults; measured verify-twin budgets land as overrides.
_KIND_PROFILES: dict[str, dict[str, Any]] = {
    "implement": {
        "token_budget": 200_000,
        "timeout_seconds": 900.0,
        "model": "",
        "brief": "",
        "backend": "grok-remote",
    },
    "review": {
        "token_budget": 100_000,
        "timeout_seconds": 600.0,
        "model": "",
        "brief": "",
        "backend": "grok-remote",
    },
}

# Typed deferral / refusal reasons (stable for tests + decision rows).
REASON_WAVE_WIDTH_ZERO = "wave_max_width_zero"
REASON_NOT_REMOTE_WAVE = "not_remote_wave_member"
REASON_NOT_READY = "not_ready"
REASON_MANIFEST_INVALID = "manifest_invalid"
REASON_ADMISSION_DEFERRED = "admission_deferred"
REASON_ADMISSION_REFUSED = "admission_refused"
REASON_SLOT_UNAVAILABLE = "heavy_slot_unavailable"
REASON_EMPTY_RESULT = "empty_result"
REASON_LANE_SPEC_INVALID = "lane_spec_invalid"
REASON_WORKER_TIMEOUT = "worker_timeout"
REASON_CONFLICT_ACTIVE = "conflict_active"

LANE_SPEC_REQUIRED: tuple[str, ...] = (
    "lane_id",
    "backend",
    "token_budget",
    "timeout_seconds",
)

# Slack added on top of batch-aware (ceil(n/width) * max timeout) join budget so
# scheduler/setup jitter does not false-timeout a still-finishing tail batch.
WAVE_JOIN_SLACK_SECONDS = 30.0


class LaneSpecError(ValueError):
    """Fail-closed LaneSpec construction / factory refusal."""


# Timeout-cap provenance for one backend. ``declared`` is the ONLY state that
# yields a refusal threshold.
CAP_DECLARED = "declared"
CAP_UNDECLARED = "undeclared"
CAP_UNSUPPORTED = "unsupported"
CAP_BACKEND_MISSING = "backend_missing"


def resolve_declared_timeout_cap(backend: str) -> tuple[str, int | None]:
    """Classify the ceiling this gate may enforce for *backend*: ``(state, cap)``.

    ``offload_profiles.resolve_adapter_timeout_cap`` deliberately SIZES a bound
    and says so in its own docstring — for an unknown, profile-less or
    ``timeout_cap=None`` backend it returns ``CURSOR_TIMEOUT_CAP`` as a *sizing
    fallback*, not as that backend's declared ceiling. Reading a refusal
    threshold off that fallback would hand six unrelated backends a
    cursor-derived number that appears nowhere in their config and would let a
    cursor-specific env var move their threshold ([CLM-03]; a claim the config
    does not support). This function therefore reads the DECLARED ceiling only:

    * ``profile.timeout_cap`` — the profile states its own ceiling;
    * ``BOUND_GROK_DERIVED`` — ``derive_grok_single_cycle_bounds`` clamps to the
      literal ``GROK_TIMEOUT_CAP`` SSOT constant (not env-movable), so that IS a
      declared ceiling even though the profile field is ``None``;
    * ``BOUND_BRIDGE_TIMEOUT`` / no profile — ``undeclared``: bounded somewhere
      else. "Not governed HERE", never "ungoverned" — the cost-class and profile
      gates keep ownership of authorization ([API-05]).

    An absent backend (``backend_missing``) and a present-but-unknown one
    (``unsupported``) are DISTINCT causes, never collapsed into one message
    ([OBS-04]). Both fail OPEN on the cap and closed on membership:
    ``cost_class_for_backend`` maps an unknown backend to ``COST_HEAVY``, which
    ``_is_remote_wave_member`` refuses, so nothing escapes unbounded.
    """
    stripped = str(backend or "").strip()
    if not stripped:
        return (CAP_BACKEND_MISSING, None)
    try:
        normalized = validate_backend(stripped)
    except RuntimeError:
        return (CAP_UNSUPPORTED, None)
    profile = OFFLOAD_AGENT_PROFILES.get(normalized)
    if profile is None:
        return (CAP_UNDECLARED, None)
    if profile.timeout_cap is not None:
        return (CAP_DECLARED, int(profile.timeout_cap))
    if profile.single_cycle_bound == BOUND_GROK_DERIVED:
        return (CAP_DECLARED, int(GROK_TIMEOUT_CAP))
    return (CAP_UNDECLARED, None)


def lane_spec_timeout_cap(backend: str) -> int | None:
    """The declared wall-clock ceiling this gate enforces, or ``None``."""
    return resolve_declared_timeout_cap(backend)[1]


def _timeout_bound_clause(backend: str) -> str:
    """State the timeout bound this gate actually APPLIES — never one it does not.

    Deliberately advertises no lower bound. ``LANE_TIMEOUT_MIN_S`` is the clamp
    floor ``codex_lane_config`` uses to *derive* a lane timeout from a measured
    p95; it is not a validation floor, and no arm of this code refuses a value
    below it. Advertising ``[300, cap]`` while accepting 60 is precisely the
    untrue-constraint defect this helper exists to remove ([OBS-04]/[CLM-03]).
    """
    state, cap = resolve_declared_timeout_cap(backend)
    stripped = str(backend or "").strip()
    if state == CAP_BACKEND_MISSING:
        return "timeout_seconds must be > 0; cap unknown (backend missing)"
    if state == CAP_UNSUPPORTED:
        return f"timeout_seconds must be > 0; cap not applied ({stripped!r} is not a supported backend)"
    if state == CAP_UNDECLARED:
        return f"timeout_seconds must be > 0; no cap declared for {stripped} (bounded elsewhere)"
    return f"timeout_seconds must be > 0 and <= {cap} (cap for {stripped})"


def _lane_spec_fields_clause() -> str:
    """Enumerate every LaneSpec field, binding each label to its own field set."""
    required = ", ".join(LANE_SPEC_REQUIRED)
    optional = ", ".join(field for field in LANE_SPEC_FIELDS if field not in LANE_SPEC_REQUIRED)
    return f"fields: {required} (required), {optional} (optional)"


def _shown_number(value: object) -> str:
    """Render a scalar for an operator: integral floats lose the trailing ``.0``."""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _lane_spec_invalid_clauses(raw: Mapping[str, object], *, enforce_cap: bool = False) -> list[str]:
    """Per-field clauses for values that are present but unusable.

    Cap enforcement is OPT-IN (*enforce_cap*) and off for construction: the
    per-backend ceiling is an admission decision that ``coordinate_wave`` takes
    AFTER the cost-class gate, so LaneSpec construction accepts exactly the set
    it accepted before this gate existed and an architecturally impossible
    backend keeps its architectural refusal reason.
    """
    invalid: list[str] = []
    if "lane_id" in raw:
        lane_id = raw["lane_id"]
        if not isinstance(lane_id, str) or not lane_id.strip():
            invalid.append(f"lane_id: must be a non-empty string, got {lane_id!r}")
    if "backend" in raw:
        backend_value = raw["backend"]
        if not isinstance(backend_value, str) or not backend_value.strip():
            invalid.append(f"backend: must be a non-empty string, got {backend_value!r}")
    if "token_budget" in raw:
        budget = raw["token_budget"]
        if isinstance(budget, bool) or not isinstance(budget, int) or budget <= 0:
            invalid.append(f"token_budget: must be a positive int, got {_shown_number(budget)}")
    backend = str(raw.get("backend") or "").strip()
    if "timeout_seconds" in raw:
        timeout_raw = raw["timeout_seconds"]
        try:
            if isinstance(timeout_raw, bool):
                raise TypeError
            timeout_value = float(timeout_raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            invalid.append(f"timeout_seconds: must be a positive number, got {_shown_number(timeout_raw)}")
        else:
            cap = lane_spec_timeout_cap(backend) if enforce_cap else None
            if timeout_value <= 0:
                invalid.append(f"timeout_seconds: must be a positive number, got {_shown_number(timeout_raw)}")
            elif cap is not None and timeout_value > cap:
                invalid.append(f"timeout_seconds: {_shown_number(timeout_raw)} exceeds cap {cap} for {backend}")
    if raw.get("speed") is not None:
        profile = OFFLOAD_AGENT_PROFILES.get(backend)
        if profile is None or not profile.allowed_speeds:
            invalid.append(f"speed: {backend or '?'} does not advertise an orchestration speed")
        elif raw["speed"] not in profile.allowed_speeds:
            advertised = ", ".join(profile.allowed_speeds)
            invalid.append(f"speed: {raw['speed']!r} is not advertised by {backend}; advertised speeds: {advertised}")
    if "lane_kind" in raw and raw.get("lane_kind") not in (None, ""):
        kind = str(raw.get("lane_kind") or "").strip()
        if kind not in ("implement", "review"):
            invalid.append(f"lane_kind: must be 'implement' or 'review', got {raw.get('lane_kind')!r}")
    return invalid


def lane_spec_refusal_clauses(raw: Mapping[str, object], *, enforce_cap: bool = False) -> list[str]:
    """Structured (machine-readable) refusal clauses: missing fields, then invalid ones."""
    missing = [field for field in LANE_SPEC_REQUIRED if field not in raw]
    invalid = _lane_spec_invalid_clauses(raw, enforce_cap=enforce_cap)
    clauses: list[str] = []
    if missing:
        clauses.append(f"missing [{', '.join(missing)}]")
    if invalid:
        clauses.append(f"invalid [{', '.join(invalid)}]")
    return clauses


def describe_lane_spec_error(raw: Mapping[str, object], *, enforce_cap: bool = False) -> str:
    """Name missing/invalid fields, the applied timeout bound, and the field list."""
    lane_id = str(raw.get("lane_id") or "").strip() or "?"
    backend = str(raw.get("backend") or "").strip()
    backend_label = backend or "?"
    clauses = lane_spec_refusal_clauses(raw, enforce_cap=enforce_cap)
    clauses.append(_timeout_bound_clause(backend))
    clauses.append(_lane_spec_fields_clause())
    return f"LaneSpec invalid for lane_id={lane_id} backend={backend_label}: {'; '.join(clauses)}"


def _lane_spec_mapping_is_invalid(raw: Mapping[str, object]) -> bool:
    if any(field not in raw for field in LANE_SPEC_REQUIRED):
        return True
    return bool(_lane_spec_invalid_clauses(raw))


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
    effort: str | None = "high"
    speed: str | None = None
    brief: str = ""
    lane_kind: str = "implement"

    def __post_init__(self) -> None:
        # Every required field routes through the same clause builder, so all
        # four get the named-field treatment (no generic fallback message), and
        # the accepted SET is unchanged from before this diagnostic existed —
        # the cap is not applied here (see ``_lane_spec_invalid_clauses``).
        raw: dict[str, object] = {
            "lane_id": self.lane_id,
            "backend": self.backend,
            "token_budget": self.token_budget,
            "timeout_seconds": self.timeout_seconds,
            "model": self.model,
            "effort": self.effort,
            "speed": self.speed,
            "brief": self.brief,
            "lane_kind": self.lane_kind,
        }
        if _lane_spec_invalid_clauses(raw):
            raise LaneSpecError(describe_lane_spec_error(raw))

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
            raise LaneSpecError(f"LaneSpec.for_kind: unknown lane_kind {lane_kind!r}; valid: {sorted(_KIND_PROFILES)}")
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
            effort=str(merged["effort"])
            if merged.get("effort")
            else default_effort_for_model(backend, str(merged.get("model") or "")),
            speed=str(merged["speed"]) if merged.get("speed") else None,
            brief=str(merged.get("brief") or ""),
            lane_kind=kind,
        )


# Derive the operator-visible field inventory from the contract itself. Keep
# this after the dataclass definition so a future field cannot be omitted.
LANE_SPEC_FIELDS: tuple[str, ...] = tuple(field.name for field in fields(LaneSpec))


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
                ok, _reason = lane_dependency_satisfied(root_path, task_ref, upstream, dependent)
            except Exception:  # noqa: BLE001 — predicate fault → not satisfied
                ok = False
            if not ok:
                ok_for_all = False
                break
        if ok_for_all:
            satisfied.add(upstream)

    return satisfied, completed


def env_wave_cap(environ: Mapping[str, str] = os.environ) -> int:
    """``WORKBAY_REMOTE_AGENT_MAX_LANES`` integer (default 20). Garbage → 0 (fail closed)."""
    raw = environ.get("WORKBAY_REMOTE_AGENT_MAX_LANES")
    if raw is None or str(raw).strip() == "":
        return _DEFAULT_ENV_CAP
    try:
        return int(str(raw).strip())
    except ValueError:
        return 0


def effective_wave_cap(
    *,
    path: Path | str | None = None,
    environ: Mapping[str, str] = os.environ,
) -> int:
    """Return the lower of the operator contract and host-learned cap."""
    learned_wave_cap.consume_reset_env(environ, path=path)
    learned = learned_wave_cap.read_learned_cap(path=learned_wave_cap.resolve_learned_cap_path(path, environ=environ))
    if learned is None:
        learned = SEED_WAVE_WIDTH
        logger.info("learned wave cap missing; using seed %s", learned)
    return min(env_wave_cap(environ), learned)


def dispatch_stagger_seconds() -> float:
    """``WORKBAY_WAVE_DISPATCH_STAGGER_SECONDS`` float (default positive).

    Explicit ``0`` (including ``-0.0``) opts out of pacing and is normalised to
    positive zero. Any value that is not a finite number ``>= 0`` (garbage
    strings, negatives, NaN, inf) falls back to
    ``_DEFAULT_DISPATCH_STAGGER_SECONDS`` (fail safe — a typo must not silently
    remove the protection). Nonzero values below
    ``_MIN_DISPATCH_STAGGER_SECONDS`` are raised to that floor so pacing is
    never "on but effectively zero". Values above
    ``_MAX_DISPATCH_STAGGER_SECONDS`` are clamped to that ceiling so a unit
    slip still paces rather than hangs the coordinator. The submit site further
    bounds the whole phase by ``_TOTAL_DISPATCH_STAGGER_BUDGET_SECONDS``.
    Contrast ``env_wave_cap``, which fails closed to 0.
    """
    raw = os.environ.get("WORKBAY_WAVE_DISPATCH_STAGGER_SECONDS")
    if raw is None or str(raw).strip() == "":
        return _DEFAULT_DISPATCH_STAGGER_SECONDS
    try:
        value = float(str(raw).strip())
    except ValueError:
        return _DEFAULT_DISPATCH_STAGGER_SECONDS
    if not math.isfinite(value) or value < 0:
        return _DEFAULT_DISPATCH_STAGGER_SECONDS
    # Explicit opt-out (and -0.0 / -0, which parse as negative zero and would
    # otherwise leak a signed zero into time.sleep).
    if value == 0:
        return 0.0
    if value < _MIN_DISPATCH_STAGGER_SECONDS:
        return _MIN_DISPATCH_STAGGER_SECONDS
    if value > _MAX_DISPATCH_STAGGER_SECONDS:
        return _MAX_DISPATCH_STAGGER_SECONDS
    return value


def _normalize_task_refs(
    task_ref: str | None,
    task_refs: Sequence[str] | None,
) -> list[str]:
    """Resolve the wave's task-ref set (order preserved, first occurrence wins).

    * ``task_refs`` — multi-plan wave; each entry is included once in call order.
    * ``task_ref`` — single-plan shape, or an extra ref appended when not already
      listed in ``task_refs``.
    """
    out: list[str] = []
    seen: set[str] = set()
    if task_refs is not None:
        for raw in task_refs:
            ref = str(raw).strip()
            if ref and ref not in seen:
                seen.add(ref)
                out.append(ref)
    if task_ref is not None:
        ref = str(task_ref).strip()
        if ref and ref not in seen:
            seen.add(ref)
            out.append(ref)
    return out


def _wave_refs_key(task_refs: Sequence[str]) -> str:
    """Stable, order-independent key for a ref set (wave_id / claim namespace)."""
    return "+".join(sorted({str(r).strip() for r in task_refs if str(r).strip()}))


def _peek_unvalidated_lane_ids(ref: str, root: Path) -> tuple[str, ...]:
    """Return ``lanes`` keys from a manifest JSON without running validation.

    ``load_manifest`` rejects colliding ``owned_paths`` before returning, so a
    quarantined plan never populates ``lane_owner``. Peeking the raw object is
    enough to attribute wave members to the failed ref.
    """
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
        manifest_dir,
    )

    path = manifest_dir(orchestrator_root=str(root)) / f"{ref}.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, UnicodeError):
        return ()
    if not isinstance(raw, Mapping):
        return ()
    lanes = raw.get("lanes")
    if not isinstance(lanes, Mapping):
        return ()
    return tuple(str(lid) for lid in lanes if str(lid))


def compute_wave_ready_ids(
    *,
    task_ref: str | None = None,
    task_refs: Sequence[str] | None = None,
    root: Path,
    satisfied: Collection[str] | None = None,
    completed: Collection[str] | None = None,
    errors: dict[str, str] | None = None,
) -> set[str]:
    """Lane ids in the current ready frontier (same oracle as wave-width sizing).

    Used by both ``resolve_wave_max_width`` (sizing) and ``coordinate_wave``
    (submit filter) so non-ready wave members are never submitted (GRPH-05).
    Fail-closed: any fact/manifest fault yields an empty set (no submits).
    When *errors* is provided, each ref whose load raised is recorded as
    ``errors[ref] = "{exc type}: {exc}"`` so callers can emit a typed
    ``manifest_invalid`` deferral instead of impersonating ``not_ready``.

    When *task_refs* spans multiple plans, this returns the **union** of each
    plan's ready ids (sizing upper bound). Callers that must gate a lane against
    its own plan should resolve readiness per ref (see ``coordinate_wave``).
    """
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
        load_manifest,
    )

    refs = _normalize_task_refs(task_ref, task_refs)
    if not refs:
        return set()

    ready: set[str] = set()
    for ref in refs:
        try:
            # Per-ref facts: shared satisfied/completed only apply to single-ref
            # calls (multi-ref recomputes per plan so facts never leak).
            if len(refs) == 1 and (satisfied is not None and completed is not None):
                sat, comp = satisfied, completed
            elif len(refs) == 1 and (satisfied is not None or completed is not None):
                built_sat, built_comp = build_ready_facts(ref, root)
                sat = satisfied if satisfied is not None else built_sat
                comp = completed if completed is not None else built_comp
            else:
                sat, comp = build_ready_facts(ref, root)
            manifest = load_manifest(ref, orchestrator_root=str(root))
        except (FileNotFoundError, OSError, RuntimeError, ValueError, TypeError) as exc:
            if errors is not None:
                errors[ref] = f"{type(exc).__name__}: {exc}"
            continue
        result = compute_ready_set(manifest, satisfied=sat, completed=comp)
        ready.update(
            str(e.get("lane_id")) for e in (result.get("ready") or []) if isinstance(e, dict) and e.get("lane_id")
        )
    return ready


def resolve_wave_max_width(
    *,
    task_ref: str | None = None,
    task_refs: Sequence[str] | None = None,
    root: Path,
    wave_lane_ids: Collection[str],
    wave_max_width: int | None = None,
    satisfied: Collection[str] | None = None,
    completed: Collection[str] | None = None,
    ready_ids: Collection[str] | None = None,
    effective_cap: int | None = None,
) -> int:
    """``min(|ready ∩ wave_lane_ids|, env_cap)``; optional explicit override clamps further.

    *ready_ids* — optional precomputed frontier (ownership-aware filter from
    ``coordinate_wave``). When omitted, readiness is resolved from *task_ref* /
    *task_refs* via :func:`compute_wave_ready_ids`.
    """
    if ready_ids is None:
        ready_ids = compute_wave_ready_ids(
            task_ref=task_ref,
            task_refs=task_refs,
            root=root,
            satisfied=satisfied,
            completed=completed,
        )
    ready_set = {str(x) for x in ready_ids if x}
    wave_ids = {str(x) for x in wave_lane_ids if x}
    frontier = len(ready_set & wave_ids)
    cap = effective_wave_cap() if effective_cap is None else effective_cap
    width = min(frontier, cap) if cap >= 0 else 0
    if wave_max_width is not None:
        try:
            explicit = int(wave_max_width)
        except (TypeError, ValueError):
            explicit = 0
        width = min(width, max(0, explicit))
    return max(0, width)


def _union_conflict_graph(
    manifests_by_ref: Mapping[str, Mapping[str, Any]],
    lane_owner: Mapping[str, str],
) -> dict[str, set[str]]:
    """Union per-manifest conflict edges and cross-plan owned-path overlaps.

    Intra-plan edges come from ``load_conflict_graph``. Cross-plan (and any
    missing exact-path) edges are derived from ``owned_paths`` so two lanes in
    different plans that own the same file are not invisible to each other.
    """
    from workbay_orchestrator_mcp.orchestration.conflict_gate import (  # noqa: PLC0415
        load_conflict_graph,
    )
    from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
        _normalize_owned_path,
        _owned_path_roots_overlap,
    )

    adj: dict[str, set[str]] = {}

    def _link(a: str, b: str) -> None:
        if not a or not b or a == b:
            return
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)

    for ref, manifest in manifests_by_ref.items():
        if not isinstance(manifest, Mapping):
            continue
        graph = load_conflict_graph(dict(manifest))
        for lid, neighbours in graph.items():
            # Prefer edges whose endpoints are owned by this ref when known;
            # still record the edge so single-plan graphs stay intact.
            for n in neighbours:
                _link(str(lid), str(n))

    # Owned-path roots for every lane we can attribute to a plan.
    owned_roots: dict[str, list[str]] = {}
    for lid, ref in lane_owner.items():
        manifest = manifests_by_ref.get(ref)
        if not isinstance(manifest, Mapping):
            continue
        lanes = manifest.get("lanes")
        if not isinstance(lanes, Mapping):
            continue
        entry = lanes.get(lid)
        if not isinstance(entry, Mapping):
            continue
        raw_paths = entry.get("owned_paths") or []
        if not isinstance(raw_paths, list):
            continue
        roots: list[str] = []
        for raw in raw_paths:
            if not isinstance(raw, str):
                continue
            try:
                roots.append(_normalize_owned_path(raw))
            except ValueError:
                roots.append(raw.strip())
        owned_roots[str(lid)] = roots

    lids = sorted(owned_roots)
    for i, left in enumerate(lids):
        left_roots = owned_roots[left]
        if not left_roots:
            continue
        for right in lids[i + 1 :]:
            right_roots = owned_roots[right]
            if not right_roots:
                continue
            overlap = False
            for lr in left_roots:
                for rr in right_roots:
                    if _owned_path_roots_overlap(lr, rr):
                        overlap = True
                        break
                if overlap:
                    break
            if overlap:
                _link(left, right)

    return adj


def _is_remote_wave_member(spec: LaneSpec) -> bool:
    """Wave membership: only COST_REMOTE (grok-remote) is wave-routable (row 26)."""
    return spec.cost_class == COST_REMOTE


def _constructive_clique_lower_bound(adj: Mapping[str, set[str]]) -> int:
    """Greedy clique from the max-degree vertex (makespan floor; [PERF-04])."""
    if not adj:
        return 0
    start = min(adj, key=lambda v: (-len(adj[v]), v))
    clique: list[str] = [start]
    for candidate in sorted(adj[start]):
        if all(candidate in adj[member] for member in clique):
            clique.append(candidate)
    return len(clique)


def _clique_lower_bound_for_lanes(
    conflict_graph: Mapping[str, set[str]],
    lane_ids: Collection[str],
) -> int:
    """Constructive clique lower bound restricted to *lane_ids*."""
    id_set = {str(x) for x in lane_ids if x}
    if not id_set:
        return 0
    adj: dict[str, set[str]] = {lid: {n for n in conflict_graph.get(lid, set()) if n in id_set} for lid in id_set}
    return _constructive_clique_lower_bound(adj)


def _entry_outcome(entry: Mapping[str, Any]) -> str:
    """Best-effort outcome string for metrics (terminal vs still_running)."""
    raw = entry.get("outcome")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    result = entry.get("result")
    if isinstance(result, dict):
        ro = result.get("outcome")
        if isinstance(ro, str) and ro.strip():
            return ro.strip()
    reason = entry.get("reason")
    if isinstance(reason, str) and reason.strip():
        return reason.strip()
    status = entry.get("status")
    if isinstance(status, str) and status.strip():
        return status.strip()
    return ""


def compute_wave_metrics(results: Mapping[str, Any]) -> dict[str, Any]:
    """Build the structured completion-row metrics payload ([PERF-02]/[PERF-04]).

    *results* is the ``coordinate_wave`` return dict (dispatched/deferred/refused
    + width meta). Lanes with ``outcome="still_running"`` are listed in
    ``per_lane`` but **excluded** from makespan/straggler populations;
    ``metrics_partial`` is True whenever any such lane is present.
    ``straggler_ratio = max/median`` over n≥2 finite terminal walls, else null
    (must use median — mean-for-median is the named row-24 mutation).
    """
    all_entries: list[Mapping[str, Any]] = []
    for key in ("dispatched", "deferred", "refused"):
        bucket = results.get(key) or []
        if isinstance(bucket, list):
            for entry in bucket:
                if isinstance(entry, dict):
                    all_entries.append(entry)

    per_lane: list[dict[str, Any]] = []
    terminal_walls: list[float] = []
    still_running_count = 0

    for entry in all_entries:
        outcome = _entry_outcome(entry)
        wall_raw = entry.get("wall_seconds")
        wall: float | None
        try:
            wall = float(wall_raw) if wall_raw is not None else None
        except (TypeError, ValueError):
            wall = None
        if wall is not None and not math.isfinite(wall):
            wall = None

        per_lane.append(
            {
                "lane_id": entry.get("lane_id"),
                "pass_id": entry.get("pass_id"),
                "wall_seconds": wall,
                "outcome": outcome,
            }
        )
        if outcome == "still_running":
            still_running_count += 1
            continue
        # Terminal-only population for makespan/straggler ([RES-14]).
        if wall is not None:
            terminal_walls.append(wall)

    makespan_seconds: float | None = max(terminal_walls) if terminal_walls else None
    wall_sum = sum(terminal_walls) if terminal_walls else 0.0
    if makespan_seconds is not None and wall_sum > 0:
        makespan_vs_sum: float | None = makespan_seconds / wall_sum
    else:
        makespan_vs_sum = None

    if len(terminal_walls) >= 2:
        med = statistics.median(terminal_walls)
        straggler_ratio: float | None = (max(terminal_walls) / med) if med > 0 else None
    else:
        straggler_ratio = None

    deferred = results.get("deferred") or []
    refused = results.get("refused") or []
    return {
        "wave_id": results.get("wave_id"),
        "requested_width": results.get("requested_width", results.get("wave_max_width")),
        "admitted_width": results.get("admitted_width"),
        "env_cap": results.get("env_cap"),
        "effective_cap": results.get("effective_cap"),
        "learned_cap": results.get("learned_cap"),
        "ready_frontier_width": results.get("ready_frontier_width"),
        "clique_lower_bound": results.get("clique_lower_bound"),
        "deferred_count": len(deferred) if isinstance(deferred, list) else 0,
        "refused_count": len(refused) if isinstance(refused, list) else 0,
        "still_running_count": still_running_count,
        "metrics_partial": still_running_count > 0,
        "per_lane": per_lane,
        "makespan_seconds": makespan_seconds,
        "makespan_vs_sum": makespan_vs_sum,
        "straggler_ratio": straggler_ratio,
    }


def coordinate_wave(
    lane_specs: Sequence[LaneSpec | Mapping[str, Any]],
    *,
    task_ref: str | None = None,
    task_refs: Sequence[str] | None = None,
    workspace_root: Path,
    run_pass: Callable[..., Any],
    await_passes: Callable[..., Any] | None = None,
    wave_max_width: int | None = None,
    wait_seconds: float = 0.0,
    state_dir: Path | None = None,
    active_probe: Callable[[str], bool] | None = None,
    before_submit: Callable[[Mapping[str, Any]], None] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Run a blocking-join wave: claim/admit, submit via *run_pass*, optional join.

    Parameters
    ----------
    task_ref
        Single-plan wave (backward-compatible). Treated as a one-element ref set
        when *task_refs* is omitted.
    task_refs
        Multi-plan wave: ready frontiers and conflict graphs are resolved per
        plan then unioned. Lane readiness is gated against the plan that owns
        the lane (first-wins when the same lane_id appears in more than one
        manifest in the ref set).
    run_pass
        Callable matching ``_run_offload_pass_impl`` kwargs (must accept
        ``reserved_slot_idx`` for gated lanes).
    await_passes
        Optional join over pass_ids (``await_offload_passes``). When ``None``
        or ``wait_seconds<=0``, results come from the thread-pool futures only.
    state_dir
        When set with *active_probe*, enables the conflict-gate filter (S2).
    active_probe
        ``Callable[[lane_id], bool]`` — True when a neighbour worker is live.
    before_submit
        Optional hook invoked with open-row meta *after* start-order is fixed
        and *before* pool submission (api writes ``dispatch_wave_open``).
    sleep
        ``Callable[[float], None]`` used to pace *between* pool submissions
        (default ``time.sleep``). Inject a no-op or recorder in tests; never
        sleeps before the first submit. Interval from
        ``dispatch_stagger_seconds()``; membership and pool concurrency are
        unchanged.
    """
    root = Path(workspace_root)
    # Read the operating cap exactly once for this wave. Keep the configured
    # env contract and persisted learned value separate in every payload.
    env_cap = env_wave_cap()
    operating_cap = effective_wave_cap()
    persisted_cap = learned_wave_cap.read_learned_cap() or SEED_WAVE_WIDTH
    refs = _normalize_task_refs(task_ref, task_refs)
    # Primary ref for single-plan call sites / join fallback; multi-ref waves
    # route run_pass via lane_owner and share a synthetic claim namespace.
    primary_ref = refs[0] if refs else (str(task_ref).strip() if task_ref else "")
    claim_task_ref = _wave_refs_key(refs) if len(refs) > 1 else primary_ref

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
                if _lane_spec_mapping_is_invalid(raw):
                    raise LaneSpecError(describe_lane_spec_error(raw))
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
                    effort=str(raw["effort"])
                    if raw.get("effort")
                    else default_effort_for_model(str(raw["backend"]), str(raw.get("model") or "")),
                    speed=str(raw["speed"]) if raw.get("speed") else None,
                    brief=str(raw.get("brief") or ""),
                    lane_kind=derive_stock_lane_kind(lane_row=raw),
                )
            else:
                raise LaneSpecError(f"unsupported lane_spec type: {type(raw)!r}")
        except (LaneSpecError, TypeError, ValueError, KeyError) as exc:
            entry: dict[str, Any] = {
                "lane_id": str(
                    getattr(raw, "lane_id", None) or (raw.get("lane_id") if isinstance(raw, Mapping) else "") or ""
                ),
                "reason": REASON_LANE_SPEC_INVALID,
                "error": str(exc),
            }
            # ``detail`` carries the STRUCTURED clause list — information the
            # prose ``error`` does not expose to a machine consumer. It is
            # omitted (never duplicated) when there are no field-level clauses,
            # e.g. a cost_class disagreement ([OBS-04]/[API-05]).
            structured = lane_spec_refusal_clauses(raw) if isinstance(raw, Mapping) else []
            if structured:
                entry["detail"] = structured
            refused.append(entry)
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
        # Cap gate LAST, on a wave member whose backend is architecturally
        # admissible. Ordering it ahead of the cost-class gate would mask a
        # structural refusal ("this backend can never join a wave") with a
        # numeric one ("1200 exceeds cap 900") derived from a cap the backend
        # never declared.
        spec_raw: dict[str, object] = {
            "lane_id": spec.lane_id,
            "backend": spec.backend,
            "token_budget": spec.token_budget,
            "timeout_seconds": spec.timeout_seconds,
        }
        cap_clauses = lane_spec_refusal_clauses(spec_raw, enforce_cap=True)
        if cap_clauses:
            refused.append(
                {
                    "lane_id": spec.lane_id,
                    "backend": spec.backend,
                    "cost_class": spec.cost_class,
                    "reason": REASON_LANE_SPEC_INVALID,
                    "timeout_cap": lane_spec_timeout_cap(spec.backend),
                    "error": describe_lane_spec_error(spec_raw, enforce_cap=True),
                    "detail": cap_clauses,
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
    refs_key = _wave_refs_key(refs) if refs else (primary_ref or "empty")
    wave_id = f"wave-{refs_key}-{first_pass_prefix or 'empty'}"

    if not specs:
        return {
            "ok": True,
            "wave_id": wave_id,
            "dispatched": dispatched,
            "deferred": deferred,
            "refused": refused,
            "wave_max_width": 0,
            "env_cap": env_cap,
            "effective_cap": operating_cap,
            "learned_cap": persisted_cap,
        }

    # Load every manifest in the ref set: lane ownership, depends_on, conflicts.
    depends_on: dict[str, list[str]] = {}
    conflict_graph: dict[str, set[str]] = {}
    lane_owner: dict[str, str] = {}
    manifests_by_ref: dict[str, dict[str, Any]] = {}
    manifest_errors: dict[str, str] = {}
    try_claim: Any = None
    release_claim: Any = None
    try:
        from workbay_orchestrator_mcp.orchestration.conflict_gate import (  # noqa: PLC0415
            release_claim as _release_claim,
        )
        from workbay_orchestrator_mcp.orchestration.conflict_gate import (
            try_claim as _try_claim,
        )
        from workbay_orchestrator_mcp.orchestration.lane_manifest import (  # noqa: PLC0415
            load_manifest,
        )

        try_claim = _try_claim
        release_claim = _release_claim
        for ref in refs:
            try:
                manifest = load_manifest(ref, orchestrator_root=str(root))
            except (FileNotFoundError, OSError, RuntimeError, ValueError, TypeError):
                # load_manifest already failed (collision, missing file, …).
                # Peek unvalidated `lanes` keys so quarantined plans still own
                # their ids; compute_wave_ready_ids records the typed error.
                for key in _peek_unvalidated_lane_ids(ref, root):
                    if key not in lane_owner:
                        lane_owner[key] = ref
                continue
            if not isinstance(manifest, dict):
                continue
            manifests_by_ref[ref] = manifest
            lanes = manifest.get("lanes")
            if isinstance(lanes, Mapping):
                for lid in lanes:
                    key = str(lid)
                    # First-wins: earlier task_ref in the provided order owns the id.
                    if key and key not in lane_owner:
                        lane_owner[key] = ref
            raw_deps = manifest.get("depends_on")
            if isinstance(raw_deps, dict):
                for dep_key, prereqs in raw_deps.items():
                    dk = str(dep_key)
                    # First-wins on depends_on entries for duplicate lane ids.
                    if dk and dk not in depends_on and isinstance(prereqs, list):
                        depends_on[dk] = list(prereqs)
        if manifests_by_ref:
            conflict_graph = _union_conflict_graph(manifests_by_ref, lane_owner)
        elif len(refs) == 1:
            # Single-ref load failed above; leave empty graph (fail open on order).
            pass
    except (ImportError, OSError, RuntimeError, ValueError, TypeError):
        # Fail open on order/graph load: membership unchanged; with empty
        # depends_on, dispatch_order falls to its lane_id-lexical order.
        try:
            from workbay_orchestrator_mcp.orchestration.conflict_gate import (  # noqa: PLC0415
                release_claim as _release_claim,
            )
            from workbay_orchestrator_mcp.orchestration.conflict_gate import (
                try_claim as _try_claim,
            )

            try_claim = _try_claim
            release_claim = _release_claim
        except ImportError:  # pragma: no cover
            release_claim = None
            try_claim = None

    # GRPH-05: only submit ready ∩ wave. Readiness is per owning plan so a lane
    # never dispatches merely because another plan's frontier shares its id.
    # A quarantined manifest is a load fault, not an unready frontier: record
    # the exception so the deferral loop can emit REASON_MANIFEST_INVALID.
    ready_by_ref: dict[str, set[str]] = {
        ref: compute_wave_ready_ids(task_ref=ref, root=root, errors=manifest_errors) for ref in refs
    }
    ready_specs: list[LaneSpec] = []
    ownership_ready_ids: set[str] = set()
    for s in specs:
        owner = lane_owner.get(s.lane_id)
        # Fallback: single-ref waves historically admitted lanes even when the
        # manifest load path was partial; multi-ref requires an owning plan.
        if owner is None and len(refs) == 1:
            owner = refs[0]
        plan_ready = ready_by_ref.get(owner or "", set())
        load_error = manifest_errors.get(owner or "")
        if owner is not None and load_error:
            deferred.append(
                {
                    "lane_id": s.lane_id,
                    "pass_id": pass_ids[s.lane_id],
                    "status": "deferred",
                    "reason": REASON_MANIFEST_INVALID,
                    "error": f"manifest for {owner} failed to load: {load_error}",
                }
            )
        elif owner is None and manifest_errors:
            failed_refs = [ref for ref in refs if ref in manifest_errors]
            cited = ", ".join(failed_refs) if failed_refs else ", ".join(manifest_errors)
            sample_ref = failed_refs[0] if failed_refs else next(iter(manifest_errors))
            deferred.append(
                {
                    "lane_id": s.lane_id,
                    "pass_id": pass_ids[s.lane_id],
                    "status": "deferred",
                    "reason": REASON_MANIFEST_INVALID,
                    "error": f"manifest for {cited} failed to load: {manifest_errors[sample_ref]}",
                }
            )
        elif owner is not None and s.lane_id in plan_ready:
            ready_specs.append(s)
            ownership_ready_ids.add(s.lane_id)
        else:
            deferred.append(
                {
                    "lane_id": s.lane_id,
                    "pass_id": pass_ids[s.lane_id],
                    "status": "deferred",
                    "reason": REASON_NOT_READY,
                    "error": (
                        f"lane {s.lane_id!r} is not in the ready frontier (unsatisfied depends_on or already completed)"
                    ),
                }
            )
    specs = ready_specs
    ready_frontier_width = len(specs)
    # Start order under the concurrency cap; membership unchanged (row 22/23).
    if specs:
        ordered_ids = dispatch_order([s.lane_id for s in specs], depends_on)
        by_id = {s.lane_id: s for s in specs}
        specs = [by_id[lid] for lid in ordered_ids if lid in by_id]

    wave_lane_ids = [s.lane_id for s in specs]
    clique_lower_bound = _clique_lower_bound_for_lanes(conflict_graph, wave_lane_ids)

    if not specs:
        return {
            "ok": True,
            "wave_id": wave_id,
            "dispatched": dispatched,
            "deferred": deferred,
            "refused": refused,
            "wave_max_width": 0,
            "requested_width": 0,
            "admitted_width": 0,
            "env_cap": env_cap,
            "effective_cap": operating_cap,
            "learned_cap": persisted_cap,
            "ready_frontier_width": ready_frontier_width,
            "manifest_errors": dict(manifest_errors),
            "clique_lower_bound": clique_lower_bound,
        }

    width = resolve_wave_max_width(
        task_ref=task_ref,
        task_refs=refs if refs else task_refs,
        root=root,
        wave_lane_ids=wave_lane_ids,
        wave_max_width=wave_max_width,
        ready_ids=ownership_ready_ids,
        effective_cap=operating_cap,
    )
    # Semaphore concurrency = filtered (ready) set size, clamped by env/explicit.
    width = min(width, len(specs))
    requested_width = width
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
            "requested_width": 0,
            "admitted_width": 0,
            "env_cap": env_cap,
            "effective_cap": operating_cap,
            "learned_cap": persisted_cap,
            "ready_frontier_width": ready_frontier_width,
            "manifest_errors": dict(manifest_errors),
            "clique_lower_bound": clique_lower_bound,
        }

    # Multi-plan union can surface a ready supply far above the VM lane cap.
    # Cap *membership* (not only concurrency) so a single unioned wave does not
    # drain the whole frontier under a tiny env cap. Single-ref waves keep the
    # established serial-batch behaviour (M1: excess ready still run under the
    # semaphore).
    if len(refs) > 1 and len(specs) > width:
        overflow = specs[width:]
        specs = specs[:width]
        wave_lane_ids = [s.lane_id for s in specs]
        for s in overflow:
            deferred.append(
                {
                    "lane_id": s.lane_id,
                    "pass_id": pass_ids[s.lane_id],
                    "status": "deferred",
                    "reason": "wave_membership_capped",
                    "error": (
                        f"lane {s.lane_id!r} deferred: multi-plan wave membership capped at wave_max_width={width}"
                    ),
                }
            )

    admit_lock = threading.Lock()
    # Serialise try_claim so two concurrent workers cannot both believe they
    # won a conflict race before either holds the flock. Mutual exclusion for
    # the duration of run_pass is provided by the flock + active_probe; claims
    # are released when the worker returns (run_pass is synchronous).
    claim_lock = threading.Lock()
    pool_sem = threading.Semaphore(width)
    gate_enabled = (
        state_dir is not None and active_probe is not None and try_claim is not None and release_claim is not None
    )
    resolved_state_dir = Path(state_dir) if state_dir is not None else None
    # Default probe is never-active when only state_dir is set (defensive).
    probe_fn: Callable[[str], bool] = active_probe if active_probe is not None else (lambda _lid: False)

    # Batch-aware join deadline: excess lanes run in serial batches of `width`,
    # so the budget is ceil(n/width) * max(timeout) + slack — not a single-batch
    # max(timeout)+slack, which false-timeouts the still-running tail batch.
    _batches = math.ceil(len(specs) / max(1, width))
    deadline_seconds = _batches * max((float(s.timeout_seconds) for s in specs), default=0.0) + WAVE_JOIN_SLACK_SECONDS

    def _run_one(spec: LaneSpec) -> dict[str, Any]:
        pass_id = pass_ids[spec.lane_id]
        cost = spec.cost_class
        reserved_idx: int | None = None
        slot_fd: int | None = None
        claim_fd: int | None = None
        stock_decision: AdmissionDecision | None = None
        owned = False
        lane_task_ref = lane_owner.get(spec.lane_id, primary_ref)
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
            # Pinned acquire order: pool_sem -> try_claim -> heavy slot -> run_pass.
            # Conflict refuse must NOT burn an admission decision or heavy slot.
            if gate_enabled and resolved_state_dir is not None:
                neighbours = conflict_graph.get(spec.lane_id, set())
                # Shared claim namespace across the whole ref set so cross-plan
                # neighbours probe the same lock directory. claim_lock only
                # serialises acquisition; the flock is released in finally as
                # soon as run_pass returns so a finished lane no longer excludes
                # its conflict neighbour for the rest of the wave.
                with claim_lock:
                    claim_fd = try_claim(  # type: ignore[misc]
                        claim_task_ref or lane_task_ref,
                        spec.lane_id,
                        neighbours,
                        probe_fn,
                        resolved_state_dir,
                    )
                    if claim_fd is None:
                        return {
                            "lane_id": spec.lane_id,
                            "pass_id": pass_id,
                            "status": "deferred",
                            "reason": REASON_CONFLICT_ACTIVE,
                            "outcome": REASON_CONFLICT_ACTIVE,
                            "error": (f"lane {spec.lane_id!r} deferred: conflict neighbour active"),
                        }

            if cost == COST_REMOTE:
                # Fully off-box workers consume no heavy-memory slot, but they
                # still consume worktree stock. Retain that claim until the
                # synchronous dispatch scope exits.
                with admit_lock:
                    decision = resolve_live_admission(root, cost, lane_kind=spec.lane_kind)
                    stock_decision = decision
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
                            "error": format_admission_gate_error(decision),
                            "admission": decision.to_dict(),
                        }
                reserved_idx = None
                slot_fd = None
            elif cost in _GATED_COST_CLASSES:
                with admit_lock:
                    decision = resolve_live_admission(root, cost, lane_kind=spec.lane_kind)
                    stock_decision = decision
                    if decision.decision != "allow":
                        reason = (
                            REASON_ADMISSION_REFUSED if decision.decision == "refuse" else REASON_ADMISSION_DEFERRED
                        )
                        return {
                            "lane_id": spec.lane_id,
                            "pass_id": pass_id,
                            "status": "deferred",
                            "reason": reason,
                            "error": format_admission_gate_error(decision),
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

            wall_seconds: float | None = None
            t0 = time.monotonic()
            try:
                result = run_pass(
                    lane_id=spec.lane_id,
                    task_ref=lane_task_ref,
                    backend=spec.backend,
                    model=spec.model or None,
                    reasoning_effort=spec.effort,
                    speed=spec.speed,
                    token_budget=spec.token_budget,
                    timeout_seconds=spec.timeout_seconds,
                    pass_id=pass_id,
                    reserved_slot_idx=reserved_idx,
                    reserved_admission=stock_decision,
                )
                wall_seconds = time.monotonic() - t0
                owned = True  # worker (or this thread) owns the fd until release below
            except Exception as exc:  # noqa: BLE001
                wall_seconds = time.monotonic() - t0
                return {
                    "lane_id": spec.lane_id,
                    "pass_id": pass_id,
                    "status": "refused",
                    "reason": "run_pass_error",
                    "error": str(exc),
                    "wall_seconds": wall_seconds,
                    "outcome": "run_pass_error",
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
                    "wall_seconds": wall_seconds,
                    "outcome": REASON_EMPTY_RESULT,
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
                        "wall_seconds": wall_seconds,
                        "outcome": error_kind or outcome,
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
                        "wall_seconds": wall_seconds,
                        "outcome": outcome or error_kind or "error",
                    }
                else:
                    # wait_seconds==0 default: non-terminal still_running is a
                    # valid dispatched status but excluded from timing metrics.
                    entry = {
                        "lane_id": spec.lane_id,
                        "pass_id": pass_id,
                        "status": "dispatched",
                        "result": result,
                        "wall_seconds": wall_seconds,
                        "outcome": outcome or "dispatched",
                    }
            return entry
        finally:
            # Release order: claim -> heavy -> pool_sem ([RES-20]).
            # run_pass is synchronous — release the conflict flock as soon as
            # the worker returns so a finished lane does not keep excluding its
            # neighbour (width=1 serial batches must still dispatch both).
            if claim_fd is not None and release_claim is not None:
                try:
                    release_claim(spec.lane_id, claim_fd)
                except OSError:
                    pass
                claim_fd = None
            if slot_fd is not None and reserved_idx is not None and owned:
                _release_heavy_slot(reserved_idx, slot_fd)
            if stock_decision is not None:
                stock_decision.release_stock_claim()
            # Only release if we acquired (timeout path returns before this try).
            if acquired:
                pool_sem.release()

    # Open-row audit hook BEFORE submission (api owns the write; not swallowed).
    if before_submit is not None:
        from datetime import datetime, timezone  # noqa: PLC0415

        before_submit(
            {
                "wave_id": wave_id,
                "members": [s.lane_id for s in specs],
                "requested_width": requested_width,
                "started_at": datetime.now(tz=timezone.utc).isoformat(),
            }
        )

    # Submit all remote members under the wave semaphore bound, in priority
    # start order (membership unchanged — all ready specs still join).
    # Manual pool (not `with`) so context-manager __exit__ shutdown(wait=True)
    # cannot re-hang the coordinator past the batch-aware join deadline.
    # Pace *between* submits (never before the first) so sandbox materialization
    # does not thundering-herd; pool still runs lanes concurrently.
    results: list[dict[str, Any]] = []
    sleep_fn: Callable[[float], None] = time.sleep if sleep is None else sleep
    stagger = dispatch_stagger_seconds()
    # Per-gap clamp is independent of the phase budget: derive an effective
    # interval so sum of sleeps across the wave stays within
    # _TOTAL_DISPATCH_STAGGER_BUDGET_SECONDS even when stagger is at the
    # per-gap ceiling (ceiling x (n-1) would otherwise grow with width).
    gaps = max(1, len(specs) - 1)
    effective = min(stagger, _TOTAL_DISPATCH_STAGGER_BUDGET_SECONDS / gaps)
    pool = ThreadPoolExecutor(max_workers=max(1, width))
    try:
        futures: dict[Any, LaneSpec] = {}
        for idx, s in enumerate(specs):
            if idx > 0 and effective > 0:
                sleep_fn(effective)
            # Fresh Context per submit: workers inherit the dispatcher's
            # ContextVars (e.g. handoff runtime config). Do not reuse one
            # Context across submits — a Context cannot be entered concurrently.
            futures[pool.submit(contextvars.copy_context().run, _run_one, s)] = s
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
    dispatched_pass_ids = [e["pass_id"] for e in results if e.get("status") == "dispatched" and e.get("pass_id")]
    join_payload: dict[str, Any] | None = None
    if await_passes is not None and wait_seconds > 0 and dispatched_pass_ids:
        try:
            join_payload = await_passes(
                dispatched_pass_ids,
                wait_seconds=wait_seconds,
                mode="all_complete",
                task_ref=primary_ref,
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

    # Many members can report the same VM capacity event. Fold them into one
    # host-wide ratchet update per wave.
    capacity_deferred = False
    for entry in deferred:
        result = entry.get("result") if isinstance(entry, dict) else None
        defer_reason = result.get("defer_reason") if isinstance(result, dict) else None
        if defer_reason in {"vm_lane_cap", "vm_memory_pressure"}:
            capacity_deferred = True
            try:
                learned_wave_cap.record_capacity_deferral(str(defer_reason))
            except learned_wave_cap.LockDeadlineExceeded as exc:
                logger.warning(
                    "lock_deadline_exceeded",
                    extra={
                        "event": "lock_deadline_exceeded",
                        "lock_path": str(exc.lock_path),
                        "boundary": "learned_wave_cap.record_capacity_deferral",
                    },
                )
            break

    # admitted_width = lanes that actually entered run_pass (not conflict/ready
    # deferred before the call). Count by presence of wall_seconds on entries.
    admitted_width = sum(
        1 for e in (*dispatched, *deferred, *refused) if isinstance(e, dict) and e.get("wall_seconds") is not None
    )
    if not capacity_deferred:
        try:
            learned_wave_cap.record_clean_wave(
                admitted_width=admitted_width,
                effective_cap=operating_cap,
                maximum_cap=min(SEED_WAVE_WIDTH, env_cap),
            )
        except learned_wave_cap.LockDeadlineExceeded as exc:
            logger.warning(
                "lock_deadline_exceeded",
                extra={
                    "event": "lock_deadline_exceeded",
                    "lock_path": str(exc.lock_path),
                    "boundary": "learned_wave_cap.record_clean_wave",
                },
            )

    return {
        "ok": True,
        "wave_id": wave_id,
        "dispatched": dispatched,
        "deferred": deferred,
        "refused": refused,
        "wave_max_width": width,
        "requested_width": requested_width,
        "admitted_width": admitted_width,
        "env_cap": env_cap,
        "effective_cap": operating_cap,
        "learned_cap": persisted_cap,
        "ready_frontier_width": ready_frontier_width,
        "manifest_errors": dict(manifest_errors),
        "clique_lower_bound": clique_lower_bound,
        "pass_ids": dispatched_pass_ids,
        "join": join_payload,
    }


def claim_gated_slot(
    workspace_root: Path,
    cost_class: str,
    *,
    lane_kind: str | None = None,
) -> tuple[int, int] | None:
    """Serialise-friendly helper: resolve admission then claim a heavy slot.

    Returns ``(idx, fd)`` on success, ``None`` when deferred/refused/full.
    Caller owns the fd and must ``_release_heavy_slot`` (or hand off).
    """
    if cost_class not in _GATED_COST_CLASSES:
        return None
    root = Path(workspace_root)
    with resolve_live_admission(root, cost_class, lane_kind=lane_kind) as decision:
        if decision.decision != "allow":
            return None
        return acquire_heavy_slot(locks_root(root), decision.derived_width)
