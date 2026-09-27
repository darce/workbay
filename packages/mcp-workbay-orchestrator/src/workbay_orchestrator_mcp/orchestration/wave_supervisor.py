"""Journal-replayed supervisor for one VM-resident wave.

The supervisor owns only the wave branch and its append-only journal ref. Lane
execution and gates come from the spec-driven default factory or an explicit
adapter override. The foreground CLI returns 0 only when every lane integrated,
2 for a refused or invalid wave, 3 when a terminal lane failed or parked, and 4
when the supervisor stopped before every lane reached a terminal state.
"""

from __future__ import annotations

import argparse
import contextvars
import importlib
import json
import os
import re
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from workbay_orchestrator_mcp.orchestration import wave_journal, wave_scheduler

_GIT_TIMEOUT_SECONDS = 15
_FULL_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_INTEGRATE_MESSAGE = "wave {wave}: integrate {lane_id}"
_MAX_SPEC_BYTES = 1024 * 1024
_DEFAULT_WIDTH = 4
_DEFAULT_ATTEMPT_BUDGET = 2
_MAX_WIDTH = 16
_MAX_ATTEMPT_BUDGET = 5
_EXIT_REFUSED = 2
_EXIT_TERMINAL_FAILURE = 3
_EXIT_INCOMPLETE = 4
_ADAPTER_CONTEXT: contextvars.ContextVar[tuple[Path, dict[str, Any]] | None] = contextvars.ContextVar(
    "wave_supervisor_adapter_context", default=None
)

_REPLAY_REQUIRED_FIELDS: dict[str, dict[str, type]] = {
    "lane.submitted": {"lane_id": str, "attempt": int, "job": dict},
    "lane.collected": {
        "lane_id": str,
        "attempt": int,
        "ok": bool,
        "branch": str,
        "tip": str,
        "detail": str,
    },
    "lane.gated": {"lane_id": str, "ok": bool, "reason": str, "tip": str},
    "lane.integrated": {"lane_id": str, "tip": str, "wave_tip": str},
    "lane.failed": {"lane_id": str, "reason": str},
    "lane.parked": {"lane_id": str, "reason": str},
    "wave.refused": {"reason": str},
}
_REPLAY_OPTIONAL_FIELDS: dict[str, dict[str, type]] = {
    "lane.gated": {"probe": int},
    "lane.failed": {"detail": str},
    "lane.parked": {"detail": str, "blocker": str, "attempt": int, "ok": bool, "tip": str},
    "wave.refused": {"problems": list},
}


class LaneExecutor(Protocol):
    """Injected, idempotent interface to remote lane jobs."""

    def submit(self, wave: str, lane_id: str, attempt: int) -> dict[str, Any]: ...

    def status(self, job: dict[str, Any]) -> str: ...

    def collect(self, job: dict[str, Any]) -> dict[str, Any]: ...


class Gate(Protocol):
    """Injected content gate for one collected lane tip."""

    def __call__(self, repo: str | os.PathLike[str], lane_id: str, tip: str) -> dict[str, Any]: ...


class Clock(Protocol):
    """The sleep operation used by :func:`run`."""

    def sleep(self, seconds: float) -> None: ...


@dataclass(frozen=True, slots=True)
class SupervisorEvent:
    """A journal event returned by ``step`` or a typed local refusal."""

    kind: str
    fields: dict[str, Any]
    event_id: str | None = None
    durable: bool = True


@dataclass(slots=True)
class SupervisorContext:
    """Dependencies and bounded-dispatch settings for a single wave."""

    repo: str | os.PathLike[str]
    spec: wave_scheduler.WaveSpec | Mapping[str, Any]
    executor: LaneExecutor
    gate: Gate
    width: int = 1
    attempt_budget: int = 1
    base_sha: str | None = None
    should_stop: Callable[[], bool] = field(default=lambda: False)


class _SupervisorError(RuntimeError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def replay(repo: str | os.PathLike[str], wave: str) -> dict[str, dict[str, Any]]:
    """Fold the wave journal into lane status and the data needed to resume.

    Each value contains a ``state`` suitable for ``wave_scheduler`` plus the
    last job/result and gate probe count. No supervisor state is written apart
    from journal events.
    """
    object_id_length = _object_id_length(repo)
    states: dict[str, dict[str, Any]] = {}
    for event in wave_journal.read(repo, wave):
        fields = event.fields
        _validate_replay_event(event, object_id_length)
        lane_id = fields.get("lane_id")
        if event.kind == "wave.refused":
            continue
        if lane_id is None:
            continue
        state = states.setdefault(lane_id, {"state": "pending", "attempt": 0, "gate_probes": 0})
        if event.kind == "lane.submitted":
            state.update(
                state="running",
                attempt=fields.get("attempt", state.get("attempt", 0)),
                job=fields.get("job"),
                branch=None,
                tip=None,
                detail=None,
            )
            state["gate_probes"] = 0
        elif event.kind == "lane.collected":
            state.update(
                state="collected",
                ok=fields.get("ok"),
                branch=fields.get("branch"),
                tip=fields.get("tip"),
                detail=fields.get("detail"),
            )
            state["gate_probes"] = 0
        elif event.kind == "lane.gated":
            if fields.get("ok") is True:
                state.update(state="gated", gate_reason=fields.get("reason"))
            else:
                state["state"] = "collected"
                state["gate_probes"] = max(state.get("gate_probes", 0), fields.get("probe", 0))
                state["gate_reason"] = fields.get("reason")
        elif event.kind == "lane.integrated":
            tip = fields["tip"]
            wave_tip = fields["wave_tip"]
            unproven_reason = _integration_unproven_reason(repo, wave, tip, wave_tip)
            if unproven_reason is None:
                state.update(state="integrated", tip=tip, wave_tip=wave_tip)
                state.pop("integration_unproven", None)
            else:
                state.update(state="gated", tip=tip, integration_unproven=unproven_reason)
        elif event.kind == "lane.failed":
            state.update(state="failed", reason=fields.get("reason"), detail=fields.get("detail"))
        elif event.kind == "lane.parked":
            state.update(
                state="parked",
                reason=fields.get("reason"),
                detail=fields.get("detail"),
                blocker=fields.get("blocker"),
            )
    return states


def step(ctx: SupervisorContext) -> list[SupervisorEvent]:
    """Advance one bounded wave round and return newly observed events."""
    wave, spec, base_sha, refusal = _prepare(ctx)
    if refusal is not None:
        if isinstance(wave, str) and _journalable_wave(wave):
            try:
                if any(event.kind == "wave.refused" for event in wave_journal.read(ctx.repo, wave)):
                    return []
            except wave_journal.JournalError:
                pass
            event = _emit(ctx, wave, "wave.refused", refusal)
            return [event]
        return [SupervisorEvent("wave.refused", refusal, durable=False)]

    assert wave is not None and spec is not None and base_sha is not None
    try:
        journal_events = wave_journal.read(ctx.repo, wave)
    except wave_journal.JournalError as exc:
        return [SupervisorEvent("wave.refused", {"reason": exc.reason}, durable=False)]
    if any(event.kind == "wave.refused" for event in journal_events):
        return []

    new_events: list[SupervisorEvent] = []
    states = replay(ctx.repo, wave)
    scheduler_states = {lane_id: states.get(lane_id, {}).get("state", "pending") for lane_id in spec.lanes}

    # Only running lanes own executor jobs. Collected and gated lanes hold no
    # executor slot, though frontier still treats them as conflict holders.
    running_lanes = sum(state.get("state") == "running" for state in states.values())
    available_width = ctx.width - running_lanes
    ready: list[str] = []
    if available_width > 0:
        try:
            ready = wave_scheduler.frontier(spec, scheduler_states, width=available_width)
        except (TypeError, ValueError):
            return [_emit(ctx, wave, "wave.refused", {"reason": "invalid_width"})]
    for lane_id in ready:
        if ctx.should_stop():
            return new_events
        attempt = int(states.get(lane_id, {}).get("attempt", 0)) + 1
        try:
            job = ctx.executor.submit(wave, lane_id, attempt)
        except Exception:
            # A submit may have committed remotely before the caller died. Let
            # it surface so a fresh process replays and retries the same key.
            raise
        if not isinstance(job, dict):
            new_events.append(_emit(ctx, wave, "lane.failed", {"lane_id": lane_id, "reason": "invalid_submit_result"}))
            states = replay(ctx.repo, wave)
            continue
        new_events.append(_emit(ctx, wave, "lane.submitted", {"lane_id": lane_id, "attempt": attempt, "job": job}))
        states = replay(ctx.repo, wave)

    for lane_id in spec.lanes:
        if ctx.should_stop():
            return new_events
        lane_state = states.get(lane_id, {})
        if lane_state.get("state") != "running":
            continue
        job = lane_state.get("job")
        if not isinstance(job, dict):
            new_events.append(_emit(ctx, wave, "lane.failed", {"lane_id": lane_id, "reason": "missing_job"}))
            states = replay(ctx.repo, wave)
            continue
        status = ctx.executor.status(job)
        if status not in {"queued", "running", "done", "lost"}:
            new_events.append(
                _emit(ctx, wave, "lane.failed", {"lane_id": lane_id, "reason": "invalid_executor_status"})
            )
            states = replay(ctx.repo, wave)
            continue
        if status in {"queued", "running"}:
            continue
        if status == "lost":
            attempt = int(lane_state.get("attempt", 0))
            if attempt >= ctx.attempt_budget:
                new_events.append(
                    _emit(
                        ctx,
                        wave,
                        "lane.parked",
                        {"lane_id": lane_id, "reason": "attempt_budget_exhausted", "attempt": attempt},
                    )
                )
            else:
                next_attempt = attempt + 1
                retry_job = ctx.executor.submit(wave, lane_id, next_attempt)
                if not isinstance(retry_job, dict):
                    new_events.append(
                        _emit(ctx, wave, "lane.failed", {"lane_id": lane_id, "reason": "invalid_submit_result"})
                    )
                else:
                    new_events.append(
                        _emit(
                            ctx,
                            wave,
                            "lane.submitted",
                            {"lane_id": lane_id, "attempt": next_attempt, "job": retry_job},
                        )
                    )
            states = replay(ctx.repo, wave)
            continue

        result = ctx.executor.collect(job)
        if not isinstance(result, dict) or type(result.get("ok")) is not bool:
            new_events.append(_emit(ctx, wave, "lane.parked", {"lane_id": lane_id, "reason": "invalid_collect_result"}))
            states = replay(ctx.repo, wave)
            continue
        branch = result.get("branch")
        tip = result.get("tip")
        detail = result.get("detail")
        if not isinstance(branch, str) or not isinstance(tip, str):
            new_events.append(_emit(ctx, wave, "lane.parked", {"lane_id": lane_id, "reason": "invalid_collect_result"}))
            states = replay(ctx.repo, wave)
            continue
        try:
            tip = _pin_commit(ctx.repo, tip)
        except _SupervisorError as exc:
            new_events.append(_emit(ctx, wave, "lane.parked", {"lane_id": lane_id, "reason": exc.reason}))
            states = replay(ctx.repo, wave)
            continue
        collected = {"lane_id": lane_id, "attempt": lane_state.get("attempt"), "ok": result["ok"]}
        collected["branch"] = branch
        collected["tip"] = tip
        collected["detail"] = detail if isinstance(detail, str) else ""
        new_events.append(_emit(ctx, wave, "lane.collected", collected))
        if ctx.should_stop():
            return new_events
        states = replay(ctx.repo, wave)

    states = replay(ctx.repo, wave)
    for lane_id in spec.lanes:
        if ctx.should_stop():
            return new_events
        lane_state = states.get(lane_id, {})
        if lane_state.get("state") != "collected":
            continue
        validation_event = _revalidate_collected(ctx, wave, base_sha, lane_id, lane_state)
        if validation_event is not None:
            new_events.append(validation_event)
            states = replay(ctx.repo, wave)
            continue
        probes = int(lane_state.get("gate_probes", 0))
        if probes >= 2:
            new_events.append(
                _emit(
                    ctx,
                    wave,
                    "lane.parked",
                    {
                        "lane_id": lane_id,
                        "reason": "gate_failed_after_reprobe",
                        "detail": lane_state.get("gate_reason", ""),
                    },
                )
            )
            states = replay(ctx.repo, wave)
            continue
        while probes < 2:
            gate_result = ctx.gate(ctx.repo, lane_id, lane_state["tip"])
            if not isinstance(gate_result, dict) or type(gate_result.get("ok")) is not bool:
                gate_result = {"ok": False, "reason": "invalid_gate_result"}
            reason = gate_result.get("reason", "")
            if not isinstance(reason, str):
                reason = "invalid_gate_result"
            if gate_result["ok"]:
                new_events.append(
                    _emit(
                        ctx,
                        wave,
                        "lane.gated",
                        {"lane_id": lane_id, "ok": True, "reason": reason, "tip": lane_state["tip"]},
                    )
                )
                break
            probes += 1
            new_events.append(
                _emit(
                    ctx,
                    wave,
                    "lane.gated",
                    {"lane_id": lane_id, "ok": False, "probe": probes, "reason": reason, "tip": lane_state["tip"]},
                )
            )
            if probes == 2:
                new_events.append(
                    _emit(
                        ctx,
                        wave,
                        "lane.parked",
                        {"lane_id": lane_id, "reason": "gate_failed_after_reprobe", "detail": reason},
                    )
                )
                if ctx.should_stop():
                    return new_events
                break
            states = replay(ctx.repo, wave)
            lane_state = states.get(lane_id, lane_state)
            if ctx.should_stop():
                return new_events
        states = replay(ctx.repo, wave)

    states = replay(ctx.repo, wave)
    integration_candidates = {
        lane_id for lane_id in spec.lanes if states.get(lane_id, {}).get("state") in {"gated", "integrated"}
    }
    for lane_id in wave_scheduler.integration_order(spec, integration_candidates):
        if ctx.should_stop():
            return new_events
        if states.get(lane_id, {}).get("state") != "gated":
            continue
        lane_state = states.get(lane_id, {})
        try:
            wave_tip = _integrate(ctx.repo, wave, base_sha, lane_id, lane_state["tip"])
        except _SupervisorError as exc:
            if exc.reason == "integration_conflict":
                new_events.append(
                    _emit(ctx, wave, "lane.parked", {"lane_id": lane_id, "reason": "integration_conflict"})
                )
            elif exc.reason == "wave_ref_cas_failed":
                # Another supervisor may have advanced the wave branch. Replay
                # and retry this gated lane on the next bounded round.
                continue
            else:
                new_events.append(_emit(ctx, wave, "lane.parked", {"lane_id": lane_id, "reason": exc.reason}))
            states = replay(ctx.repo, wave)
            continue
        new_events.append(
            _emit(ctx, wave, "lane.integrated", {"lane_id": lane_id, "tip": lane_state["tip"], "wave_tip": wave_tip})
        )
        states = replay(ctx.repo, wave)

    states = replay(ctx.repo, wave)
    scheduler_states = {lane_id: states.get(lane_id, {}).get("state", "pending") for lane_id in spec.lanes}
    for lane_id, blocker in wave_scheduler.blocked(spec, scheduler_states).items():
        if states.get(lane_id, {}).get("state", "pending") != "pending":
            continue
        if ctx.should_stop():
            return new_events
        new_events.append(
            _emit(
                ctx,
                wave,
                "lane.parked",
                {"lane_id": lane_id, "reason": "blocked_by_ancestor", "blocker": blocker},
            )
        )
        states = replay(ctx.repo, wave)
    return new_events


def run(ctx: SupervisorContext, *, max_rounds: int, poll_s: float, clock: Clock) -> list[SupervisorEvent]:
    """Run bounded rounds until every lane is terminal or the limit is hit."""
    events: list[SupervisorEvent] = []
    for round_index in range(max_rounds):
        round_events = step(ctx)
        events.extend(round_events)
        if any(event.kind == "wave.refused" for event in round_events):
            break
        if _terminal(ctx):
            break
        if ctx.should_stop():
            break
        if round_index + 1 < max_rounds:
            clock.sleep(poll_s)
    return events


def _prepare(
    ctx: SupervisorContext,
) -> tuple[str | None, wave_scheduler.WaveSpec | None, str | None, dict[str, Any] | None]:
    raw = ctx.spec
    raw_wave = (
        raw.wave if isinstance(raw, wave_scheduler.WaveSpec) else raw.get("wave") if isinstance(raw, Mapping) else None
    )
    if isinstance(raw, wave_scheduler.WaveSpec):
        spec = raw
        base_sha = ctx.base_sha
    else:
        try:
            spec = wave_scheduler.WaveSpec.from_dict(raw)
        except (TypeError, ValueError):
            return (raw_wave if isinstance(raw_wave, str) else None, None, None, {"reason": "invalid_wave_spec"})
        base_value = ctx.base_sha if ctx.base_sha is not None else raw.get("base_sha")
        base_sha = base_value if isinstance(base_value, str) else None

    wave = spec.wave
    try:
        wave_journal.validate_wave_id(wave)
    except wave_journal.JournalError as exc:
        return (wave, spec, base_sha, {"reason": exc.reason})
    problems = wave_scheduler.validate(spec)
    if problems:
        return (wave, spec, base_sha, {"reason": "invalid_wave_spec", "problems": problems})
    if type(ctx.width) is not int or ctx.width < 1:
        return (wave, spec, base_sha, {"reason": "invalid_width"})
    if type(ctx.attempt_budget) is not int or ctx.attempt_budget < 1:
        return (wave, spec, base_sha, {"reason": "invalid_attempt_budget"})
    valid_base = False
    if isinstance(base_sha, str) and _FULL_SHA_RE.fullmatch(base_sha):
        try:
            valid_base = _is_commit(ctx.repo, base_sha)
        except _SupervisorError as exc:
            return (wave, spec, base_sha, {"reason": exc.reason})
    if not valid_base:
        return (wave, spec, base_sha, {"reason": "invalid_base_sha"})
    return wave, spec, base_sha, None


def _emit(ctx: SupervisorContext, wave: str, kind: str, fields: dict[str, Any]) -> SupervisorEvent:
    existing = wave_journal.read(ctx.repo, wave)
    seq = len(existing) + 1
    parent = existing[-1].event_id if existing else None
    result = wave_journal.append(
        ctx.repo,
        wave,
        kind,
        fields,
        producer=f"supervisor:{wave}",
        producer_seq=seq,
        causal_parent=parent,
    )
    if result.get("ok") is not True:
        reason = result.get("reason")
        raise _SupervisorError(reason if isinstance(reason, str) else "journal_append_failed")
    event_id = result.get("event_id")
    if not isinstance(event_id, str):
        events = wave_journal.read(ctx.repo, wave)
        wanted_seq = seq
        event = next(
            (item for item in events if item.producer == f"supervisor:{wave}" and item.producer_seq == wanted_seq),
            None,
        )
        if event is None:
            return SupervisorEvent(kind, fields, durable=False)
        event_id = event.event_id
    return SupervisorEvent(kind, fields, event_id)


def _is_commit(repo: str | os.PathLike[str], commit: str) -> bool:
    result = _git(repo, "cat-file", "-e", f"{commit}^{{commit}}")
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    raise _SupervisorError("commit_check_failed")


def _pin_commit(repo: str | os.PathLike[str], tip: str) -> str:
    """Resolve a collected full SHA once and return its immutable commit id."""
    object_id_length = _object_id_length(repo)
    if not _is_full_object_id(tip, object_id_length):
        raise _SupervisorError("invalid_tip_sha")
    result = _git(repo, "rev-parse", "--verify", f"{tip}^{{commit}}")
    if result.returncode != 0:
        raise _SupervisorError("invalid_tip_commit")
    resolved = result.stdout.decode("ascii", errors="replace").strip()
    if not _is_full_object_id(resolved, object_id_length):
        raise _SupervisorError("invalid_tip_commit")
    return resolved


def _validate_replay_event(event: wave_journal.JournalEvent, object_id_length: int) -> None:
    """Refuse malformed fields on known events before folding their state."""
    if event.kind not in _REPLAY_REQUIRED_FIELDS:
        return
    fields = event.fields
    for field_name, expected_type in _REPLAY_REQUIRED_FIELDS[event.kind].items():
        value = fields.get(field_name)
        valid = type(value) is expected_type if expected_type in {int, bool} else isinstance(value, expected_type)
        if not valid:
            raise _SupervisorError(f"invalid_journal_event:{event.kind}.{field_name}")
    for field_name, expected_type in _REPLAY_OPTIONAL_FIELDS.get(event.kind, {}).items():
        if field_name not in fields:
            continue
        value = fields[field_name]
        valid = type(value) is expected_type if expected_type in {int, bool} else isinstance(value, expected_type)
        if not valid:
            raise _SupervisorError(f"invalid_journal_event:{event.kind}.{field_name}")

    lane_id = fields.get("lane_id")
    if lane_id is not None and not lane_id:
        raise _SupervisorError(f"invalid_journal_event:{event.kind}.lane_id")
    if event.kind == "lane.submitted" and fields["attempt"] < 1:
        raise _SupervisorError(f"invalid_journal_event:{event.kind}.attempt")
    if event.kind == "lane.collected":
        if fields["attempt"] < 1:
            raise _SupervisorError(f"invalid_journal_event:{event.kind}.attempt")
        _require_journal_sha(event, "tip", fields["tip"], object_id_length)
    elif event.kind == "lane.gated":
        _require_journal_sha(event, "tip", fields["tip"], object_id_length)
        if fields["ok"] is False:
            probe = fields.get("probe")
            if type(probe) is not int or probe < 1:
                raise _SupervisorError(f"invalid_journal_event:{event.kind}.probe")
    elif event.kind == "lane.integrated":
        _require_journal_sha(event, "tip", fields["tip"], object_id_length)
        if not _is_full_object_id(fields["wave_tip"], object_id_length):
            raise _SupervisorError(f"invalid_journal_event:{event.kind}.wave_tip")
    elif event.kind == "lane.parked":
        if "attempt" in fields and fields["attempt"] < 1:
            raise _SupervisorError(f"invalid_journal_event:{event.kind}.attempt")
        if "tip" in fields:
            _require_journal_sha(event, "tip", fields["tip"], object_id_length)


def _require_journal_sha(
    event: wave_journal.JournalEvent,
    field_name: str,
    value: str,
    object_id_length: int,
) -> None:
    if not _is_full_object_id(value, object_id_length):
        raise _SupervisorError(f"invalid_journal_event:{event.kind}.{field_name}")


def _is_full_object_id(value: str, object_id_length: int) -> bool:
    return len(value) == object_id_length and all("0" <= char <= "9" or "a" <= char <= "f" for char in value)


def _object_id_length(repo: str | os.PathLike[str]) -> int:
    result = _git(repo, "rev-parse", "--show-object-format")
    if result.returncode != 0:
        raise _SupervisorError("object_format_check_failed")
    object_format = result.stdout.decode("ascii", errors="replace").strip()
    if object_format == "sha1":
        return 40
    if object_format == "sha256":
        return 64
    raise _SupervisorError("unsupported_object_format")


def _is_commit_ahead(repo: str | os.PathLike[str], base: str, tip: str) -> bool:
    if tip == base:
        return False
    result = _git(repo, "merge-base", "--is-ancestor", base, tip)
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    raise _SupervisorError("content_ancestry_check_failed")


def _revalidate_collected(
    ctx: SupervisorContext,
    wave: str,
    base_sha: str,
    lane_id: str,
    lane_state: Mapping[str, Any],
) -> SupervisorEvent | None:
    """Apply the collection result/content rule before any gate can accept it."""
    ok = lane_state["ok"]
    tip = lane_state["tip"]
    try:
        ahead = _is_commit_ahead(ctx.repo, base_sha, tip)
    except _SupervisorError as exc:
        return _emit(ctx, wave, "lane.parked", {"lane_id": lane_id, "reason": exc.reason})
    if (ok and not ahead) or (not ok and ahead):
        return _emit(
            ctx,
            wave,
            "lane.parked",
            {"lane_id": lane_id, "reason": "ok_content_disagree", "ok": ok, "tip": tip},
        )
    if not ok:
        detail = lane_state.get("detail", "")
        return _emit(
            ctx,
            wave,
            "lane.failed",
            {"lane_id": lane_id, "reason": "lane_failed", "detail": detail},
        )
    return None


def _integration_unproven_reason(
    repo: str | os.PathLike[str],
    wave: str,
    tip: str,
    wave_tip: str,
) -> str | None:
    """Return why an integration event cannot be proven against current refs."""
    try:
        if not _is_commit_ancestor_or_equal(repo, tip, wave_tip):
            return "tip_not_in_recorded_wave"
        ref = f"refs/heads/wave/{wave}"
        symbolic = _git(repo, "symbolic-ref", "--quiet", ref)
        if symbolic.returncode == 0:
            return "wave_ref_symbolic"
        if symbolic.returncode != 1:
            raise _SupervisorError("wave_ref_read_failed")
        current_tip = _ref_tip(repo, ref)
        if current_tip is None:
            return "wave_ref_missing"
        if not _is_commit_ancestor_or_equal(repo, wave_tip, current_tip):
            return "recorded_wave_tip_not_current"
    except _SupervisorError as exc:
        return exc.reason
    return None


def _is_commit_ancestor_or_equal(repo: str | os.PathLike[str], ancestor: str, descendant: str) -> bool:
    result = _git(repo, "merge-base", "--is-ancestor", ancestor, descendant)
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    raise _SupervisorError("integration_proof_failed")


def _integrate(repo: str | os.PathLike[str], wave: str, base: str, lane_id: str, lane_tip: str) -> str:
    ref = f"refs/heads/wave/{wave}"
    _ensure_wave_ref(repo, ref, base)
    old_tip = _ref_tip(repo, ref)
    if old_tip is None:
        raise _SupervisorError("wave_ref_missing")
    ancestry = _git(repo, "merge-base", "--is-ancestor", base, old_tip)
    if ancestry.returncode == 1:
        raise _SupervisorError("wave_ref_base_mismatch")
    if ancestry.returncode != 0:
        raise _SupervisorError("wave_ref_read_failed")
    lane_in_wave = _git(repo, "merge-base", "--is-ancestor", lane_tip, old_tip)
    if lane_in_wave.returncode == 0:
        return old_tip
    if lane_in_wave.returncode != 1:
        raise _SupervisorError("wave_ref_read_failed")

    merge = _git(repo, "merge-tree", "--write-tree", old_tip, lane_tip)
    if merge.returncode == 1:
        raise _SupervisorError("integration_conflict")
    if merge.returncode != 0:
        raise _SupervisorError("merge_tree_failed")
    tree = merge.stdout.decode("ascii", errors="replace").splitlines()[0].strip()
    if not _FULL_SHA_RE.fullmatch(tree):
        raise _SupervisorError("merge_tree_invalid_result")

    message = _INTEGRATE_MESSAGE.format(wave=wave, lane_id=lane_id)
    current_tree = _git(repo, "rev-parse", f"{old_tip}^{{tree}}")
    current_subject = _git(repo, "show", "-s", "--format=%s", old_tip)
    if (
        current_tree.returncode == 0
        and current_tree.stdout.decode().strip() == tree
        and current_subject.stdout.decode().rstrip("\n") == message
    ):
        return old_tip

    commit = _git(repo, "commit-tree", tree, "-p", old_tip, "-p", lane_tip, "-m", message)
    if commit.returncode != 0:
        raise _SupervisorError("commit_tree_failed")
    new_tip = commit.stdout.decode("ascii", errors="replace").strip()
    update = _git(repo, "update-ref", ref, new_tip, old_tip)
    if update.returncode != 0:
        raise _SupervisorError("wave_ref_cas_failed")
    return new_tip


def _ensure_wave_ref(repo: str | os.PathLike[str], ref: str, base: str) -> None:
    symbolic = _git(repo, "symbolic-ref", "--quiet", ref)
    if symbolic.returncode == 0:
        raise _SupervisorError("wave_ref_symbolic")
    if symbolic.returncode != 1:
        raise _SupervisorError("wave_ref_read_failed")
    current = _ref_tip(repo, ref)
    if current is not None:
        return
    zero = "0" * len(base)
    created = _git(repo, "update-ref", ref, base, zero)
    if created.returncode == 0:
        return
    # A concurrent supervisor may have created the same branch from the same
    # base between the missing-ref probe and the create-if-absent update.
    if _ref_tip(repo, ref) is None:
        raise _SupervisorError("wave_ref_create_failed")


def _ref_tip(repo: str | os.PathLike[str], ref: str) -> str | None:
    result = _git(repo, "rev-parse", "--verify", "--quiet", ref)
    if result.returncode == 1:
        return None
    if result.returncode != 0:
        raise _SupervisorError("wave_ref_read_failed")
    return result.stdout.decode("ascii", errors="replace").strip()


def _git(repo: str | os.PathLike[str], *args: str) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            ["git", "-C", os.fspath(repo), *args],
            capture_output=True,
            timeout=_GIT_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise _SupervisorError("git_timeout") from exc
    except OSError as exc:
        raise _SupervisorError("git_unavailable") from exc


def _resolve_ref_commit(repo: str | os.PathLike[str], ref: str, object_id_length: int, reason: str) -> str:
    result = _git(repo, "rev-parse", "--verify", "--quiet", "--end-of-options", f"{ref}^{{commit}}")
    if result.returncode == 1:
        raise _SupervisorError(reason)
    if result.returncode != 0:
        raise _SupervisorError("wave_ref_resolution_failed")
    commit = result.stdout.decode("ascii", errors="replace").strip()
    if not _is_full_object_id(commit, object_id_length):
        raise _SupervisorError(reason)
    return commit


def _read_pinned_spec(repo: str | os.PathLike[str], inputs_commit: str) -> bytes:
    object_path = f"{inputs_commit}:wave.json"
    size_result = _git(repo, "cat-file", "-s", object_path)
    if size_result.returncode != 0:
        raise _SupervisorError("wave_inputs_missing")
    try:
        size = int(size_result.stdout.decode("ascii").strip())
    except (UnicodeDecodeError, ValueError) as exc:
        raise _SupervisorError("invalid_published_spec") from exc
    if size < 0 or size > _MAX_SPEC_BYTES:
        raise _SupervisorError("published_spec_too_large")
    data_result = _git(repo, "cat-file", "blob", object_path)
    if data_result.returncode != 0 or len(data_result.stdout) != size:
        raise _SupervisorError("invalid_published_spec")
    return data_result.stdout


def _load_published_spec(repo: str | os.PathLike[str], wave: str) -> tuple[dict[str, Any], str]:
    try:
        wave_journal.validate_wave_id(wave)
    except wave_journal.JournalError as exc:
        raise _SupervisorError("invalid_wave_id") from exc

    object_id_length = _object_id_length(repo)
    inputs_ref = f"refs/workbay/wave-inputs/{wave}"
    base_ref = f"refs/heads/wave-base/{wave}"
    inputs_commit = _resolve_ref_commit(repo, inputs_ref, object_id_length, "wave_inputs_missing")
    pinned_base = _resolve_ref_commit(repo, base_ref, object_id_length, "wave_base_missing")
    raw = _read_pinned_spec(repo, inputs_commit)
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _SupervisorError("invalid_published_spec") from exc
    if not isinstance(document, dict):
        raise _SupervisorError("invalid_published_spec")
    if document.get("wave") != wave:
        raise _SupervisorError("wave_mismatch")
    if "base_sha" in document and document.get("base_sha") != pinned_base:
        raise _SupervisorError("pinned_base_mismatch")
    try:
        problems = wave_scheduler.validate(document)
    except (TypeError, ValueError) as exc:
        raise _SupervisorError("invalid_wave_spec") from exc
    if problems:
        raise _SupervisorError("invalid_wave_spec")
    return document, pinned_base


def _load_spec_file(path: str | os.PathLike[str]) -> dict[str, Any]:
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(_MAX_SPEC_BYTES + 1)
    except OSError as exc:
        raise _SupervisorError("invalid_spec_file") from exc
    if len(raw) > _MAX_SPEC_BYTES:
        raise _SupervisorError("spec_file_too_large")
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _SupervisorError("invalid_spec_file") from exc
    if not isinstance(document, dict):
        raise _SupervisorError("invalid_wave_spec")
    return document


def _terminal(ctx: SupervisorContext) -> bool:
    wave, spec, _base, refusal = _prepare(ctx)
    if refusal is not None or wave is None or spec is None:
        return refusal is not None
    if any(event.kind == "wave.refused" for event in wave_journal.read(ctx.repo, wave)):
        return True
    states = replay(ctx.repo, wave)
    terminal = {"integrated", "failed", "parked"}
    return all(states.get(lane_id, {}).get("state") in terminal for lane_id in spec.lanes)


def _journalable_wave(wave: str) -> bool:
    try:
        wave_journal.validate_wave_id(wave)
        return True
    except wave_journal.JournalError:
        return False


def main(argv: list[str] | None = None) -> int:
    """Foreground CLI for explicit specs or immutable published wave inputs."""
    parser = argparse.ArgumentParser(prog="wave_supervisor")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run", help="advance a wave in the foreground")
    run_parser.add_argument("--repo", required=True)
    input_mode = run_parser.add_mutually_exclusive_group(required=True)
    input_mode.add_argument("--spec", help="load a compatibility spec file")
    input_mode.add_argument("--wave", help="load immutable published inputs for a wave")
    run_parser.add_argument("--width", type=int, default=_DEFAULT_WIDTH)
    run_parser.add_argument("--attempt-budget", type=int, default=_DEFAULT_ATTEMPT_BUDGET)
    run_parser.add_argument("--max-rounds", type=int, default=2**31 - 1)
    run_parser.add_argument("--poll-s", type=float, default=5.0)
    args = parser.parse_args(argv)
    if args.command != "run":
        return 2

    try:
        if not 1 <= args.width <= _MAX_WIDTH:
            raise _SupervisorError("invalid_width")
        if not 1 <= args.attempt_budget <= _MAX_ATTEMPT_BUDGET:
            raise _SupervisorError("invalid_attempt_budget")
        if args.wave is not None:
            spec, pinned_base = _load_published_spec(args.repo, args.wave)
        else:
            assert args.spec is not None
            spec = _load_spec_file(args.spec)
            pinned_base = None
    except _SupervisorError as exc:
        print(json.dumps({"ok": False, "reason": exc.reason}), file=sys.stderr)
        return 2

    repo = Path(args.repo).resolve()
    context_token = _ADAPTER_CONTEXT.set((repo, spec))
    try:
        executor, gate = _load_adapter()
    except (ImportError, AttributeError, TypeError, ValueError, _SupervisorError):
        print(json.dumps({"ok": False, "reason": "executor_unconfigured"}), file=sys.stderr)
        return 2
    finally:
        _ADAPTER_CONTEXT.reset(context_token)

    stop = False

    def request_stop(_signum: int, _frame: Any) -> None:
        nonlocal stop
        stop = True

    previous_handler = signal.signal(signal.SIGTERM, request_stop)
    context = SupervisorContext(
        repo=repo,
        spec=spec,
        executor=executor,
        gate=gate,
        width=args.width,
        attempt_budget=args.attempt_budget,
        base_sha=pinned_base,
        should_stop=lambda: stop,
    )
    try:
        events = run(context, max_rounds=args.max_rounds, poll_s=args.poll_s, clock=time)
    finally:
        signal.signal(signal.SIGTERM, previous_handler)

    for event in events:
        print(json.dumps({"kind": event.kind, "event_id": event.event_id, "fields": event.fields}, sort_keys=True))
    wave, parsed_spec, _base_sha, refusal = _prepare(context)
    if refusal is not None:
        return _EXIT_REFUSED
    assert wave is not None and parsed_spec is not None
    states = replay(context.repo, wave)
    if any(event.kind == "wave.refused" for event in wave_journal.read(context.repo, wave)):
        return _EXIT_REFUSED
    lane_states = [states.get(lane_id, {}).get("state") for lane_id in parsed_spec.lanes]
    if all(state == "integrated" for state in lane_states):
        return 0
    if all(state in {"integrated", "failed", "parked"} for state in lane_states):
        return _EXIT_TERMINAL_FAILURE
    return _EXIT_INCOMPLETE


def _load_adapter() -> tuple[LaneExecutor, Gate]:
    """Load the explicit zero-argument override or the spec-driven default factory."""
    target = os.environ.get("WORKBAY_WAVE_SUPERVISOR_ADAPTER", "")
    if target:
        if ":" not in target:
            raise ValueError("adapter factory is not configured")
        module_name, factory_name = target.split(":", 1)
        if not module_name or not factory_name:
            raise ValueError("adapter factory is not configured")
        factory = getattr(importlib.import_module(module_name), factory_name)
        value = factory()
    else:
        context = _ADAPTER_CONTEXT.get()
        if context is None:
            raise ValueError("adapter factory is not configured")
        repo, spec = context
        module = importlib.import_module(".wave_executor", package=__package__)
        factory = getattr(module, "create")
        value = factory(repo=repo, spec=spec)
    if isinstance(value, Mapping):
        executor, gate = value.get("executor"), value.get("gate")
    elif isinstance(value, tuple) and len(value) == 2:
        executor, gate = value
    else:
        raise TypeError("adapter factory must return executor and gate")
    if executor is None or not callable(gate):
        raise TypeError("adapter factory must return executor and gate")
    return executor, gate


if __name__ == "__main__":  # pragma: no cover - exercised as a module entry point
    raise SystemExit(main())
