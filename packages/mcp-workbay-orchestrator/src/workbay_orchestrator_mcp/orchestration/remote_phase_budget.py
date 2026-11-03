"""Two distinct clocks for one remote dispatch (internal).

``run_offload_pass(timeout_seconds=T, turn_timeout_seconds=M)`` authorizes two
clocks, and they must stay distinct all the way to the VM [RES-02]:

* ``T`` is the whole lifecycle: setup, model, self-verify and harvest. The
  offload engine fixes it once as a monotonic deadline (``RemotePhaseBudget``).
* ``M`` bounds only the model process. It travels as ``AgentSpec.lane_timeout_s``
  and ``scripts/remote_agent.sh`` clamps the model subprocess to it.

Before this carrier existed, ``M`` became the adapter's transport timeout and
therefore the whole job: ``RemoteJobSpec.bound_seconds=M`` and ``--timeout``
``M - headroom``, so a 900/600 request ran a 585-second VM span with no time
left for verification.

Only relative seconds cross the host/VM boundary. They are measured from the
monotonic deadline at the moment of each spawn, so no absolute wall-clock epoch
is projected across the transport, and a redispatch recomputes the remainder
instead of restarting the outer window.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, replace
from typing import Any

PHASE_BUDGET_INVALID = "phase_budget_invalid"
PHASE_BUDGET_EXHAUSTED = "phase_budget_exhausted"

# Marker around the VM-side model-clock clamp in ``scripts/remote_agent.sh``.
# A transport script must contain it before the host may hand it a whole-body
# ``--timeout`` larger than the model clock: a script without it ignores
# ``AgentSpec.lane_timeout_s`` and would let the model fill the whole body.
MODEL_CLOCK_TRANSPORT_TOKEN = "BEGIN PHASE_BUDGET_MODEL_CLOCK"
MODEL_CLOCK_TRANSPORT_END_TOKEN = "END PHASE_BUDGET_MODEL_CLOCK"

# Carrier states: only ``complete`` admits a whole-body ``--timeout``; only a
# wholly ``absent`` carrier is the legacy transport. A partial advertisement is
# neither and must not be dispatched.
MODEL_CLOCK_CARRIER_COMPLETE = "complete"
MODEL_CLOCK_CARRIER_ABSENT = "absent"
MODEL_CLOCK_CARRIER_MALFORMED = "malformed"

# One indirection so tests can drive the clock without patching ``time`` globally.
_monotonic = time.monotonic


class PhaseBudgetError(ValueError):
    """Typed refusal: the phase budget is malformed or already spent."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


def model_clock_carrier_state(script_text: str) -> str:
    """Classify the selected transport's model-clock carrier from admitted bytes."""
    lines = [line.strip() for line in script_text.splitlines()]
    begins = [i for i, line in enumerate(lines) if line == f"# {MODEL_CLOCK_TRANSPORT_TOKEN}"]
    ends = [i for i, line in enumerate(lines) if line == f"# {MODEL_CLOCK_TRANSPORT_END_TOKEN}"]
    if len(begins) == len(ends) == 1 and begins[0] < ends[0]:
        return MODEL_CLOCK_CARRIER_COMPLETE
    if MODEL_CLOCK_TRANSPORT_TOKEN not in script_text and MODEL_CLOCK_TRANSPORT_END_TOKEN not in script_text:
        return MODEL_CLOCK_CARRIER_ABSENT
    return MODEL_CLOCK_CARRIER_MALFORMED


def remote_bound_seconds(local_seconds: int, headroom_seconds: int) -> int:
    """Return the VM ``--timeout`` that sits under the host transport bound.

    The remote agent must self-terminate (and its artifacts still fetch) before
    the host gives up, so the remote bound reserves the backend's post-turn
    fetch headroom. At or below ``3 * headroom`` there is no room for that
    reservation and remote == local is accepted explicitly.
    """
    if headroom_seconds > 0 and local_seconds > headroom_seconds * 3:
        return local_seconds - headroom_seconds
    return local_seconds


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


@dataclass(frozen=True)
class RemotePhaseBudget:
    """Caller-authorized total lifecycle clock, fixed once at pass start."""

    total_seconds: int
    deadline_monotonic: float

    @classmethod
    def start(cls, total_seconds: float, *, deadline_monotonic: float) -> RemotePhaseBudget:
        """Carry the engine's pass deadline; ``total_seconds`` is what the caller authorized."""
        return cls(total_seconds=int(math.ceil(float(total_seconds))), deadline_monotonic=float(deadline_monotonic))

    def validate(self, *, now: float | None = None) -> None:
        if not _positive_int(self.total_seconds):
            raise PhaseBudgetError(PHASE_BUDGET_INVALID, f"total_seconds={self.total_seconds!r} is not a positive int")
        deadline = self.deadline_monotonic
        if isinstance(deadline, bool) or not isinstance(deadline, (int, float)) or not math.isfinite(deadline):
            raise PhaseBudgetError(PHASE_BUDGET_INVALID, f"deadline_monotonic={deadline!r} is not finite")
        current = _monotonic() if now is None else now
        # A deadline further out than the caller authorized is forged: no stage
        # may grant itself more than the whole lifecycle.
        if deadline - current > self.total_seconds + 1:
            raise PhaseBudgetError(
                PHASE_BUDGET_INVALID,
                f"remaining={deadline - current:.1f}s exceeds authorized total_seconds={self.total_seconds}",
            )

    def remaining_seconds(self, *, now: float | None = None) -> int:
        current = _monotonic() if now is None else now
        return max(0, int(math.floor(self.deadline_monotonic - current)))


@dataclass(frozen=True)
class DispatchClocks:
    """Clocks for one spawn, all derived from the same monotonic reading."""

    total_seconds: int
    remaining_seconds: int
    transport_seconds: int
    remote_seconds: int
    model_seconds: int

    def receipt(self) -> dict[str, Any]:
        return {
            "status": "applied",
            "requested_total_seconds": self.total_seconds,
            "remaining_total_seconds": self.remaining_seconds,
            "effective_transport_seconds": self.transport_seconds,
            "effective_remote_seconds": self.remote_seconds,
            "requested_model_seconds": self.model_seconds,
            "effective_model_seconds": min(self.model_seconds, self.remote_seconds),
        }


def plan_dispatch_clocks(
    budget: Any,
    *,
    model_seconds: int,
    headroom_seconds: int,
    now: float | None = None,
) -> DispatchClocks:
    """Derive one spawn's transport/remote/model clocks from the total deadline.

    Raises :class:`PhaseBudgetError` (``phase_budget_invalid`` or
    ``phase_budget_exhausted``) instead of returning a zero or unbounded clock
    [AGT-10].
    """
    if not isinstance(budget, RemotePhaseBudget):
        raise PhaseBudgetError(PHASE_BUDGET_INVALID, f"carrier is {type(budget).__name__}, not RemotePhaseBudget")
    current = _monotonic() if now is None else now
    budget.validate(now=current)
    if not _positive_int(model_seconds):
        raise PhaseBudgetError(PHASE_BUDGET_INVALID, f"model_seconds={model_seconds!r} is not a positive int")
    if model_seconds > budget.total_seconds:
        raise PhaseBudgetError(
            PHASE_BUDGET_INVALID,
            f"model_seconds={model_seconds} exceeds authorized total_seconds={budget.total_seconds}",
        )
    remaining = budget.remaining_seconds(now=current)
    if remaining < 1:
        raise PhaseBudgetError(
            PHASE_BUDGET_EXHAUSTED,
            f"total lifecycle deadline spent (total_seconds={budget.total_seconds}, remaining={remaining})",
        )
    return DispatchClocks(
        total_seconds=budget.total_seconds,
        remaining_seconds=remaining,
        transport_seconds=remaining,
        remote_seconds=remote_bound_seconds(remaining, int(headroom_seconds)),
        model_seconds=int(model_seconds),
    )


def plan_fallback_clocks(
    budget: Any,
    *,
    model_seconds: int,
    headroom_seconds: int,
    now: float | None = None,
) -> DispatchClocks:
    """Clocks for a legacy transport that runs the model clock as its whole body.

    The legacy body is still part of the caller's one lifecycle: it is the
    smaller of the model clock and the ORIGINAL remainder, and a spent deadline
    is the same typed refusal as for a carrier-bearing spawn. The fallback never
    sets the deadline aside or grants a fresh model allowance [RES-02].
    """
    clocks = plan_dispatch_clocks(budget, model_seconds=model_seconds, headroom_seconds=headroom_seconds, now=now)
    body = min(clocks.model_seconds, clocks.remaining_seconds)
    return replace(clocks, transport_seconds=body, remote_seconds=remote_bound_seconds(body, int(headroom_seconds)))
