"""Execute stop-reason membership registry (internal / E8R2).

STOP_REASON_REGISTRY is the system of record for the three derived membership
views — checkpoint eligibility, salvage eligibility, and remote admissibility —
and the flags that drive those views. It does not claim ownership of every
production control-flow comparison or writer of a stop-reason string; some sites
still hard-code vocabulary literals outside this module.

The module-level frozensets STOP_REASONS_CHECKPOINT, SALVAGE_STOP_REASONS, and
REMOTE_ADMISSIBLE_STOP_REASONS are derived from that registry at import time.

Package consumers (offload_pass checkpoint/salvage bindings, worker_daemon remote
allowlist) import those derived bindings. Import them via the package path
``workbay_orchestrator_mcp.orchestration.stop_reasons`` so bindings share one
module object. A bare ``from stop_reasons import ...`` after placing the
orchestration directory on ``sys.path`` dual-loads this file under a second
sys.modules key and yields value-equal but non-identical frozensets.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StopReasonSpec:
    name: str
    checkpoint_eligible: bool  # dirty work may be committed as a wip checkpoint
    salvage_eligible: bool  # a RED self-verify may still be salvage-committed
    remote_admissible: bool  # a backend raw_payload may name this reason


# Host-owned sentinel for rejected remote reasons. Never accepted FROM a payload.
UNKNOWN_REMOTE_STOP = "unknown_remote_stop"

# System of record for the closed vocabulary. Downstream frozensets below are derived.
STOP_REASON_REGISTRY: tuple[StopReasonSpec, ...] = (
    StopReasonSpec(
        name="max_turns",
        checkpoint_eligible=True,
        salvage_eligible=False,
        remote_admissible=False,
    ),
    StopReasonSpec(
        name="agent_exit_with_work",
        checkpoint_eligible=True,
        salvage_eligible=True,
        remote_admissible=True,
    ),
    StopReasonSpec(
        name="wall_clock_expiry",
        checkpoint_eligible=True,
        salvage_eligible=False,
        remote_admissible=True,
    ),
    StopReasonSpec(
        name=UNKNOWN_REMOTE_STOP,
        checkpoint_eligible=True,
        salvage_eligible=False,
        remote_admissible=False,
    ),
)


def checkpoint_stop_reasons(
    registry: tuple[StopReasonSpec, ...] = STOP_REASON_REGISTRY,
) -> frozenset[str]:
    return frozenset(s.name for s in registry if s.checkpoint_eligible)


def salvage_stop_reasons(
    registry: tuple[StopReasonSpec, ...] = STOP_REASON_REGISTRY,
) -> frozenset[str]:
    return frozenset(s.name for s in registry if s.salvage_eligible)


def remote_admissible_stop_reasons(
    registry: tuple[StopReasonSpec, ...] = STOP_REASON_REGISTRY,
) -> frozenset[str]:
    return frozenset(s.name for s in registry if s.remote_admissible)


# Module-level derived views (import-time bindings for consumers).
STOP_REASONS_CHECKPOINT = checkpoint_stop_reasons()
SALVAGE_STOP_REASONS = salvage_stop_reasons()
REMOTE_ADMISSIBLE_STOP_REASONS = remote_admissible_stop_reasons()
