"""Plan-checklist attestation markers (internal S3)."""

from __future__ import annotations

import re

ATTESTATION_DECISION_PREFIX = "attestation:"
CRITERION_RE = re.compile(r"^[a-z0-9_]+$")


def attestation_decision_id(criterion: str) -> str:
    """Stable decision id for criterion ``criterion``."""

    if not CRITERION_RE.match(criterion):
        raise ValueError(f"invalid attestation criterion: {criterion!r}")
    return f"{ATTESTATION_DECISION_PREFIX}{criterion}"


def is_attestation_decision(decision: str) -> bool:
    return decision.startswith(ATTESTATION_DECISION_PREFIX)


def extract_attestation_criterion(decision: str) -> str | None:
    if not is_attestation_decision(decision):
        return None
    criterion = decision[len(ATTESTATION_DECISION_PREFIX) :]
    return criterion if CRITERION_RE.match(criterion) else None


def attestation_session_for(criterion: str) -> str:
    """Fixed session for idempotent replay (task_ref + decision + session unique)."""

    return attestation_decision_id(criterion)
