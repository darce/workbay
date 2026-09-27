"""Canonical effort vocabulary shared by dispatchers and Codex transports.

Concrete effort and selection mode are separate concepts. MAX is an explicit
Codex effort, not an alias for xhigh. Automatic scoring remains capped at xhigh
until an operator explicitly selects max (directly or through a manifest).
"""

CODEX_REASONING_EFFORTS = ("low", "medium", "high", "xhigh", "max")
CONCRETE_EFFORTS = frozenset(CODEX_REASONING_EFFORTS)
EFFORT_MODES = ("inherit", "auto")
WORKER_REASONING_EFFORT_CHOICES = (*EFFORT_MODES, *CODEX_REASONING_EFFORTS)
AUTO_EFFORT_LADDER = CODEX_REASONING_EFFORTS[:-1]
# Other backend profiles retain their existing capabilities.
STANDARD_WORKER_REASONING_EFFORT_CHOICES = (*EFFORT_MODES, *AUTO_EFFORT_LADDER)


def validate_reasoning_effort(value: object, *, allow_modes: bool = True) -> str | None:
    """Narrow an untrusted input to an exact protocol value without rewriting it."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("reasoning effort must be a string")
    if value != value.strip().lower():
        raise ValueError(
            f"effort {value!r} is not canonical; use lowercase with no surrounding whitespace"
        )
    allowed = (
        WORKER_REASONING_EFFORT_CHOICES if allow_modes else CODEX_REASONING_EFFORTS
    )
    if value not in allowed:
        raise ValueError(
            f"effort {value!r} not in {sorted(allowed)}; refusing to ship a substituted effort"
        )
    return value
