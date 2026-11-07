"""Pure admission for the observed Codex CLI native stdin ceiling.

The ceiling is an empirical transport observation, not a model context limit
or a catalogue capability: Codex CLI 0.159.2 refused a stdin prompt above
1,048,576 Unicode code points (incident j6eb69df6cc763974238f). Secret scanning
keeps its own byte policy; fitting this ceiling does not waive it.

Callers measure the complete final stdin text (initial prompt, or the retry
prompt with its whole prior-attempt brief) and refuse before spend. Nothing
here clips or rewrites the candidate [AGT-10].
"""

from __future__ import annotations

import hashlib
from typing import Literal

OBSERVED_CODEX_MAX_CHARS = 1_048_576
OBSERVED_CODEX_EVIDENCE = (
    "Codex CLI 0.159.2 native incident j6eb69df6cc763974238f: observed 1048576 Unicode code-point stdin ceiling"
)
# Backends whose stdin is known to reach the observed Codex CLI ceiling. Other
# transports are not assigned an unrelated incident bound.
OBSERVED_CODEX_CEILING_BACKENDS = frozenset({"codex-remote"})


def evaluate_prompt_input_budget(
    final_prompt: str, *, backend_id: str, phase: Literal["initial", "retry"]
) -> dict[str, object] | None:
    """Return a typed refusal when the whole candidate exceeds the ceiling.

    ``final_prompt`` is the exact text staged as stdin. Schema argv travels
    separately and is never counted here. ``None`` means the candidate fits.
    """
    if backend_id not in OBSERVED_CODEX_CEILING_BACKENDS:
        return None
    actual_chars = len(final_prompt)
    if actual_chars <= OBSERVED_CODEX_MAX_CHARS:
        return None
    encoded = final_prompt.encode("utf-8")
    return {
        "phase": phase,
        "reason": "prompt_input_budget_exceeded",
        "input_unit": "unicode_code_points",
        "actual_chars": actual_chars,
        "actual_utf8_bytes": len(encoded),
        "source_sha256": hashlib.sha256(encoded).hexdigest(),
        "max_chars": OBSERVED_CODEX_MAX_CHARS,
        "evidence": OBSERVED_CODEX_EVIDENCE,
        "retryable": False,
    }
