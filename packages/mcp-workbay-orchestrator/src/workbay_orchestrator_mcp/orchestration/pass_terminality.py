"""Shared terminality rules for pass outcomes."""

from __future__ import annotations

from .offload_pass import PASS_OUTCOMES

NONTERMINAL_PASS_OUTCOMES = frozenset({"still_running", "remote_job_pending"})

assert NONTERMINAL_PASS_OUTCOMES <= PASS_OUTCOMES


def pass_outcome_is_terminal(outcome: object) -> bool:
    """Return whether an outcome can authorize work that requires a finished pass.

    ``still_running`` is not terminal because the pass is actively running.
    ``remote_job_pending`` is not terminal because the VM job was submitted and may still be running; a later pass with the same pass_id harvests it.
    """
    return isinstance(outcome, str) and bool(outcome) and outcome not in NONTERMINAL_PASS_OUTCOMES
