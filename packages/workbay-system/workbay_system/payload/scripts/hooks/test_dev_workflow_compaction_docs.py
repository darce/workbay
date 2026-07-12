"""internal — development-workflow.md mentions the internal
session-compaction surfaces.

Operators reach for ``development-workflow.md`` when a hook fires or
they need to know how to disable / throttle the compaction. This test
locks in that the three env vars, the bootstrap scope flag, and the
manual ``make compact-now`` target each appear in that file so the
operator-facing reference does not drift away from the implementation.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

DOC_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "workbay"
    / "rules"
    / "development-workflow.md"
)
CONTRACT_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "workbay"
    / "contracts"
    / "harness-protocol.yaml"
)
ROOT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[6]
    / "docs"
    / "workbay"
    / "contracts"
    / "harness-protocol.yaml"
)


@pytest.fixture(scope="module")
def doc_text() -> str:
    return DOC_PATH.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "contract_path",
    [CONTRACT_PATH, ROOT_CONTRACT_PATH],
    ids=["payload", "root"],
)
def test_harness_protocol_names_workbay_disable_first(contract_path: Path) -> None:
    compaction_block = contract_path.read_text(encoding="utf-8").split("compaction:", 1)[-1]
    disable_section = compaction_block.split("threshold_tokens:", 1)[0]
    assert "WORKBAY_HANDOFF_COMPACTION_DISABLED" in disable_section
    assert not disable_section.strip().startswith("AGENT_HANDOFF_COMPACTION_DISABLED")
    agent_idx = disable_section.find("AGENT_HANDOFF_COMPACTION_DISABLED")
    workbay_idx = disable_section.find("WORKBAY_HANDOFF_COMPACTION_DISABLED")
    if agent_idx >= 0:
        assert workbay_idx < agent_idx, (
            "AGENT_HANDOFF_COMPACTION_DISABLED must not precede the canonical "
            "WORKBAY_HANDOFF_COMPACTION_DISABLED in disable precedence prose"
        )


def test_doc_has_session_compaction_section(doc_text: str) -> None:
    assert "## Session Compaction" in doc_text, (
        "development-workflow.md must include a top-level "
        "'Session Compaction' section per internal."
    )


@pytest.mark.parametrize(
    "marker",
    [
        # Canonical WORKBAY_HANDOFF_COMPACTION_* names — the ONLY names the
        # resolver reads (compaction.py _COMPACTION_ENV_FIELDS; no fallback).
        "WORKBAY_HANDOFF_COMPACTION_DISABLED",
        "WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TURNS",
        "WORKBAY_HANDOFF_COMPACTION_MIN_NEW_TOKENS",
        "WORKBAY_HANDOFF_COMPACTION_THRESHOLD_TOKENS",
        "WORKBAY_HANDOFF_COMPACTION_THRESHOLD_CHARS",
        "WORKBAY_HANDOFF_ACTIVE_TASK",
        "threshold_tokens",
        # Historical names must remain documented (flagged as not-read) so
        # operators with stale exports discover the rename from one page.
        "AGENT_HANDOFF_COMPACTION_DISABLED",
        "AGENT_HANDOFF_COMPACTION_MIN_NEW_TURNS",
        "AGENT_HANDOFF_COMPACTION_MIN_NEW_TOKENS",
        "WORKBAY_COMPACTION_DISABLED",
        "WORKBAY_COMPACTION_MIN_NEW_TURNS",
        "WORKBAY_COMPACTION_MIN_NEW_TOKENS",
        "deprecated",
        "make compact-now",
        "--install-claude-stop-hook-local",
        "capture-agent-errors",
        "managed by default",
        "compact-session.py",
    ],
)
def test_doc_mentions_compaction_surfaces(doc_text: str, marker: str) -> None:
    assert marker in doc_text, (
        f"development-workflow.md must mention {marker!r} in the "
        "Session Compaction section (internal + "
        "BR-internal)."
    )


# ---------------------------------------------------------------------------
# internal — the re-injection section ("enabled vs wired vs
# reinjected" triad) is the operator-facing reference for the SessionStart
# hook; lock its surfaces so the prose cannot drift from the implementation.
# ---------------------------------------------------------------------------


def test_doc_has_reinjection_triad_section(doc_text: str) -> None:
    assert "### Enabled vs wired vs reinjected" in doc_text, (
        "development-workflow.md must keep the 'Enabled vs wired vs "
        "reinjected' re-injection section (internal)."
    )


@pytest.mark.parametrize(
    "removed_flag",
    [
        "--install-claude-stop-hook",
        "--install-claude-reinject-hook",
        "--install-claude-error-hook",
        "--install-claude-error-hook-local",
    ],
)
def test_doc_does_not_mention_removed_claude_hook_flags(
    doc_text: str, removed_flag: str
) -> None:
    """Shared / redundant Claude hook install flags were removed in implementation note."""
    assert not re.search(re.escape(removed_flag) + r"(?!-local)", doc_text), (
        f"development-workflow.md must not mention removed flag {removed_flag!r}"
    )


@pytest.mark.parametrize(
    "marker",
    [
        "--install-claude-reinject-hook-local",
        "WORKBAY_REINJECT_SOURCES",
        "WORKBAY_REINJECT_BUDGET_CHARS",
        "`1500`",
        "`compact,resume`",
        "reinject-context",
        "reinject skipped:",
        "latest_compaction_id",
    ],
)
def test_doc_mentions_reinjection_surfaces(doc_text: str, marker: str) -> None:
    assert marker in doc_text, (
        f"development-workflow.md must mention {marker!r} in the "
        "re-injection section (internal)."
    )
