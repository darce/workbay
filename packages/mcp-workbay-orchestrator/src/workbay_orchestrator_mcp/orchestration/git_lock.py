"""Classify git mutation failures without changing lock-file state."""

from __future__ import annotations


GIT_INDEX_LOCK_HELD = "git_index_lock_held"


def classify_git_failure(stderr: str) -> str | None:
    """Return the typed outcome for a recognized held git lock.

    Git's exact prose varies by version.  The paired ``.lock`` / ``File
    exists`` tokens cover index and ref lock failures, while the process hint
    covers the other stable spelling emitted for an existing index lock.
    This is classification only: callers must never unlink the named lock.
    """
    text = stderr or ""
    if ".lock" in text and "File exists" in text:
        return GIT_INDEX_LOCK_HELD
    if "Another git process seems to be running" in text:
        return GIT_INDEX_LOCK_HELD
    return None
