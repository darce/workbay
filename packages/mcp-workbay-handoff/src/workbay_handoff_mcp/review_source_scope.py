"""Read-only Git proof for review applicability; never called under a write lock."""

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Mapping
from typing import Any

from .shared_write_context import WriteLockHeldAcrossBlockingWorkError, run_subprocess


class SourceProbe:
    """Bound and cache unique probes. Errors are unknown, never non-ancestry."""

    def __init__(self, workspace: str):
        self.workspace = workspace
        self.deadline = time.monotonic() + 15
        self.cache: dict[tuple[str, ...], Any] = {}

    def git(self, *args: str):
        if args not in self.cache:
            remaining = self.deadline - time.monotonic()
            if len(self.cache) >= 256 or remaining <= 0:
                return None
            try:
                self.cache[args] = run_subprocess(
                    ["git", "-C", self.workspace, *args],
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    timeout=min(10, remaining),
                    env={**os.environ, "LC_ALL": "C"},
                )
            except WriteLockHeldAcrossBlockingWorkError:
                raise
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
                self.cache[args] = None
        return self.cache[args]

    def commit(self, token: str) -> str | None:
        result = self.git("rev-parse", "--verify", "--end-of-options", f"{token}^{{commit}}")
        if result is None or result.returncode != 0:
            return None
        sha = result.stdout.strip()
        return sha if len(sha) in {40, 64} and all(c in "0123456789abcdef" for c in sha) else None

    def branch(self, name: str) -> str | None:
        # Exact local refs only: unknown review-stage aliases remain unknown.
        ref = name if name.startswith("refs/heads/") else f"refs/heads/{name}"
        result = self.git("show-ref", "--verify", "--hash", ref)
        if result is None or result.returncode != 0:
            return None
        return self.commit(result.stdout.strip())

    def ancestor(self, older: str, newer: str) -> bool | None:
        result = self.git("merge-base", "--is-ancestor", older, newer)
        if result is None or result.returncode not in (0, 1):
            return None
        return result.returncode == 0


def foreign_source_snapshots(
    probe: SourceProbe,
    revisions: tuple[str, str],
    actor: Mapping[str, Any] | None,
    rows: list[dict],
) -> dict[int, dict]:
    """Require consistent explicit context and independent canonical source refs.

    Reachable baseline/head findings, older/newer reviewed sources, same-lane
    findings and unknown metadata all retain the existing severity bar.
    """

    def immutable_commit(value: object) -> bool:
        return (
            isinstance(value, str) and 7 <= len(value) <= 64 and all(char in "0123456789abcdefABCDEF" for char in value)
        )

    if not actor or not all(actor.get(key) for key in ("branch", "commit_sha", "lane_id")):
        return {}
    if not immutable_commit(actor["commit_sha"]):
        return {}
    baseline, head = revisions
    target = probe.branch(str(actor["branch"]))
    if target != head or probe.commit(str(actor["commit_sha"])) != head:
        return {}
    # Main may have advanced after the reviewed baseline was pinned. Its
    # reachable findings remain real integration blockers, not foreign work.
    main = probe.branch("main")
    result = {}
    for row in rows:
        if row.get("origin") == "listed_open":
            continue
        branch, commit, lane = row.get("branch"), row.get("commit_sha"), row.get("lane_id")
        if not branch or not immutable_commit(commit) or not lane or lane == actor["lane_id"]:
            continue
        source_tip = probe.branch(str(branch))
        source = probe.commit(str(commit))
        if not source_tip or not source or source_tip == target:
            continue
        if main is None or probe.ancestor(source, main) is not False:
            continue
        if probe.ancestor(source, source_tip) is not True:
            continue
        # Both sides of the range matter, including a newer baseline endpoint.
        pairs = ((source, baseline), (source, head), (head, source), (source_tip, head), (head, source_tip))
        if all(probe.ancestor(a, b) is False for a, b in pairs):
            result[int(row["id"])] = row
    return result
