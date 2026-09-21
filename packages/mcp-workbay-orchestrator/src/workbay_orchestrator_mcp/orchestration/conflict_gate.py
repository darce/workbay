"""Cross-process conflict exclusion gate (internal).

Claim authority is a per-lane flock file under
``<state_dir>/conflict_claims/<task_ref>/<lane_id>.lock``, modelled on
``host_resources.py`` heavy-slot discipline:

- open ``O_CREAT|O_RDWR``, then ``LOCK_EX|LOCK_NB``;
- hold the fd for the claimer's lifetime;
- release = pop process-local registry then ``close(fd)`` (pop-before-close);
- NEVER unlink claim files (POSIX flock is per-inode; unlink dual-admits).

The in-process ``lane_id → fd`` dict is a fast path only (idempotent self
re-claim); the flock is the cross-process authority. [CON-13] is cited for
lock-own-before-probe acquisition order only. [RES-10] fencing; [RES-20]
every-path release on the start-failure bracket (daemon consumer).
"""

from __future__ import annotations

import fcntl
import os
import re
import subprocess
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, NotRequired, TypedDict

__all__ = [
    "check_lane_base_freshness",
    "BaseFreshness",
    "observe_integration_base",
    "claim_path",
    "load_conflict_graph",
    "release_claim",
    "try_claim",
]

# Process-local fast path only — flock is the authority (host_resources trap:
# a fresh LOCK_NB open fails against our own held fd, so step (0) consults
# this registry before opening).
# Keyed by (task_ref, lane_id): a long-lived process handling multiple
# task_refs must not hand task B the fd registered for task A [RES-10].
_CLAIM_REGISTRY: dict[tuple[str, str], int] = {}

_FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")

# Deterministic pair-race test seam (row 14b): when set to a directory path,
# try_claim signals ready after locking own file and waits for a "go" file
# before probing neighbours.
_CLAIM_BARRIER_ENV = "WORKBAY_TEST_CONFLICT_CLAIM_BARRIER"


def claim_path(state_dir: Path | str, task_ref: str, lane_id: str) -> Path:
    """Return the durable claim lock path for *lane_id* under *task_ref*."""
    return Path(state_dir) / "conflict_claims" / task_ref / f"{lane_id}.lock"


def load_conflict_graph(manifest: dict[str, Any]) -> dict[str, set[str]]:
    """Build adjacency ``lane_id → neighbours`` from persisted ``conflict_edges``.

    Edges are undirected sorted pairs. Unknown/malformed entries are skipped
    (structural validation is ``validate_manifest``'s job before dispatch).
    """
    adj: dict[str, set[str]] = {}
    raw = manifest.get("conflict_edges")
    if not isinstance(raw, list):
        return adj
    for entry in raw:
        if not isinstance(entry, (list, tuple)) or len(entry) != 2:
            continue
        a, b = entry[0], entry[1]
        if not isinstance(a, str) or not isinstance(b, str):
            continue
        a, b = a.strip(), b.strip()
        if not a or not b or a == b:
            continue
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)
    return adj


def _barrier_after_own_lock() -> None:
    """Env-gated rendezvous between step (1) and step (2) for row-14b tests."""
    raw = os.environ.get(_CLAIM_BARRIER_ENV)
    if not raw or not str(raw).strip():
        return
    barrier = Path(str(raw).strip())
    try:
        barrier.mkdir(parents=True, exist_ok=True)
        ready = barrier / f"ready.{os.getpid()}"
        ready.touch()
        go = barrier / "go"
        deadline = time.monotonic() + 30.0
        while not go.exists():
            if time.monotonic() >= deadline:
                break
            time.sleep(0.005)
    except OSError:
        return


def try_claim(
    task_ref: str,
    lane_id: str,
    neighbours: set[str] | list[str] | frozenset[str],
    active_probe: Callable[[str], bool],
    state_dir: Path | str,
) -> int | None:
    """Try to claim *lane_id* against its conflict *neighbours*.

    Steps ([CON-13] acquisition order — lock own before probe):
      (0) if this process already holds *(task_ref, lane_id)*'s fd, return it
          (idempotent; per-task keying prevents cross-task dual-admit);
      (1) open + ``LOCK_EX|LOCK_NB`` own claim file — fail ⇒ refuse;
      (2) while holding own, probe each neighbour (open+LOCK_NB; unlock+close on
          success — probe never retains); locked neighbour ⇒ release own + refuse;
      (3) ``active_probe(neighbour)`` True ⇒ release own + refuse;
      (4) else return own fd (registered in the process-local dict).
    """
    # (0) idempotent re-claim — registry consult before any open.
    registry_key = (str(task_ref), str(lane_id))
    held = _CLAIM_REGISTRY.get(registry_key)
    if held is not None:
        return held

    neighbour_set = {str(n) for n in neighbours if str(n) and str(n) != lane_id}
    path = claim_path(state_dir, task_ref, lane_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    # (1) lock OWN claim file first.
    fd = os.open(str(path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None

    # Row-14b seam: after own lock, before neighbour probes.
    _barrier_after_own_lock()

    # (2) probe each neighbour while holding own.
    for neighbour in sorted(neighbour_set):
        npath = claim_path(state_dir, task_ref, neighbour)
        npath.parent.mkdir(parents=True, exist_ok=True)
        nfd = os.open(str(npath), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            # Probe with a SHARED lock (finding 7543): two lanes concurrently
            # probing the same UNCLAIMED neighbour must not exclude each other
            # (an EX probe momentarily EX-locks it → spurious mutual refuse +
            # needless skip). LOCK_SH probers coexist, while the owner's own
            # LOCK_EX (step 1) still blocks any SH probe — so a genuinely
            # claimed neighbour is still detected. Dual-admit-impossibility is
            # guaranteed by each lane holding its OWN LOCK_EX, not by the probe.
            fcntl.flock(nfd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError:
            # Neighbour held (owner's LOCK_EX) — release own and refuse.
            os.close(nfd)
            release_claim(lane_id, fd)
            return None
        # Probe success: immediately unlock + close (never retain).
        try:
            fcntl.flock(nfd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(nfd)

    # (3) secondary liveness: worker subprocess that outlived a crashed claimer.
    for neighbour in sorted(neighbour_set):
        try:
            if active_probe(neighbour):
                release_claim(lane_id, fd)
                return None
        except Exception:  # noqa: BLE001 — probe faults must not brick dispatch
            # Fail open on probe errors: flock is the authority; a broken
            # manage_worker status must not permanently exclude neighbours.
            continue

    # (4) admit — register then return.
    _CLAIM_REGISTRY[registry_key] = fd
    return fd


def release_claim(lane_id: str, fd: int) -> None:
    """Release a claim: pop registry entry whose VALUE is *fd*, THEN close.

    Signature keeps ``(lane_id, fd)`` for frozen pins; the registry is keyed by
    ``(task_ref, lane_id)``, so we scan by fd value rather than by *lane_id*
    alone. Pop-before-close closes the dead-fd re-dispatch window (row 32 / r9):
    if we closed first, a concurrent re-claim could receive the same fd number
    and a later pop would de-register the live holder. NEVER unlink the claim
    file (POSIX flock is per-inode).
    """
    # Pop any registry entry holding this fd (scan by value, not lane_id key).
    for key, held_fd in list(_CLAIM_REGISTRY.items()):
        if held_fd == fd:
            _CLAIM_REGISTRY.pop(key, None)
            break
    try:
        os.close(fd)
    except OSError:
        pass


def _git_stdout(repo: Path, *args: str) -> str | None:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _git_is_ancestor(repo: Path, commit: str, tip: str) -> bool:
    result = subprocess.run(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", commit, tip],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _worktree_is_dirty(worktree_path: Path) -> bool:
    result = subprocess.run(
        ["git", "-C", str(worktree_path), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        # Unresolvable status ⇒ treat as dirty (fail-closed: refuse, never dispatch).
        return True
    return bool(result.stdout.strip())


def check_lane_base_freshness(
    lane_id: str,
    manifest: dict[str, Any],
    worktree_path: Path | str | None,
    landings: dict[str, str],
) -> str:
    """Return ``"ok"``, ``"reprovision"``, or ``"refuse"`` for claim-time freshness.

    ``landings`` maps conflict-neighbour lane_id → landing ``commit_sha`` (callers
    build this via ``latest_lane_landing``, omitting neighbours with no landing).

    Order contract (both surfaces): run this BEFORE ``try_claim``. The daemon is
    the only re-provision authority; the wave maps outcomes through without
    recreating worktrees.

    Fail-closed (row 19b): absent ``base_sha`` with no resolvable HEAD is STALE.
    """
    lanes = manifest.get("lanes") if isinstance(manifest, dict) else None
    lane = lanes.get(lane_id) if isinstance(lanes, dict) else None
    base_sha = None
    if isinstance(lane, dict):
        raw_base = lane.get("base_sha")
        if isinstance(raw_base, str) and raw_base.strip():
            base_sha = raw_base.strip()

    wt: Path | None = Path(worktree_path) if worktree_path else None
    effective_base: str | None = None
    if wt is not None and wt.exists():
        head = _git_stdout(wt, "rev-parse", "HEAD")
        if head and _FULL_SHA_RE.fullmatch(head):
            effective_base = head

    # Absent base_sha with no resolvable HEAD = STALE, fail-closed.
    if effective_base is None:
        if base_sha and _FULL_SHA_RE.fullmatch(base_sha):
            # Manifest cache exists but worktree HEAD unreadable — still stale.
            return "reprovision"
        return "reprovision"

    graph = load_conflict_graph(manifest if isinstance(manifest, dict) else {})
    neighbours = graph.get(lane_id, set())
    stale_neighbour: str | None = None
    for neighbour in sorted(neighbours):
        landing_sha = landings.get(neighbour)
        if not isinstance(landing_sha, str) or not _FULL_SHA_RE.fullmatch(landing_sha.strip()):
            continue
        landing_sha = landing_sha.strip()
        if wt is None or not _git_is_ancestor(wt, landing_sha, effective_base):
            stale_neighbour = neighbour
            break

    if stale_neighbour is None:
        return "ok"

    # Stale vs a neighbour landing: dirty worktree refuses; clean re-provisions.
    if wt is not None and _worktree_is_dirty(wt):
        return "refuse"
    return "reprovision"


class BaseFreshness(TypedDict):
    """Host observation; identities have already been validated by the caller."""

    schema_version: Literal[1]
    repo_id: str
    task_ref: str
    lane_id: str
    dispatch_id: str
    pass_id: str
    integration_ref: str | None
    integration_sha: str | None
    lane_head_sha: str | None
    merge_base_sha: str | None
    commits_behind_base: int | None
    commits_ahead: int | None
    status: Literal['current', 'behind', 'unknown']
    reason: str
    policy: Literal['observe', 'enforce_current']
    observed_at: str
    policy_revision: str | None
    snapshot_digest: str | None
    ref_candidates: NotRequired[list[str]]


def observe_integration_base(
    repo: Path | str,
    *,
    integration_ref: str | None,
    lane_head: str,
    repo_id: str,
    task_ref: str,
    lane_id: str,
    dispatch_id: str,
    pass_id: str,
    policy: Literal['observe', 'enforce_current'] = 'observe',
    policy_revision: str | None = None,
    snapshot_digest: str | None = None,
) -> BaseFreshness:
    """Observe one frozen host tip pair without changing repository state.

    Accept unambiguous literal refs (including HEAD) or full commit SHAs, not
    revision expressions. Bare names are expanded before resolution.
    Caller-owned identity validation remains at the existing
    dispatch boundary. No fetch or authority fallback is performed.
    """
    observation: BaseFreshness = {
        'schema_version': 1, 'repo_id': repo_id, 'task_ref': task_ref,
        'lane_id': lane_id, 'dispatch_id': dispatch_id, 'pass_id': pass_id,
        'integration_ref': integration_ref, 'integration_sha': None,
        'lane_head_sha': None, 'merge_base_sha': None,
        'commits_behind_base': None, 'commits_ahead': None,
        'status': 'unknown', 'reason': 'integration_base_missing',
        'policy': policy, 'observed_at': datetime.now(timezone.utc).isoformat(),
        'policy_revision': policy_revision, 'snapshot_digest': snapshot_digest,
    }
    if not integration_ref:
        return observation

    def git(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ['git', '-C', str(repo), *args], capture_output=True,
            text=True, check=False, timeout=10,
        )

    try:
        for ref in (integration_ref, lane_head):
            if (not isinstance(ref, str) or not ref or ref.startswith('-')
                    or any(char in ref for char in ('\x00', '\n', '\r'))):
                observation['reason'] = 'invalid_ref'
                return observation
        for ref in (integration_ref, lane_head):
            if ref != 'HEAD' and not _FULL_SHA_RE.fullmatch(ref):
                validation = git('check-ref-format', '--allow-onelevel', ref)
                if validation.returncode != 0:
                    observation['reason'] = (
                        'invalid_ref' if validation.returncode == 1 else 'git_command_failed'
                    )
                    return observation
        for ref, field, missing in (
            (integration_ref, 'integration_sha', 'integration_ref_missing'),
            (lane_head, 'lane_head_sha', 'lane_head_missing'),
        ):
            if ref != 'HEAD' and not _FULL_SHA_RE.fullmatch(ref) and not ref.startswith(
                ('refs/heads/', 'refs/remotes/', 'refs/tags/')
            ):
                # Use exact DWIM expansions, not for-each-ref's prefix matching:
                # a branch and tag must never silently select the older object.
                listing = git('for-each-ref', '--format=%(refname)')
                if listing.returncode != 0:
                    observation['reason'] = 'git_command_failed'
                    return observation
                expansions = {
                    f'refs/{ref}', f'refs/heads/{ref}', f'refs/tags/{ref}',
                    f'refs/remotes/{ref}', f'refs/remotes/{ref}/HEAD',
                }
                candidates = sorted(set(listing.stdout.splitlines()) & expansions)
                if len(candidates) > 1:
                    observation['reason'] = 'ambiguous_ref'
                    observation['ref_candidates'] = candidates
                    return observation
                if not candidates:
                    observation['reason'] = missing
                    return observation
                ref = candidates[0]
                if not ref.startswith(('refs/heads/', 'refs/remotes/', 'refs/tags/')):
                    observation['reason'] = 'invalid_ref'
                    return observation
            resolved = git('rev-parse', '--verify', '--end-of-options', f'{ref}^{{commit}}')
            if resolved.returncode != 0:
                observation['reason'] = missing
                return observation
            sha = resolved.stdout.strip()
            if not _FULL_SHA_RE.fullmatch(sha):
                observation['reason'] = 'git_command_failed'
                return observation
            observation[field] = sha
        integration_sha = observation['integration_sha']
        lane_sha = observation['lane_head_sha']
        assert integration_sha is not None and lane_sha is not None
        shallow = git('rev-parse', '--is-shallow-repository')
        if shallow.returncode != 0 or shallow.stdout.strip() not in ('true', 'false'):
            observation['reason'] = 'git_command_failed'
            return observation
        if shallow.stdout.strip() == 'true':
            observation['reason'] = 'shallow_history'
            return observation
        ancestor = git('merge-base', lane_sha, integration_sha)
        if ancestor.returncode != 0:
            observation['reason'] = (
                'unrelated_history' if ancestor.returncode == 1 else 'git_command_failed'
            )
            return observation
        if not _FULL_SHA_RE.fullmatch(ancestor.stdout.strip()):
            observation['reason'] = 'git_command_failed'
            return observation
        observation['merge_base_sha'] = ancestor.stdout.strip()
        counts = git('rev-list', '--left-right', '--count', f'{lane_sha}...{integration_sha}')
        if counts.returncode != 0 or not re.fullmatch(r'\d+\s+\d+', counts.stdout.strip()):
            observation['reason'] = 'git_command_failed'
            return observation
        ahead, behind = map(int, counts.stdout.split())
        observation['commits_ahead'] = ahead
        observation['commits_behind_base'] = behind
        observation['status'] = 'behind' if behind else 'current'
        observation['reason'] = 'observed_lag' if behind else 'observed_current'
    except ValueError:
        observation['reason'] = 'invalid_ref'
    except (OSError, subprocess.SubprocessError):
        observation['reason'] = 'git_command_failed'
    return observation
