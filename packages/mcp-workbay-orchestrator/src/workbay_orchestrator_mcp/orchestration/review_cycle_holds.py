"""Review-cycle holds computed from git: one ``holder_blocks`` for every caller.

A holder blocks an incoming source exactly when it has commits that are not on
the integration SHA and the paths those commits touch overlap the paths the
incoming source touches or owns::

    blocks(holder) := unlanded(tip, I) AND paths(merge-base(I, tip)..tip) ∩ incoming != {}

Dispatch (``prepare``/``shell_reserve``), the hold preview
(``subject_hold_diagnostics``/``retained_subject_resolution``) and the holder
diagnostics formatter all loop over :func:`holder_blocks`. Stored release,
stranded and close-release records are audit only; nothing here reads them.

The evaluation has two phases. :func:`take_snapshot` reads the lane registry
(read-only SQLite URI), the receipts and the ledger without taking any lock,
then runs every git probe -- before any ledger or handoff write lock is taken
(CON-21). :func:`holder_blocks` is then a pure function of that snapshot, and
:func:`evaluate_holds` returns its rows sorted by (task, lane, branch).

* ``unknown`` (a ref, object or the integration SHA cannot be read) holds with
  its reason named; it is never treated as clear (CAL-02).
* ``retry`` (registry/receipt/ledger contention while taking the snapshot)
  never holds; callers surface the typed retryable dispatch outcome.

Canon v0.26.2: GRPH-14 Record provenance as a walkable lineage graph; CON-21
no subprocess under a write lock; CAL-02 an unknown is never clear; SECD-05
complete mediation on every dispatch.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import re
import sqlite3
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

BLOCKS = Literal[True, False, "unknown", "retry"]
REASONS = (
    "self", "no_unlanded_commits", "holder_tip_absent", "discarded", "paths_disjoint", "paths_overlap",
    "holder_tip_unreadable", "integration_unreadable", "snapshot_contended", "incoming_scope_unknown",
)
INCOMING_SCOPE_UNKNOWN = "incoming_scope_unknown"
#: Round-accounting outcomes of :func:`retained_subject_for` (decision 48657).
LINEAGE_STATUSES = ("incoming", "retained", "ambiguous", "unknown")
SNAPSHOT_CONTENDED = "snapshot_contended"
DISCARD_DECISION_PREFIX = "lane_discarded_"
_HEX = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
_GIT_TIMEOUT_S = 30
_SNAPSHOT_ATTEMPTS = 3
_UNREADABLE = "unreadable"
_ABSENT = "absent"
_REPLACED = "replaced"
_EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
# Lane statuses whose agent may still write (gdnclose01 vocabulary).
_LIVE_LANE_STATUSES = frozenset({"planned", "active", "blocked", "review", "validated", "gated"})


class SnapshotContended(RuntimeError):
    """A registry, receipt or ledger read was contended; retry, never hold."""


def discard_decision_id(task_ref: str, lane_id: str, tip: str) -> str:
    """The gdnclose01 discard decision title for one exact tip."""
    return f"{DISCARD_DECISION_PREFIX}{task_ref}_{lane_id}_{tip}"


def _short(branch: object) -> str | None:
    if not isinstance(branch, str) or not branch.strip():
        return None
    return branch.strip().removeprefix("refs/heads/")


def _roots(entries: object) -> tuple[str, ...]:
    if not isinstance(entries, (list, tuple)):
        return ()
    roots = set()
    for item in entries:
        if isinstance(item, str):
            value = item.strip().strip("/")
            roots.add(value.removeprefix("./"))
    return tuple(sorted(roots))


# ---------------------------------------------------------------- snapshot rows


@dataclass(frozen=True)
class Incoming:
    branch: str
    task_ref: str | None = None
    lane_id: str | None = None
    subject_branch: str | None = None
    integration_branch: str = "main"
    tip_ref: str = "HEAD"
    owned_roots: tuple[str, ...] | None = None  # None: incoming scope unknown (holds incoming_scope_unknown)
    #: Authenticated incoming task-subject proof (accounting only, never a hold input):
    #: the base the task was cut from, its authorized roots, and its subject (``None``: unproven).
    proof_base: str | None = None
    proof_roots: tuple[str, ...] | None = None
    proof_subject: str | None = None


@dataclass(frozen=True)
class Holder:
    branch: str
    task_ref: str | None
    lane_id: str | None
    lane_status: str | None
    owned_roots: tuple[str, ...]
    lane_tips: tuple[str, ...]
    lane_identities: tuple[tuple[str, str], ...]
    dispatch_id: str | None
    receipt: str | None
    subject: str | None
    receipt_head: str | None
    in_flight: bool
    unreadable: bool = False  # a receipt naming this holder could not be decoded
    receipt_error: str | None = None

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.task_ref or "", self.lane_id or "", self.branch)


@dataclass(frozen=True)
class TipFacts:
    tip: str
    state: Literal["ok", "absent", "unreadable"]
    landed: bool | None = None
    paths: tuple[str, ...] = ()
    #: A reviewed first-parent ``Workbay-Tombstone`` pins this exact tip, and the
    #: branch still sits at it (decision 48672): the tip is discarded for holds.
    superseded: bool = False
    #: A landed tip that sits on the integration's first-parent mainline: the
    #: branch is zero commits ahead and never contributed work of its own
    #: (``None``: not probed; only asked for an in-flight holder).
    mainline: bool | None = None


@dataclass(frozen=True)
class HolderFacts:
    live: str | None  # sha, None (ref absent) or _UNREADABLE
    tips: tuple[TipFacts, ...]


@dataclass(frozen=True)
class Snapshot:
    incoming: Incoming
    integration_sha: str | None
    incoming_paths: tuple[str, ...]
    incoming_unreadable: bool
    holders: tuple[Holder, ...]
    facts: dict = field(default_factory=dict)  # Holder.key -> HolderFacts
    discarded: frozenset[str] = frozenset()
    contended: bool = False
    #: The incoming branch's own receipts (never holders), sorted by receipt name.
    own_receipts: tuple[dict, ...] = ()
    #: Round accounting by git lineage (decision 48657); never read by holder_blocks.
    lineage: "Lineage | None" = None


@dataclass(frozen=True)
class Lineage:
    """Which subject account the incoming source is charged to (accounting, never a hold).

    ``incoming``: no review-cycle receipt is in its lineage, charge the incoming subject.
    ``retained``: the nearest lineage receipts all name ``subject``; charge that account.
    ``ambiguous``: equally near receipts name several subjects (typed SubjectAmbiguous).
    ``unknown``: the incoming tip or an ancestry probe could not be read (typed refusal).
    """

    status: Literal["incoming", "retained", "ambiguous", "unknown"]
    subject: str | None = None
    distance: int | None = None
    receipts: tuple[dict, ...] = ()  # the nearest receipts, sorted by receipt name
    excluded: tuple[dict, ...] = ()  # lineage receipts an authenticated distinct task does not continue
    reason: str | None = None
    #: Task refs the registry binds to the incoming branch (accounting only):
    #: without a task proof a registered lane of a distinct task never charges a
    #: foreign subject (finding 31645).
    incoming_tasks: tuple[str, ...] = ()
    #: The task each nearest receipt was issued for (``None``: unattributable),
    #: aligned with ``receipts``.
    charged_tasks: tuple[str | None, ...] = ()


@dataclass(frozen=True)
class HolderBlock:
    blocks: BLOCKS
    reason: str
    overlap_paths: tuple[str, ...]
    holder_tip: str | None
    holder_lane: str | None
    holder_task: str | None
    holder_branch: str | None
    dispatch_id: str | None
    receipt: str | None
    subject: str | None
    integration_sha: str | None
    receipt_error: str | None = None

    @property
    def holds(self) -> bool:
        """Only a proven overlap or a named unknown holds; retry never does."""
        return self.blocks is True or self.blocks == "unknown"

    def as_row(self) -> dict:
        row = dict(
            receipt=self.receipt, dispatch_id=self.dispatch_id, subject=self.subject or self.holder_branch,
            source_branch=self.holder_branch, source_head=self.holder_tip, holder_tip=self.holder_tip,
            holder_lane=self.holder_lane, holder_task=self.holder_task, overlap_paths=list(self.overlap_paths),
            blocks=self.blocks, reason=self.reason, integration_sha=self.integration_sha,
            predicates_tried=["holder_blocks"],
        )
        if self.receipt_error is not None:
            row["receipt_error"] = self.receipt_error
        return row


# ---------------------------------------------------------------- git probes (no lock held)


def _git(repo: Path, *args: str) -> tuple[int, bytes] | None:
    """One bounded git probe; ``None`` is an unreadable answer, never an empty one.

    Probes read the exact object graph: ``refs/replace`` and grafts cannot mask
    an unlanded holder tip (SECD-05), and a promisor remote is never contacted
    to lazily fetch a missing object (``GIT_NO_LAZY_FETCH``), so a missing
    object stays an observable fact.
    """
    env = {**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_NO_REPLACE_OBJECTS": "1", "GIT_NO_LAZY_FETCH": "1",
           "GIT_GRAFT_FILE": os.devnull}
    env.pop("GIT_SHALLOW_FILE", None)
    try:
        proc = subprocess.run(["git", "--no-replace-objects", "-C", str(repo), *args], capture_output=True,
                              timeout=_GIT_TIMEOUT_S, env=env)
    except (OSError, subprocess.SubprocessError):
        return None
    return proc.returncode, proc.stdout


def _live_ref(repo: Path, branch: str) -> str | None:
    ref = "refs/heads/" + branch
    answer = _git(repo, "for-each-ref", "--format=%(objectname) %(refname)", ref)
    if answer is None or answer[0] != 0:
        return _UNREADABLE
    for line in answer[1].decode(errors="replace").splitlines():
        sha, _, name = line.partition(" ")
        if name == ref:
            return sha if _HEX.fullmatch(sha) else _UNREADABLE
    # for-each-ref omits a broken (dangling symbolic) ref: that ref exists but
    # cannot be read, which is unknown, never absent.
    symbolic = _git(repo, "symbolic-ref", "-q", ref)
    if symbolic is None or symbolic[0] not in (0, 1):
        return _UNREADABLE
    return _UNREADABLE if symbolic[0] == 0 else None


def _batch_live_refs(repo: Path) -> tuple[dict[str, str], Path | None] | None:
    """Resolve all live refs/heads/* in one git call (CAL-02, ALG-02).

    Preserves fail-closed dangling symbolic ref detection: any branch absent
    from for-each-ref whose loose ref file exists under ``<git-common-dir>/refs/heads``
    falls back to :func:`_live_ref`.
    """
    answer = _git(repo, "for-each-ref", "--format=%(objectname) %(refname)", "refs/heads")
    if answer is None or answer[0] != 0:
        return None
    live_map: dict[str, str] = {}
    for line in answer[1].decode(errors="replace").splitlines():
        sha, _, name = line.partition(" ")
        if name.startswith("refs/heads/"):
            live_map[name] = sha if _HEX.fullmatch(sha) else _UNREADABLE
    common_ans = _git(repo, "rev-parse", "--git-common-dir")
    common_dir: Path | None = None
    if common_ans is not None and common_ans[0] == 0:
        raw_dir = common_ans[1].decode(errors="replace").strip()
        if raw_dir:
            cand = Path(raw_dir)
            common_dir = cand if cand.is_absolute() else (repo / cand).resolve()
    return live_map, common_dir


def _lookup_live_ref(
    repo: Path, branch: str, batch: tuple[dict[str, str], Path | None] | None
) -> str | None:
    if batch is None:
        return _live_ref(repo, branch)
    live_map, common_dir = batch
    ref = "refs/heads/" + branch
    if ref not in live_map and common_dir is not None and ".." not in branch.split("/"):
        try:
            if not os.path.lexists(common_dir / "refs" / "heads" / branch):
                return None
        except OSError:
            pass
    return _live_ref(repo, branch)


_GIT_FALSE = frozenset({b"false", b"no", b"off", b"0"})


def _storage_complete(repo: Path) -> bool:
    """Only complete local storage can prove a commit absent (CAL-02).

    A shallow clone, ``extensions.partialClone`` or any promisor remote
    (``remote.<name>.promisor``) may be missing objects it never fetched.
    """
    shallow = _git(repo, "rev-parse", "--is-shallow-repository")
    if shallow is None or shallow[0] != 0 or shallow[1].strip() != b"false":
        return False
    partial = _git(repo, "config", "--get", "extensions.partialClone")
    if partial is None or partial[0] != 1:
        return False
    promisor = _git(repo, "config", "--null", "--get-regexp", r"^remote\..*\.promisor$")
    if promisor is None or promisor[0] not in (0, 1):
        return False
    for entry in promisor[1].split(b"\0") if promisor[0] == 0 else ():
        if not entry:
            continue
        _, _, value = entry.partition(b"\n")
        # A bare key is boolean true; only an explicit false is not a promisor.
        if value.strip().lower() not in _GIT_FALSE:
            return False
    return True


def _replace_refs(repo: Path, memo: dict | None = None) -> frozenset[str] | None:
    """Replace-ref object names present in ``refs/replace/``; probed at most once per snapshot."""
    if memo is not None and "replace_refs" in memo:
        return memo["replace_refs"]
    answer = _git(repo, "for-each-ref", "--format=%(refname)", "refs/replace")
    if answer is None or answer[0] != 0:
        result = None
    else:
        result = frozenset(
            line.strip().removeprefix("refs/replace/")
            for line in answer[1].decode(errors="replace").splitlines()
            if line.strip().startswith("refs/replace/")
        )
    if memo is not None:
        memo["replace_refs"] = result
    return result


def _missing_state(repo: Path, sha: str, memo: dict | None = None) -> str:
    """Classify an object missing from raw storage: ``replaced``, ``absent``, or ``unreadable``."""
    replaced = _replace_refs(repo, memo)
    if replaced is None:
        return _UNREADABLE
    if sha in replaced:
        return _REPLACED
    if memo is None:
        complete = _storage_complete(repo)
    else:
        if "complete" not in memo:
            memo["complete"] = _storage_complete(repo)
        complete = memo["complete"]
    return _ABSENT if complete else _UNREADABLE


def _batch_commit_states(repo: Path, shas: set[str], memo: dict) -> None:
    """Type all candidate commit SHAs in one ``cat-file --batch-check`` call (PERF-06)."""
    states = memo.setdefault("states", {})
    pending = sorted(sha for sha in shas if sha not in states and _HEX.fullmatch(sha))
    if not pending:
        return
    env = {**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_NO_REPLACE_OBJECTS": "1", "GIT_NO_LAZY_FETCH": "1",
           "GIT_GRAFT_FILE": os.devnull}
    env.pop("GIT_SHALLOW_FILE", None)
    payload = "\n".join(pending) + "\n"
    try:
        proc = subprocess.run(
            ["git", "--no-replace-objects", "-C", str(repo), "cat-file", "--batch-check=%(objecttype)"],
            input=payload, capture_output=True, text=True, timeout=_GIT_TIMEOUT_S, env=env,
        )
    except (OSError, subprocess.SubprocessError):
        for sha in pending:
            states[sha] = _UNREADABLE
        return
    if proc.returncode != 0:
        for sha in pending:
            states[sha] = _UNREADABLE
        return
    lines = proc.stdout.splitlines()
    if len(lines) != len(pending):
        for sha in pending:
            states[sha] = _UNREADABLE
        return
    for sha, line in zip(pending, lines, strict=True):
        kind = line.strip()
        if kind == "commit":
            states[sha] = "ok"
        elif kind.endswith("missing"):
            states[sha] = _missing_state(repo, sha, memo)
        else:
            states[sha] = _UNREADABLE


def _commit_state(repo: Path, sha: str, memo: dict | None = None) -> str:
    """``ok``, ``absent`` (proven missing from complete storage), ``replaced``, or ``unreadable``.

    ``memo`` (one per snapshot) reuses batch-checked states and probes storage
    completeness once, not once per absent object.
    """
    if memo is not None:
        cached = memo.get("states", {}).get(sha)
        if cached is not None:
            return cached
    answer = _git(repo, "cat-file", "-t", sha)
    if answer is None:
        state = _UNREADABLE
    elif answer[0] == 0:
        state = "ok" if answer[1].strip() == b"commit" else _UNREADABLE
    else:
        state = _missing_state(repo, sha, memo)
    if memo is not None:
        memo.setdefault("states", {})[sha] = state
    return state


def _changed(repo: Path, integration: str, tip: str) -> tuple[str, ...] | None:
    if integration == tip:
        return ()
    base = _git(repo, "merge-base", integration, tip)
    if base is None or base[0] not in (0, 1):
        return None
    start = base[1].strip().decode() if base[0] == 0 else _EMPTY_TREE
    diff = _git(repo, "diff", "--name-only", "--no-renames", "-z", start, tip, "--")
    if diff is None or diff[0] != 0:
        return None
    return tuple(sorted({os.fsdecode(item) for item in diff[1].split(b"\0") if item}))


def _batch_integration_ancestors(repo: Path, integration: str, memo: dict) -> frozenset[str] | None:
    key = ("ancestors", integration)
    if key in memo:
        cached = memo[key]
        return cached if isinstance(cached, frozenset) else None
    answer = _git(repo, "rev-list", integration)
    if answer is None or answer[0] != 0:
        memo[key] = None
        return None
    ancestors = frozenset(line for line in answer[1].decode(errors="replace").splitlines() if _HEX.fullmatch(line))
    memo[key] = ancestors
    return ancestors


def _tip_facts(repo: Path, integration: str, tip: str, memo: dict | None = None) -> TipFacts:
    cache_key = ("tip_facts", integration, tip)
    if memo is not None and cache_key in memo:
        cached = memo[cache_key]
        if isinstance(cached, TipFacts):
            return cached
    state = _commit_state(repo, tip, memo)
    if state == _REPLACED:
        # A replace ref resolves an oid that does not exist in raw storage:
        # never report absent; treat as unlanded so missing incoming scope holds unknown.
        fact = TipFacts(tip, "ok", landed=False, paths=())
        if memo is not None:
            memo[cache_key] = fact
        return fact
    if state != "ok":
        fact = TipFacts(tip, state)
        if memo is not None:
            memo[cache_key] = fact
        return fact
    ancestors = memo.get(("ancestors", integration)) if memo is not None else None
    if isinstance(ancestors, frozenset):
        if tip in ancestors:
            fact = TipFacts(tip, "ok", landed=True)
            if memo is not None:
                memo[cache_key] = fact
            return fact
    else:
        answer = _git(repo, "merge-base", "--is-ancestor", tip, integration)
        if answer is None or answer[0] not in (0, 1):
            fact = TipFacts(tip, _UNREADABLE)
            if memo is not None:
                memo[cache_key] = fact
            return fact
        if answer[0] == 0:
            fact = TipFacts(tip, "ok", landed=True)
            if memo is not None:
                memo[cache_key] = fact
            return fact
    paths = _changed(repo, integration, tip)
    if paths is None:
        fact = TipFacts(tip, _UNREADABLE)
    else:
        fact = TipFacts(tip, "ok", landed=False, paths=paths)
    if memo is not None:
        memo[cache_key] = fact
    return fact


def _on_first_parent(repo: Path, tip: str, integration: str) -> bool:
    """``tip`` is on ``integration``'s first-parent chain; an unreadable probe answers yes (fail-safe).

    The first-parent walk from the integration SHA that stops at commits
    reachable from ``tip`` is ``n`` long; ``tip`` is on that chain exactly when
    ``integration~n`` is ``tip``.
    """
    answer = _git(repo, "rev-list", "--first-parent", "--count", f"{tip}..{integration}", "--")
    try:
        count = int(answer[1].strip()) if answer is not None and answer[0] == 0 else None
    except ValueError:
        count = None
    if count is None:
        return True
    resolved = _git(repo, "rev-parse", "--verify", "--quiet", f"{integration}~{count}^{{commit}}")
    if resolved is None or resolved[0] != 0:
        return True
    return resolved[1].decode(errors="replace").strip() == tip


def _is_ancestor(repo: Path, older: str, newer: str) -> bool:
    answer = _git(repo, "merge-base", "--is-ancestor", older, newer)
    return answer is not None and answer[0] == 0


def _pinned_supersession(repo: Path, branch: str, tip: str, integration: str, integration_branch: str) -> bool:
    """A reviewed first-parent ``Workbay-Tombstone`` pins ``<branch>@<tip>`` (decision 48672).

    The record must name this branch at this exact 40-hex tip (``superseded
    superseded_by_integration:<carrier40>``) on the integration first-parent
    history and carry its ``Workbay-Supersede-Plan``; landing_log's own
    ``reviewed_supersession_hold`` re-verifies carrier, plan digest and first
    parent. An unpinned, unreviewed, mismatched or unreadable record proves
    nothing and the tip keeps its git answer (SECD-05). Callers only ask while
    the branch still sits at ``tip``.
    """
    from . import landing_log as log

    if not _HEX.fullmatch(tip) or len(tip) != 40:
        return False
    answer = _git(repo, "log", "--first-parent", "--fixed-strings",
                  f"--grep=@{tip} superseded superseded_by_integration:", "--format=%H%x00%B%x1e", integration, "--")
    if answer is None or answer[0] != 0:
        return False
    for record in answer[1].decode(errors="replace").split("\x1e"):
        commit, separator, message = record.lstrip("\n").partition("\x00")
        if not separator or not _HEX.fullmatch(commit.strip()):
            continue
        pinned = [entry for entry in log.parse_tombstones(message) if isinstance(entry, log.Tombstone)
                  and _short(entry.branch) == branch and entry.tip == tip and entry.disposition == "superseded"
                  and entry.kind == "superseded_by_integration"]
        if not pinned:
            continue
        refusal = log.reviewed_supersession_hold(repo, branch=branch, tip=tip, receipt=commit.strip(),
                                                 integration_ref="refs/heads/" + integration_branch)
        if refusal is None:
            return True
    return False


def _ledger_rows(ledger: dict | None) -> dict | None:
    """The ledger's rows; ``None`` (unreadable, cannot prove a pass settled) unless authenticated.

    The ledger file is the ``{"data": {..., "rows": ...}, "sha256": ...}``
    envelope; a digest mismatch or a malformed shape is unreadable (CAL-02).
    """
    from .review_cycle_ledger import _digest, _encoded

    if ledger is None:
        return {}
    if "data" in ledger or "sha256" in ledger:
        data = ledger.get("data")
        if not isinstance(data, dict) or ledger.get("sha256") != _digest(_encoded(data)):
            return None
        rows = data.get("rows")
    else:
        rows = ledger.get("rows")  # legacy unenveloped shape
    return rows if isinstance(rows, dict) else None


# ---------------------------------------------------------------- registry/receipt/ledger reads (no lock)


def _retrying(read):
    """Bounded retry with jitter; persistent contention is typed, never a hold."""
    for attempt in range(_SNAPSHOT_ATTEMPTS):
        try:
            return read()
        except SnapshotContended:
            if attempt + 1 == _SNAPSHOT_ATTEMPTS:
                raise
            time.sleep(random.uniform(0.02, 0.1) * (attempt + 1))
    raise SnapshotContended(SNAPSHOT_CONTENDED)  # pragma: no cover


def _registry(db_path: Path) -> tuple[list[dict], frozenset[str]]:
    if not db_path.exists():
        return [], frozenset()

    def read():
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
        except sqlite3.OperationalError as exc:
            raise SnapshotContended(SNAPSHOT_CONTENDED) from exc
        try:
            conn.row_factory = sqlite3.Row
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            lanes = ([dict(row) for row in conn.execute(
                "SELECT id, task_ref, lane_id, branch, status, lane_kind, branch_tip_sha, worktree_path, notes "
                "FROM worktree_lanes ORDER BY id")] if "worktree_lanes" in tables else [])
            discarded = (frozenset(row[0] for row in conn.execute(
                "SELECT decision FROM decisions WHERE decision LIKE ?", (DISCARD_DECISION_PREFIX + "%",)))
                if "decisions" in tables else frozenset())
            return lanes, discarded
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc) or "busy" in str(exc):
                raise SnapshotContended(SNAPSHOT_CONTENDED) from exc
            raise
        finally:
            conn.close()

    return _retrying(read)


def _json_file(path: Path) -> dict | None:
    def read():
        try:
            value = json.loads(path.read_bytes())
        except FileNotFoundError:
            return None
        except ValueError as exc:
            raise SnapshotContended(SNAPSHOT_CONTENDED) from exc
        return value if isinstance(value, dict) else None

    return _retrying(read)


def _receipts(directory: Path) -> tuple[list[tuple[str, dict]], list[tuple[str, str]]]:
    receipts, unreadable = [], []
    for path in sorted(directory.glob("*.json")) if directory.is_dir() else ():
        try:
            receipt = _json_file(path)
        except (SnapshotContended, OSError) as exc:
            cause = exc.__cause__ or exc
            unreadable.append((path.name, f"{type(cause).__name__}: {cause}"))
            continue
        if receipt is not None:
            receipts.append((path.name, receipt))
    return receipts, unreadable


def _manifest_roots(root: Path, task_ref: object, lane_id: object) -> tuple[str, ...]:
    if not isinstance(task_ref, str) or not isinstance(lane_id, str) or "/" in task_ref or task_ref.startswith("."):
        return ()
    try:
        manifest = json.loads((root / "config/lane-orchestration" / f"{task_ref}.json").read_text())
    except (OSError, ValueError):
        return ()
    lanes = manifest.get("lanes") if isinstance(manifest, dict) else None
    lane = lanes.get(lane_id) if isinstance(lanes, dict) else None
    return _roots(lane.get("owned_paths")) if isinstance(lane, dict) else ()


def _binding_roots(row: dict) -> tuple[str, ...]:
    from .review_ratchet import task_subject_binding

    binding = task_subject_binding(row)
    return _roots(binding.get("owned_paths")) if isinstance(binding, dict) else ()


def _receipt_identity(receipt: dict) -> tuple[str | None, str | None]:
    for key in ("source_issuer", "managed_identity"):
        value = receipt.get(key)
        if isinstance(value, dict) and isinstance(value.get("task_ref"), str) and isinstance(value.get("lane_id"), str):
            return value["task_ref"], value["lane_id"]
    stamp = receipt.get("source_task_subject")
    if isinstance(stamp, dict) and isinstance(stamp.get("task_ref"), str) and isinstance(stamp.get("lane_id"), str):
        return stamp["task_ref"], stamp["lane_id"]
    return None, None


def _holders(root: Path, lanes: list[dict], receipts: list[tuple[str, dict]], unreadable: list[tuple[str, str]],
             ledger_rows: dict | None) -> list[Holder]:
    by_branch: dict[str, dict] = {}

    def entry(branch: str) -> dict:
        return by_branch.setdefault(branch, dict(lanes=[], receipts=[]))

    for row in lanes:
        branch = _short(row.get("branch"))
        if branch is None or row.get("lane_kind") == "review":
            continue
        entry(branch)["lanes"].append(row)
    for name, receipt in receipts:
        identity = receipt.get("identity") if isinstance(receipt.get("identity"), dict) else {}
        branch, head = _short(receipt.get("source_branch")), receipt.get("source_head")
        if branch is None or not isinstance(head, str) or identity.get("kind") == "analysis":
            continue
        entry(branch)["receipts"].append((name, receipt, identity))
    holders = []
    for branch, group in sorted(by_branch.items()):
        rows = group["lanes"]

        def sequence(item):
            _, _, identity = item
            dispatch_id = identity.get("dispatch_id")
            row = (ledger_rows or {}).get(dispatch_id) if isinstance(dispatch_id, str) else None
            issued = row.get("issued_sequence") if isinstance(row, dict) else None
            return (type(issued) is int, issued if type(issued) is int else -1, str(dispatch_id))

        latest = max(group["receipts"], key=sequence) if group["receipts"] else None
        latest_identity = latest[2] if latest else {}
        task_ref = lane_id = None
        if latest is not None:
            task_ref, lane_id = _receipt_identity(latest[1])
        lane = None
        if task_ref is not None:
            lane = next((row for row in reversed(rows) if (row.get("task_ref"), row.get("lane_id")) == (task_ref, lane_id)),
                        None)
        if lane is None and rows:
            lane = rows[-1]
        if lane is not None and task_ref is None:
            task_ref, lane_id = lane.get("task_ref"), lane.get("lane_id")
        roots = set()
        for row in rows:
            roots.update(_manifest_roots(root, row.get("task_ref"), row.get("lane_id")) or _binding_roots(row))
        live_status = lane.get("status") if lane is not None else None
        in_flight = False
        for _, receipt, identity in group["receipts"]:
            dispatch_id = identity.get("dispatch_id")
            if receipt.get("state") != "transport_started" or not isinstance(dispatch_id, str):
                continue
            row = ledger_rows.get(dispatch_id) if isinstance(ledger_rows, dict) else None
            # An unreadable ledger cannot prove the pass settled.
            if ledger_rows is None or (isinstance(row, dict) and row.get("completion") is None):
                in_flight = True
        holders.append(Holder(
            branch=branch, task_ref=task_ref, lane_id=lane_id, lane_status=live_status,
            owned_roots=tuple(sorted(roots)),
            lane_tips=tuple(sorted({row["branch_tip_sha"] for row in rows
                                    if isinstance(row.get("branch_tip_sha"), str)
                                    and _HEX.fullmatch(row["branch_tip_sha"])})),
            lane_identities=tuple(sorted({(row["task_ref"], row["lane_id"]) for row in rows
                                          if isinstance(row.get("task_ref"), str) and isinstance(row.get("lane_id"), str)}
                                         | ({(task_ref, lane_id)} if task_ref and lane_id else set()))),
            dispatch_id=latest_identity.get("dispatch_id"),
            receipt=latest[0] if latest else None,
            subject=latest_identity.get("subject_branch"),
            receipt_head=latest[1].get("source_head") if latest else None,
            in_flight=in_flight and live_status in _LIVE_LANE_STATUSES,
        ))
    for name, error in unreadable:
        # A receipt we cannot decode may be the only record of a holder's tip.
        holders.append(Holder(branch="", task_ref=None, lane_id=None, lane_status=None, owned_roots=(),
                              lane_tips=(), lane_identities=(), dispatch_id=None, receipt=name, subject=None,
                              receipt_head=None, in_flight=False, unreadable=True, receipt_error=error))
    return holders


def _is_self(incoming: Incoming, holder: Holder) -> bool:
    if holder.branch and holder.branch in {incoming.branch, _short(incoming.subject_branch)}:
        return True
    return bool(incoming.task_ref and incoming.lane_id
                and (holder.task_ref, holder.lane_id) == (incoming.task_ref, incoming.lane_id))


def _rev_count(repo: Path, spec: str) -> int | None:
    answer = _git(repo, "rev-list", "--count", spec, "--")
    if answer is None or answer[0] != 0:
        return None
    try:
        return int(answer[1].strip())
    except ValueError:
        return None


def _ancestry(repo: Path, older: str, newer: str) -> bool | None:
    answer = _git(repo, "merge-base", "--is-ancestor", older, newer)
    if answer is None or answer[0] not in (0, 1):
        return None
    return answer[0] == 0


def _stamped_roots(receipt: dict) -> tuple[str, ...]:
    stamp = receipt.get("source_task_subject")
    if not isinstance(stamp, dict):
        return ()
    return tuple(sorted(set(_roots(stamp.get("owned_paths"))) | set(_roots(stamp.get("changed_files")))))


def retained_subject_for(repo: Path, incoming: Incoming, incoming_tip: str | None,
                         receipts: list[tuple[str, dict]], incoming_paths: tuple[str, ...] | None, *,
                         integration: str | None = None, incoming_tasks: tuple[str, ...] = (),
                         memo: dict | None = None) -> Lineage:
    """Round accounting follows git subject lineage (decision 48657). Pure git plus a receipt read.

    Candidates are the review-cycle receipts whose source head is an ancestor of
    the incoming tip, plus the incoming branch's own receipts (direct match,
    HG01-F2). The nearest by merge-base distance (``git rev-list --count
    head...tip``) charges its subject's round account; equally near receipts
    naming several subjects are ambiguous; none charges the incoming subject.
    A new registry row or a re-registered lane therefore never restarts at
    round one while its tip still descends from a charged source.

    An authenticated incoming task (``proof_base``) does not continue a
    foreign-subject receipt that is settled upstream history: one strictly
    below the base the task was cut from, or one at that base whose stamped
    scope is disjoint from the task's authorized scope and its diff. Without
    that proof, a foreign receipt from outside the incoming task is settled
    history if its head is strictly behind integration. At the integration tip,
    only an unbound branch can shed a stamped foreign receipt (#56). Unlanded
    reviewed heads keep their lineage (decision 48657).

    The incoming branch's own receipt whose ``source_head`` is not a SHA is
    unreadable lineage, never skipped (CAL-02). Ancestry and distance are
    probed once per distinct head (``memo``). Runs before any lock (CON-21);
    never a hold input (rule R5): ``holder_blocks`` stays subject-free.
    """
    tasks = tuple(sorted(set(incoming_tasks)))
    if incoming_tip is None:
        return Lineage("unknown", reason="subject_lineage_unreadable", incoming_tasks=tasks)
    memo = {} if memo is None else memo
    probes: dict = memo.setdefault("lineage", {})

    def probe(key: tuple, run):
        if key not in probes:
            probes[key] = run()
        return probes[key]

    proven = incoming.proof_base is not None and _HEX.fullmatch(incoming.proof_base) is not None
    bound_tasks = {*tasks, incoming.task_ref} - {None}
    unbound = not bound_tasks and incoming.subject_branch is None and incoming.proof_subject is None
    scope = None
    if proven and incoming.proof_roots is not None and incoming_paths is not None:
        scope = set(incoming.proof_roots) | set(incoming_paths)
    expected = _short(incoming.proof_subject) or _short(incoming.subject_branch) or incoming.branch
    nearest: list[tuple[int, str, dict]] = []
    excluded = []
    unreadable_own: list[dict] = []
    for name, receipt in receipts:
        identity = receipt.get("identity") if isinstance(receipt.get("identity"), dict) else {}
        head, subject = receipt.get("source_head"), _short(identity.get("subject_branch"))
        own = bool(incoming.branch) and _short(receipt.get("source_branch")) == incoming.branch
        if identity.get("kind") == "analysis":
            continue
        if not isinstance(head, str) or not _HEX.fullmatch(head):
            if own and "source_head" in receipt:
                # The incoming branch's own record names no readable head: its
                # lineage is unknown unless a readable receipt already proves the
                # exact same authenticated subject (CAL-02).
                unreadable_own.append(dict(receipt=name, dispatch_id=identity.get("dispatch_id"), subject=subject,
                                           source_branch=receipt.get("source_branch"), source_head=head,
                                           has_source_task_subject=receipt.get("source_task_subject") is not None,
                                           predicates_tried=["direct_match"]))
            continue
        if not subject:
            continue
        pre_state = memo.get("states", {}).get(head)
        if pre_state is not None:
            if pre_state == _ABSENT:
                if not own:
                    continue  # a vanished foreign head cannot be an ancestor of a readable tip (PERF-06: skip merge-base)
                return Lineage("unknown", reason="subject_lineage_unreadable", incoming_tasks=tasks,
                               receipts=(dict(receipt=name, subject=subject, source_head=head),))
            if pre_state != "ok":
                return Lineage("unknown", reason="subject_lineage_unreadable", incoming_tasks=tasks,
                               receipts=(dict(receipt=name, subject=subject, source_head=head),))
        ancestor = probe(("ancestry", head, incoming_tip), lambda: _ancestry(repo, head, incoming_tip))
        if ancestor is None:
            if pre_state is None:
                state = probe(("state", head), lambda: _commit_state(repo, head, memo))
                if state == _ABSENT and not own:
                    continue
            return Lineage("unknown", reason="subject_lineage_unreadable", incoming_tasks=tasks,
                           receipts=(dict(receipt=name, subject=subject, source_head=head),))
        if not ancestor and not own:
            continue
        if not ancestor and own and pre_state is None:
            state = probe(("state", head), lambda: _commit_state(repo, head, memo))
            if state != "ok":
                return Lineage("unknown", reason="subject_lineage_unreadable", incoming_tasks=tasks,
                               receipts=(dict(receipt=name, subject=subject, source_head=head),))
        row = dict(receipt=name, dispatch_id=identity.get("dispatch_id"), subject=subject,
                   source_branch=receipt.get("source_branch"), source_head=head,
                   has_source_task_subject=receipt.get("source_task_subject") is not None,
                   predicates_tried=["retained_subject_for"])
        receipt_task = _receipt_identity(receipt)[0]
        if proven and subject != expected and not own:
            upstream = head == incoming.proof_base or probe(
                ("ancestry", head, incoming.proof_base), lambda: _ancestry(repo, head, incoming.proof_base))
            if upstream is None:
                return Lineage("unknown", reason="subject_lineage_unreadable", receipts=(row,), incoming_tasks=tasks)
            if upstream and head != incoming.proof_base:
                excluded.append(dict(row, excluded="upstream_of_task_base"))
                continue
            if upstream and scope is not None and not _overlap(set(_stamped_roots(receipt)), scope):
                excluded.append(dict(row, excluded="scope_disjoint_at_task_base"))
                continue
            if (not upstream and integration is not None and _HEX.fullmatch(integration) is not None
                    and scope is not None and not _overlap(set(_stamped_roots(receipt)), scope)):
                landed = probe(("ancestry", head, integration), lambda: _ancestry(repo, head, integration))
                if landed is None:
                    return Lineage("unknown", reason="subject_lineage_unreadable", receipts=(row,),
                                   incoming_tasks=tasks)
                if landed:
                    excluded.append(dict(row, excluded="landed_on_integration"))
                    continue
        if (not proven and subject != expected and not own and receipt_task not in bound_tasks
                and integration is not None and _HEX.fullmatch(integration) is not None
                and (head != integration or (unbound and isinstance(receipt.get("source_task_subject"), dict)))):
            landed = probe(("ancestry", head, integration), lambda: _ancestry(repo, head, integration))
            if landed is None:
                return Lineage("unknown", reason="subject_lineage_unreadable", receipts=(row,), incoming_tasks=tasks)
            if landed is True:
                excluded.append(dict(row, excluded="landed_on_integration"))
                continue
        distance = probe(("distance", head, incoming_tip), lambda: _rev_count(repo, f"{head}...{incoming_tip}"))
        if distance is None:
            return Lineage("unknown", reason="subject_lineage_unreadable", receipts=(row,), incoming_tasks=tasks)
        nearest.append((distance, name, dict(row, distance=distance), receipt_task))
    if not nearest:
        if unreadable_own:
            return Lineage("unknown", reason="subject_lineage_unreadable", incoming_tasks=tasks,
                           receipts=tuple(unreadable_own))
        return Lineage("incoming", expected, excluded=tuple(excluded), incoming_tasks=tasks)
    best = min(item[0] for item in nearest)
    chosen = [item for item in sorted(nearest, key=lambda item: item[1]) if item[0] == best]
    rows = tuple(item[2] for item in chosen)
    charged = tuple(item[3] for item in chosen)
    subjects = {row["subject"] for row in rows}
    if len(subjects) > 1:
        return Lineage("ambiguous", None, best, rows, tuple(excluded), "subject_lineage_ambiguous", tasks, charged)
    chosen_subject = subjects.pop()
    if unreadable_own and not (proven and chosen_subject == _short(incoming.proof_subject)
                               and all(row.get("subject") == chosen_subject for row in unreadable_own)):
        return Lineage("unknown", reason="subject_lineage_unreadable", incoming_tasks=tasks,
                       receipts=tuple(unreadable_own))
    return Lineage("retained", chosen_subject, best, rows, tuple(excluded), incoming_tasks=tasks,
                   charged_tasks=charged)


def take_snapshot(repo: Path, incoming: Incoming, *, orchestrator_root: Path | None = None) -> Snapshot:
    """Read every input once, then run every git probe. Takes no lock."""
    from workbay_handoff_mcp.config import RuntimeConfig

    from .review_cycle_dispatch import authority

    private = authority(repo)
    root = orchestrator_root or private.parent.parent
    try:
        lanes, discarded = _registry(RuntimeConfig.for_repo(root).db_path)
        receipts, unreadable = _receipts(private / "review_cycle_dispatch")
        try:
            ledger_rows = _ledger_rows(_json_file(private / "review_cycle_ledger.json"))
        except (SnapshotContended, OSError, ValueError, TypeError):
            ledger_rows = None
    except SnapshotContended:
        return Snapshot(incoming=incoming, integration_sha=None, incoming_paths=(), incoming_unreadable=False,
                        holders=(), contended=True)
    holders = _holders(root, lanes, receipts, unreadable, ledger_rows)
    registered = [row for row in lanes
                  if _short(row.get("branch")) == incoming.branch and row.get("lane_kind") != "review"]
    incoming_tasks = tuple(sorted({row["task_ref"] for row in registered if isinstance(row.get("task_ref"), str)}))
    if incoming.owned_roots is None:
        # A registered incoming lane declares its scope in the registry/manifest.
        declared = set()
        for row in registered:
            declared.update(_manifest_roots(root, row.get("task_ref"), row.get("lane_id")) or _binding_roots(row))
        if declared:
            incoming = Incoming(**{**incoming.__dict__, "owned_roots": tuple(sorted(declared))})
    memo: dict = {}

    answer = _git(repo, "rev-parse", "--verify", "--quiet", f"refs/heads/{incoming.integration_branch}^{{commit}}")
    integration = answer[1].decode().strip() if answer is not None and answer[0] == 0 else None
    if integration is not None and not _HEX.fullmatch(integration):
        integration = None
    incoming_paths: tuple[str, ...] = ()
    incoming_unreadable = False
    facts = {}
    # The pinned review source wins; otherwise the incoming branch ref, then HEAD.
    refs = [incoming.tip_ref] if incoming.tip_ref != "HEAD" else (
        ([f"refs/heads/{incoming.branch}"] if incoming.branch else []) + ["HEAD"])
    incoming_tip = None
    for ref in refs:
        tip = _git(repo, "rev-parse", "--verify", "--quiet", ref + "^{commit}")
        if tip is not None and tip[0] == 0 and _HEX.fullmatch(tip[1].decode().strip()):
            incoming_tip = tip[1].decode().strip()
            break
    if integration is not None:
        changed = _changed(repo, integration, incoming_tip) if incoming_tip else None
        if changed is None:
            incoming_unreadable = True
        else:
            incoming_paths = changed
        candidate_shas: set[str] = set()
        for _, receipt in receipts:
            head = receipt.get("source_head")
            if isinstance(head, str) and _HEX.fullmatch(head):
                candidate_shas.add(head)
        live_batch = _batch_live_refs(repo)
        holder_live: dict[tuple[str, str, str], str | None] = {}
        for holder in holders:
            if holder.unreadable or _is_self(incoming, holder):
                continue
            live = _lookup_live_ref(repo, holder.branch, live_batch)
            holder_live[holder.key] = live
            if live not in (None, _UNREADABLE):
                candidate_shas.add(live)
            for tip_sha in (holder.receipt_head, *holder.lane_tips):
                if isinstance(tip_sha, str) and _HEX.fullmatch(tip_sha):
                    candidate_shas.add(tip_sha)
        _batch_commit_states(repo, candidate_shas, memo)
        _batch_integration_ancestors(repo, integration, memo)
        for holder in holders:
            if holder.unreadable or _is_self(incoming, holder):
                continue
            live = holder_live[holder.key]
            known = []
            if live not in (None, _UNREADABLE):
                known.append(live)
            for tip_sha in (holder.receipt_head, *holder.lane_tips):
                if isinstance(tip_sha, str) and _HEX.fullmatch(tip_sha) and tip_sha not in known:
                    known.append(tip_sha)
            tips = []
            for tip_sha in known:
                # A last-known tip already contained in the live tip adds nothing.
                if (tip_sha != live and live not in (None, _UNREADABLE)
                        and _commit_state(repo, tip_sha, memo) == "ok" and _is_ancestor(repo, tip_sha, live)):
                    continue
                fact = _tip_facts(repo, integration, tip_sha, memo)
                # Only the exact tip a reviewed tombstone pins, and only while the
                # branch still sits there, is superseded (decision 48672).
                if (fact.state == "ok" and fact.landed is False and tip_sha == live
                        and _pinned_supersession(repo, holder.branch, tip_sha, integration,
                                                 incoming.integration_branch)):
                    fact = TipFacts(fact.tip, fact.state, fact.landed, fact.paths, superseded=True)
                tips.append(fact)
            facts[holder.key] = HolderFacts(live=live, tips=tuple(tips))
    own = []
    for name, receipt in receipts:
        identity = receipt.get("identity") if isinstance(receipt.get("identity"), dict) else {}
        if (_short(receipt.get("source_branch")) == incoming.branch and isinstance(receipt.get("source_head"), str)
                and identity.get("kind") != "analysis" and isinstance(identity.get("subject_branch"), str)):
            own.append(dict(receipt=name, dispatch_id=identity.get("dispatch_id"), subject=identity["subject_branch"],
                            source_branch=receipt["source_branch"], source_head=receipt["source_head"],
                            has_source_task_subject=receipt.get("source_task_subject") is not None,
                            predicates_tried=["direct_match"]))
    lineage = retained_subject_for(repo, incoming, incoming_tip, receipts,
                                   None if incoming_unreadable or integration is None else incoming_paths,
                                   integration=integration, incoming_tasks=incoming_tasks, memo=memo)
    return Snapshot(incoming=incoming, integration_sha=integration, incoming_paths=incoming_paths,
                    incoming_unreadable=incoming_unreadable, holders=tuple(sorted(holders, key=lambda h: h.key)),
                    facts=facts, discarded=frozenset(discarded), own_receipts=tuple(own), lineage=lineage)


# ---------------------------------------------------------------- the decision (pure)


def _overlaps(left: str, right: str) -> bool:
    return left == right or left.startswith(right + "/") or right.startswith(left + "/")


def _overlap(holder_paths: set[str], incoming: set[str] | None) -> tuple[str, ...]:
    if incoming is None:
        return tuple(sorted(holder_paths))
    found = set()
    for path in holder_paths:
        for other in incoming:
            if _overlaps(path, other):
                found.add(path if len(path) >= len(other) else other)
    return tuple(sorted(found))


def holder_blocks(snapshot: Snapshot, incoming: Incoming, holder: Holder) -> HolderBlock:
    """Pure decision for one holder against one incoming source."""
    def result(blocks: BLOCKS, reason: str, *, tip: str | None = None, overlap: tuple[str, ...] = ()) -> HolderBlock:
        return HolderBlock(blocks=blocks, reason=reason, overlap_paths=tuple(sorted(overlap)),
                           holder_tip=tip, holder_lane=holder.lane_id, holder_task=holder.task_ref,
                           holder_branch=holder.branch or None, dispatch_id=holder.dispatch_id,
                           receipt=holder.receipt, subject=holder.subject, integration_sha=snapshot.integration_sha,
                           receipt_error=holder.receipt_error)

    if snapshot.contended:
        return result("retry", SNAPSHOT_CONTENDED)
    if holder.unreadable:
        return result("unknown", "holder_tip_unreadable")
    if _is_self(incoming, holder):
        return result(False, "self")
    if snapshot.integration_sha is None:
        return result("unknown", "integration_unreadable")
    facts = snapshot.facts.get(holder.key)
    if facts is None:
        return result("unknown", "holder_tip_unreadable")
    incoming_set = (None if snapshot.incoming_unreadable or incoming.owned_roots is None
                    else set(snapshot.incoming_paths) | set(incoming.owned_roots))
    if facts.live == _UNREADABLE:
        return result("unknown", "holder_tip_unreadable")
    outcomes = []
    for tip in facts.tips:
        if tip.state == _UNREADABLE:
            return result("unknown", "holder_tip_unreadable", tip=tip.tip)
        if tip.state == _ABSENT:
            if tip.tip == facts.live:
                return result("unknown", "holder_tip_unreadable", tip=tip.tip)
            outcomes.append(("holder_tip_absent", tip.tip, ()))
            continue
        if tip.landed:
            outcomes.append(("no_unlanded_commits", tip.tip, ()))
            continue
        if tip.superseded or any(discard_decision_id(task, lane, tip.tip) in snapshot.discarded
                                 for task, lane in holder.lane_identities):
            outcomes.append(("discarded", tip.tip, ()))
            continue
        paths = set(tip.paths)
        if holder.in_flight:
            paths |= set(holder.owned_roots)
        if incoming_set is None:
            # No proof, no declared roots (or an unreadable incoming diff): the
            # overlap cannot be decided, which holds and is never clear (HG01-F1).
            return result("unknown", INCOMING_SCOPE_UNKNOWN, tip=tip.tip, overlap=tuple(sorted(paths)))
        overlap = _overlap(paths, incoming_set)
        if overlap:
            return result(True, "paths_overlap", tip=tip.tip, overlap=overlap)
        outcomes.append(("paths_disjoint", tip.tip, ()))
    if not outcomes:
        return result(False, "holder_tip_absent", tip=None)
    # Name the live tip's outcome first, then the strongest last-known one.
    order = ("paths_disjoint", "discarded", "no_unlanded_commits", "holder_tip_absent")
    live = next((item for item in outcomes if item[1] == facts.live), None)
    reason, tip, _ = live or min(outcomes, key=lambda item: order.index(item[0]))
    return result(False, reason, tip=tip)


def evaluate_holds(repo: Path, incoming: Incoming, *, orchestrator_root: Path | None = None,
                   own: list | None = None, lineage: list | None = None) -> list[HolderBlock]:
    """Every holder's decision, sorted by (task, lane, branch).

    ``own`` receives the own receipts and ``lineage`` the round-accounting
    :class:`Lineage` (both from the same lock-free snapshot)."""
    snapshot = take_snapshot(repo, incoming, orchestrator_root=orchestrator_root)
    if own is not None:
        own.extend(snapshot.own_receipts)
    if lineage is not None and snapshot.lineage is not None:
        lineage.append(snapshot.lineage)
    if snapshot.contended:
        return [holder_blocks(snapshot, snapshot.incoming, Holder(
            branch="", task_ref=None, lane_id=None, lane_status=None, owned_roots=(), lane_tips=(),
            lane_identities=(), dispatch_id=None, receipt=None, subject=None, receipt_head=None, in_flight=False))]
    return [holder_blocks(snapshot, snapshot.incoming, holder) for holder in snapshot.holders]


def receipt_name(dispatch_id: str) -> str:
    return hashlib.sha256(dispatch_id.encode()).hexdigest() + ".json"
