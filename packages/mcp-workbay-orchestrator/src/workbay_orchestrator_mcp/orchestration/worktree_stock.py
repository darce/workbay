"""Worktree stock measurement and guarded review-stock reclamation.

Answers one question honestly: how many lane worktrees are registered,
how many are still outstanding (non-terminal lane rows), how many
unregistered sibling paths exist, and how much volume headroom is left.

Contract: a probe that could not read the stock reports UNKNOWN (None)
with the probe named in ``probe_errors`` — never a comfortable zero.
A listing that ran and genuinely found worktrees is distinguishable from
a listing that produced nothing: a real repository always lists at least
its primary worktree, so a successful listing that parses to zero entries
(empty or unparseable output) is reported as UNKNOWN, not as zero stock.
Marker presence is not enough: every record must parse completely, and
the collector stats the parsed primary path. A truncated record is
UNKNOWN with the listing probe named — the same shape as a failed
command. An unresolvable primary is also UNKNOWN, but names its own
probe: ``primary_missing`` when the checkout is not on disk,
``primary_unreadable`` when it is present but cannot be stat'ed (for
example its parent denies permission). The three facts — the listing
command failed, the primary moved after capture, the primary cannot be
stat'ed — stay distinguishable.

Enumeration rule: registered worktrees come from the repository-global
porcelain listing of the primary checkout, tallied by the normalised
spelling only. De-duplication is lexical: two strings the normaliser
maps to one key count once (``/a/b`` and ``/a/./b``); a symlink twin is
a different spelling and counts separately because the core performs
no filesystem access (see ``test_measure_core_does_not_collapse_symlink_twins``).
Unregistered paths are immediate siblings of the collector root as the
caller spelled it — not the parent of a symlink's target — that are
linked worktrees of this repository and are not in the registered set.
The collector applies that discriminator; the core only tallies the
paths it is given. An unrelated repository (``.git`` directory) or a
non-git directory is not stock, regardless of naming shape. The volume
probe is a single stat on that parent directory; it assumes all sibling
worktrees share that volume (a linked worktree on another volume would
not have its own volume measured).

The measurement core remains pure/read-only.  The separately named review
reclaimer removes only linked worktrees whose completion evidence is re-read at
delete time; it never deletes a branch or changes a lane row.  Non-terminal
review worktrees (planned/review/active/blocked) stay on disk until the row is
harvested to a terminal status; in-use/assigned-worker ambiguity also refuses.
Tech debt: the reclaim allow-list names pass-engine outcomes
(``finished``, ``handoff_ready``, ``review_complete``) that worker_reports
never stores.

When split ceilings are absent, worktree_stock_ceiling caps the landable pool and the record-only review pool independently rather than capping total unlanded worktrees.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from dataclasses import dataclass

# Kept as a single imported sentence so the CLI admission reason and this
# module docstring cannot drift from each other.
WORKTREE_STOCK_CEILING_CONTRACT = (
    "When split ceilings are absent, worktree_stock_ceiling caps the "
    "landable pool and the record-only review pool independently rather "
    "than capping total unlanded worktrees."
)

TERMINAL_STATUSES = frozenset({"merged", "closed", "closed_stale"})
_RECLAIM_SUCCESS_OUTCOMES = frozenset({"finished", "handoff_ready", "review_complete"})

PROBE_WORKTREE_LIST = "worktree_list"
PROBE_PRIMARY_MISSING = "primary_missing"
PROBE_PRIMARY_UNREADABLE = "primary_unreadable"
PROBE_LANE_ROWS = "lane_rows"
PROBE_SIBLING_SCAN = "sibling_scan"
PROBE_DISK_USAGE = "disk_usage"
PROBE_NORMALISE = "normalise"

PRIMARY_PRESENT = "present"
PRIMARY_MISSING = "missing"
PRIMARY_UNREADABLE = "unreadable"

_LIST_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class WorktreeStock:
    registered_worktrees: int | None
    primary_worktrees: int | None
    linked_worktrees: int | None
    outstanding_unlanded: int | None
    unregistered_paths: int | None
    volume_free_bytes: int | None
    volume_total_bytes: int | None
    probe_errors: tuple[str, ...] = ()
    # Backward-compatible split of ``outstanding_unlanded``.  Older callers
    # construct snapshots without these fields; admission treats that shape as
    # a legacy unsplit snapshot and falls back to the aggregate.
    outstanding_landable: int | None = None
    outstanding_record_only: int | None = None
    reclaimable_record_only_worktrees: tuple[str, ...] = ()
    unmerged_landable_worktrees: tuple[str, ...] = ()
    # True when listing and lane rows were actually read, even if the class
    # split is None. False is the legacy never-measured constructor default
    # and must not be reused as the origin for a measured-but-unclassifiable
    # snapshot — admission would then fall back to the aggregate.
    stock_classes_measured: bool = False


def _norm_path(path):
    """Absolute lexical path so listing spellings compare as one directory.

    Does not follow symlinks or stat the filesystem: identical arguments
    must yield identical results regardless of host layout. ``abspath``
    already normalises ``.``, ``..`` and a trailing separator.

    Symlink agreement is an explicit caller responsibility. The collector
    injects a resolving normaliser; a direct ``measure_worktree_stock``
    caller that omits ``path_normaliser`` gets this lexical default and
    will count a symlink-spelled sibling as unregistered. A raising or
    non-str normaliser is ``PROBE_NORMALISE``, not a collapsed count.
    """
    return os.path.abspath(os.fspath(path))


class _NormaliseFailed(Exception):
    """``path_normaliser`` raised or returned a non-str."""


def _call_normaliser(normalise, path):
    """Apply ``normalise`` and refuse anything that is not a ``str``."""
    try:
        result = normalise(path)
    except Exception as exc:
        raise _NormaliseFailed from exc
    if not isinstance(result, str):
        raise _NormaliseFailed
    return result


def _note_normalise_error(probe_errors):
    if PROBE_NORMALISE not in probe_errors:
        probe_errors.append(PROBE_NORMALISE)


@dataclass(frozen=True)
class _ParsedRecord:
    """One complete porcelain record: its verbatim path and whether it is bare."""

    path: str
    bare: bool
    head: str | None = None
    branch: str | None = None


def _listing_path_from_marker(lines):
    """Return the verbatim path field, or None if the marker is unusable."""
    if not lines or not lines[0].startswith("worktree "):
        return None
    path = lines[0][len("worktree "):]
    if not path or not os.path.isabs(path):
        return None
    return path


def _porcelain_attr_kind(line):
    """Classify one porcelain body line, or None if the line is unrecognised."""
    if line in {"bare", "detached", "locked", "prunable"}:
        return line, True
    prefixes = (
        ("HEAD ", "head"),
        ("branch ", "branch"),
        ("locked ", "locked"),
        ("prunable ", "prunable"),
    )
    for prefix, kind in prefixes:
        if not line.startswith(prefix):
            continue
        value = line[len(prefix):]
        if kind in {"head", "branch"}:
            if not value:
                return None
            return kind, value
        return kind, True
    return None


def _apply_porcelain_attribute(seen, line):
    """Record one unique attribute. False means the record is malformed."""
    parsed = _porcelain_attr_kind(line)
    if parsed is None:
        return False
    kind, value = parsed
    if kind in seen:
        return False
    seen[kind] = value
    return True


def _complete_parsed_record(path, seen):
    """Build the record once every body line has been accepted."""
    if "bare" in seen:
        if {"head", "branch", "detached"} & seen.keys():
            return None
        return _ParsedRecord(path, bare=True)
    if "head" not in seen:
        return None
    if ("branch" in seen) == ("detached" in seen):
        return None
    return _ParsedRecord(path, bare=False, head=seen.get("head"), branch=seen.get("branch"))


def _parse_one_record(lines):
    """Return the parsed record, or None if the porcelain record is incomplete.

    A complete record starts with ``worktree <path>`` and then either ``bare``
    or ``HEAD <rev>`` plus exactly one of ``branch <ref>`` / ``detached``.
    Extra porcelain attributes (locked, prunable) are ignored. Any other
    unrecognised line is a refuse: a path that embeds a newline is split
    across listing lines and the suffix looks like an extra attribute,
    which would otherwise become a confident wrong count.

    The marker line delimits the path field, so leading or trailing
    whitespace inside it is data: a checkout directory whose name ends in
    a space, or whose last component starts with a space, is a legal
    name that the listing command emits verbatim. The refused path-field
    shapes are empty (which the listing grammar cannot produce) and
    non-absolute: a relative field is joined to cwd by any normaliser
    and would silently disagree with every sibling. Do not strip the
    field; that refuses valid git-emitted names.

    A repeated attribute within one record is malformed: the second
    occurrence must refuse the listing, exactly as a duplicate
    ``worktree`` marker already does. Extra porcelain attributes (locked,
    prunable) are still tolerated once each.
    """
    path = _listing_path_from_marker(lines)
    if path is None:
        return None
    seen = {}
    for line in lines[1:]:
        if not _apply_porcelain_attribute(seen, line):
            return None
    return _complete_parsed_record(path, seen)


def _row_key(row):
    return str(row.get("task_ref") or ""), str(row.get("lane_id") or "")


def _latest_reports_by_lane(review_reports):
    """Newest-first report rows keyed by task/lane.

    The global report query used by the live collector is newest-first.  A
    later non-ready or superseded report therefore revokes an older completion
    signal instead of allowing stale evidence to delete a worktree.
    """
    latest = {}
    for report in review_reports or ():
        latest.setdefault(_row_key(report), report)
    return latest


def _normalised_branch(branch):
    text = str(branch or "")
    return text if text.startswith("refs/heads/") else f"refs/heads/{text}"


def _row_in_use_state(row, in_use_probe=None):
    """True if occupied, False if idle, None if the probe is ambiguous."""
    if in_use_probe is not None:
        try:
            return in_use_probe(row)
        except Exception:  # noqa: BLE001 -- doubt refuses reclaim
            return None
    if row.get("in_use") in (True, 1, "1"):
        return True
    assigned = row.get("assigned_worker")
    if assigned not in (None, "", 0, False):
        return True
    if row.get("status") in ("active", "blocked"):
        return True
    return False


def _blockers_are_empty(report):
    blockers = report.get("blockers")
    if blockers is None:
        blockers = report.get("blockers_json")
    return blockers in (None, "", "[]", [], ())


def _report_authorizes_reclaim(report, record, row):
    """True only for a successful, empty-blocker report matching the live tip."""
    if not report or report.get("status") == "superseded":
        return False
    if report.get("merge_ready") not in (1, True):
        return False
    outcome = str(report.get("outcome") or "").strip().lower()
    if outcome not in _RECLAIM_SUCCESS_OUTCOMES:
        return False
    if not _blockers_are_empty(report):
        return False
    commit = report.get("commit_sha")
    if not isinstance(commit, str) or not commit or commit != record.head:
        return False
    return _normalised_branch(row.get("branch")) == record.branch


def _reclaimable_review_paths(records, lane_rows, review_reports, normalise, in_use_probe=None):
    """Return idle review paths with completion evidence matching their live tip."""
    rows_by_path = {}
    for row in lane_rows or ():
        row_path = row.get("worktree_path")
        if row_path:
            rows_by_path.setdefault(_call_normaliser(normalise, row_path), row)
    latest_reports = _latest_reports_by_lane(review_reports)
    reclaimable = []
    for record in records:
        row = rows_by_path.get(_call_normaliser(normalise, record.path))
        if not row or row.get("lane_kind") != "review":
            continue
        # A review worktree is live product until the row is harvested. Planned
        # and review statuses stay on disk even when a merge_ready report exists;
        # only a terminal row may be removed, and the branch/row are preserved.
        if row.get("status") not in TERMINAL_STATUSES:
            continue
        occupied = _row_in_use_state(row, in_use_probe=in_use_probe)
        if occupied is not False:
            continue
        report = latest_reports.get(_row_key(row))
        if not _report_authorizes_reclaim(report, record, row):
            continue
        reclaimable.append(record.path)
    return tuple(sorted(reclaimable))


def _parse_worktree_listing(text):
    """Return parsed records, or None if any record is structurally incomplete.

    Empty input returns an empty list (the existing unknown-via-zero-entries
    path). A marker line without a complete record is not a listing.
    """
    records = []
    current = []
    for line in _split_porcelain_lines(text):
        if line == "":
            if current:
                records.append(current)
                current = []
            continue
        current.append(line)
    if current:
        records.append(current)
    if not records:
        return []

    parsed_records = []
    for record in records:
        parsed = _parse_one_record(record)
        if parsed is None:
            return None
        parsed_records.append(parsed)
    return parsed_records


def _split_porcelain_lines(text):
    """Split listing text on the porcelain delimiter, a single newline.

    Git emits ``\\n`` (and on some hosts CRLF). ``str.splitlines()`` also
    breaks on VT, FF, NEL, LS and PS, which can appear inside a path
    field. Strip at most one trailing CR so CRLF matches LF.
    """
    lines = []
    for raw in text.split("\n"):
        if raw.endswith("\r"):
            raw = raw[:-1]
        lines.append(raw)
    return lines


def _dedupe_listed_records(records, normalise):
    """Collapse lexical spelling twins; the first record stays the main entry."""
    unique_records = []
    seen = set()
    for record in records:
        key = _call_normaliser(normalise, record.path)
        if key not in seen:
            seen.add(key)
            unique_records.append(record)
    registered_paths = [record.path for record in unique_records]
    registered = len(unique_records)
    primary = 0 if unique_records[0].bare else 1
    linked = registered - 1
    return unique_records, registered, primary, linked, registered_paths


def _measure_listing_stock(listing_text, primary_state, normalise, probe_errors):
    """Parse, validate, and de-dupe the porcelain listing."""
    if listing_text is None:
        probe_errors.append(PROBE_WORKTREE_LIST)
        return [], None, None, None, [], False
    records = _parse_worktree_listing(listing_text) or []
    if not records:
        # A real repository always lists at least its primary worktree,
        # so zero parsed entries means the probe saw nothing (empty or
        # unparseable output) — that is unknown, not a healthy zero.
        probe_errors.append(PROBE_WORKTREE_LIST)
        return [], None, None, None, [], False
    if primary_state == PRIMARY_MISSING:
        # The listing was read successfully and completely; a primary
        # that is not on disk is a different fact and names its own
        # probe, so an operator is not sent to repair the listing.
        probe_errors.append(PROBE_PRIMARY_MISSING)
        return [], None, None, None, [], False
    if primary_state == PRIMARY_UNREADABLE:
        # Present but not stat'able (e.g. the parent denies permission)
        # is a third fact, distinct from both of the above.
        probe_errors.append(PROBE_PRIMARY_UNREADABLE)
        return [], None, None, None, [], False
    try:
        records, registered, primary, linked, registered_paths = _dedupe_listed_records(
            records, normalise
        )
        return records, registered, primary, linked, registered_paths, True
    except _NormaliseFailed:
        _note_normalise_error(probe_errors)
        return [], None, None, None, [], False


def _rows_by_normalised_path(lane_rows, normalise):
    rows_by_path = {}
    for row in lane_rows:
        row_path = row.get("worktree_path")
        if row_path:
            # The global lane query is newest-first. Preserve the first row
            # for a normalised path so an older row from another task cannot
            # overwrite current ownership.
            rows_by_path.setdefault(_call_normaliser(normalise, row_path), row)
    return rows_by_path


def _outstanding_from_registered(registered_paths, rows_by_path, normalise):
    outstanding_rows = []
    outstanding_paths = []
    for path in registered_paths:
        row = rows_by_path.get(_call_normaliser(normalise, path))
        if row and row.get("status") not in TERMINAL_STATUSES:
            outstanding_rows.append(row)
            outstanding_paths.append(path)
    return outstanding_rows, outstanding_paths


def _leftover_listed_counts(records, outstanding_paths, rows_by_path, normalise):
    """Charge leftover listed trees to the class of their row.

    The primary checkout is the repository itself, not leftover lane stock.
    Review leftovers occupy record-only stock. Implement leftovers and
    row-less leftovers occupy landable stock.
    """
    outstanding_set = {_call_normaliser(normalise, path) for path in outstanding_paths}
    leftover_landable = 0
    leftover_record_only = 0
    for record in records[1:]:
        key = _call_normaliser(normalise, record.path)
        if key in outstanding_set:
            continue
        row = rows_by_path.get(key)
        if row and row.get("lane_kind") == "review":
            leftover_record_only += 1
        else:
            leftover_landable += 1
    return leftover_landable, leftover_record_only


def _fold_physical_count(count, extra):
    """Add leftover/unregistered physical stock, or fail closed when unknown."""
    if count is None or extra is None:
        return None
    return count + extra


def _split_outstanding_classes(
    outstanding_rows,
    outstanding_paths,
    records,
    lane_rows,
    review_reports,
    normalise,
    rows_by_path,
):
    # Missing lane_kind is an old-schema row whose database default was
    # implement.  An explicit unknown kind cannot be assigned safely, so
    # both split readings become unknown. Listing and rows were still
    # measured; callers must not treat that None split as a legacy
    # never-measured snapshot.
    unknown_kind = any(
        row.get("lane_kind", "implement") not in ("implement", "review")
        for row in outstanding_rows
    )
    if unknown_kind:
        return None, None, (), ()
    landable = sum(
        row.get("lane_kind", "implement") == "implement" for row in outstanding_rows
    )
    record_only = sum(row.get("lane_kind") == "review" for row in outstanding_rows)
    unmerged_landable = tuple(
        path
        for path, row in zip(outstanding_paths, outstanding_rows)
        if row.get("lane_kind", "implement") == "implement"
    )
    reclaimable_record_only = _reclaimable_review_paths(
        records,
        lane_rows,
        review_reports,
        normalise,
    )
    leftover_landable, leftover_record_only = _leftover_listed_counts(
        records, outstanding_paths, rows_by_path, normalise
    )
    landable = _fold_physical_count(landable, leftover_landable)
    record_only = _fold_physical_count(record_only, leftover_record_only)
    return landable, record_only, reclaimable_record_only, unmerged_landable


def _measure_outstanding_stock(
    listing_ok,
    lane_rows,
    registered_paths,
    records,
    review_reports,
    normalise,
    probe_errors,
):
    if not listing_ok:
        return None, None, None, (), (), False
    if lane_rows is None:
        probe_errors.append(PROBE_LANE_ROWS)
        return None, None, None, (), (), False
    try:
        rows_by_path = _rows_by_normalised_path(lane_rows, normalise)
        outstanding_rows, outstanding_paths = _outstanding_from_registered(
            registered_paths, rows_by_path, normalise
        )
        outstanding = len(outstanding_rows)
        landable, record_only, reclaimable, unmerged = _split_outstanding_classes(
            outstanding_rows,
            outstanding_paths,
            records,
            lane_rows,
            review_reports,
            normalise,
            rows_by_path,
        )
        return outstanding, landable, record_only, reclaimable, unmerged, True
    except _NormaliseFailed:
        _note_normalise_error(probe_errors)
        return None, None, None, (), (), False


def _measure_unregistered_stock(
    listing_ok, sibling_paths, registered_paths, normalise, probe_errors
):
    if not listing_ok:
        return None
    if sibling_paths is None:
        probe_errors.append(PROBE_SIBLING_SCAN)
        return None
    # The collector supplies the sibling set. Every supplied path that
    # is not registered is unregistered stock. No name-prefix gate: the
    # collector, not the core, decides which physical siblings are
    # stock. Both sides are normalised so a relative collector root
    # cannot double-count registered paths.
    try:
        registered_set = {
            _call_normaliser(normalise, path) for path in registered_paths
        }
        return sum(
            1
            for path in sibling_paths
            if _call_normaliser(normalise, path) not in registered_set
        )
    except _NormaliseFailed:
        _note_normalise_error(probe_errors)
        return None


def _measure_volume_stock(worktree_parent, registered_paths, normalise, disk_usage, probe_errors):
    parent = worktree_parent
    if parent is None and registered_paths:
        try:
            parent = os.path.dirname(_call_normaliser(normalise, registered_paths[0]))
        except _NormaliseFailed:
            _note_normalise_error(probe_errors)
    if parent is None:
        probe_errors.append(PROBE_DISK_USAGE)
        return None, None
    try:
        usage = disk_usage(parent)
        return usage.free, usage.total
    except Exception:
        probe_errors.append(PROBE_DISK_USAGE)
        return None, None


def measure_worktree_stock(
    listing_text,
    lane_rows,
    *,
    sibling_paths=None,
    worktree_parent=None,
    disk_usage=shutil.disk_usage,
    primary_state=None,
    path_normaliser=None,
    review_reports=(),
):
    """Pure core of the gauge. ``None`` inputs mean the probe failed.

    ``primary_state`` is the collector's existence verdict on the parsed
    primary path: ``PRIMARY_PRESENT``, ``PRIMARY_MISSING`` or
    ``PRIMARY_UNREADABLE``. It defaults to ``None``, which skips the
    verdict — the core performs no filesystem probe of its own, so
    identical arguments always return identical results.

    ``path_normaliser`` defaults to lexical ``_norm_path``. The collector
    may inject a resolving normaliser the same way it injects
    ``primary_state``; the default never follows symlinks or stats.
    Symlink agreement is the caller's to supply. Every application is
    fail-closed: a raising normaliser or a non-str return is
    ``PROBE_NORMALISE``, never a collapsed tally key.
    """
    normalise = _norm_path if path_normaliser is None else path_normaliser
    probe_errors = []
    records, registered, primary, linked, registered_paths, listing_ok = (
        _measure_listing_stock(listing_text, primary_state, normalise, probe_errors)
    )
    (
        outstanding,
        landable,
        record_only,
        reclaimable_record_only,
        unmerged_landable,
        classes_measured,
    ) = _measure_outstanding_stock(
        listing_ok,
        lane_rows,
        registered_paths,
        records,
        review_reports,
        normalise,
        probe_errors,
    )
    unregistered = _measure_unregistered_stock(
        listing_ok, sibling_paths, registered_paths, normalise, probe_errors
    )
    free, total = _measure_volume_stock(
        worktree_parent, registered_paths, normalise, disk_usage, probe_errors
    )
    if unregistered is None:
        # Unknown sibling occupancy is not a comfortable zero for either
        # class. Do not snapshot stock_classes_measured from the pre-fold
        # ints while leaving review at the pre-fold record-only count.
        landable = None
        record_only = None
    else:
        landable = _fold_physical_count(landable, unregistered)
    return WorktreeStock(
        registered_worktrees=registered,
        primary_worktrees=primary,
        linked_worktrees=linked,
        outstanding_unlanded=outstanding,
        unregistered_paths=unregistered,
        volume_free_bytes=free,
        volume_total_bytes=total,
        probe_errors=tuple(probe_errors),
        outstanding_landable=landable,
        outstanding_record_only=record_only,
        reclaimable_record_only_worktrees=reclaimable_record_only,
        unmerged_landable_worktrees=unmerged_landable,
        stock_classes_measured=classes_measured,
    )


def _load_reclaim_candidates(root, load_state, run, in_use_probe=None):
    """Re-read every fact used to authorize a review-worktree removal."""
    try:
        lane_rows, review_reports = load_state()
        if lane_rows is None or review_reports is None:
            return ()
        proc = run(
            ["git", "-C", str(root), "worktree", "list", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=_LIST_TIMEOUT_SECONDS,
        )
        if proc.returncode != 0:
            return ()
        records = _parse_worktree_listing(proc.stdout)
        if not records:
            return ()
        return _reclaimable_review_paths(
            records, lane_rows, review_reports, os.path.realpath, in_use_probe=in_use_probe
        )
    except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError, _NormaliseFailed, TypeError, ValueError):
        return ()


def reclaim_completed_review_worktrees(root, *, load_state, run=subprocess.run, in_use_probe=None):
    """Remove completed review worktrees while preserving branches and lane rows.

    Completion is a newest non-superseded ready report whose commit matches the
    live review branch tip on a terminal review row.  Candidates are only hints:
    lane rows, reports, the registered worktree, its branch, its HEAD, and
    in-use/assigned-worker state are all re-read immediately before each
    deletion.  A final clean-tree probe uses the review worktree as its cwd.
    Any doubt skips the candidate; removal is never forced and no branch or
    database mutation is performed here.
    """
    root = os.path.abspath(os.fspath(root))
    candidates = _load_reclaim_candidates(root, load_state, run, in_use_probe=in_use_probe)
    removed = []
    for candidate in candidates:
        if candidate not in _load_reclaim_candidates(root, load_state, run, in_use_probe=in_use_probe):
            continue
        if os.path.realpath(candidate) == os.path.realpath(root):
            continue
        try:
            clean = run(
                ["git", "-C", candidate, "status", "--porcelain"],
                capture_output=True,
                text=True,
                timeout=_LIST_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError):
            continue
        if clean.returncode != 0 or clean.stdout:
            continue
        try:
            result = run(
                ["git", "-C", root, "worktree", "remove", candidate],
                capture_output=True,
                text=True,
                timeout=_LIST_TIMEOUT_SECONDS,
            )
        except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError):
            continue
        if result.returncode == 0:
            removed.append(candidate)
    return tuple(removed)


def _gitdir_pointer(git_file):
    """Return the gitdir path from a linked-worktree ``.git`` file, or None."""
    try:
        with open(git_file, encoding="utf-8") as handle:
            first = handle.readline()
    except (OSError, UnicodeDecodeError):
        return None
    stripped = first.strip()
    prefix = "gitdir:"
    if not stripped.lower().startswith(prefix):
        return None
    pointer = stripped[len(prefix) :].strip()
    return pointer or None


def _resolve_gitdir_pointer(git_file, pointer):
    if os.path.isabs(pointer):
        return os.path.abspath(pointer)
    return os.path.normpath(os.path.join(os.path.dirname(git_file), pointer))


def _repo_worktrees_dir(root):
    """Administrative ``.git/worktrees`` directory for the checkout at ``root``.

    A primary checkout has a ``.git`` directory; a linked worktree has a
    ``.git`` file whose gitdir pointer sits inside that administrative
    area. Missing ``.git`` falls back to the lexical primary layout so a
    planted leftover can still be recognised. Unreadable ``.git`` returns
    ``None`` so the collector can fail the sibling probe closed.
    """
    git_path = os.path.join(os.fspath(root), ".git")
    try:
        mode = os.lstat(git_path).st_mode
    except FileNotFoundError:
        return os.path.join(os.path.abspath(git_path), "worktrees")
    except OSError:
        return None
    if stat.S_ISREG(mode):
        pointer = _gitdir_pointer(git_path)
        if pointer is None:
            return None
        gitdir = _resolve_gitdir_pointer(git_path, pointer)
        return os.path.dirname(os.path.realpath(gitdir))
    if stat.S_ISDIR(mode):
        return os.path.join(os.path.realpath(git_path), "worktrees")
    return None


def _is_linked_worktree_of(path, worktrees_dir):
    """True when ``path`` is a linked worktree of the repo at ``worktrees_dir``.

    A linked worktree has a ``.git`` *file* (not a directory) whose
    contents are a gitdir pointer into this repository's worktrees
    administrative area. Unrelated repositories have a ``.git``
    directory. Classification failures return False so one unreadable
    sibling cannot zero or crash the scan.
    """
    git_path = os.path.join(path, ".git")
    try:
        mode = os.lstat(git_path).st_mode
    except OSError:
        return False
    if not stat.S_ISREG(mode):
        return False
    pointer = _gitdir_pointer(git_path)
    if pointer is None:
        return False
    gitdir = os.path.realpath(_resolve_gitdir_pointer(git_path, pointer))
    admin = os.path.realpath(worktrees_dir)
    try:
        common = os.path.commonpath([gitdir, admin])
    except ValueError:
        return False
    return common == admin and gitdir != admin


def _list_sibling_paths(parent, repo_root):
    """Linked worktrees of ``repo_root`` among the immediate children of ``parent``.

    Unrelated repositories and non-git directories are not stock. A
    sibling that cannot be classified is skipped, not a scan failure.
    """
    worktrees_dir = _repo_worktrees_dir(repo_root)
    if worktrees_dir is None:
        raise OSError("cannot resolve repository worktrees directory")
    found = []
    for name in os.listdir(parent):
        path = os.path.join(parent, name)
        try:
            is_dir = os.path.isdir(path)
        except OSError:
            continue
        if is_dir and _is_linked_worktree_of(path, worktrees_dir):
            found.append(path)
    return sorted(found)


def _first_listed_path(text):
    """Verbatim path field of the first porcelain record, or None.

    Derived from the same parsed records the collector already hands to
    the core, so a CRLF listing cannot disagree with the parser on the
    primary spelling.
    """
    records = _parse_worktree_listing(text)
    if not records:
        return None
    return records[0].path


def _probe_primary(path):
    """Existence verdict for the parsed primary path.

    A plain directory test cannot separate a checkout that is missing
    from one it merely cannot stat — both answer False — so stat
    directly: ``FileNotFoundError`` means missing, any other ``OSError``
    (for example ``PermissionError`` from a parent that denies search,
    or ``NotADirectoryError``) means present but unreadable.
    """
    try:
        mode = os.stat(path).st_mode
    except FileNotFoundError:
        return PRIMARY_MISSING
    except OSError:
        return PRIMARY_UNREADABLE
    if not stat.S_ISDIR(mode):
        return PRIMARY_MISSING
    return PRIMARY_PRESENT


def collect_worktree_stock(
    root,
    *,
    lane_rows=(),
    review_reports=(),
    run=subprocess.run,
    disk_usage=shutil.disk_usage,
):
    """Collect the gauge reading for the checkout at ``root``.

    Each sub-probe is independently degradable: a failed listing, an
    unreadable sibling directory, or a failed volume stat degrades only
    its own fields. The sibling scan itself is a collector filesystem
    verdict: only directories whose ``.git`` file points at this
    repository's worktrees area are handed to the core as stock.
    """
    # The sibling scan runs in the parent of the root as the caller
    # spelled it: resolving through a symlink first would scan next to
    # the link's target and miss leftovers placed beside the link. The
    # gauge normalises both sides of the comparison, so the spelled
    # spelling still matches the registered set. Identity of "this
    # repository" still follows the spelled checkout's ``.git``.
    spelled_root = os.path.abspath(os.fspath(root))
    root = _norm_path(root)
    worktree_parent = os.path.dirname(spelled_root)

    try:
        proc = run(
            ["git", "-C", root, "worktree", "list", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=_LIST_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired, UnicodeDecodeError):
        listing_text = None
    else:
        listing_text = proc.stdout if proc.returncode == 0 else None

    # The existence probe lives here, in the collector, so the pure core
    # never touches the filesystem behind the caller's back.
    primary_state = None
    if listing_text is not None:
        primary_path = _first_listed_path(listing_text)
        if primary_path is not None:
            primary_state = _probe_primary(primary_path)

    try:
        sibling_paths = _list_sibling_paths(worktree_parent, spelled_root)
    except OSError:
        sibling_paths = None

    return measure_worktree_stock(
        listing_text,
        lane_rows,
        sibling_paths=sibling_paths,
        worktree_parent=worktree_parent,
        disk_usage=disk_usage,
        primary_state=primary_state,
        path_normaliser=os.path.realpath,
        review_reports=review_reports,
    )
