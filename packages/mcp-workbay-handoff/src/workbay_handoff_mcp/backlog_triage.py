"""Open-findings backlog classifier + apply (implementation note Slices 1–2).

implementation note — classify buckets:

- ``rebrand_orphaned`` — path is covered by the rename map and neither the old
  nor the mapped path resolves to a live file (safe bulk-disposition candidate).
- ``remappable`` — path maps to a *live* file via the rename map (open-preserving
  re-anchor candidate; **never** treat as rebrand_orphaned — false-close guard).
- ``live`` — not rename-map covered (or no map supplied); leave alone.
- ``high_needs_human`` — high severity; never bulk-dispositioned.

implementation note — ``apply_reviewed_manifest`` executes a reviewed manifest via sanctioned
``disposition`` / ``reanchor`` ops: batched, idempotent, concurrency-skip.

The done/archived-orphan axis is **reused** from
``review_findings_queries._collect_stale_nonscratch_open_finding_items`` and
joined onto each classified row — this module does not reimplement that query.
"""

from __future__ import annotations

import os
import sqlite3
import stat
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, cast

from .enums import FindingSeverity, FindingStatus
from .review_findings_api import review_findings
from .review_findings_queries import _collect_stale_nonscratch_open_finding_items

BUCKET_REBRAND_ORPHANED = "rebrand_orphaned"
BUCKET_REMAPPABLE = "remappable"
BUCKET_LIVE = "live"
BUCKET_HIGH_NEEDS_HUMAN = "high_needs_human"

ALL_BUCKETS = (
    BUCKET_REBRAND_ORPHANED,
    BUCKET_REMAPPABLE,
    BUCKET_LIVE,
    BUCKET_HIGH_NEEDS_HUMAN,
)

ACTION_REANCHOR = "reanchor"
ACTION_DISPOSITION = "disposition"
ACTION_SKIP = "skip"

DEFAULT_APPLY_BATCH_SIZE = 200
DEFAULT_DISPOSITION_STATUS = "wontfix"
PLAN_ID = "0097"

# Git rename bulk scan (S2) and bounded orphan probe (S3) budgets.
DEFAULT_GIT_RENAME_LOG_TIMEOUT_S = 30.0
DEFAULT_ORPHAN_PROBE_TIMEOUT_S = 5.0
DEFAULT_ORPHAN_PROBE_PATH_CAP = 50


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_rename_map(rename_map: Mapping[str, str] | None) -> dict[str, str]:
    if not rename_map:
        return {}
    out: dict[str, str] = {}
    for raw_old, raw_new in rename_map.items():
        old = str(raw_old or "").strip().replace("\\", "/")
        new = str(raw_new or "").strip().replace("\\", "/")
        if not old or not new:
            continue
        out[old] = new
    return out


def _map_path(file_path: str, rename_map: dict[str, str]) -> str | None:
    """Return mapped path for *file_path*, or None if not covered by the map.

    Supports exact keys and longest directory/prefix keys, e.g. a
    ``workstate-x`` → ``workbay-x`` prefix key (brand-check: allow, implementation note).
    """
    normalized = (file_path or "").strip().replace("\\", "/")
    if not normalized or not rename_map:
        return None
    if normalized in rename_map:
        return rename_map[normalized]

    best_old: str | None = None
    best_new: str | None = None
    best_len = -1
    for old, new in rename_map.items():
        old_prefix = old.rstrip("/")
        if not old_prefix:
            continue
        if normalized == old_prefix or normalized.startswith(old_prefix + "/"):
            if len(old_prefix) > best_len:
                best_len = len(old_prefix)
                best_old = old_prefix
                best_new = new.rstrip("/")
    if best_old is None or best_new is None:
        return None
    suffix = normalized[len(best_old) :]  # includes leading '/' when present
    return f"{best_new}{suffix}" if suffix else best_new


def _default_path_is_live(rel_path: str, *, workspace_root: Path) -> bool:
    rel = (rel_path or "").strip().replace("\\", "/")
    if not rel or rel.startswith("/"):
        return False
    root = workspace_root.resolve()
    candidate = (root / rel).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    # S1: directory-anchored findings are live when the directory exists.
    # ``is_file()`` alone false-orphaned rename-map-covered package/docs roots.
    return candidate.is_file() or candidate.is_dir()


def _default_path_escapes_root(rel_path: str, *, workspace_root: Path) -> bool:
    """Fail-safe signal: True when *rel_path* cannot be *confirmed* to live
    under ``workspace_root`` — absolute, unresolvable, or a symlink whose real
    target escapes the root (e.g. this repo's ``docs/**/contracts`` mirrors).

    Such a path has *unknown* liveness, distinct from a resolved-but-missing
    file (genuinely dead). ``_default_path_is_live`` returns False for BOTH, so
    the classifier must consult this predicate before ever routing a
    rename-map-covered path into the auto-close ``rebrand_orphaned`` bucket —
    the headline "renamed-but-LIVE never rebrand_orphaned" guarantee.
    """
    rel = (rel_path or "").strip().replace("\\", "/")
    if not rel:
        return False
    if rel.startswith("/"):
        return True
    try:
        root = workspace_root.resolve()
        candidate = (root / rel).resolve()
        candidate.relative_to(root)
    except (ValueError, OSError):
        return True
    return False


# Bytes git treats as whitespace in validate_headref after ``ref:`` (not the
# full Unicode whitespace class that ``str.lstrip()`` would strip).
_GIT_HEADREF_WS = " \t\n\r"


def _validate_headref(head: Path) -> bool:
    """Port of git's ``validate_headref`` (refs.c): is *head* a well-formed HEAD.

    Accepts a symlink whose link text begins ``refs/`` (unresolved — a dangling
    symlink to ``refs/heads/main`` is a valid unborn branch), a regular file
    whose first 40 characters are hexadecimal (detached HEAD), or a regular
    file beginning ``ref:`` whose remainder, after leading space/tab/LF/CR only,
    begins ``refs/``. ``PermissionError`` is treated as success (unreadable entry
    pins as a repository; deliberate divergence from git, which walks past —
    this helper has no fatal error channel); other ``OSError`` fails.
    """
    try:
        st = os.lstat(head)
    except PermissionError:
        return True
    except OSError:
        return False

    mode = st.st_mode
    if stat.S_ISLNK(mode):
        try:
            link = os.readlink(head)
        except PermissionError:
            return True
        except OSError:
            return False
        return link.startswith("refs/")

    if not stat.S_ISREG(mode):
        return False

    try:
        with open(head, "rb") as fh:
            data = fh.read(255)
    except PermissionError:
        return True
    except OSError:
        return False

    text = data.decode("utf-8", errors="replace")
    if len(text) >= 40 and all(c in "0123456789abcdefABCDEF" for c in text[:40]):
        return True
    if text.startswith("ref:"):
        return text[4:].lstrip(_GIT_HEADREF_WS).startswith("refs/")
    return False


def _is_git_directory(suspect: Path) -> bool:
    """Port of git's ``is_git_directory`` (setup.c): three filesystem checks.

    Requires a well-formed ``HEAD`` via :func:`_validate_headref`, then that
    ``objects`` and ``refs`` are accessible under the *common* directory with
    ``os.access(..., X_OK)`` (git's probe — executable bit, not is-dir). The
    common directory defaults to *suspect* only when ``suspect/commondir`` is
    absent. When the entry is present it must be a readable regular file
    (after following a symlink): a zero-length file is refused (git dies with
    ``failed to read commondir``); otherwise the whole body is read and a
    trailing run of CR/LF only is stripped. A non-empty result names the
    common directory, resolved against *suspect* when relative; a body that
    is only trailing CR/LF leaves the default (*suspect*) in place (git
    accepts that shape). A present but unusable entry rejects the candidate
    (returns False): non-regular target (directory, FIFO, whether named
    directly or reached through a link — so ``open`` never blocks), broken
    symlink, undecodable body, or any other read/probe failure. Only a missing
    ``commondir`` entry keeps the default common directory. Linked worktrees
    hold ``HEAD`` + ``commondir`` without local ``objects``/``refs``; submodule
    gitdirs under ``.git/modules`` hold all three locally with no
    ``commondir``. ``PermissionError`` while reading ``commondir`` is the sole
    deliberate pin (returns True; git walks past — this helper has no fatal
    error channel); every other present-but-unusable probe failure rejects.
    """
    if not _validate_headref(suspect / "HEAD"):
        return False

    common = suspect
    try:
        commondir = suspect / "commondir"
        try:
            # Existence without following: a missing entry keeps the default
            # common dir. A present entry that cannot be opened as a regular
            # file must reject — not fall through as if the entry were absent.
            os.lstat(commondir)
        except FileNotFoundError:
            pass
        else:
            # Dereference deliberately: git's open() follows a symlink to a
            # regular file. S_ISREG still refuses a FIFO target (stat never
            # blocks on a FIFO; only open does), so the long-lived-server
            # hang guard survives for both a named FIFO and a link to one.
            try:
                cd_st = os.stat(commondir)
            except FileNotFoundError:
                # Broken symlink: entry exists, target does not.
                return False
            if not stat.S_ISREG(cd_st.st_mode):
                return False
            # Zero-length file: git refuses with "failed to read commondir".
            # Newlines-only (non-zero size, empty after CR/LF strip) keeps the
            # default common dir and must not take this arm.
            if cd_st.st_size == 0:
                return False
            with open(commondir, "rb") as fh:
                raw = fh.read().decode("utf-8")
            # Whole file, trailing CR/LF run only (not a first-line read; not
            # spaces/tabs). Matches git's commondir parse.
            body = raw.rstrip("\r\n")
            if body:
                common_path = Path(body)
                common = common_path if common_path.is_absolute() else suspect / common_path
    except PermissionError:
        return True
    except (OSError, UnicodeDecodeError, ValueError):
        # ValueError: NUL in the path must not abort the walk (not an OSError).
        # Present-but-unusable commondir rejects the candidate (git: failed to
        # read commondir); only PermissionError above is the deliberate pin.
        return False

    for name in ("objects", "refs"):
        try:
            # git probes with access(X_OK): accepts an executable regular file,
            # rejects mode-644 files and unsearchable (mode-000) directories.
            # os.access returns False on permission failure rather than raising.
            if not os.access(common / name, os.X_OK):
                return False
        except (OSError, ValueError):
            return False
    return True


def _ascend_to_repo_root(start: Path) -> Path:
    """Pin a *derived* liveness base to the enclosing repo root.

    Returns the nearest ancestor (including *start*) whose ``.git`` entry
    actually denotes a repository, else *start* unchanged. Validation is
    filesystem-only (no shell-out to git) and ports the core of git's
    ``is_git_directory`` / ``validate_headref`` so litter shapes git refuses
    do not truncate the walk.

    - Entry probe uses ``os.lstat`` on the candidate ``.git`` path (not
      ``Path.exists``, which can swallow or mis-report ``PermissionError``).
      Missing → skip. ``PermissionError`` → accept the candidate when the
      candidate directory itself is reachable (unreadable ``.git`` or
      mode-000 repo dir); when the candidate is also unreadable, keep
      walking (an ancestor is the unreadable repo). Other ``OSError`` → skip.
      When the entry is a symlink, a second dereferencing ``os.stat``
      supplies the type (an ``lstat`` mode is neither ``S_ISREG`` nor
      ``S_ISDIR`` for a link, so both arms below would be skipped). That
      probe carries its own arms: a broken link → skip; ``PermissionError``
      on the target → accept the candidate *without* the candidate
      reachability re-check the ``lstat`` arm above applies; other
      ``OSError`` (including ``ELOOP``) → skip.
    - A ``.git`` *file* (linked-worktree pointer) is accepted only when its
      raw text begins at byte zero with the exact literal ``gitdir: ``
      (lowercase, single space after the colon; no leading whitespace, no
      leading blank lines, no case folding). The remainder of the *whole*
      buffer after that prefix, after stripping a trailing run of CR and LF
      only (spaces and tabs stay part of the path; interior newlines and
      non-trailing CR stay too — same rule as ``commondir``), must be a
      non-empty path. Relative payloads are resolved against the directory
      that holds the ``.git`` file (never against the process cwd). The
      pointer *target* must pass :func:`_is_git_directory` (same checks as a
      ``.git`` directory). ``PermissionError`` while reading the file accepts
      the candidate; unreadable targets pin via the helpers' own
      ``PermissionError`` arms. Other ``OSError`` or ``UnicodeDecodeError``
      while reading is litter. An invalid target is walked past (this helper
      has no error channel; walking past finds the enclosing repository).
    - A ``.git`` *directory* is accepted when :func:`_is_git_directory` holds:
      (1) well-formed ``HEAD`` via :func:`_validate_headref` (symlink into
      ``refs/``, ``ref:`` + ``refs/``, or 40-char hex detached oid);
      (2) ``objects`` and (3) ``refs`` under the common directory
      (``commondir`` when present and non-empty, else the ``.git`` directory
      itself), each probed with ``access(X_OK)``. Failure → litter (keep
      walking). ``PermissionError`` inside the helpers accepts the candidate.
    - Initial ``start.resolve()`` catches ``OSError``, ``RuntimeError``
      (symlink loops), and ``ValueError`` (an embedded NUL byte that the os
      layer rejects before it can ever become an errno) and returns *start*
      unchanged so probe errors never abort the walk.

    Deliberate divergences from git (three, each intentional):

    (a) Permission arms: git walks past a candidate it cannot read and, if
    the walk ends empty, dies with a fatal error. This helper has no error
    channel at all: it must return a Path, and every caller treats that Path
    as the repository root. Walking past an unreadable candidate would
    resolve to a higher repository (or the filesystem root) and mis-bucket
    findings against the wrong identity, auto-closing work nobody examined.
    Pinning the unreadable candidate is the visible no-op safety stop
    (triage finds nothing).

    (b) An invalid ``.git`` *file* is a hard stop in git, not a walk-past.
    Host-verified: with a real checkout at *repo* and
    ``repo/deep/.git`` a regular file containing the text ``not a gitdir``,
    git exits 128 (``fatal: invalid gitfile format``) and resolves no
    repository, while this helper walks past and pins *repo*. Git is itself
    asymmetric here: for an invalid ``.git`` *directory* with a bad
    ``HEAD``, git *does* keep walking, and both sides agree on the parent.
    This helper walks past both. Walking past is deliberate because the
    helper has no error channel and must return a Path; pinning a directory
    git refuses to operate in would be worse than attributing to the
    enclosing checkout.

    (c) Bare repositories are deliberately not recognised. This helper only
    ever inspects a ``.git`` entry, so a directory holding ``HEAD``,
    ``objects`` and ``refs`` directly is never a candidate. Host-verified:
    git resolves a bare repository root that this helper walks straight
    past. Reason: bare-shaped directories in a normal checkout are the
    internal stores (``.git`` itself and ``.git/modules/NAME``), and for
    finding attribution the enclosing checkout is the correct bucket, not
    the store. This is a scope boundary, not an oversight; bare support is
    not implemented.

    Used only for derived roots (runtime config / ``Path.cwd()`` fallback)
    so a worktree or subdirectory cwd still pins to its enclosing repo and
    genuinely live files are not mis-bucketed ``rebrand_orphaned``. Explicit
    ``workspace_root`` declarations bypass this helper entirely: an ancestor
    repository must not relocate a caller-declared root (see
    ``_resolve_workspace_root``).
    """
    try:
        base = start.resolve()
    except (OSError, RuntimeError, ValueError):
        return start
    for candidate in (base, *base.parents):
        try:
            git_entry = candidate / ".git"
            try:
                entry_st = os.lstat(git_entry)
            except FileNotFoundError:
                continue
            except PermissionError:
                # Unreadable .git (or unsearchable candidate) pins only when
                # the candidate itself is reachable. PermissionError under a
                # deeper path means an unreadable *ancestor* — keep walking
                # until that ancestor is the candidate.
                try:
                    os.lstat(candidate)
                except PermissionError:
                    continue
                except OSError:
                    continue
                return candidate
            except OSError:
                continue

            entry_mode = entry_st.st_mode
            if stat.S_ISLNK(entry_mode):
                # A .git symlink denotes a repository when it resolves, so
                # the type comes from a dereferencing probe; lstat above
                # reports the link itself and matches neither arm below.
                try:
                    entry_mode = os.stat(git_entry).st_mode
                except FileNotFoundError:
                    continue
                except PermissionError:
                    return candidate
                except OSError:
                    continue

            # Linked worktree: .git is a file with a gitdir: pointer.
            if stat.S_ISREG(entry_mode):
                try:
                    # Binary read so universal-newlines cannot swallow CR as
                    # a newline; we only rstrip an explicit CR/LF run below.
                    text = git_entry.read_bytes().decode("utf-8")
                except PermissionError:
                    return candidate
                except (OSError, UnicodeDecodeError):
                    continue
                # Exact gitfile prefix at byte zero (git rejects all variants).
                # Whole buffer after prefix; strip only a trailing CR/LF run
                # (spaces/tabs stay; interior newlines stay — same as
                # commondir). First-line partition would false-accept a valid
                # path followed by a second-line garbage that git refuses.
                text = text.rstrip("\r\n")
                if not text.startswith("gitdir: "):
                    continue
                payload = text[len("gitdir: ") :]
                if not payload:
                    continue
                try:
                    target = Path(payload)
                    if not target.is_absolute():
                        target = candidate / target
                    if not _is_git_directory(target):
                        continue
                except (OSError, ValueError):
                    continue
                return candidate

            # Primary checkout: .git is a directory; require is_git_directory
            # (well-formed HEAD + common objects/refs), not mere HEAD presence.
            if stat.S_ISDIR(entry_mode):
                if not _is_git_directory(git_entry):
                    continue
                return candidate
        except OSError:
            continue
    return base


def _resolve_workspace_root(workspace_root: Path | None) -> Path:
    if workspace_root is not None:
        # Declared roots are authoritative: resolve only, never ascend.
        # An enclosing .git (e.g. /tmp/.git above a TMPDIR fixture) must not
        # relocate the liveness base; ascent applies only to derived roots.
        try:
            return Path(workspace_root).resolve()
        except OSError:
            return Path(workspace_root)
    try:
        from .runtime import get_runtime_config

        return _ascend_to_repo_root(get_runtime_config().workspace_root)
    except Exception:
        return _ascend_to_repo_root(Path.cwd())


def _parse_git_rename_log(stdout: str) -> dict[str, str]:
    """Parse ``git log -M --diff-filter=R --name-status`` lines into old→new pairs.

    Score forms ``R100`` / ``R026`` are accepted (score is glued to ``R``).
    Git log is newest-first; the first observation of an *old* key wins so the
    most recent rename for that source path is kept before chaining.
    """
    pairs: dict[str, str] = {}
    for raw_line in (stdout or "").splitlines():
        line = raw_line.strip()
        if not line or line[0] != "R":
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            parts = line.split()
        if len(parts) != 3:
            continue
        status, old_raw, new_raw = parts
        if not status.startswith("R"):
            continue
        old = str(old_raw or "").strip().replace("\\", "/")
        new = str(new_raw or "").strip().replace("\\", "/")
        if not old or not new:
            continue
        if old not in pairs:
            pairs[old] = new
    return pairs


def _chain_rename_map(pairs: Mapping[str, str]) -> dict[str, str]:
    """Transitive closure of rename pairs to a fixed point, with cycle guard.

    Each source maps to the newest reachable path. Cyclic or malformed chains
    terminate (do not hang) and keep the last safe hop before the cycle.
    """
    result: dict[str, str] = {}
    for old in pairs:
        seen: set[str] = set()
        current = old
        last = old
        while current in pairs and current not in seen:
            seen.add(current)
            last = pairs[current]
            current = last
        if last != old:
            result[old] = last
    return result


def _run_git(
    args: list[str],
    *,
    cwd: Path,
    timeout_s: float,
) -> str | None:
    """Run a git command; return stdout on success, else None. Never raises."""
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout


def build_git_rename_map(
    workspace_root: Path,
    *,
    runner: Callable[[list[str], Path, float], str | None] | None = None,
    timeout_s: float = DEFAULT_GIT_RENAME_LOG_TIMEOUT_S,
) -> dict[str, str]:
    """Build a transitive old→newest rename map from git history (S2).

    One bulk scan: ``git log -M --diff-filter=R --name-status --format= --all``.
    Returns ``{}`` on any failure (git absent, non-zero, not a repo, timeout).
    Never raises — classify must never fail because git is unavailable.
    """
    run = runner or (lambda args, cwd, timeout: _run_git(args, cwd=cwd, timeout_s=timeout))
    try:
        stdout = run(
            ["log", "-M", "--diff-filter=R", "--name-status", "--format=", "--all"],
            Path(workspace_root),
            float(timeout_s),
        )
        if stdout is None:
            return {}
        pairs = _parse_git_rename_log(stdout)
        return _chain_rename_map(pairs)
    except Exception:
        return {}


def _default_orphan_escalation_probe(
    dead_path: str,
    *,
    workspace_root: Path,
    path_is_live: Callable[[str], bool],
    runner: Callable[[list[str], Path, float], str | None] | None = None,
    timeout_s: float = DEFAULT_ORPHAN_PROBE_TIMEOUT_S,
) -> str | None:
    """Low-similarity rename probe for a would-be-orphan path (S3).

    1. Find the commit that deleted *dead_path*.
    2. Re-diff that single commit at ``-M10%``.
    3. If a rename pair's target is live, return that target; else None.

    Failure/timeout → None (keep original classification). Never raises.
    """
    rel = (dead_path or "").strip().replace("\\", "/")
    if not rel:
        return None
    run = runner or (lambda args, cwd, timeout: _run_git(args, cwd=cwd, timeout_s=timeout))
    root = Path(workspace_root)
    try:
        sha_out = run(
            ["log", "--diff-filter=D", "--pretty=format:%H", "-1", "--", rel],
            root,
            float(timeout_s),
        )
        if not sha_out:
            return None
        sha = sha_out.strip().splitlines()[0].strip() if sha_out.strip() else ""
        if not sha:
            return None
        show_out = run(
            ["show", sha, "-M10%", "--name-status", "--format="],
            root,
            float(timeout_s),
        )
        if show_out is None:
            return None
        for raw_line in show_out.splitlines():
            line = raw_line.strip()
            if not line or line[0] != "R":
                continue
            parts = line.split("\t")
            if len(parts) != 3:
                parts = line.split()
            if len(parts) != 3:
                continue
            status, old_raw, new_raw = parts
            if not status.startswith("R"):
                continue
            old = str(old_raw or "").strip().replace("\\", "/")
            new = str(new_raw or "").strip().replace("\\", "/")
            if old != rel or not new:
                continue
            if path_is_live(new):
                return new
        return None
    except Exception:
        return None


def classify_open_findings(
    conn: sqlite3.Connection,
    rename_map: Mapping[str, str] | None,
    *,
    workspace_root: Path | None = None,
    path_is_live: Callable[[str], bool] | None = None,
    use_git_rename_map: bool = False,
    git_rename_map_builder: Callable[[], Mapping[str, str]] | None = None,
    orphan_escalation_probe: Callable[[str], str | None] | None = None,
    orphan_probe_path_cap: int = DEFAULT_ORPHAN_PROBE_PATH_CAP,
) -> dict[str, Any]:
    """Classify every open finding into triage buckets. Pure read; no writes.

    Parameters
    ----------
    conn:
        Open handoff DB connection (read-only usage).
    rename_map:
        Old-path → new-path map (implementation note). Empty/None degrades safely: no
        finding is classified as rebrand_orphaned or remappable *from the
        caller map alone*. Git-derived coverage is opt-in via
        *use_git_rename_map* or an injected *git_rename_map_builder*.
    workspace_root:
        Root for filesystem live-path checks. Defaults to runtime config
        workspace, then cwd.
    path_is_live:
        Optional override for the live-file predicate (tests inject this).
    use_git_rename_map:
        When True and no *git_rename_map_builder* is injected, build a
        git-derived rename map via :func:`build_git_rename_map`. Default
        False restores the empty-caller-map fail-safe (no unattended
        remappable / rebrand_orphaned writes).
    git_rename_map_builder:
        Optional zero-arg provider of a git-derived rename map (S2). When
        injected, takes precedence over *use_git_rename_map* (tests rely
        on this seam). Inject ``lambda: {}`` to avoid shelling out.
    orphan_escalation_probe:
        Optional ``dead_path → live_successor|None`` probe (S3). Defaults to
        the bounded low-similarity git probe. Inject a no-op or recorder in
        tests. The probe may only move findings *out* of ``rebrand_orphaned``.
    orphan_probe_path_cap:
        Max number of would-be-orphan paths to probe (S3 bound).
    """
    root = _resolve_workspace_root(workspace_root)
    is_live = path_is_live or (lambda p: _default_path_is_live(p, workspace_root=root))

    # S2: git-derived map is opt-in. An injected builder always wins (tests);
    # otherwise build only when use_git_rename_map is True. Default off keeps
    # the empty-caller-map fail-safe for production CLI callers.
    if git_rename_map_builder is not None:
        try:
            git_map = _normalize_rename_map(git_rename_map_builder())
        except Exception:
            git_map = {}
    elif use_git_rename_map:
        git_map = build_git_rename_map(root)
    else:
        git_map = {}
    caller_map = _normalize_rename_map(rename_map)
    # Git layer composes under the caller map (caller wins on collision).
    normalized_map = {**git_map, **caller_map}

    # S3: only would-be-orphans may be probed; bound how many *distinct* paths
    # we touch. Memoize by dead path so repeated findings on the same path
    # share one cap slot and one probe invocation (B2).
    probe_cap = max(0, int(orphan_probe_path_cap))
    probe_memo: dict[str, str | None] = {}
    if orphan_escalation_probe is not None:
        probe_fn: Callable[[str], str | None] = orphan_escalation_probe
    else:

        def probe_fn(dead: str) -> str | None:
            return _default_orphan_escalation_probe(
                dead,
                workspace_root=root,
                path_is_live=is_live,
            )

    # Done/archived-orphan axis — reuse existing collector (do not reimplement).
    stale_items = _collect_stale_nonscratch_open_finding_items(conn, batch_size=None)
    stale_by_db_id: dict[int, dict[str, object]] = {int(cast(int, item["finding_db_id"])): item for item in stale_items}

    rows = conn.execute(
        """
        SELECT rf.id, rf.task_ref, rf.finding_id, rf.severity, rf.file_path, rf.description
        FROM review_findings rf
        WHERE rf.status = ?
        ORDER BY rf.task_ref, rf.id
        """,
        (FindingStatus.OPEN.value,),
    ).fetchall()

    buckets: dict[str, list[dict[str, Any]]] = {name: [] for name in ALL_BUCKETS}

    for row in rows:
        finding_db_id = int(row["id"])
        task_ref = str(row["task_ref"])
        finding_id = str(row["finding_id"])
        severity = str(row["severity"])
        file_path = str(row["file_path"] or "")
        description = str(row["description"] or "")

        stale = stale_by_db_id.get(finding_db_id)
        mapped_path = _map_path(file_path, normalized_map)

        old_live = bool(file_path) and is_live(file_path)
        mapped_live = is_live(mapped_path) if mapped_path else False
        probe_outcome: str | None = None

        # Fail-safe liveness signal: a path whose liveness cannot be confirmed
        # under the workspace root (absolute, unresolvable, or symlink-escaping
        # — e.g. the repo's docs/**/contracts mirrors, or a wrong cwd) is
        # UNKNOWN, not dead. It must never fall into the auto-close
        # rebrand_orphaned bucket. Uses the real filesystem regardless of any
        # injected ``path_is_live`` predicate (escape is an FS-safety axis).
        liveness_unknown = (bool(file_path) and _default_path_escapes_root(file_path, workspace_root=root)) or (
            _default_path_escapes_root(mapped_path, workspace_root=root) if mapped_path else False
        )

        if severity == FindingSeverity.HIGH.value:
            bucket = BUCKET_HIGH_NEEDS_HUMAN
            if mapped_path and mapped_live:
                rationale = (
                    "high severity with live mapped path; requires explicit "
                    "human re-anchor-or-disposition decision (no bulk write)"
                )
            elif mapped_path and not (old_live or mapped_live):
                rationale = (
                    "high severity on rebrand-orphaned path; requires explicit human disposition (no bulk write)"
                )
            else:
                rationale = "high severity open finding; requires human triage"
        elif mapped_path is not None and mapped_live:
            # False-close guard: renamed-but-live must never land in rebrand_orphaned.
            # The re-anchor target MUST be live — bucketing remappable on old_live
            # alone (pre-rename path on disk, mapped target dead) would let Slice-2
            # re-anchor rewrite file_path to a DEAD target (BR-S1-01). Only claim
            # "maps to live file" when the mapped target is actually live.
            bucket = BUCKET_REMAPPABLE
            rationale = (
                f"path maps to live file via rename_map "
                f"({file_path!r} → {mapped_path!r}); open-preserving re-anchor candidate"
            )
        elif mapped_path is not None and old_live:
            # Pre-rename/old path still on disk but the mapped re-anchor target is
            # dead. NOT remappable (re-anchor target is dead) and NOT
            # rebrand_orphaned (a file still exists on disk, so it is not orphaned
            # — auto-close would false-close it). Leave live for explicit human
            # handling (BR-S1-01).
            bucket = BUCKET_LIVE
            rationale = (
                f"old path still on disk but mapped re-anchor target is dead "
                f"({file_path!r} → {mapped_path!r}); not remappable (dead target) and "
                f"not rebrand_orphaned (a file exists) — leave live for human handling"
            )
        elif mapped_path is not None and liveness_unknown:
            # Fail-safe: rename-map-covered but liveness unconfirmable
            # (out-of-root / symlink-escape / unresolvable). Never auto-close;
            # leave live for explicit human handling — a genuinely LIVE renamed
            # file behind a symlink mirror must not be treated as an orphan.
            bucket = BUCKET_LIVE
            rationale = (
                f"path covered by rename_map but liveness is unconfirmable "
                f"(out-of-root/symlink-escape/unresolvable: {file_path!r} → {mapped_path!r}); "
                f"fail-safe to live, never rebrand_orphaned"
            )
        elif mapped_path is not None:
            # Would-be rebrand_orphaned. S3 fail-safe: bounded low-similarity
            # probe may only promote out of this destructive bucket.
            # B2: memoize by dead path; charge cap per distinct path not per visit.
            successor: str | None = None
            any_candidate_resolved = False
            cap_blocked_unresolved = False
            for dead_candidate in (mapped_path, file_path):
                if not dead_candidate or is_live(dead_candidate):
                    continue
                cand = str(dead_candidate).strip().replace("\\", "/")
                if not cand:
                    continue
                if cand in probe_memo:
                    any_candidate_resolved = True
                    found = probe_memo[cand]
                elif len(probe_memo) < probe_cap:
                    try:
                        raw_found = probe_fn(cand)
                    except Exception:
                        raw_found = None
                    found_norm: str | None = None
                    if raw_found:
                        found_norm = str(raw_found).strip().replace("\\", "/") or None
                        if found_norm and not is_live(found_norm):
                            found_norm = None
                    probe_memo[cand] = found_norm
                    any_candidate_resolved = True
                    found = found_norm
                else:
                    # Budget exhausted before this path could be probed.
                    cap_blocked_unresolved = True
                    continue
                if found:
                    successor = found
                    break
            if successor is not None:
                mapped_path = successor
                mapped_live = True
                bucket = BUCKET_REMAPPABLE
                probe_outcome = "promoted"
                rationale = (
                    f"would-be orphan escalated via low-similarity rename probe "
                    f"({file_path!r} → {mapped_path!r}); open-preserving re-anchor candidate"
                )
            elif not any_candidate_resolved and cap_blocked_unresolved:
                bucket = BUCKET_REBRAND_ORPHANED
                probe_outcome = "not_probed_cap_exhausted"
                rationale = (
                    f"path covered by rename_map but neither old nor mapped path is live "
                    f"({file_path!r} → {mapped_path!r}); orphan escalation probe skipped "
                    f"because probe path budget exhausted (cap={probe_cap})"
                )
            else:
                bucket = BUCKET_REBRAND_ORPHANED
                probe_outcome = "probed_no_successor"
                rationale = (
                    f"path covered by rename_map but neither old nor mapped path is live "
                    f"({file_path!r} → {mapped_path!r})"
                )
        else:
            bucket = BUCKET_LIVE
            if not normalized_map:
                rationale = "no rename_map coverage (empty or absent map); leave live"
            else:
                rationale = "file_path not covered by rename_map; leave live"

        entry: dict[str, Any] = {
            "task_ref": task_ref,
            "finding_id": finding_id,
            "finding_db_id": finding_db_id,
            "severity": severity,
            "file_path": file_path,
            "description": description,
            "mapped_path": mapped_path,
            "bucket": bucket,
            "rationale": rationale,
            "old_path_live": old_live,
            "mapped_path_live": mapped_live,
            "stale_task": stale is not None,
            "has_live_handoff_row": (bool(stale["has_live_handoff_row"]) if stale is not None else None),
            "handoff_status": (stale["handoff_status"] if stale is not None else None),
        }
        if probe_outcome is not None:
            entry["probe_outcome"] = probe_outcome
        buckets[bucket].append(entry)

    counts = {name: len(buckets[name]) for name in ALL_BUCKETS}
    open_total = sum(counts.values())
    # degrade / rename_map_size are keyed on the *caller* map only so omitting
    # --rename-map still yields empty_rename_map even when a git layer is on.
    degrade = "empty_rename_map" if not caller_map else None
    generated_at = _utcnow_iso()
    probes_used = len(probe_memo)
    probe_cap_exhausted = probes_used >= probe_cap and probe_cap >= 0

    # internal [OBS-08]: stamp debt digest so DASHBOARD can tell
    # healthy-zero dead-path from "classifier has not run". Best-effort —
    # classify remains pure for callers that only consume the return value;
    # stamp failure never fails the classify result.
    try:
        from .review_findings_queries import (  # noqa: PLC0415
            collect_finding_debt_digest,
            stamp_finding_debt_digest,
        )

        digest = collect_finding_debt_digest(conn, workspace_root=root)
        # Prefer classifier dead-path (rebrand_orphaned) when any rename map is live.
        if normalized_map:
            digest["dead_path_count"] = counts.get(BUCKET_REBRAND_ORPHANED, 0)
        stamp_finding_debt_digest(
            digest,
            last_run_at=generated_at,
            source="classify",
            workspace_root=root,
        )
    except Exception:
        pass

    return {
        "ok": True,
        "generated_at": generated_at,
        "open_total": open_total,
        "counts": {
            **counts,
            "stale_task_open": len(stale_items),
        },
        "buckets": buckets,
        "rename_map_size": len(caller_map),
        "caller_rename_map_size": len(caller_map),
        "git_rename_map_size": len(git_map),
        "degrade": degrade,
        "probe_cap": probe_cap,
        "probes_used": probes_used,
        "probe_cap_exhausted": probe_cap_exhausted,
        "plan": PLAN_ID,
        "slice": 1,
        "mode": "classify",
    }


def _disposition_evidence_for_entry(entry: Mapping[str, Any]) -> str:
    """Build rename-map provenance string for a manifest entry."""
    explicit = entry.get("disposition_evidence")
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()
    old = str(entry.get("file_path") or "").strip()
    new = str(entry.get("mapped_path") or entry.get("target_file_path") or "").strip()
    if old and new:
        return f"{old} → {new}"
    if old:
        return f"file_path={old}"
    return "plan:0097 backlog triage"


def _resolve_entry_action(entry: Mapping[str, Any]) -> str | None:
    """Return action name, or None to skip (no bulk write)."""
    explicit = entry.get("action")
    if explicit is not None:
        action = str(explicit).strip().lower()
        if action in {ACTION_REANCHOR, ACTION_DISPOSITION, ACTION_SKIP}:
            return action
        return ACTION_SKIP

    bucket = str(entry.get("bucket") or "").strip()
    if bucket == BUCKET_REMAPPABLE:
        return ACTION_REANCHOR
    if bucket == BUCKET_REBRAND_ORPHANED:
        return ACTION_DISPOSITION
    # high_needs_human and live require explicit action; never bulk-write.
    return None


def _flatten_manifest_entries(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Accept classifier-shaped buckets or a flat reviewed ``entries`` list."""
    raw_entries = manifest.get("entries")
    if isinstance(raw_entries, list):
        return [dict(item) for item in raw_entries if isinstance(item, Mapping)]

    entries: list[dict[str, Any]] = []
    buckets = manifest.get("buckets") or {}
    if isinstance(buckets, Mapping):
        for bucket_name, items in buckets.items():
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                row = dict(item)
                row.setdefault("bucket", bucket_name)
                entries.append(row)
    return entries


def _live_finding_row(
    conn: sqlite3.Connection,
    *,
    task_ref: str,
    finding_id: str,
    finding_db_id: int | None,
) -> sqlite3.Row | None:
    if finding_db_id is not None:
        row: sqlite3.Row | None = conn.execute(
            "SELECT id, task_ref, finding_id, status, file_path, resolution_notes, severity "
            "FROM review_findings WHERE id = ? AND task_ref = ?",
            (finding_db_id, task_ref),
        ).fetchone()
        if row is not None:
            return row
    fallback: sqlite3.Row | None = conn.execute(
        "SELECT id, task_ref, finding_id, status, file_path, resolution_notes, severity "
        "FROM review_findings WHERE finding_id = ? AND task_ref = ?",
        (finding_id, task_ref),
    ).fetchone()
    return fallback


def _result_outcome(envelope: object) -> tuple[bool, dict[str, Any]]:
    """Normalize review_findings envelope (schema v2 or flat)."""
    if not isinstance(envelope, Mapping):
        return False, {"error": "non-mapping response"}
    if envelope.get("schema_version") == 2:
        data = envelope.get("data") if isinstance(envelope.get("data"), Mapping) else {}
        ok = bool(envelope.get("ok"))
        return ok, dict(data) if isinstance(data, Mapping) else {}
    ok = bool(envelope.get("ok"))
    return ok, dict(envelope)


def apply_reviewed_manifest(
    conn: sqlite3.Connection,
    manifest: Mapping[str, Any] | None,
    *,
    batch_size: int = DEFAULT_APPLY_BATCH_SIZE,
    dry_run: bool = False,
    actor: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a reviewed triage manifest via sanctioned disposition/reanchor ops.

    - Chunks entries for iteration/telemetry (``batch_size``, default 200 —
      mirrors apply_stale_findings_gc's chunk size and reports a ``batches``
      count). This is a reporting/iteration boundary only: each entry commits
      in its own sanctioned-op transaction (see below); there is **no**
      per-batch commit boundary or backpressure throttle, so ``batch_size``
      does not change durability or transaction granularity ([RES-14]).
    - Idempotent re-run no-ops ([RES-01]).
    - Concurrency-skip ([CON-11]): re-reads live status/file_path before write;
      skips rows whose status left open or whose file_path changed since classify.
    - High-severity / live buckets are skipped unless the entry carries an
      explicit ``action``.
    - Empty/missing manifest degrades safely (no writes).

    Parameters
    ----------
    conn:
        Open handoff DB connection used for live-state re-checks only. Writes
        go through ``review_findings`` (sanctioned ops), each of which opens and
        commits its own short per-entry transaction — finer-grained than one
        giant apply txn, but committed per entry, not per batch ([RES-14]).
    """
    bounded = max(1, int(batch_size))
    if not manifest:
        return {
            "ok": True,
            "mode": "apply",
            "plan": PLAN_ID,
            "slice": 2,
            "generated_at": _utcnow_iso(),
            "degrade": "empty_manifest",
            "batch_size": bounded,
            "dry_run": dry_run,
            "counts": {
                "considered": 0,
                "applied": 0,
                "reanchored": 0,
                "dispositioned": 0,
                "already_applied": 0,
                "skipped_concurrency": 0,
                "skipped_policy": 0,
                "errors": 0,
                "batches": 0,
            },
            "results": [],
        }

    entries = _flatten_manifest_entries(manifest)
    if not entries:
        return {
            "ok": True,
            "mode": "apply",
            "plan": PLAN_ID,
            "slice": 2,
            "generated_at": _utcnow_iso(),
            "degrade": "empty_manifest",
            "batch_size": bounded,
            "dry_run": dry_run,
            "counts": {
                "considered": 0,
                "applied": 0,
                "reanchored": 0,
                "dispositioned": 0,
                "already_applied": 0,
                "skipped_concurrency": 0,
                "skipped_policy": 0,
                "errors": 0,
                "batches": 0,
            },
            "results": [],
        }

    results: list[dict[str, Any]] = []
    counts = {
        "considered": 0,
        "applied": 0,
        "reanchored": 0,
        "dispositioned": 0,
        "already_applied": 0,
        "skipped_concurrency": 0,
        "skipped_policy": 0,
        "errors": 0,
        "batches": 0,
    }

    for batch_start in range(0, len(entries), bounded):
        batch = entries[batch_start : batch_start + bounded]
        counts["batches"] += 1
        for entry in batch:
            counts["considered"] += 1
            task_ref = str(entry.get("task_ref") or "").strip()
            finding_id = str(entry.get("finding_id") or "").strip()
            finding_db_id_raw = entry.get("finding_db_id")
            finding_db_id: int | None
            try:
                finding_db_id = int(finding_db_id_raw) if finding_db_id_raw is not None else None
            except (TypeError, ValueError):
                finding_db_id = None
            expected_path = str(entry.get("file_path") or "").strip().replace("\\", "/")
            base_result: dict[str, Any] = {
                "task_ref": task_ref,
                "finding_id": finding_id,
                "finding_db_id": finding_db_id,
                "bucket": entry.get("bucket"),
                "expected_file_path": expected_path or None,
            }

            if not task_ref or not finding_id:
                counts["errors"] += 1
                results.append({**base_result, "outcome": "error", "error": "task_ref and finding_id required"})
                continue

            action = _resolve_entry_action(entry)
            if action is None or action == ACTION_SKIP:
                counts["skipped_policy"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "skipped_policy",
                        "reason": "no bulk action (high_needs_human/live require explicit action)",
                    }
                )
                continue

            # B2: unprobed orphans (budget exhausted) have not earned terminal wontfix.
            # Refuse auto-disposition even when bucket is rebrand_orphaned.
            if (
                action == ACTION_DISPOSITION
                and str(entry.get("probe_outcome") or "").strip() == "not_probed_cap_exhausted"
            ):
                counts["skipped_policy"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "skipped_policy",
                        "reason": (
                            "probe_outcome=not_probed_cap_exhausted; "
                            "refuse auto-disposition until probe budget covers this path"
                        ),
                        "probe_outcome": "not_probed_cap_exhausted",
                    }
                )
                continue

            live = _live_finding_row(
                conn,
                task_ref=task_ref,
                finding_id=finding_id,
                finding_db_id=finding_db_id,
            )
            if live is None:
                counts["skipped_concurrency"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "skipped_concurrency",
                        "reason": "finding row not found at apply time",
                    }
                )
                continue

            live_status = str(live["status"])
            live_path = str(live["file_path"] or "").replace("\\", "/")
            live_notes = str(live["resolution_notes"] or "")

            if action == ACTION_REANCHOR:
                target_path = (
                    str(entry.get("mapped_path") or entry.get("target_file_path") or "").strip().replace("\\", "/")
                )
                if not target_path:
                    counts["errors"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "error",
                            "error": "reanchor requires mapped_path/target_file_path",
                        }
                    )
                    continue

                # Idempotent: already open at mapped path.
                if live_status == FindingStatus.OPEN.value and live_path == target_path:
                    counts["already_applied"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "already_applied",
                            "action": ACTION_REANCHOR,
                            "file_path": live_path,
                            "status": live_status,
                        }
                    )
                    continue

                # Concurrency-skip: non-open or path drifted from classify-time.
                if live_status != FindingStatus.OPEN.value:
                    counts["skipped_concurrency"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "skipped_concurrency",
                            "reason": "live status is non-open",
                            "live_status": live_status,
                        }
                    )
                    continue
                if expected_path and live_path != expected_path:
                    counts["skipped_concurrency"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "skipped_concurrency",
                            "reason": "live file_path changed since classify",
                            "live_file_path": live_path,
                        }
                    )
                    continue

                evidence = _disposition_evidence_for_entry(entry)
                notes: str | None = (
                    str(entry.get("resolution_notes") or "").strip()
                    or f"plan:{PLAN_ID} reanchor; disposition_evidence={evidence}"
                )
                if dry_run:
                    counts["applied"] += 1
                    counts["reanchored"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "would_reanchor",
                            "action": ACTION_REANCHOR,
                            "target_file_path": target_path,
                            "resolution_notes": notes,
                        }
                    )
                    continue

                envelope = review_findings(
                    review=cast(
                        Any,
                        {
                            "operation": "reanchor",
                            "task_ref": task_ref,
                            "finding_id": finding_id,
                            "file_path": target_path,
                            "expected_file_path": expected_path or None,
                            "resolution_notes": notes,
                            **({"actor": dict(actor)} if actor else {}),
                        },
                    )
                )
                ok, data = _result_outcome(envelope)
                if not ok:
                    # Treat expected_file_path mismatch as concurrency-skip, not hard error.
                    err = str(data.get("error") or "")
                    if "concurrency skip" in err or "expected_file_path" in err:
                        counts["skipped_concurrency"] += 1
                        results.append(
                            {
                                **base_result,
                                "outcome": "skipped_concurrency",
                                "reason": err,
                                "live_file_path": data.get("current_file_path"),
                            }
                        )
                    else:
                        counts["errors"] += 1
                        results.append({**base_result, "outcome": "error", "error": err or data})
                    continue
                if data.get("already_applied"):
                    counts["already_applied"] += 1
                    results.append(
                        {
                            **base_result,
                            "outcome": "already_applied",
                            "action": ACTION_REANCHOR,
                            "file_path": target_path,
                            "status": FindingStatus.OPEN.value,
                        }
                    )
                    continue
                counts["applied"] += 1
                counts["reanchored"] += 1
                finding_raw = data.get("finding")
                finding = finding_raw if isinstance(finding_raw, Mapping) else {}
                results.append(
                    {
                        **base_result,
                        "outcome": "reanchored",
                        "action": ACTION_REANCHOR,
                        "file_path": finding.get("file_path", target_path),
                        "status": finding.get("status", FindingStatus.OPEN.value),
                    }
                )
                continue

            # disposition
            target_status = str(
                entry.get("disposition_status") or entry.get("status") or DEFAULT_DISPOSITION_STATUS
            ).strip()
            if target_status not in {"deferred", "wontfix", "fixed"}:
                counts["errors"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "error",
                        "error": f"invalid disposition status: {target_status!r}",
                    }
                )
                continue

            evidence = _disposition_evidence_for_entry(entry)
            # Idempotent: already at terminal status with evidence (or matching status).
            if live_status == target_status:
                counts["already_applied"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "already_applied",
                        "action": ACTION_DISPOSITION,
                        "status": live_status,
                        "resolution_notes": live_notes or None,
                    }
                )
                continue

            if live_status != FindingStatus.OPEN.value:
                counts["skipped_concurrency"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "skipped_concurrency",
                        "reason": "live status is non-open",
                        "live_status": live_status,
                    }
                )
                continue
            if expected_path and live_path != expected_path:
                counts["skipped_concurrency"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "skipped_concurrency",
                        "reason": "live file_path changed since classify",
                        "live_file_path": live_path,
                    }
                )
                continue

            notes = str(entry.get("resolution_notes") or "").strip() or None
            if dry_run:
                counts["applied"] += 1
                counts["dispositioned"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "would_disposition",
                        "action": ACTION_DISPOSITION,
                        "status": target_status,
                        "disposition_evidence": evidence,
                        "resolution_notes": notes,
                    }
                )
                continue

            envelope = review_findings(
                review=cast(
                    Any,
                    {
                        "operation": "disposition",
                        "task_ref": task_ref,
                        "finding_id": finding_id,
                        "status": target_status,
                        "resolution_notes": notes,
                        "disposition_evidence": evidence,
                        **({"actor": dict(actor)} if actor else {}),
                    },
                )
            )
            ok, data = _result_outcome(envelope)
            if not ok:
                counts["errors"] += 1
                results.append(
                    {
                        **base_result,
                        "outcome": "error",
                        "error": data.get("error") or data,
                    }
                )
                continue
            counts["applied"] += 1
            counts["dispositioned"] += 1
            finding_raw = data.get("finding")
            finding = finding_raw if isinstance(finding_raw, Mapping) else {}
            results.append(
                {
                    **base_result,
                    "outcome": "dispositioned",
                    "action": ACTION_DISPOSITION,
                    "status": finding.get("status", target_status),
                    "disposition_evidence": evidence,
                    "resolution_notes": finding.get("resolution_notes"),
                }
            )

    return {
        "ok": counts["errors"] == 0,
        "mode": "apply",
        "plan": PLAN_ID,
        "slice": 2,
        "generated_at": _utcnow_iso(),
        "degrade": None,
        "batch_size": bounded,
        "dry_run": dry_run,
        "counts": counts,
        "results": results,
    }
