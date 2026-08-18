"""Stdlib codemap-index freshness mini-probe for the doctor ``codemap_freshness`` facet (implementation note S3).

``workbay-system`` must NOT import ``workbay_orchestrator_mcp``, so this
duplicates a *bounded* subset of the orchestrator's
``check_codemap_index_freshness`` + ``_indexed_sha_from_status`` semantics in
stdlib. The duplication is deliberate: the doctor's other facets also read
git/filesystem directly instead of routing through the MCP.

What this probe establishes (and what it does not)
-------------------------------------------------
``confirmed`` + not-``stale`` (doctor maps to ``status=fresh``) means:

  * the CLI answered successfully, and
  * it supplied a usable indexed commit sha, and
  * that sha matches worktree HEAD under a prefix-safe rule (min 12 hex chars),
    and
  * ``detect_changes`` confirmed no content drift (or the probe never reached
    that path because an earlier check already ruled out fresh).

``detect_changes`` is consulted **only** on the would-be confirmed-fresh path
(ready status + sha match). It can only weaken a verdict (fresh → stale or
unconfirmed); it never upgrades stale/unconfirmed to fresh. Failures of
``detect_changes`` (timeout, non-zero exit, unparseable output) map to
``confirmed=False`` (doctor: ``unconfirmed``), never to a silent fresh. [AGT-10]

Everything degrades gracefully: a missing CLI yields ``cli_present=False``;
an unreadable status yields ``available=False`` with ``cli_present`` preserved
when discovery already succeeded; a ready-but-uncheckable payload yields
``available=True`` with ``confirmed=False`` (doctor: ``unconfirmed``).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

CODEMAP_CLI_NAME = "codebase-memory-mcp"
# Assignment bound: ≤5s (orchestrator uses a similar short CLI timeout). [RES-02]
INDEX_STATUS_TIMEOUT_SECONDS = 5.0
DETECT_CHANGES_TIMEOUT_SECONDS = 5.0
HEAD_TIMEOUT_SECONDS = 5.0

# Minimum hex length before a prefix match is accepted. Shorter shas collide
# too readily (e.g. "dead" vs "deadbeef…") and must not clear a mismatch.
# Stdlib-only standalone: cannot import the orchestrator package. Value must
# agree with codemap_adapter._MIN_SHA_COMPARE_LEN; enforced by
# packages/mcp-workbay-orchestrator/tests/test_codemap_compare_policy_single_source_pin.py.
_MIN_SHA_COMPARE_LEN = 12

_INDEXED_SHA_KEYS = ("head_sha", "commit_sha", "git_sha", "indexed_commit", "revision")
# Absent / empty status is *not* evidence of readiness.
_READY_STATUS_FLAGS = frozenset({"ready", "ok", "fresh", "indexed"})
_STALE_STATUS_FLAGS = frozenset({"stale", "outdated", "dirty", "needs_refresh"})

REINDEX_REMEDY_PREFIX = (
    "codemap_stale: reindex with "
    f"{CODEMAP_CLI_NAME} cli index_repository"
)

_UNCONFIRMED_NOTE = (
    "CLI reported ready/status but supplied no comparable indexed commit "
    "to verify against HEAD"
)


@dataclass(frozen=True)
class CodemapFreshnessSnapshot:
    """Result of one offline-safe index-status probe. Never raises upstream.

    ``confirmed`` is True only when the probe obtained a usable indexed sha
    and could compare it to HEAD (match or mismatch), and — on the match path
    — content drift was also checkable (or drift was found, which is still a
    confirmed stale). Ready-without-sha, too-short-to-compare shas, and
    detect_changes failures leave ``confirmed=False`` so the doctor can
    surface ``unconfirmed`` instead of a false ``fresh``.
    """

    available: bool
    cli_present: bool
    stale: bool
    head_sha: str
    indexed_sha: str
    status_flag: str
    cli_path: str = ""
    note: str = ""
    confirmed: bool = False


WhichFn = Callable[[str], str | None]
RunFn = Callable[..., subprocess.CompletedProcess[str]]


def _worktree_head_sha(
    worktree_path: Path,
    *,
    run: RunFn = subprocess.run,
) -> str:
    try:
        completed = run(
            ["git", "-C", str(worktree_path), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=HEAD_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return ""
    sha = (completed.stdout or "").strip()
    if completed.returncode == 0 and sha:
        return sha
    return ""


def primary_repo_root(
    worktree_path: Path,
    *,
    run: RunFn = subprocess.run,
) -> Path | None:
    """Primary repository root for a (possibly linked) git worktree.

    Codemap indexes the *primary* checkout, not lane worktrees. Derive the
    root via ``git rev-parse --git-common-dir`` (parent of the shared ``.git``).

    Returns ``None`` on degenerate cases — never guess a project key from the
    caller's path: common-dir failure, non-``.git`` common dir (bare), or
    non-repo paths. Callers map ``None`` to the existing unconfirmed/unavailable
    shape. Implemented locally so the mini-probe stays orchestrator-free.
    """
    resolved = Path(worktree_path).expanduser().resolve()
    try:
        completed = run(
            ["git", "-C", str(resolved), "rev-parse", "--git-common-dir"],
            capture_output=True,
            text=True,
            timeout=HEAD_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    common = (completed.stdout or "").strip()
    if completed.returncode != 0 or not common:
        return None
    common_path = Path(common)
    if not common_path.is_absolute():
        common_path = resolved / common_path
    try:
        common_path = common_path.resolve()
    except OSError:
        return None
    # Bare repos and unexpected layouts: do not invent a primary root.
    if common_path.name != ".git":
        return None
    return common_path.parent


def project_key_for_root(root: Path | str) -> str:
    """Map an absolute primary-root path to the CLI project name.

    Convention: absolute path, leading ``/`` stripped, remaining ``/`` → ``-``
    (and ``\\`` → ``-``). Must be applied to the *primary* root only — never to
    a lane worktree path (those are not indexed projects).
    """
    resolved = str(Path(root).expanduser().resolve())
    if resolved.startswith("/"):
        resolved = resolved[1:]
    return resolved.replace("/", "-").replace("\\", "-")


def indexed_sha_from_status(status: Mapping[str, Any] | None) -> str:
    """Indexed commit sha from an index_status payload, when it carries one.

    Real CLI/MCP ``index_status`` nests the indexed commit under ``git.head_sha``
    (top-level keys are only edges/git/nodes/project/root_path/status). Top-level
    sha keys remain a fallback for older / synthetic shapes. Key names mirror
    orchestrator ``_indexed_sha_from_status``.
    """
    if not isinstance(status, Mapping):
        return ""

    def _first_sha(mapping: Mapping[str, Any]) -> str:
        for key in _INDEXED_SHA_KEYS:
            raw = mapping.get(key)
            if raw is not None and str(raw).strip():
                return str(raw).strip()
        return ""

    # Nested ``git`` first — measured production shape (CLI and MCP identical).
    # Take head_sha via _INDEXED_SHA_KEYS; never git.base_sha. base_sha is the
    # merge-base / compare base of the git context, not the commit the index was
    # built at. Matching on base_sha would false-confirm after HEAD advanced
    # past that base. Non-Mapping / missing / blank git degrades to "" (no raise).
    git = status.get("git")
    if isinstance(git, Mapping):
        nested = _first_sha(git)
        if nested:
            return nested

    # Fallback: top-level sha keys (tests / older payload shapes).
    return _first_sha(status)


def _extract_json_object(text: str) -> dict[str, Any] | None:
    """Parse the last JSON object line from CLI stdout (skip log lines)."""
    stripped = (text or "").strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    found: dict[str, Any] | None = None
    for line in stripped.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            found = parsed
    if found is not None:
        return found
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(stripped[start : end + 1])
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
    return None


def reindex_remedy_note(repo_path: Path | str) -> str:
    """Concrete operator remedy naming the reindex CLI invocation. [OBS-04]"""
    body = json.dumps({"repo_path": str(repo_path)}, sort_keys=True, separators=(",", ":"))
    return f"{REINDEX_REMEDY_PREFIX} '{body}'"


def _parse_boolish(value: Any) -> bool | None:
    """Parse an explicit boolean-ish value; return None when not evidence.

    Accepts real booleans and the strings ``true``/``false``/``1``/``0``
    (case-insensitive). Anything else — including the string ``"false"`` being
    truthy under bare ``bool()`` — is not treated as evidence of staleness.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value == 1:
            return True
        if value == 0:
            return False
        return None
    if isinstance(value, str):
        s = value.strip().lower()
        if s in ("true", "1"):
            return True
        if s in ("false", "0"):
            return False
    return None


def _sha_compare(head_sha: str, indexed_sha: str) -> str:
    """Compare HEAD to an indexed sha.

    Returns one of:
      * ``match`` — equal or safe prefix match (shorter side ≥ 12 hex chars)
      * ``mismatch`` — both long enough and neither is a prefix of the other
      * ``missing`` — no indexed sha
      * ``incomparable`` — present but too short / no HEAD to compare safely
    """
    head = (head_sha or "").strip().lower()
    indexed = (indexed_sha or "").strip().lower()
    if not indexed:
        return "missing"
    if not head:
        return "incomparable"
    if head == indexed:
        return "match"
    shorter = min(len(head), len(indexed))
    if shorter < _MIN_SHA_COMPARE_LEN:
        return "incomparable"
    if head.startswith(indexed) or indexed.startswith(head):
        return "match"
    return "mismatch"


def _status_verdict(
    status: Mapping[str, Any],
    *,
    head_sha: str,
    indexed_sha: str,
) -> tuple[bool, bool, str]:
    """Return ``(stale, confirmed, status_flag)`` for a parsed CLI payload.

    * ``stale=True, confirmed=True`` — explicit stale flag or sha mismatch.
    * ``stale=False, confirmed=True`` — status is in the ready allowlist AND a
      comparable indexed sha matches HEAD (sha match alone is not enough;
      caller still consults detect_changes before claiming fresh).
    * ``stale=False, confirmed=False`` — CLI answered but nothing checkable,
      or status is not affirmatively ready: doctor maps this to ``unconfirmed``.

    Ready statuses (safe for confirmed-fresh when sha also matches):
      ``ready``, ``ok``, ``fresh``, ``indexed`` — each is an affirmative claim
      that the index is usable. Everything else (including absent/blank status,
      ``error``, ``failed``, transitional names) is not readiness evidence.
    """
    status_flag = str(status.get("status") or "").strip().lower()
    boolish = _parse_boolish(status.get("stale"))
    explicit_stale = (boolish is True) or status_flag in _STALE_STATUS_FLAGS
    cmp = _sha_compare(head_sha, indexed_sha)

    if explicit_stale:
        return True, True, status_flag
    if cmp == "mismatch":
        return True, True, status_flag
    if cmp == "match":
        # Matching sha is necessary but not sufficient: an index can sit at the
        # right commit and still be unusable (status=error / failed / blank).
        # Only an allowlisted ready status may produce confirmed-fresh. [AGT-10]
        if status_flag in _READY_STATUS_FLAGS:
            return False, True, status_flag
        return False, False, status_flag
    # missing / incomparable: not evidence of stale, not evidence of fresh.
    return False, False, status_flag


def _content_drift_from_detect_payload(data: Mapping[str, Any]) -> bool | None:
    """Interpret a detect_changes JSON body.

    Returns:
      * ``True`` — content drift found
      * ``False`` — clean (no drift signals)
      * ``None`` — unusable / unknown payload shape (caller → unconfirmed)

    Mirrors orchestrator ``check_codemap_index_freshness`` keys:
    ``changed_files`` / ``changes`` / ``changed_count`` / ``stale``.
    """
    if not isinstance(data, Mapping):
        return None
    # Orchestrator precedence (lane_context_packet ~385): falsy values fall
    # through the or-chain. ``{"changed_files": [], "changes": ["src/x.py"]}``
    # must be stale, not clean — empty list is falsy so it yields to ``changes``.
    has_list_key = "changed_files" in data or "changes" in data
    changed = data.get("changed_files") or data.get("changes") or []
    changed_count = data.get("changed_count")
    stale_flag = _parse_boolish(data.get("stale"))

    recognized = False
    if isinstance(changed_count, int) and not isinstance(changed_count, bool):
        recognized = True
        if changed_count > 0:
            return True
    if has_list_key and isinstance(changed, list):
        recognized = True
        if len(changed) > 0:
            return True
    if stale_flag is not None:
        recognized = True
        if stale_flag is True:
            return True
    if not recognized:
        # Dict present but none of the orchestrator drift keys — cannot verify.
        return None
    return False


def _consult_detect_changes(
    *,
    cli_path: str,
    project: str,
    reindex_path: Path,
    run: RunFn,
) -> tuple[str, str]:
    """Run detect_changes on the would-be-fresh path.

    Returns ``(verdict, note)`` where verdict is one of:
      * ``clean`` — no content drift (fresh may stand)
      * ``stale`` — drift found (use reindex remedy note)
      * ``unconfirmed`` — could not verify (timeout / error / bad payload)

    Never upgrades a prior non-fresh verdict; caller only invokes this after
    status+sha already look confirmed-fresh. Never raises. [RES-02][AGT-10]

    Uses the non-deprecated flag form: ``cli detect_changes --project <name>``.
    The CLI requires ``project`` (not ``repo_path``); raw JSON is deprecated.
    """
    # Argv: <cli> cli detect_changes --project <indexed-project-name>
    argv = [cli_path, "cli", "detect_changes", "--project", project]
    try:
        completed = run(
            argv,
            capture_output=True,
            text=True,
            timeout=DETECT_CHANGES_TIMEOUT_SECONDS,
            check=False,
        )
    except FileNotFoundError:
        return "unconfirmed", "codemap detect_changes CLI disappeared mid-call"
    except subprocess.TimeoutExpired:
        return "unconfirmed", "codemap detect_changes timed out"
    except OSError as exc:
        return "unconfirmed", f"codemap detect_changes os_error: {exc}"

    if completed.returncode != 0:
        return "unconfirmed", f"codemap detect_changes exit={completed.returncode}"

    data = _extract_json_object(completed.stdout or "")
    if data is None:
        return "unconfirmed", "codemap detect_changes output unparseable"

    drift = _content_drift_from_detect_payload(data)
    if drift is True:
        # Content-drift path is only reached after sha match + ready status.
        # Name both facts so a doctor line with identical head=/indexed= still
        # explains why the verdict is stale (not a sha-compare bug). Keep the
        # concrete reindex remedy — that is what the operator acts on.
        # Wording must differ from the index_status stale path below.
        return (
            "stale",
            f"sha match; content drift via detect_changes; "
            f"{reindex_remedy_note(reindex_path)}",
        )
    if drift is None:
        return (
            "unconfirmed",
            "codemap detect_changes payload missing drift fields "
            "(changed_files/changes/changed_count/stale)",
        )
    return "clean", "sha match; content verified (no drift)"


def probe_codemap_freshness(
    worktree_path: str | Path,
    *,
    which: WhichFn | None = None,
    run: RunFn | None = None,
) -> CodemapFreshnessSnapshot:
    """Never raises; missing CLI / timeout / bad JSON → typed unavailable.

    ``which`` / ``run`` are injectable seams for deterministic unit tests
    ([TEST-08]); production callers leave them unset.

    Post-discovery failures keep ``cli_present=True`` so the doctor surfaces
    ``unavailable`` rather than a false ``unconfigured`` skip.
    """
    which_fn: WhichFn = which if which is not None else shutil.which
    run_fn: RunFn = run if run is not None else subprocess.run
    # Track whether CLI discovery succeeded *before* later steps can raise.
    cli_discovered = False

    def which_tracked(name: str) -> str | None:
        nonlocal cli_discovered
        path = which_fn(name)
        if path:
            cli_discovered = True
        return path

    try:
        return _probe_codemap_freshness_impl(
            Path(worktree_path).expanduser().resolve(),
            which=which_tracked,
            run=run_fn,
        )
    except Exception as exc:  # noqa: BLE001 -- the facet must never crash `make doctor`
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=cli_discovered,
            stale=False,
            head_sha="",
            indexed_sha="",
            status_flag="",
            note=f"codemap freshness probe failed: {type(exc).__name__}: {exc}",
            confirmed=False,
        )


def _worktree_context_note(primary_head: str, worktree_head: str) -> str:
    """Explicit note fragment when the caller is not the primary worktree.

    Index covers the primary repo; a lane worktree HEAD that differs is
    informational only — not an actionable-stale reindex signal.
    """
    return (
        f"index covers the primary repo at {primary_head}; "
        f"this worktree is at {worktree_head}"
    )


def _probe_codemap_freshness_impl(
    resolved_wt: Path,
    *,
    which: WhichFn,
    run: RunFn,
) -> CodemapFreshnessSnapshot:
    # Resolve primary root first: project name + HEAD comparison both key off
    # the indexed primary checkout, not the caller's lane worktree. [D2][D3]
    primary = primary_repo_root(resolved_wt, run=run)
    worktree_head = _worktree_head_sha(resolved_wt, run=run)
    if primary is None:
        # Degenerate: non-repo, bare, or common-dir failure — do not invent a
        # project key from the worktree path (would name an unindexed project).
        cli_path_early = which(CODEMAP_CLI_NAME) or ""
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=bool(cli_path_early),
            stale=False,
            head_sha=worktree_head,
            indexed_sha="",
            status_flag="",
            cli_path=cli_path_early if cli_path_early else "",
            note=(
                "codemap primary repository root unresolvable "
                "(need git worktree with shared .git; bare/non-repo unsupported)"
            ),
            confirmed=False,
        )

    primary_head = _worktree_head_sha(primary, run=run)
    # Snapshot head_sha is the sha we compare against the index (primary HEAD).
    head_sha = primary_head
    same_worktree = primary.resolve() == resolved_wt.resolve()
    project = project_key_for_root(primary)

    cli_path = which(CODEMAP_CLI_NAME)
    if not cli_path:
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=False,
            stale=False,
            head_sha=head_sha,
            indexed_sha="",
            status_flag="",
            note="codemap CLI not on PATH (optional; unconfigured)",
            confirmed=False,
        )

    # Prefer an executable path; still treat a which-hit as "present".
    if not os.path.isfile(cli_path) or not os.access(cli_path, os.X_OK):
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=True,
            stale=False,
            head_sha=head_sha,
            indexed_sha="",
            status_flag="",
            cli_path=cli_path,
            note="codemap CLI on PATH but not executable",
            confirmed=False,
        )

    # Non-deprecated flag form: cli index_status --project <name>.
    # The CLI rejects repo_path (exit 1, missing required argument: project).
    argv = [cli_path, "cli", "index_status", "--project", project]

    try:
        completed = run(
            argv,
            capture_output=True,
            text=True,
            timeout=INDEX_STATUS_TIMEOUT_SECONDS,
            check=False,
        )
    except FileNotFoundError:
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=False,
            stale=False,
            head_sha=head_sha,
            indexed_sha="",
            status_flag="",
            note="codemap CLI disappeared mid-call",
            confirmed=False,
        )
    except subprocess.TimeoutExpired:
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=True,
            stale=False,
            head_sha=head_sha,
            indexed_sha="",
            status_flag="",
            cli_path=cli_path,
            note="codemap index_status timed out",
            confirmed=False,
        )
    except OSError as exc:
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=True,
            stale=False,
            head_sha=head_sha,
            indexed_sha="",
            status_flag="",
            cli_path=cli_path,
            note=f"codemap index_status os_error: {exc}",
            confirmed=False,
        )

    # Any non-zero exit → typed unavailable (assignment; unknown ≠ bad).
    if completed.returncode != 0:
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=True,
            stale=False,
            head_sha=head_sha,
            indexed_sha="",
            status_flag="",
            cli_path=cli_path,
            note=f"codemap index_status exit={completed.returncode}",
            confirmed=False,
        )

    data = _extract_json_object(completed.stdout or "")
    if data is None:
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=True,
            stale=False,
            head_sha=head_sha,
            indexed_sha="",
            status_flag="",
            cli_path=cli_path,
            note="codemap index_status output unparseable",
            confirmed=False,
        )

    # Truthy error key → typed unavailable regardless of status/sha. An
    # error+status=error+head_sha==HEAD payload previously fell through to
    # sha-match fresh/ok; that is the silent-degradation class 0147 forbids.
    err_raw = data.get("error")
    # A falsey boolean-ish error value (false / 0 / "false" / "0") is not an
    # error — str(False)=='False' is truthy, so guard on the parsed value.
    if (
        _parse_boolish(err_raw) is not False
        and err_raw is not None
        and str(err_raw).strip()
    ):
        err = str(err_raw).strip()[:200]
        status_flag = str(data.get("status") or "").strip().lower()
        if status_flag:
            note = f"codemap index_status status={status_flag} error: {err}"
        else:
            note = f"codemap index_status error: {err}"
        return CodemapFreshnessSnapshot(
            available=False,
            cli_present=True,
            stale=False,
            head_sha=head_sha,
            indexed_sha=indexed_sha_from_status(data),
            status_flag=status_flag,
            cli_path=cli_path,
            note=note,
            confirmed=False,
        )

    indexed_sha = indexed_sha_from_status(data)
    # Compare index against *primary* HEAD, not the lane worktree HEAD. [D3]
    stale, confirmed, status_flag = _status_verdict(
        data, head_sha=head_sha, indexed_sha=indexed_sha
    )

    def _maybe_append_worktree_context(base: str) -> str:
        if same_worktree or not worktree_head:
            return base
        ctx = _worktree_context_note(primary_head or head_sha, worktree_head)
        if not base:
            return ctx
        return f"{base}; {ctx}"

    if stale:
        # index_status already stale (sha mismatch or explicit status flag)
        # before detect_changes runs. Distinct from the content-drift note so
        # operators can tell which path fired; both keep the reindex remedy.
        note = (
            f"index sha mismatch or status flagged stale; "
            f"{reindex_remedy_note(primary)}"
        )
    elif not confirmed:
        if status_flag and status_flag not in _READY_STATUS_FLAGS:
            note = (
                f"codemap index status={status_flag!r} is not a ready state "
                f"(cannot confirm freshness)"
            )
        else:
            note = _UNCONFIRMED_NOTE
        note = _maybe_append_worktree_context(note)
    else:
        # Would-be confirmed-fresh (ready + primary-sha match): consult
        # detect_changes for content drift. Cheap path only.
        drift_verdict, drift_note = _consult_detect_changes(
            cli_path=cli_path,
            project=project,
            reindex_path=primary,
            run=run,
        )
        if drift_verdict == "stale":
            stale = True
            confirmed = True
            note = drift_note
        elif drift_verdict == "unconfirmed":
            confirmed = False
            note = _maybe_append_worktree_context(drift_note)
        else:
            note = _maybe_append_worktree_context(drift_note)
    return CodemapFreshnessSnapshot(
        available=True,
        cli_present=True,
        stale=stale,
        head_sha=head_sha,
        indexed_sha=indexed_sha,
        status_flag=status_flag,
        cli_path=cli_path,
        note=note,
        confirmed=confirmed,
    )
