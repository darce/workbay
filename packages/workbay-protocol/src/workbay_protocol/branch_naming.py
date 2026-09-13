"""Canonical feature-branch naming rule.

This module is the SOLE owner of ``TASK_REF_RE``,
``derive_task_ref_candidates`` and ``format_suggested_branch_name``.
Every gate (post-checkout warn, PreToolUse block, pre-commit hard gate,
pre-push mirror) imports from here. ``workbay_handoff_mcp`` re-exports
the same objects without redefinition. See implementation note for context.

Case convention
---------------

Branches are lowercase (``feature/internal-37-foo``). Task refs in the
WorkBay handoff task table are uppercase (``internal``).
``derive_task_ref_candidates`` returns lowercase candidates; callers
``.upper()`` each candidate before intersecting against the live task
table. ``format_suggested_branch_name`` accepts task refs in either
case and lowercases its output so the formatted suggestion always
matches ``TASK_REF_RE``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field

__all__ = [
    "BRANCH_GRAMMAR_REGISTRY",
    "BranchClassification",
    "BranchGrammarEntry",
    "TASK_REF_RE",
    "__protocol_version__",
    "classify_branch",
    "derive_task_ref_candidates",
    "extract_plan_id",
    "format_lane_id_with_plan",
    "format_suggested_branch_name",
    "is_allowed_branch",
    "select_task_ref_candidate",
]

__protocol_version__ = "1"


TASK_REF_RE: re.Pattern[str] = re.compile(
    r"^feature/"
    # Task ref must start with a letter so leading-digit segments
    # ("123-foo") and leading hyphens ("-x") are rejected.
    r"(?=[a-z])"
    # Must contain at least one digit somewhere in the matched group so
    # purely alphabetic refs ("foo-bar", "no-digits-here") are rejected.
    r"(?=[a-z0-9-]*\d)"
    # >=2 hyphen-separated lowercase / digit segments; each segment is
    # non-empty. The alternation enforces a `<prefix>-<rest>` shape so
    # single-word branches like ``feature/foo`` are rejected and
    # conventional task refs (``feature/internal-37``,
    # ``feature/maint-dirty-br-01``) still match.
    r"(?P<task_ref>[a-z0-9]+(?:-[a-z0-9]+)+)"
    r"$"
)


def derive_task_ref_candidates(branch_name: str) -> list[str]:
    """Return the lowercase candidate task refs implied by a branch name.

    Returns an empty list for non-conforming branch names. For
    conforming names the algorithm walks progressively shorter prefixes
    of the matched ``task_ref`` group, dropping prefixes that no longer
    contain a digit. Callers ``.upper()`` each candidate before
    intersecting against the live task table.

    Examples
    --------
    >>> derive_task_ref_candidates("feature/internal-37-branch-naming-enforcement")
    ['internal-37-branch-naming-enforcement', 'internal-37']
    >>> derive_task_ref_candidates("feature/maint-dirty-br-01")
    ['maint-dirty-br-01', 'maint-dirty-br']
    >>> derive_task_ref_candidates("fix/foo")
    []
    """
    match = TASK_REF_RE.match(branch_name)
    if match is None:
        return []
    full = match.group("task_ref")
    segments = full.split("-")
    candidates: list[str] = []
    for end in range(len(segments), 0, -1):
        candidate = "-".join(segments[:end])
        if not _has_digit(candidate):
            # Once we drop below the digit-bearing prefix, every shorter
            # prefix is also digit-less and would never intersect with
            # the live task table — stop walking.
            break
        candidates.append(candidate)
    return candidates


def select_task_ref_candidate(
    branch_name: str,
    known_task_refs: Iterable[str] | None = None,
) -> str | None:
    """Return the canonical UPPERCASE task ref for a branch.

    - If ``known_task_refs`` is non-empty, returns the **first** candidate
      from :func:`derive_task_ref_candidates` (longest-to-shortest) whose
      uppercase form appears in ``known_task_refs``. This is the
      "most-specific registered candidate wins" rule that lets
      ``feature/<base>-<n>-fu-...`` branches resolve to the follow-up ref
      when both the base and the follow-up are registered. When the
      registry is non-empty but no candidate intersects, returns
      ``None`` — the strict invariant is that we never name a candidate
      that is absent from a populated registry (internal).
    - If ``known_task_refs`` is empty or ``None`` (no registry context
      available at all), falls back to the shortest digit-bearing
      prefix. This is the no-context degradation path that keeps
      environments without a configured registry resolving identically
      to the historical lifecycle mirror.
    - Returns ``None`` when ``branch_name`` is non-conforming (no
      candidates derivable).
    """
    candidates = derive_task_ref_candidates(branch_name)
    if not candidates:
        return None
    if known_task_refs is None:
        return candidates[-1].upper()
    known_upper = {ref.upper() for ref in known_task_refs}
    if not known_upper:
        return candidates[-1].upper()
    for candidate in candidates:
        if candidate.upper() in known_upper:
            return candidate.upper()
    return None


def format_suggested_branch_name(
    task_ref: str | None,
    *,
    slug: str | None = None,
    plan_id: str | None = None,
) -> str | None:
    """Render a "did you mean ..." branch suggestion for a task ref.

    Returns ``feature/<task-ref>`` (with optional ``-<slug>`` and, when the
    task implements a numbered plan doc, a trailing ``-plan<NNNN>`` segment)
    lowercased so the result is guaranteed to match ``TASK_REF_RE``. The plan
    segment is appended last so it never displaces the digit-bearing task-ref
    prefix that :func:`derive_task_ref_candidates` recovers — ``plan0043``
    parses exactly like a slug segment, so no resolver change is needed. When
    ``task_ref`` is empty / ``None`` (cold-start before ``task-start``),
    returns ``None`` so the caller can fall back to a generic message
    instead of crashing.
    """
    if not task_ref:
        return None
    base = f"feature/{task_ref.lower()}"
    if slug:
        base = f"{base}-{slug.lower()}"
    if plan_id:
        base = f"{base}-plan{plan_id.lower()}"
    return base


# Accept both POSIX ``/`` and Windows ``\`` separators: ``str(Path.relative_to)``
# yields backslashes on Windows, and this module ships in the cross-platform
# protocol package, so a ``/``-only pattern would silently drop the plan suffix
# there.
_PLAN_ID_RE: re.Pattern[str] = re.compile(r"(?:^|[\\/])(\d+)-[^\\/]+$")


def extract_plan_id(plan_path: str | None) -> str | None:
    """Return the numeric plan id embedded in a plan-doc path, or ``None``.

    A ``docs/plans/<NNNN>-<slug>.md`` path yields ``"<NNNN>"`` (e.g.
    ``0099-multi-plan-demo-r2.md`` -> ``"0099"``). Used to suffix
    feature-branch / worktree names with the plan a task implements
    (``feature/<task-ref>-plan<NNNN>``). Returns ``None`` when ``plan_path`` is
    empty or its basename carries no leading ``<digits>-`` prefix.
    """
    if not plan_path:
        return None
    match = _PLAN_ID_RE.search(plan_path)
    if match is None:
        return None
    return match.group(1)


# ASCII decimal digits only. Stated as an explicit ``[0-9]`` class rather
# than ``\\d`` so the alphabet does not inherit Unicode decimal digits from
# a flag someone can change later. Mint admission and the already-prefixed
# guard both derive from this single body so they cannot disagree.
_PLAN_ID_BODY = r"[0-9]+"

# Leading ``plan<digits>-`` on a lane id. Digit body only — the sole alphabet
# any real producer (``extract_plan_id``) emits. Case-insensitive so an
# already-minted id is recognized even if an operator re-supplies mixed-case
# input (``Plan0191-...``).
#
# Invariant: the mint only ever emits ``plan`` + ``_PLAN_ID_BODY`` + ``-``,
# which this guard recognises by construction. A non-ASCII-digit ``plan_id``
# is ignored (treated like absent) rather than minted: an accepted non-digit
# segment would make the guard ambiguous against ordinary lane ids that
# merely begin with the letters ``plan`` (``planning-``, ``planner-``, …),
# and a mint/guard alphabet split double-prefixes values in the gap.
_LANE_PLAN_PREFIX_RE: re.Pattern[str] = re.compile(
    rf"^plan{_PLAN_ID_BODY}-", re.IGNORECASE
)


def format_lane_id_with_plan(lane_id: str, plan_id: str | None) -> str:
    """Return ``lane_id`` with a leading plan segment when ``plan_id`` is set.

    Shape
    -----
    ``plan<digits>-<lane_id>`` — hyphen separator, plan segment **leading**.
    The segment spelling is ``plan`` + ASCII digit plan id (e.g. plan id
    ``"0190"`` -> ``plan0190-hermetic-argv-repo-01``). Non-``[0-9]``
    ``plan_id`` values are ignored (see Contract).

    Why leading, when branch names put the plan segment last
    --------------------------------------------------------
    :func:`format_suggested_branch_name` appends ``-plan<NNNN>`` last so it
    never displaces the digit-bearing task-ref prefix that
    :func:`derive_task_ref_candidates` recovers. A lane id has no such
    prefix to protect, so a leading segment is viable: it keeps the
    human-readable lane slug as the tail and still interpolates through
    every derivation site that already embeds ``lane_id`` (branch,
    worktree path, ``docs/lanes/`` keys) without touching those sites.

    Contract
    --------
    - No ``plan_id`` (``None`` or empty): return ``lane_id`` unchanged.
    - Non-ASCII-digit ``plan_id``: return ``lane_id`` unchanged, same as
      absent. Mint admission and the already-prefixed guard share one
      explicit ``[0-9]`` body (``_PLAN_ID_BODY``) so they cannot disagree:
      ``str.isdigit()`` admits superscripts and other non-decimal digits,
      and regex ``\\d`` is a different set again — a gap value would be
      minted into a segment the guard cannot re-read (double-prefix).
      ASCII digits are also the only alphabet ``extract_plan_id`` is
      expected to feed real callers, and they match branch/worktree grammar.
    - Idempotent: an id that already starts with ``plan[0-9]+-`` is
      returned unchanged (no second prefix). **The mint only ever emits
      that shape, which the guard recognises by construction.**
    - Does not validate or reject unprefixed ids; grandfathering is
      intentional for existing rows.
    """
    if not plan_id:
        return lane_id
    # Outside ASCII [0-9]: same as absent. Shared body with the guard so
    # we never mint a segment the guard cannot re-read, and do not widen
    # the guard to swallow ``planning-`` etc.
    if re.fullmatch(_PLAN_ID_BODY, plan_id) is None:
        return lane_id
    if _LANE_PLAN_PREFIX_RE.match(lane_id):
        return lane_id
    segment = f"plan{plan_id.lower()}"
    return f"{segment}-{lane_id}"


def _has_digit(value: str) -> bool:
    return any(ch.isdigit() for ch in value)


# ---------------------------------------------------------------------------
# Branch grammar registry (internal)
# ---------------------------------------------------------------------------
#
# Canonical pattern (``feature/<task-ref>``) plus each documented
# exception lives here as a single tuple of ``BranchGrammarEntry``
# rows. Consumers (``check_branch_naming``, lifecycle resolver mirror,
# bootstrap pre-commit gate) read from this registry instead of
# hand-rolling exception lists.


@dataclass(frozen=True)
class BranchGrammarEntry:
    kind: str
    regex: re.Pattern[str]
    allowed_in: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class BranchClassification:
    kind: str
    branch: str


_RELEASE_RE = re.compile(r"^release/\d+(\.\d+){1,2}(?:-[a-z0-9]+)?$")
_HOTFIX_RE = re.compile(r"^hotfix/[a-z][a-z0-9-]*$")
_MAINT_RE = re.compile(r"^maint/[a-z][a-z0-9-]*$")
_REVERT_RE = re.compile(r"^revert/[a-z][a-z0-9-]*$")
_MAIN_RE = re.compile(r"^(main|master)$")

_ALL_MODES: frozenset[str] = frozenset({"post_checkout_warn", "pretooluse", "pre_commit", "pre_push"})

BRANCH_GRAMMAR_REGISTRY: tuple[BranchGrammarEntry, ...] = (
    BranchGrammarEntry(kind="feature", regex=TASK_REF_RE, allowed_in=_ALL_MODES),
    BranchGrammarEntry(kind="release", regex=_RELEASE_RE, allowed_in=_ALL_MODES),
    BranchGrammarEntry(kind="hotfix", regex=_HOTFIX_RE, allowed_in=_ALL_MODES),
    BranchGrammarEntry(kind="maint", regex=_MAINT_RE, allowed_in=_ALL_MODES),
    BranchGrammarEntry(kind="revert", regex=_REVERT_RE, allowed_in=_ALL_MODES),
    BranchGrammarEntry(kind="main", regex=_MAIN_RE, allowed_in=_ALL_MODES),
)


def classify_branch(name: str) -> BranchClassification | None:
    """Return the :class:`BranchClassification` for ``name`` or ``None``.

    Unknown patterns return ``None`` (fail-closed); each consumer
    decides whether unknown means warn or block.
    """

    for entry in BRANCH_GRAMMAR_REGISTRY:
        if entry.regex.match(name):
            return BranchClassification(kind=entry.kind, branch=name)
    return None


def is_allowed_branch(name: str, *, mode: str) -> bool:
    """Return ``True`` when ``name`` matches a registered pattern allowed in ``mode``.

    ``mode`` is a free-form selector — consumers pass their gate name
    (``post_checkout_warn``, ``pretooluse``, ``pre_commit``,
    ``pre_push``) and the registry's per-entry ``allowed_in`` set
    decides whether to admit it.
    """

    for entry in BRANCH_GRAMMAR_REGISTRY:
        if entry.regex.match(name) and mode in entry.allowed_in:
            return True
    return False
