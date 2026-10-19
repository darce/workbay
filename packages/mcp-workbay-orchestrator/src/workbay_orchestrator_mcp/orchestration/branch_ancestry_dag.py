"""Passive ancestry-DAG view over a branch containment mapping.

``maximal_elements``, ``transitive_ancestors``, ``reap_order``, and
``discharged_by_reachability`` answer which tips need a content probe and
which other names are discharged because their commits are wholly
reachable from a tip already adjudicated safe.

Reap safety is reachability of work, not equality of tree. A safe tip
discharges its ancestors (free positives only). A safe ancestor says
nothing about its descendants — never infer "live" downward.

This module is read-only and internal. It never dispatches, never mutates
caller state, and never performs I/O of its own. The containment adapter
takes an injected command runner; the graph layer never calls it.

Citations
---------
``[GRPH-01]`` / directed acyclic order
    Model containment as a DAG, then topologically sort. Only an acyclic
    dependency graph yields a legal reap order.

``[GRPH-02]`` / cycle is one indivisible unit
    A directed cycle that is not a same-commit tie has no topological
    order. The pure layer refuses with ``AncestryCycleError`` rather than
    looping or emitting a partial permutation.

``[GRPH-04]`` / deliberate transitive closure
    "Everything under a tip" is a reachability query, not a one-hop
    check. A missed hop is a silent leak of a dominated ancestor.

``[GRPH-30]`` / loop-until-dry dedupe
    The present reaper rediscovers dominated names every round because it
    probes each row in isolation. This view exists so a later slice can
    discharge by reachability and stop resurfacing the same dead ends.

``[GRPH-31]`` / critical-path schedule, run backwards
    Reap adjudicates sinks first: maximal (uncontained) names, then the
    names they dominate. ``reap_order`` is that reverse-topological
    permutation, with name-sort ties so the output is reproducible.

``[Release It! 5.4]`` / Steady State
    For every mechanism that accumulates a resource, another must recycle
    it at the same or greater rate. Lane creation does not throttle when
    reap falls behind; the backlog must become an explicit number.

``[DDIA ch. 3]`` / compaction backlog
    Storage engines do not throttle writes when compaction lags;
    unmerged segments accumulate until the disk fills. The reap backlog
    is the same class of observable.

A guard whose degenerate input is its strongest pass is inverted. Empty
mapping and empty maximal set return empty collections, never a vacuous
all-clear.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import NamedTuple


class AncestryCycleError(ValueError):
    """Containment mapping has a directed cycle that is not a same-commit tie."""


class AncestryProbeError(ValueError):
    """A containment listing failed or was unreadable; the mapping is refused."""


class AncestryCanonError(AncestryProbeError):
    """Candidates were supplied but none had a usable canonical ref.

    An empty-looking containment map would be indistinguishable from a
    graph in which nothing is contained. Adapter failure is a typed
    refusal instead.
    """


class CommandOutcome(NamedTuple):
    """Observable listing result. Non-zero *exit_code* is failure, not emptiness."""

    stdout: str
    exit_code: int = 0


CommandRunner = Callable[[Sequence[str]], CommandOutcome]


class _Graph:
    """Normalized containment graph. Never exposed to callers."""

    def __init__(
        self,
        nodes: frozenset[str],
        containers: dict[str, frozenset[str]],
        strict_containers: dict[str, frozenset[str]],
    ) -> None:
        self.nodes = nodes
        self.containers = containers
        self.strict_containers = strict_containers


def _normalize(
    contained_by: Mapping[str, Collection[str]],
) -> dict[str, frozenset[str]]:
    """Copy the mapping; include dangling container names; drop self-entries."""
    nodes: set[str] = set()
    raw: dict[str, set[str]] = {}
    for key, vals in contained_by.items():
        if not isinstance(key, str) or not key:
            continue
        nodes.add(key)
        cleaned: set[str] = set()
        for item in vals:
            if not isinstance(item, str) or not item or item == key:
                continue
            cleaned.add(item)
            nodes.add(item)
        raw[key] = cleaned
    return {name: frozenset(raw.get(name, ())) for name in nodes}


def _strongly_connected(
    containers: Mapping[str, frozenset[str]],
) -> list[list[str]]:
    """Tarjan SCCs over contained → container edges."""
    index = 0
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    components: list[list[str]] = []

    def strongconnect(vertex: str) -> None:
        nonlocal index
        indices[vertex] = index
        lowlink[vertex] = index
        index += 1
        stack.append(vertex)
        on_stack.add(vertex)
        for nxt in containers.get(vertex, ()):
            if nxt not in containers:
                continue
            if nxt not in indices:
                strongconnect(nxt)
                lowlink[vertex] = min(lowlink[vertex], lowlink[nxt])
            elif nxt in on_stack:
                lowlink[vertex] = min(lowlink[vertex], indices[nxt])
        if lowlink[vertex] == indices[vertex]:
            comp: list[str] = []
            while True:
                member = stack.pop()
                on_stack.discard(member)
                comp.append(member)
                if member == vertex:
                    break
            components.append(comp)

    for vertex in sorted(containers):
        if vertex not in indices:
            strongconnect(vertex)
    return components


def _is_same_commit_tie(
    members: Collection[str],
    containers: Mapping[str, frozenset[str]],
) -> bool:
    """True when every pair mutually contains and external containers match.

    Two (or more) names on one commit form a complete bidirected clique
    and share the same containers outside the clique. A directed cycle
    that fails either check is malformed, not a tie.
    """
    member_set = frozenset(members)
    if len(member_set) <= 1:
        return True
    for left in member_set:
        left_containers = containers.get(left, frozenset())
        for right in member_set:
            if left == right:
                continue
            if right not in left_containers:
                return False
    externals: frozenset[str] | None = None
    for member in member_set:
        ext = frozenset(
            name for name in containers.get(member, frozenset()) if name not in member_set
        )
        if externals is None:
            externals = ext
        elif ext != externals:
            return False
    return True


def _analyze(contained_by: Mapping[str, Collection[str]]) -> _Graph:
    """Normalize, refuse non-tie cycles, and split strict vs tie edges."""
    containers = _normalize(contained_by)
    if not containers:
        return _Graph(nodes=frozenset(), containers={}, strict_containers={})

    class_of: dict[str, frozenset[str]] = {}
    for component in _strongly_connected(containers):
        members = frozenset(component)
        if not _is_same_commit_tie(members, containers):
            raise AncestryCycleError(
                "containment mapping has a directed cycle: "
                + ", ".join(sorted(members))
            )
        for member in members:
            class_of[member] = members

    strict = {
        node: frozenset(name for name in conts if name not in class_of[node])
        for node, conts in containers.items()
    }
    return _Graph(
        nodes=frozenset(containers),
        containers=containers,
        strict_containers=strict,
    )


def _ancestors_on(graph: _Graph, branch: str) -> set[str]:
    """Transitive ancestors of *branch* on the already-checked graph."""
    if branch not in graph.nodes:
        return set()
    reverse: dict[str, list[str]] = {node: [] for node in graph.nodes}
    for node, conts in graph.containers.items():
        for container in conts:
            if container in reverse:
                reverse[container].append(node)
    seen: set[str] = set()
    stack = list(reverse.get(branch, ()))
    while stack:
        node = stack.pop()
        if node == branch or node in seen:
            continue
        seen.add(node)
        stack.extend(reverse.get(node, ()))
    return seen


def maximal_elements(contained_by: Mapping[str, Collection[str]]) -> list[str]:
    """Return names contained by no name outside their same-commit class.

    Empty mapping → ``[]``. Never a boolean all-clear. Raises
    ``AncestryCycleError`` on a malformed cyclic mapping.
    """
    graph = _analyze(contained_by)
    if not graph.nodes:
        return []
    return sorted(name for name in graph.nodes if not graph.strict_containers[name])


def transitive_ancestors(
    contained_by: Mapping[str, Collection[str]],
    branch: str,
) -> set[str]:
    """Return names whose commits are reachable from *branch* (excluding itself).

    Empty mapping or an unknown *branch* → empty set. Raises
    ``AncestryCycleError`` on a malformed cyclic mapping.
    """
    graph = _analyze(contained_by)
    if not graph.nodes:
        return set()
    return _ancestors_on(graph, branch)


def reap_order(contained_by: Mapping[str, Collection[str]]) -> list[str]:
    """Return a reverse-topological permutation, maximals first.

    Kahn layers on strict containment (same-commit ties are incomparable).
    Each layer is sorted by name. Empty mapping → ``[]``. Raises
    ``AncestryCycleError`` when no such order exists.
    """
    graph = _analyze(contained_by)
    if not graph.nodes:
        return []

    remaining_out = {node: set(graph.strict_containers[node]) for node in graph.nodes}
    incoming: dict[str, list[str]] = {node: [] for node in graph.nodes}
    for node, conts in remaining_out.items():
        for container in conts:
            incoming[container].append(node)

    order: list[str] = []
    emitted: set[str] = set()
    ready = sorted(node for node, outs in remaining_out.items() if not outs)
    while ready:
        wave = list(ready)
        order.extend(wave)
        emitted.update(wave)
        newly: set[str] = set()
        for node in wave:
            for predecessor in incoming[node]:
                remaining_out[predecessor].discard(node)
                if not remaining_out[predecessor] and predecessor not in emitted:
                    newly.add(predecessor)
        ready = sorted(newly)

    if len(order) != len(graph.nodes):
        leftover = graph.nodes - set(order)
        raise AncestryCycleError(
            "containment mapping has a directed cycle: " + ", ".join(sorted(leftover))
        )
    return order


def discharged_by_reachability(
    contained_by: Mapping[str, Collection[str]],
    safe: Collection[str],
) -> set[str]:
    """Return additional names discharged because a safe tip reaches their work.

    Free positives only: ancestors of each safe name, minus the safe set
    itself. A safe root does not discharge its descendants. Empty mapping
    → empty set. Raises ``AncestryCycleError`` on a malformed cyclic mapping.
    """
    graph = _analyze(contained_by)
    if not graph.nodes:
        return set()
    safe_set = {name for name in safe if isinstance(name, str) and name}
    extra: set[str] = set()
    for name in safe_set:
        extra |= _ancestors_on(graph, name)
    extra -= safe_set
    return extra


_REV_PARSE_RULES: tuple[str, ...] = (
    "{}",
    "refs/{}",
    "refs/tags/{}",
    "refs/heads/{}",
    "refs/remotes/{}",
    "refs/remotes/{}/HEAD",
)


def _ref_namespace(run: CommandRunner) -> frozenset[str]:
    """Snapshot every ref in the repository; refuse when the listing is unreadable.

    Repository context enters through the same injected runner as the
    containment probes: one namespace listing per ``containment_map``
    call, never one per name. A non-zero exit, or a result that is not
    ``CommandOutcome``, is a typed refusal — an unreadable namespace must
    not degrade into guesses.
    """
    outcome = run(("git", "for-each-ref", "--format=%(refname)"))
    if not isinstance(outcome, CommandOutcome):
        raise AncestryProbeError(
            "ref namespace listing runner must return CommandOutcome; "
            "unknown must not be spelled as uncontained"
        )
    if outcome.exit_code != 0:
        raise AncestryProbeError(
            f"ref namespace listing failed with exit {outcome.exit_code}"
        )
    return frozenset(
        line.strip() for line in (outcome.stdout or "").splitlines() if line.strip()
    )


def _canonical_ref(name: str, refs: Collection[str]) -> str | None:
    """Resolve a caller spelling to its symbolic full name against *refs*.

    The spelling is tried against the repository's ref namespace in the
    tool's documented lookup order: the literal spelling, ``refs/<name>``,
    ``refs/tags/<name>``, ``refs/heads/<name>``, ``refs/remotes/<name>``,
    then ``refs/remotes/<name>/HEAD``. Exactly one existing match is the
    canonical key, so a local branch and a remote-tracking ref that share
    a short name never share a key. More than one match raises
    ``AncestryCanonError``: an ambiguous spelling is a typed refusal,
    never a guess. No match returns ``None``: a spelling that denotes no
    ref — the symbolic head name, a revision expression, a bare namespace
    prefix — is never given a branch-shaped key.
    """
    text = name.strip()
    if not text:
        return None
    namespace = frozenset(refs)
    matches = [
        candidate
        for candidate in (rule.format(text) for rule in _REV_PARSE_RULES)
        if candidate in namespace
    ]
    if len(matches) > 1:
        raise AncestryCanonError(
            f"ambiguous spelling {name!r} resolves to "
            + ", ".join(sorted(matches))
            + "; ambiguous must refuse, never guess"
        )
    return matches[0] if matches else None


def _listing_ref(name: str) -> str | None:
    """Canonical key for one ``git branch --format=%(refname:short)`` line.

    The listing command emits local branches only, so a short line — or
    the ambiguous-short form ``heads/<name>`` emitted when another ref
    shares the name — denotes ``refs/heads/<name>`` by provenance, not by
    guess. Caller spellings never take this path; they resolve against
    the repository in ``_canonical_ref``.
    """
    text = name.strip()
    if not text:
        return None
    if text.startswith("refs/"):
        parts = text.split("/")
        if len(parts) < 3 or not parts[-1]:
            return None
        return text
    if text.startswith("heads/"):
        rest = text[6:]
        return f"refs/heads/{rest}" if rest else None
    return f"refs/heads/{text}"


def _candidate_index(
    candidates: Collection[str], refs: Collection[str]
) -> dict[str, list[str]]:
    """Map each resolved ref to the caller spellings that resolve to it.

    Distinct refs stay distinct keys. Two spellings of the same ref
    (``feature/foo`` and ``refs/heads/feature/foo``) share one canonical
    entry and remain separate caller-facing names.
    """
    index: dict[str, list[str]] = {}
    for name in candidates:
        if not isinstance(name, str) or not name:
            continue
        canon = _canonical_ref(name, refs)
        if canon is None:
            continue
        bucket = index.setdefault(canon, [])
        if name not in bucket:
            bucket.append(name)
    return index


# Local-branch convention used when the caller omits integration_ref.
# The containment_map default, the resolution carve-out, and the
# refs/heads/ qualification all read this name. A second literal of the
# same characters is not a coupling: renaming the declared default
# while leaving that leftover spelling restores the abort-every-mapping
# defect the carve-out exists to remove.
_DEFAULT_INTEGRATION_REF = "main"


def containment_map(
    branches: Collection[str],
    run: CommandRunner,
    *,
    integration_ref: str = _DEFAULT_INTEGRATION_REF,
) -> dict[str, list[str]]:
    """Build a containment mapping with one ``branch --contains`` query per name.

    *run* is an injected command runner (``argv → CommandOutcome``). One
    ``for-each-ref`` namespace snapshot is taken per call to resolve
    caller spellings; each name is then probed once. A non-zero exit, or
    a result that is not ``CommandOutcome``, refuses the whole mapping:
    unknown is never spelled as uncontained. Each listing is intersected
    with the caller candidate set so a name the caller did not pass
    cannot become a graph node. The queried name and *integration_ref*
    are stripped so a self-listing cannot empty the maximal set. Empty
    *branches* → ``{}`` without invoking *run*.

    Canonical form
    --------------
    Comparison uses the git symbolic full name on **both** sides, and
    caller spellings are resolved against the repository (see
    ``_canonical_ref``): one ``for-each-ref`` snapshot is taken per call
    and every candidate is resolved against it in the tool's documented
    lookup order. Exactly one existing match is the canonical key; more
    than one match is an ``AncestryCanonError``, because ambiguity must
    refuse rather than guess; no match means the spelling denotes no ref
    and receives no key. A local branch and a remote-tracking ref that
    share a short name therefore never share a canonical key, and the key
    records which ref was measured. Each containment probe is dispatched
    with the resolved full name, never the raw spelling, so a stored
    row's meaning cannot drift with the listing command's own lookup
    order.

    Listing lines keep a fixed local-branch mapping (``_listing_ref``):
    ``git branch --format=%(refname:short)`` emits only local branches,
    so a short line — or the ambiguous-short form ``heads/<name>`` —
    denotes ``refs/heads/<name>`` by provenance. A tag that shares that
    short name stays distinct when spelled ``refs/tags/<name>`` or
    ``tags/<name>`` and never collapses into the branch key. Container
    names emitted in the mapping are the caller-supplied candidate
    spellings, not the listing spelling, so keys and edges stay one
    identity system.

    If *branches* is non-empty but no candidate resolves to a ref in the
    repository, raise ``AncestryCanonError``. That is adapter failure,
    not an empty containment graph.
    """
    candidates = {name for name in branches if isinstance(name, str) and name}
    if not candidates:
        return {}
    namespace = _ref_namespace(run)
    index = _candidate_index(candidates, namespace)
    if not index:
        raise AncestryCanonError(
            "candidate set is non-empty but no name resolves to a ref in "
            "the repository; unknown must not be spelled as uncontained"
        )

    if not integration_ref:
        integration_canon = None
    elif integration_ref == _DEFAULT_INTEGRATION_REF:
        # Adapter default is a local-branch convention, not a caller
        # spelling. Qualify it before namespace resolution so a tag of
        # the same short name cannot abort a mapping the caller never
        # scoped. An explicit refs/… integration_ref still resolves as
        # written; an ambiguous caller candidate still refuses below.
        integration_canon = _canonical_ref(
            f"refs/heads/{_DEFAULT_INTEGRATION_REF}", namespace
        )
    else:
        integration_canon = _canonical_ref(integration_ref, namespace)

    built: dict[str, list[str]] = {}
    for branch in branches:
        if not isinstance(branch, str) or not branch:
            continue
        branch_canon = _canonical_ref(branch, namespace)
        if branch_canon is None:
            continue
        outcome = run(
            (
                "git",
                "branch",
                "--contains",
                branch_canon,
                "--format=%(refname:short)",
            )
        )
        if not isinstance(outcome, CommandOutcome):
            raise AncestryProbeError(
                "containment listing runner must return CommandOutcome; "
                "unknown must not be spelled as uncontained"
            )
        if outcome.exit_code != 0:
            raise AncestryProbeError(
                f"containment listing failed for {branch!r} "
                f"with exit {outcome.exit_code}"
            )
        names: list[str] = []
        seen: set[str] = set()
        for line in (outcome.stdout or "").splitlines():
            name = line.strip()
            if not name:
                continue
            listing_canon = _listing_ref(name)
            if listing_canon is None:
                continue
            if listing_canon == branch_canon:
                continue
            if integration_canon and listing_canon == integration_canon:
                continue
            matched = index.get(listing_canon)
            if not matched:
                continue
            for original in matched:
                if original in seen:
                    continue
                seen.add(original)
                names.append(original)
        names.sort()
        built[branch] = names
    return built
