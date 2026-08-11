#!/usr/bin/env python3
"""Consistency gate for the hgfix3 repair pass.

The previous wave passed a quote-based merge gate and still shipped two
inverted claims. That gate asked only "did each lane do what it said it did",
which every lane could answer yes to while the merged document contradicted
itself. This gate asks the different question: are the claims that appear in
more than one place still saying the same thing, and do the line citations
point at what the document says they point at?

Every check here is stated as an invariant over the whole file, not over a
lane's range, because the defect class it exists to catch lives between ranges.

The PIVOT_SHAPES list is a non-exhaustive sample of contrast syntaxes. A green
negative-pivot result bounds density under those shapes; it does not prove that
no manufactured contrast remains (space-optional punctuation, en-dash, colon,
and bare parenthetical forms can still carry contrast without matching a shape).
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

DOC = Path(
    "docs/assessments/harness-graph-subagent-research-2026-08/"
    "workbay-feature-and-remote-subagent-ingest-2026-08-04.md"
)
ANCHORS = Path(".lane-fix/verified-anchors.json")
DASH = "[-–—]"

# WRIT-30 names 20+ as the tell. The document currently runs 113 in prose.
PROSE_EM_DASH_CAP = 20

# WRIT-07 allows the pivot where a reader would genuinely assume the wrong
# thing. Wave 2 widened the detector (was 12 under the narrow shapes; 33 under
# the widened set) and repaired unearned contrasts content-first. Cap equals
# the post-repair achieved count of load-bearing survivors only.
NEGATIVE_PIVOT_CAP = 3


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def check_codex_availability(lines: list[str], errors: list[str]) -> None:
    """codex-remote's dispatchability is stated in more than one place.

    The wave demoted it in the alignment table and left the schedule asserting
    the old status. Either both say available or neither does.
    """
    available = [
        n
        for n, ln in enumerate(lines, 1)
        if re.search(r"`codex-remote`\s*\*{0,2}Available", ln)
    ]
    demoted = [
        n
        for n, ln in enumerate(lines, 1)
        if "codex-remote" in ln and "availability unverified" in ln.lower()
    ]
    if available and demoted:
        errors.append(
            f"codex-remote status is inverted: lines {available} call it Available "
            f"while lines {demoted} call it registered/availability-unverified. "
            f"Both loci must agree."
        )
    elif available and not demoted:
        errors.append(
            f"lines {available} assert codex-remote Available with no dated probe "
            f"evidence anywhere in the document (GATE-CIT-01/GATE-REG-A4)"
        )


def check_defer_gate(text: str, lines: list[str], errors: list[str]) -> None:
    """The L1-L3 defer precondition is stated in three places.

    Part E's Defer Notes row, the Deferred section, and Suggested next steps.
    Lane b fixed the first and lane c rewrote the third to match its old text.
    """
    generic = [
        n
        for n, ln in enumerate(lines, 1)
        if re.search(rf"L1{DASH}L3.*revisit after \*{{0,2}}P0{DASH}P2", ln, re.I)
        or re.search(rf"revisit after \*{{0,2}}P0{DASH}P2.*L1{DASH}L3", ln, re.I)
    ]
    if generic:
        errors.append(
            f"lines {generic} still gate L1{chr(0x2013)}L3 on 'revisit after "
            f"P0{chr(0x2013)}P2', which the Deferred section rejects by name "
            f"('not merely after P0{chr(0x2013)}P2 close'). GATE-INT-07/GATE-REG-C3."
        )
    # The specific gate must survive somewhere, or the repair overshot into
    # deleting the precondition rather than restating it.
    if "outside-loop evaluators" not in text:
        errors.append(
            "the L1/L2 precondition (outside-loop evaluators) is no longer stated "
            "anywhere; the repair must restate it, not remove it"
        )
    if not re.search(rf"L3 after Q9|Q9 \+ (proven )?entity need", text):
        errors.append(
            "the L3 precondition (Q9 + proven entity need) is no longer stated "
            "anywhere; the repair must restate it, not remove it"
        )


def pinned_source_revision(text: str) -> str | None:
    """Return the 40-char Source revision SHA from the document metadata block."""
    head = "\n".join(text.splitlines()[:25])
    m = re.search(
        r"\*\*Source revision\*\*:\s*`([0-9a-f]{40})`",
        head,
    )
    if m:
        return m.group(1)
    m = re.search(r"\b([0-9a-f]{40})\b", head)
    return m.group(1) if m else None


def revision_is_resolvable(sha: str) -> bool:
    """True when git can resolve sha as a commit object in this checkout."""
    try:
        proc = subprocess.run(
            ["git", "cat-file", "-e", f"{sha}^{{commit}}"],
            capture_output=True,
            text=True,
            check=False,
        )
        return proc.returncode == 0
    except OSError:
        return False


def source_text_at_revision(path: str, sha: str) -> str | None:
    """Return file contents at sha, or None if git cannot produce them."""
    try:
        proc = subprocess.run(
            ["git", "show", f"{sha}:{path}"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout


def check_citations(text: str, spec: dict, errors: list[str]) -> None:
    """Every backend_registry.py line citation must point at real code.

    Citations resolve against the document's pinned Source revision, because
    that is the tree the path:line anchors were written against. Measuring the
    working tree when it has moved past the pin produces false reds. When the
    pin is not present in this checkout (shallow clone / history-stripped),
    skip the citation check loudly and leave other scope checks to decide.
    """
    src = Path(spec["source"])
    pin = pinned_source_revision(text)
    if not pin:
        errors.append(
            "cannot verify citations: metadata block names no 40-char Source "
            "revision SHA to resolve path:line anchors against"
        )
        return

    if not revision_is_resolvable(pin):
        print(
            "SKIP citation check: pinned source revision "
            f"{pin} is not present in this checkout; "
            "path:line citations were not measured against the working tree "
            "(wrong tree would be a false red). Other scope checks still run."
        )
        return

    src_text = source_text_at_revision(str(src), pin)
    if src_text is None:
        # Pin exists but the path is missing at that revision: still an error.
        errors.append(
            f"cannot verify citations: {src} is missing at pinned revision {pin}"
        )
        return
    src_lines = src_text.splitlines()

    for m in re.finditer(rf"backend_registry\.py:(\d+)(?:{DASH}(\d+))?", text):
        start = int(m.group(1))
        end = int(m.group(2) or start)
        if end < start:
            errors.append(f"citation :{start}{chr(0x2013)}{end} has end before start")
            continue
        span = range(start, min(end, start + 24) + 1)
        body = [src_lines[n - 1].strip() for n in span if 1 <= n <= len(src_lines)]
        if not body:
            errors.append(f"citation backend_registry.py:{start} is past end of file")
            continue
        # A docstring line counts as substantive: it is inside the symbol the
        # document is pointing at, and an off-by-one into a docstring is a
        # citation that still lands on the right thing.
        if not any(b and not b.startswith("#") for b in body):
            errors.append(
                f"citation backend_registry.py:{start}"
                + (f"{chr(0x2013)}{end}" if end != start else "")
                + f" points at blank/comment only: {body[:2]!r}"
            )

    # Every symbol the document names must still exist under that name (at pin).
    for name, needle in spec["symbols"].items():
        if needle not in src_text:
            errors.append(f"symbol {name} no longer present as {needle!r}")


def prose_lines(lines: list[str]) -> list[tuple[int, str]]:
    """Lines outside fenced blocks, with code spans blanked.

    Every WRIT check below is about prose the reader parses as prose. A glyph
    inside a fence is a rendered artifact and a glyph inside backticks is an
    identifier being quoted; neither is the defect the rule names, and counting
    them is how a style gate turns into noise nobody reads.
    """
    out: list[tuple[int, str]] = []
    fenced = False
    for n, ln in enumerate(lines, 1):
        if ln.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if fenced:
            continue
        out.append((n, re.sub(r"`[^`]*`", "``", ln)))
    return out


def check_metadata_pin(text: str, errors: list[str]) -> None:
    """The document must name the revision its line citations resolve against.

    HARM-CITE-01: 46 path:line citations across 15 files, and a metadata block
    naming only a date. Seven of the cited files have already drifted between
    the grounding revision and main, so without a pin a reader has no tree to
    resolve an anchor in and cannot tell a stale citation from a fabricated one.
    """
    head = "\n".join(text.splitlines()[:20])
    if not re.search(r"\b[0-9a-f]{40}\b", head):
        errors.append(
            "the metadata block names no 40-char commit SHA, so none of the "
            "path:line citations can be resolved against a known tree "
            "(HARM-CITE-01, rules WRIT-41/RAG-07)"
        )


def check_revision_narration(lines: list[str], errors: list[str]) -> None:
    """WRIT-42: the document must not narrate its own revision history.

    HARM-SCAFFOLD-04. A cold reader should not be able to tell the document was
    revised, let alone partly reconstruct what the earlier draft said.
    """
    pattern = re.compile(
        r"\b(from the earlier draft|previously cited|the earlier draft|"
        r"was removed|were removed|was withdrawn|were withdrawn|"
        r"rationale withdrawn|column was removed|citations were removed|"
        r"the original column|that was self-attestation|is relabelled)\b",
        re.I,
    )
    hits = [n for n, ln in prose_lines(lines) if pattern.search(ln)]
    if hits:
        errors.append(
            f"lines {hits} narrate the document's own revision history "
            f"(names a prior draft state). WRIT-42 / HARM-SCAFFOLD-04: state "
            f"the current position flat."
        )


def check_naked_rule_ids(lines: list[str], errors: list[str]) -> None:
    """WRIT-42, second instance: review-process residue leaking into prose.

    A bracketed rule id is a note from the reviewer to the author. It is not
    addressed to the reader and does not belong in the shipped document.
    """
    hits = [
        n
        for n, ln in prose_lines(lines)
        if re.search(r"\[(WRIT|BIAS|NAME|RAG|RSCH|OBS|MEAS|PROV)-\d+\]", ln)
    ]
    if hits:
        errors.append(
            f"lines {hits} carry bare review rule ids in prose "
            f"(e.g. '[WRIT-36]'). WRIT-42: strip the scaffolding; if the point "
            f"matters, say it in the reader's terms."
        )


def check_ascii(lines: list[str], errors: list[str]) -> None:
    """WRIT-32: type ASCII in code-adjacent text. Fenced diagrams are exempt."""
    arrows = [n for n, ln in prose_lines(lines) if "→" in ln or "⇒" in ln]
    curly = [n for n, ln in prose_lines(lines) if re.search(r"[“”‘’]", ln)]
    odd = [n for n, ln in prose_lines(lines) if re.search(r"[⟂≠·]", ln)]
    if arrows:
        errors.append(f"lines {arrows} use a unicode arrow outside a fence; WRIT-32 wants `->`")
    if curly:
        errors.append(f"lines {curly} use curly quotes/apostrophes; WRIT-32 wants straight ones")
    if odd:
        errors.append(
            f"lines {odd} use a perpendicular/not-equal/middot glyph as a word; "
            f"WRIT-32: these are unpronounceable and ungreppable"
        )


def check_em_dashes(lines: list[str], errors: list[str]) -> None:
    """WRIT-30 fires at 20+ prose em dashes. Table cells are counted separately.

    The dash in a table cell is this document's empty-value marker, which is a
    different (WRIT-36) problem; conflating the two would demand 77 cosmetic
    cell edits and bury the prose finding that actually matters.
    """
    count = sum(ln.count("—") for n, ln in prose_lines(lines) if not ln.startswith("|"))
    if count > PROSE_EM_DASH_CAP:
        errors.append(
            f"{count} em dashes in prose (cap {PROSE_EM_DASH_CAP}); WRIT-30. "
            f"Convert the definitional ones to colons and the appositive ones "
            f"to commas or parentheses."
        )


# Shape labels for WRIT-07 / GATE-W2-01. Widened after r08070c78: the first wave
# only matched trailing ", not X" and "not X but/rather than/instead of", so
# parenthetical, sentence-initial, bare-rather-than, and "over <verb>ing"
# contrasts were invisible while the gate stayed green at the old cap.
# Non-exhaustive sample: green bounds density under these shapes, not absence
# of manufactured contrast (other contrast-bearing syntax is not matched).
PIVOT_SHAPES: list[tuple[str, re.Pattern[str]]] = [
    (
        "punct-not",
        re.compile(
            r"[,;—]\s+\*{0,2}(?:not|never)\*{0,2}\s+(?!only\b|just\b|yet\b)",
            re.I,
        ),
    ),
    (
        "not-but",
        re.compile(
            r"\b(?:not|never)\b[^.;|]{0,80}?\s+(?:but|rather than|instead of)\b",
            re.I,
        ),
    ),
    (
        "paren-aux-not",
        # ( ... is/does/do not ... ) — the shape the prior wave rewrote into
        # after ", not by effort." became "(effort is not the sort key)".
        re.compile(
            r"\([^)]*\b(?:is|are|was|were|does|do|did|has|have|cannot|can)\s+not\b[^)]*\)",
            re.I,
        ),
    ),
    (
        "sent-init-not",
        # Sentence-initial "Not " (optionally bold), at line start or after .!?
        re.compile(r"(?:^|(?<=[.!?]\s))\*{0,2}Not\s+\S+"),
    ),
    (
        "bare-rather-than",
        re.compile(r"\brather than\b", re.I),
    ),
    (
        "over-verbing",
        re.compile(r"\bover\s+\w+ing\b", re.I),
    ),
]


def find_negative_pivots(lines: list[str]) -> list[tuple[int, str, str]]:
    """Return every manufactured-contrast match as (line, shape-label, text).

    bare-rather-than excludes spans already covered by not-but (a preceding
    not/never within 80 chars on the same line), so "not X rather than Y"
    counts once under not-but only.
    """
    hits: list[tuple[int, str, str]] = []
    for n, ln in prose_lines(lines):
        not_but_spans: list[tuple[int, int]] = []
        for label, pat in PIVOT_SHAPES:
            if label == "bare-rather-than":
                for m in pat.finditer(ln):
                    prefix = ln[max(0, m.start() - 80) : m.start()]
                    if re.search(r"\b(?:not|never)\b", prefix, re.I):
                        continue
                    # Also skip if this "rather than" sits inside a not-but match.
                    if any(s <= m.start() < e for s, e in not_but_spans):
                        continue
                    hits.append((n, label, m.group()))
                continue
            for m in pat.finditer(ln):
                if label == "not-but":
                    not_but_spans.append((m.start(), m.end()))
                hits.append((n, label, m.group()))
    return hits


def check_negative_parallelism(lines: list[str], errors: list[str]) -> None:
    """WRIT-07 / GATE-W2-01: manufactured 'not X, but Y' pivots.

    Counted over the whole file rather than enumerated as a line list on
    purpose. An enumerated list is a specification the lane can fit to --
    repair exactly the named lines, leave the density untouched everywhere
    else, and still read as done. A cap makes the invariant the target.

    The dominant shape in this document is the trailing negated appositive
    ("Y, **not** X"), not the textbook "not X but Y" -- which occurs exactly
    once. A detector written for the textbook shape finds nothing here and
    reads as compliance. Both shapes are counted; table cells count too,
    because this document's tables carry prose and GATE-W2-01 cited them.

    After r08070c78 the detector also covers parenthetical aux-not,
    sentence-initial Not, bare rather-than, and over-<verb>ing contrast.
    """
    hits = find_negative_pivots(lines)
    count = len(hits)
    if count > NEGATIVE_PIVOT_CAP:
        detail = "; ".join(
            f"L{n} [{label}] {re.sub(r'\s+', ' ', text).strip()!r}"
            for n, label, text in hits
        )
        errors.append(
            f"{count} manufactured negative pivots (cap {NEGATIVE_PIVOT_CAP}); "
            f"WRIT-07 / GATE-W2-01. State the positive claim first; keep the "
            f"contrast only where a reader would actually assume the wrong thing. "
            f"Hits: {detail}"
        )


def audit_pivots(lines: list[str]) -> int:
    """Print every pivot match, per-shape counts, and total. Returns total."""
    hits = find_negative_pivots(lines)
    for n, label, text in hits:
        # Single-line matched text for host re-check; collapse internal newlines.
        flat = re.sub(r"\s+", " ", text).strip()
        print(f"{n}\t{label}\t{flat}")
    counts: dict[str, int] = {}
    for _n, label, _t in hits:
        counts[label] = counts.get(label, 0) + 1
    print("---")
    for label, _pat in PIVOT_SHAPES:
        print(f"{label}: {counts.get(label, 0)}")
    total = len(hits)
    print(f"total: {total}")
    return total


def check_graphrag_deferred(text: str, errors: list[str]) -> None:
    """R2E-05: the largest deferred item is missing from the deferred section.

    The document defends triplicating this position, then omits it from the one
    section a reader consults to find out what is deferred.
    """
    m = re.search(r"^##+ *Deferred or rejected directions *$", text, re.M)
    if not m:
        errors.append("the 'Deferred or rejected directions' section is gone")
        return
    nxt = re.search(r"^##+ ", text[m.end() :], re.M)
    body = text[m.end() : m.end() + (nxt.start() if nxt else len(text))]
    if "GraphRAG" not in body:
        errors.append(
            "'Deferred or rejected directions' does not mention GraphRAG, "
            "though the document defers it in three other places (R2E-05). "
            "The section a reader consults for deferrals omits the largest one."
        )


def check_unblocks_invariant(text: str, lines: list[str], errors: list[str]) -> None:
    """HG0804-R2-28: the sort invariant names a value the table never produces."""
    if not re.search(r"`Unblocks`\s*cell\s*is\s*`none`", text):
        return
    cells = [
        ln
        for n, ln in enumerate(lines, 1)
        if ln.startswith("|") and re.search(r"\|\s*none\s*\|", ln)
    ]
    if not cells:
        errors.append(
            "the Part E sort invariant conditions on an `Unblocks` cell being "
            "`none`, but no table row uses that token (the empty value is an "
            "em dash), so the invariant can never fire (HG0804-R2-28)"
        )


def main() -> int:
    # The repair runs as two sequential lanes on one branch. Each lane's gate
    # must be green when that lane is done, or the pass reports a failed
    # self-verify and throws away committed work for defects that were never in
    # scope. --scope selects the lane's own invariants; the default runs all of
    # them and is what the merge gate uses.
    # --audit=pivots lists every negative-pivot match and exits without running
    # the consistency checks (does not disturb --scope=all|a|b exits).
    scope = "all"
    audit: str | None = None
    for arg in sys.argv[1:]:
        if arg.startswith("--scope="):
            scope = arg.split("=", 1)[1]
        elif arg.startswith("--audit="):
            audit = arg.split("=", 1)[1]
        elif arg == "--audit":
            print("usage: --audit=pivots")
            return 2

    text = DOC.read_text()
    lines = text.splitlines()

    if audit is not None:
        if audit != "pivots":
            print(f"unknown --audit={audit}; expected pivots")
            return 2
        audit_pivots(lines)
        return 0

    if scope not in {"all", "a", "b"}:
        print(f"unknown --scope={scope}; expected all|a|b")
        return 2

    spec = json.loads(ANCHORS.read_text())
    errors: list[str] = []

    if scope in {"all", "a"}:
        check_codex_availability(lines, errors)
        check_defer_gate(text, lines, errors)
        check_citations(text, spec, errors)
        check_metadata_pin(text, errors)
        check_unblocks_invariant(text, lines, errors)
    if scope in {"all", "b"}:
        check_revision_narration(lines, errors)
        check_naked_rule_ids(lines, errors)
        check_ascii(lines, errors)
        check_em_dashes(lines, errors)
        check_negative_parallelism(lines, errors)
        check_graphrag_deferred(text, errors)

    if errors:
        print(f"CONSISTENCY GATE FAIL ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
        return 1
    print(f"CONSISTENCY GATE OK: {len(lines)} lines, all cross-section claims agree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
