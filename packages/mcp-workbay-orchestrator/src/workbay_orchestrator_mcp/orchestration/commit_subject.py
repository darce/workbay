"""Shared commit-subject screen for the remote-turn and checkpoint sinks.

The lane-commit sink was removed on ``feature/wb-lane-commit-sink-01``: it drove
two make targets that never existed, and merge_ready no longer builds a commit
subject at all. Both surviving callers live in ``offload_pass``.

Trust boundary (SEC-01 / REF-26): agent-controlled ``summary`` prose becomes a
git commit subject. One sanitizing helper is used by every sink; git history
must never credit an LLM, model, or vendor.

Accept-or-replace only: the candidate is kept whole or replaced whole. Nothing
is ever excised mid-string.

Round 3 screens credit *grammar*, not vendor identity:

1. Credit idioms (unconditional — evidence must not unlock).
2. Attribution grammar (unconditional, determiner-guarded).
3. Bare vendor tokens (evidence-gated; list is no longer load-bearing).
"""

from __future__ import annotations

import bisect
import hashlib
import re
import unicodedata
from typing import Iterable

# Git's hard wrap for a commit subject applies to the whole first line.
_MAX_COMMIT_SUBJECT = 72
_REMOTE_TURN_SUBJECT_PREFIX = "offload: "
_FALLBACK_BODY = "remote turn"

_ROBOT_EMOJI = "\U0001f916"

# Unicode dash class folded to ASCII hyphen during normalization.
# NFKC folds neither U+2010 nor U+2011 (SG-16).
_UNICODE_DASHES = (
    "\u2010"  # hyphen
    "\u2011"  # non-breaking hyphen
    "\u2012"  # figure dash
    "\u2013"  # en dash
    "\u2014"  # em dash
    "\u2015"  # horizontal bar
    "\u2212"  # minus sign
)
_DASH_FOLD_TABLE = str.maketrans({ch: "-" for ch in _UNICODE_DASHES})

# Format characters (Unicode category Cf) stripped by category after mark strip.
# U+2800 BRAILLE PATTERN BLANK (So) is stripped by explicit exception.
# Other invisible categories are out of scope and not claimed.

# Explicit exception: U+2800 is So (Symbol, other), not Cf/Mn/Mc/Me.
_BRAILLE_PATTERN_BLANK = "\u2800"

# ---------------------------------------------------------------------------
# Tier 1 — credit idioms (unconditional)
# ---------------------------------------------------------------------------

# Co-Authored-By in any spacing/hyphenation: Co-Authored-By, Coauthored by,
# co authored by, Co-Author-By, plus Unicode-hyphen variants after dash fold.
_COAUTHOR_RE = re.compile(r"co[\s\-]*author(?:ed)?[\s\-]*by")

# Offload-Backend: only as a subject that *is* the trailer (at start). A turn
# that *describes deleting* the trailer ("drop the Offload-Backend: …") can
# still land when the patch backs the claim via the vendor-token path. Mid-line
# forms without evidence are refused by the "offload-backend" token (SG-18).
_OFFLOAD_BACKEND_AT_START_RE = re.compile(r"^\s*offload[\s\-]*backend\s*:")

# Credit:/Credits: colon form is *not* unconditional on the colon alone.
# Ordinary accounting subjects ("show remaining credits: 42", type-prefix
# "credit: add line item") must survive; only attribution targets refuse.
# That arm lives in ``_CREDIT_FORM_OBJECT_RE`` / ``_has_credit_form_attribution``
# (compiled after the shared object-wrapper constant below).
# Credit to / Credits to is the same idea for the "to" form — see
# ``_CREDIT_TO_OBJECT_RE`` / ``_has_credit_to_attribution``.

# ``generated with`` is handled by ``_has_generated_with_credit`` so a
# non-agent object (``Generated with deterministic seed handling``) and a
# determiner-led non-agent NP (``Generated with a timeout parameter``) can
# survive while generic-agent and named-referent forms still refuse.
_UNCONDITIONAL_PHRASES: tuple[str | re.Pattern[str], ...] = (
    "with the help of",
    "with assistance from",
)

# ---------------------------------------------------------------------------
# Tier 2 — attribution grammar (unconditional, determiner-guarded)
# ---------------------------------------------------------------------------

# Core authorship verbs for the Tier-2 byline grammar (ARCH-13). Trailer
# <verb>-By: derives from a strict superset so trailer coverage can widen
# without leaking trailer-only verbs into bare-object byline refusal.
_AUTH_VERBS = (
    "built|written|authored|generated|created|implemented|developed|coded|"
    "produced|drafted|ported|crafted|made"
)
# Trailer-only verbs: refuse <Verb>-By: trailers and determiner-led agent
# bylines, but do not bare-object-refuse ordinary engineering subjects
# ("powered by Redis").
_TRAILER_ONLY_VERBS = "assisted|powered"
_TRAILER_VERBS = f"{_AUTH_VERBS}|{_TRAILER_ONLY_VERBS}"

# Optional hyphenated role prefix before the trailer verb (Co-, Re-, AI-,
# auto-, machine-, …). Without this, ``Co-developed-by:`` satisfies neither
# arm: the line-start arm is caret-anchored on the bare verb, and the mid-line
# arm's ``(?<![\w-])`` lookbehind refuses when a hyphen precedes the verb.
# The prefix is zero-or-more ``word-`` segments so multi-segment roles still
# match while bare ``Authored-By:`` remains the zero-prefix case.
_TRAILER_ROLE_PREFIX = r"(?:[\w]+-)*"

# <verb>-By: trailer form at line start is unconditional credit (a genuine
# trailer occupies and begins its own line). Mid-line forms are judged by the
# object after the colon (see ``_has_trailer_verb_by_credit``), not by position
# alone: machine credit smuggled after a type prefix
# (``feat: Authored-By: an AI assistant``) still refuses. Built from
# _TRAILER_VERBS (superset of _AUTH_VERBS) so trailer coverage can never fall
# below the byline auth set. Role prefixes are absorbed so ``Co-developed-by:``
# / ``AI-Generated-By:`` refuse at start too.
_TRAILER_VERB_BY_RE = re.compile(
    rf"^{_TRAILER_ROLE_PREFIX}(?:{_TRAILER_VERBS})[\s\-]*by\s*:"
)
# by/with + determiner → ordinary description, not a byline.
_DETERMINERS = frozenset(
    {
        "the",
        "a",
        "an",
        "our",
        "its",
        "their",
        "this",
        "that",
        "these",
        "those",
        "hand",
    }
)
# Capture the first word after by/with so we can apply the determiner guard.
# Core auth verbs: bare object is always a byline. Trailer-only verbs use a
# softer arm in ``_has_attribution_byline`` (agent head only on bare objects).
# Optional wrapper punctuation adjacent to the object slot (quotes, brackets,
# emphasis, code spans) is skipped at the match site so a wrapped determiner
# still reaches the Tier-2 NP scan. The slot itself stays ``(\w+)``: never a
# non-space run, and never whole-subject quote stripping (both over-reach).
_BYLINE_OBJECT_WRAPPER = r"""["'\[\(\{*`\u201c\u201d\u2018\u2019]"""
_BYLINE_RE = re.compile(
    rf"\b(?:{_AUTH_VERBS})\s+(?:by|with)\s+{_BYLINE_OBJECT_WRAPPER}*(\w+)"
)
_SOFT_BYLINE_RE = re.compile(
    rf"\b(?:{_TRAILER_ONLY_VERBS})\s+(?:by|with)\s+{_BYLINE_OBJECT_WRAPPER}*(\w+)"
)
# ``generated with`` + object, compiled after the shared wrapper constant.
_GENERATED_WITH_OBJECT_RE = re.compile(
    rf"generated with\s+{_BYLINE_OBJECT_WRAPPER}*(\w+)"
)
# Mid-line <verb>-By: + object. Line-start is handled by ``_TRAILER_VERB_BY_RE``;
# this arm captures the verb and object so authorship verbs bare-object-refuse
# while trailer-only verbs keep the soft arm for format documentation. The
# same optional role prefix is absorbed so ``patch Re-Authored-By: an AI
# assistant`` refuses while documenting mentions with non-agent objects still
# survive.
_TRAILER_VERB_BY_OBJECT_RE = re.compile(
    rf"(?<![\w-]){_TRAILER_ROLE_PREFIX}({_TRAILER_VERBS})[\s\-]*by\s*:\s*"
    rf"{_BYLINE_OBJECT_WRAPPER}*(\w+)"
)
# credit/credits to + object: same determiner and generic-agent judgement as
# the Tier-2 byline arm. Bare objects and pure agent NPs refuse; ordinary
# determiner-led domain objects (invoice, customer account) survive.
_CREDIT_TO_OBJECT_RE = re.compile(
    rf"(?<![\w-])credits?\s+to\s+{_BYLINE_OBJECT_WRAPPER}*(\w+)"
)
# credit:/credits: + object. Lookbehind kills accredit:/noncredit:/microcredit:.
# Object grammar (not the colon alone) decides credit vs ordinary billing —
# see ``_has_credit_form_attribution``.
_CREDIT_FORM_OBJECT_RE = re.compile(
    rf"(?<![\w-])credits?\s*:\s*{_BYLINE_OBJECT_WRAPPER}*(\w+)"
)

# Content tokens that mark machine credit inside a post-determiner NP.
# Refusal is a head-plus-modifier property of the span: the NP must end in a
# generic agent head (or multiword agent phrase), and every pre-head token
# must be adjectival. Domain nouns (``user agent``, ``cost model``) and
# non-agent heads (``assistant manager``, ``AI team``) survive.
# Generic agent heads used by the post-determiner NP head+modifier test.
# ``llm`` is also a Tier-3 vendor token: evidence can unlock the bare-token
# arm, so the NP head path must still refuse determiner-led bare ``LLM``
# bylines. ``copilot`` stays vendor-only (always-on token tier covers it).
_GENERIC_AGENT_HEADS = frozenset(
    {
        "assistant",
        "agent",
        "model",
        "ai",
        "bot",
        "chatbot",
        "llm",
    }
)
_GENERIC_AGENT_HEAD_PHRASES = frozenset(
    {
        "language model",
        "coding assistant",
        "pair programmer",
        "autonomous agent",
        "artificial intelligence",
        "automated system",
        "neural network",
    }
)
# Closed ordinary adjectives used as credit-byline modifiers.
# Includes genuine modifiers that the removed suffix tier used to promote
# (``capable``, ``powered``) so credit bylines like ``highly capable assistant``
# and ``AI-powered assistant`` still refuse without a morphological free-for-all.
_NP_ADJECTIVES = frozenset(
    {
        "advanced",
        "autonomous",
        "large",
        "helpful",
        "clever",
        "smart",
        "new",
        "internal",
        "external",
        "nightly",
        "shared",
        "trusted",
        "fast",
        "slow",
        "small",
        "deep",
        "general",
        "generic",
        "simple",
        "modern",
        "silent",
        "quiet",
        "friendly",
        "clean",
        "safe",
        "robust",
        "lightweight",
        "underlying",
        "capable",
        "powered",
    }
)
# Non-final tokens of multiword agent phrases may modify a head
# (``automated agent``, ``coding agent``). Phrase-final tokens must NOT be
# promoted alone: ``network`` from ``neural network`` is not adjectival in
# ``the network model``, or ordinary domain prose dies as false-positive credit.
_GENERIC_AGENT_PHRASE_MODIFIERS = frozenset(
    word
    for phrase in _GENERIC_AGENT_HEAD_PHRASES
    for word in phrase.split()[:-1]
)
# Closed trailing adverbial adjuncts stripped from the NP span before the
# head test. Only this list is stripped — unknown trailing tokens stay as
# heads so ``the agent lead`` / ``the assistant manager`` survive.
_NP_TRAILING_ADJUNCTS = frozenset(
    {
        "yesterday",
        "today",
        "tomorrow",
        "tonight",
        "overnight",
        "again",
        "earlier",
        "later",
        "now",
        "recently",
        "previously",
        "initially",
        "finally",
        "already",
        "twice",
        "once",
        "meanwhile",
        "afterwards",
        "beforehand",
        "lately",
        "soon",
        "then",
    }
)
# Closed hyphen-joined compound modifiers that may precede an agent head.
# Unknown hyphenated forms stay non-adjectival so ``risk-scoring model``
# survives; never treat every hyphenated token as adjectival.
_NP_COMPOUND_MODIFIERS = frozenset(
    {
        "in-house",
        "on-call",
        "so-called",
        "off-the-shelf",
        "as-yet-unnamed",
        "home-grown",
        "home-brewed",
        "purpose-built",
        "general-purpose",
        "open-source",
        "third-party",
        "fine-tuned",
        "pre-trained",
        "state-of-the-art",
        "in-built",
    }
)
# Clause-boundary tokens that end the post-determiner noun phrase. The scan
# stops here so an agent noun in a later clause does not mark ordinary prose.
_NP_CLAUSE_BOUNDARIES = frozenset(
    {
        # prepositions / infinitive marker
        "using",
        "to",
        "for",
        "after",
        "with",
        "from",
        "in",
        "on",
        "by",
        "at",
        "into",
        "onto",
        "upon",
        "over",
        "under",
        "through",
        "during",
        "before",
        "since",
        "until",
        "via",
        "per",
        "about",
        "against",
        "between",
        "among",
        "without",
        "within",
        "across",
        "around",
        "behind",
        "beyond",
        "near",
        "toward",
        "towards",
        # subordinators / relative pronouns
        "that",
        "which",
        "who",
        "whom",
        "whose",
        "where",
        "when",
        "while",
        "because",
        "if",
        "unless",
        "although",
        "though",
        "whereas",
        "whether",
        # conjunctions and related
        "and",
        "or",
        "but",
        "nor",
        "yet",
        "so",
        "as",
        "than",
    }
)


def _singularize_agent_token(word: str) -> str:
    """Strip a trailing ``s`` so plural heads match singular forms."""
    if len(word) > 1 and word.endswith("s"):
        return word[:-1]
    return word


def _bare_generic_agent_head(word: str) -> bool:
    """True when *word* is a single-token generic agent head (no hyphen rules)."""
    return word in _GENERIC_AGENT_HEADS or _singularize_agent_token(word) in _GENERIC_AGENT_HEADS


def _word_is_generic_agent_head(word: str) -> bool:
    """True when *word* is a generic agent head, including hyphenated forms.

    A hyphen-joined token is a head when either its de-hyphenated concatenation
    is a head (``chat-bot`` → ``chatbot``), or its last hyphen segment is a
    head and every earlier segment is adjectival (``AI-assistant``).
    ``agent-pool`` is not a head because ``pool`` is not a head.
    """
    if _bare_generic_agent_head(word):
        return True
    if "-" not in word:
        return False
    parts = word.split("-")
    joined = "".join(parts)
    if _bare_generic_agent_head(joined):
        return True
    if _bare_generic_agent_head(parts[-1]) and all(
        _is_adjectival(part) for part in parts[:-1]
    ):
        return True
    return False


def _pair_is_generic_agent_phrase(left: str, right: str) -> bool:
    pair = f"{left} {right}"
    if pair in _GENERIC_AGENT_HEAD_PHRASES:
        return True
    singular_right = _singularize_agent_token(right)
    if singular_right != right:
        return f"{left} {singular_right}" in _GENERIC_AGENT_HEAD_PHRASES
    return False


def _is_adjectival(word: str) -> bool:
    """Whether *word* may modify a generic agent head in a credit byline.

    Resolution order is load-bearing:

    1. Agent head → adjectival (``ai assistant`` dies).
    2. Non-final multiword-phrase modifiers → adjectival (``automated agent``
       dies via ``automated`` from ``automated system``). Phrase-final tokens
       are never promoted alone, so ``network`` from ``neural network`` does
       not kill ordinary domain prose such as ``the network model``.
    3. Closed compound-modifier set → adjectival (``in-house agent`` dies).
    4. Closed adjective set → adjectival.
    5. Default → not adjectival (unknown modifiers survive; leak is preferred
       over destroying a real subject). There is no morphological suffix tier
       and no blanket hyphen rule: every correct promotion lives in a closed
       set, and treating any hyphenated token as adjectival would destroy
       ordinary subjects such as ``risk-scoring model``.

    Whole multiword phrases (``neural network``, ``language model``) are still
    recognized as heads via ``_pair_is_generic_agent_phrase``.
    """
    if _word_is_generic_agent_head(word):
        return True
    if (
        word in _GENERIC_AGENT_PHRASE_MODIFIERS
        or _singularize_agent_token(word) in _GENERIC_AGENT_PHRASE_MODIFIERS
    ):
        return True
    if word in _NP_COMPOUND_MODIFIERS:
        return True
    if word in _NP_ADJECTIVES:
        return True
    return False


def _np_word_to_content_tokens(word: str) -> list[str]:
    """Expand one NP token for the pure-agent content walk.

    Closed compound modifiers stay whole so ``in-house`` is one adjectival
    token. Hyphenated agent heads stay whole so ``chat-bot`` / ``AI-assistant``
    count as heads. Everything else splits on hyphens so multiword phrases
    (``neural-network``) and mixed compounds (``user-agent``, ``risk-scoring``)
    keep segment-level purity.
    """
    if "-" not in word:
        return [word]
    if word in _NP_COMPOUND_MODIFIERS:
        return [word]
    if _word_is_generic_agent_head(word):
        return [word]
    return word.split("-")


def _content_tokens_are_pure_generic_agent(content: list[str]) -> bool:
    """True when *content* is a generic-agent NP under the head+modifier test.

    Step 1: empty span is not credit.
    Step 2: span must end in an agent head (last token) or agent phrase (last two).
    Step 3: every pre-head token must be adjectival, walking right-to-left so a
    trailing ``-ly`` adverb is accepted only when the token immediately to its
    right was itself accepted as adjectival. An adverb modifies the adjective
    that follows it, never the head noun directly, so requiring an accepted
    adjective to its right is what separates ``fully autonomous agent`` from
    ``assembly model``. A multiword agent phrase in the pre-head region counts
    as one adjectival unit only when both of its tokens are present in order
    (``neural network model``), never when a single phrase component appears
    alone (``network model``).
    Step 4: otherwise refuse (return True).
    """
    if not content:
        return False
    n = len(content)
    # Prefer a two-token phrase head when present; otherwise a single-token head.
    if n >= 2 and _pair_is_generic_agent_phrase(content[-2], content[-1]):
        head_start = n - 2
    elif _word_is_generic_agent_head(content[-1]):
        head_start = n - 1
    else:
        return False
    # Right-to-left pre-head walk. A bare head noun is never an adjective for
    # the adverb guard (``assembly model`` must survive). The left token of a
    # multiword agent phrase can seed the guard when it is an ordinary
    # adjective, so ``fully`` attaches to ``autonomous`` in
    # ``fully autonomous agent`` rather than to the head noun ``agent``.
    right_was_adjectival = False
    if head_start == n - 2:
        phrase_left = content[head_start]
        if phrase_left in _NP_ADJECTIVES:
            right_was_adjectival = True
    index = head_start - 1
    while index >= 0:
        # Whole multiword phrase in context: consume both tokens as one unit.
        if index >= 1 and _pair_is_generic_agent_phrase(
            content[index - 1], content[index]
        ):
            right_was_adjectival = True
            index -= 2
            continue
        token = content[index]
        if _is_adjectival(token):
            right_was_adjectival = True
            index -= 1
            continue
        if token.endswith("ly") and right_was_adjectival:
            right_was_adjectival = True
            index -= 1
            continue
        return False
    return True


def _fold_hyphen_compounds(
    spans: list[tuple[str, int, int]],
) -> list[str]:
    """Join word-hyphen-word(-word)* runs into single tokens.

    Keeps ``in-house`` as one unit so the boundary set never sees a bare
    preposition ``in`` that was only the first half of a compound modifier.
    Folds only when spans are adjacent (real hyphens); a spaced dash is a
    clause break and must not collapse ``team - model`` into one token.
    """
    tokens: list[str] = []
    i = 0
    n = len(spans)
    while i < n:
        tok, start, end = spans[i]
        if re.fullmatch(r"\w+", tok):
            parts = [tok]
            j = i
            while (
                j + 2 < n
                and spans[j + 1][0] == "-"
                and re.fullmatch(r"\w+", spans[j + 2][0])
                and spans[j][2] == spans[j + 1][1]
                and spans[j + 1][2] == spans[j + 2][1]
            ):
                parts.append(spans[j + 2][0])
                j += 2
            if len(parts) > 1:
                tokens.append("-".join(parts))
                i = j + 1
            else:
                tokens.append(tok)
                i += 1
        else:
            tokens.append(tok)
            i += 1
    return tokens


def _spans_for_np_scan(text: str) -> list[tuple[str, int, int]]:
    """Word and punctuation spans for the post-determiner NP walk.

    Preserve punctuation so clause breaks end the span. Keep start/end so
    hyphen folding can require adjacency and leave spaced dashes as breaks.
    Tokenization of a subject does not depend on which byline match is under
    test; callers should compute this once per subject.
    """
    return [(m.group(0), m.start(), m.end()) for m in re.finditer(r"\w+|[^\w\s]", text)]


def _fold_next_token(
    spans: list[tuple[str, int, int]], i: int
) -> tuple[str, int] | None:
    """Fold one token at *i* the same way as ``_fold_hyphen_compounds``.

    Returns ``(token, next_index)``, or ``None`` when *i* is past the end.
    Streaming one token at a time lets the NP walk stop at a clause boundary
    without folding the entire remaining suffix.
    """
    n = len(spans)
    if i >= n:
        return None
    tok, _start, _end = spans[i]
    if re.fullmatch(r"\w+", tok):
        parts = [tok]
        j = i
        while (
            j + 2 < n
            and spans[j + 1][0] == "-"
            and re.fullmatch(r"\w+", spans[j + 2][0])
            and spans[j][2] == spans[j + 1][1]
            and spans[j + 1][2] == spans[j + 2][1]
        ):
            parts.append(spans[j + 2][0])
            j += 2
        if len(parts) > 1:
            return "-".join(parts), j + 1
        return tok, i + 1
    return tok, i + 1


def _post_determiner_head_is_generic_agent_from_spans(
    spans: list[tuple[str, int, int]],
    start_idx: int,
    *,
    strip_trailing_adjuncts: bool = True,
) -> bool:
    """True when the post-determiner NP from *start_idx* is machine credit.

    Same head+modifier rules as ``_post_determiner_head_is_generic_agent``, but
    consumes a pre-tokenized span list so each byline match does not re-tokenize
    the whole remaining suffix.
    """
    np_words: list[str] = []
    i = start_idx
    n = len(spans)
    while i < n:
        stepped = _fold_next_token(spans, i)
        if stepped is None:
            break
        tok, next_i = stepped
        if not re.fullmatch(r"\w+(?:-\w+)*", tok):
            # Non-word token (punctuation) ends the noun phrase.
            break
        if tok in _NP_CLAUSE_BOUNDARIES:
            # Spaced closed compound at the start of the span (``in house``).
            if not np_words:
                peeked = _fold_next_token(spans, next_i)
                if peeked is not None:
                    nxt, after_nxt = peeked
                    if re.fullmatch(r"\w+(?:-\w+)*", nxt):
                        compound = f"{tok}-{nxt}"
                        if compound in _NP_COMPOUND_MODIFIERS:
                            np_words.append(compound)
                            i = after_nxt
                            continue
            # Free-standing preposition/subordinator/conjunction ends the span.
            break
        np_words.append(tok)
        i = next_i
    if not np_words:
        return False
    # Strip closed trailing adverbial adjuncts before the head test. Never
    # empty the span; a token outside the closed list is a head, not an adjunct.
    if strip_trailing_adjuncts:
        while len(np_words) > 1 and np_words[-1] in _NP_TRAILING_ADJUNCTS:
            np_words.pop()
    # Compound modifiers and hyphenated agent heads stay whole; other hyphen
    # forms split so segment purity still governs mixed compounds.
    content = [
        seg for word in np_words for seg in _np_word_to_content_tokens(word)
    ]
    return _content_tokens_are_pure_generic_agent(content)


def _post_determiner_np_is_bare_from_spans(
    spans: list[tuple[str, int, int]],
    start_idx: int,
) -> bool:
    """True when the post-determiner NP from *start_idx* holds one word or fewer.

    Same walk as ``_post_determiner_head_is_generic_agent_from_spans``:
    ``_fold_next_token`` stepping, non-word-token stop, and
    ``_NP_CLAUSE_BOUNDARIES`` stop (including spaced closed-compound absorption
    at an empty span). Counts collected words only — no trailing-adjunct
    strip and no agent-head purity test. Used by the ``generated with``
    determiner branch to refuse bare non-agent heads such as a single
    ordinary noun after a determiner.
    """
    np_words: list[str] = []
    i = start_idx
    n = len(spans)
    while i < n:
        stepped = _fold_next_token(spans, i)
        if stepped is None:
            break
        tok, next_i = stepped
        if not re.fullmatch(r"\w+(?:-\w+)*", tok):
            break
        if tok in _NP_CLAUSE_BOUNDARIES:
            if not np_words:
                peeked = _fold_next_token(spans, next_i)
                if peeked is not None:
                    nxt, after_nxt = peeked
                    if re.fullmatch(r"\w+(?:-\w+)*", nxt):
                        compound = f"{tok}-{nxt}"
                        if compound in _NP_COMPOUND_MODIFIERS:
                            np_words.append(compound)
                            i = after_nxt
                            continue
            break
        np_words.append(tok)
        i = next_i
    return len(np_words) <= 1


def _post_determiner_head_is_generic_agent(
    rest: str,
    *,
    strip_trailing_adjuncts: bool = True,
) -> bool:
    """True when the post-determiner NP is machine credit under head+modifier.

    Scans tokens after the determiner up to a clause boundary: any non-word
    punctuation token, or a free-standing function word from
    ``_NP_CLAUSE_BOUNDARIES``. Hyphen-joined compounds are folded first so a
    boundary word that is only the first half of a modifier cannot empty the
    span, but only across real (span-adjacent) hyphens — a spaced dash remains
    a clause break. A spaced closed compound (``in house``) is absorbed only
    when the span is still empty and the joined form is in
    ``_NP_COMPOUND_MODIFIERS``; a genuine preposition still ends the span
    because ``in-the`` is not a member.

    After the span is built, closed trailing adjuncts from
    ``_NP_TRAILING_ADJUNCTS`` are stripped when *strip_trailing_adjuncts* is
    true (never the entire span). Core authorship verbs strip; soft trailer
    verbs keep residual adjunct padding so ``Reviewed by a large language
    model today`` stays ordinary while pure soft-agent NPs still refuse.
    Refusal is then head-plus-modifier: the content span must end in a generic
    agent head or multiword agent phrase, and every pre-head token must be
    adjectival. Non-agent heads (``assistant manager``, ``AI team``) survive.
    Plurals match agent heads by stripping a trailing ``s`` before lookup.
    """
    spans = _spans_for_np_scan(rest)
    if not spans:
        return False
    return _post_determiner_head_is_generic_agent_from_spans(
        spans, 0, strip_trailing_adjuncts=strip_trailing_adjuncts
    )


# ---------------------------------------------------------------------------
# Tier 3 — bare vendor tokens (evidence-gated; do not grow this list)
# ---------------------------------------------------------------------------

# Deliberately omit "agent" — ordinary prose throughout this repo.
# ``aider`` is a listed LLM coding tool: bare-token evidence-gating uses this
# list, and Tier-2 byline referents include it (via ``_BYLINE_CREDIT_REFERENTS``)
# so ``Authored by aider`` refuses. Trailing word-boundary capture on the
# byline object slot keeps ordinary prose like ``airflow`` / ``aiohttp`` from
# matching the generic head ``ai`` (WIDTH-49); that boundary does not require
# carving ``aider`` out of the referent set.
VENDOR_CREDIT_TOKENS: tuple[str, ...] = (
    "grok",
    "claude",
    "anthropic",
    "openai",
    "gpt",
    "codex",
    "gemini",
    "llama",
    "mistral",
    "copilot",
    "cursor",
    "llm",
    "offload-backend",
    "sonnet",
    "opus",
    "haiku",
    "xai",
    "deepseek",
    "qwen",
    "aider",
    "devin",
    "chatgpt",
)

# Registered offload backend ids: repository subject matter, not credit.
# Matched as whole compounds so a bare ``grok`` token still evidence-gates.
_REGISTERED_BACKEND_IDS: frozenset[str] = frozenset(
    {
        "grok-remote",
        "grok-cli",
        "claude-code",
        "codex-cli",
        "cursor-cli",
    }
)

# Unlisted model/product names that Tier-2 bylines must still refuse (SG-14 /
# overtrigger pins). Kept out of ``VENDOR_CREDIT_TOKENS`` so bare subject-matter
# mentions (``add Groq backend support``) are not Tier-3 destroyed.
# Known AI coding tools omitted from ``VENDOR_CREDIT_TOKENS`` for the same
# bare-mention reason are included here so auth-verb and ``generated with``
# bylines naming them still refuse without widening generic-head sets.
_BYLINE_EXTRA_REFERENTS: frozenset[str] = frozenset(
    {
        "groq",
        "fable",
        "kimi",
        "windsurf",
        "cline",
        "minimax",
        "devstral",
        "moonshot",
        "codeium",
        "jules",
        "tabnine",
        "phind",
        "bard",
        "codewhisperer",
        "cody",
        "ollama",
        "autogpt",
        "continue.dev",
    }
)

# Model-tier / version words skipped when looking for a claim verb after a
# vendor token (``Gemini 2.5 Pro composed`` → claim verb ``composed``).
_MODEL_TIER_WORDS: frozenset[str] = frozenset(
    {
        "pro",
        "turbo",
        "scout",
        "zero",
        "large",
        "small",
        "mini",
        "flash",
        "lite",
        "opus",
        "sonnet",
        "haiku",
        "max",
        "ultra",
        "preview",
        "chat",
        "code",
        "r1",
        "v3",
        "v2",
        "v1",
    }
)

# Verbs that mark subject-position machine credit after a vendor/model name.
_SUBJECT_CLAIM_VERBS: frozenset[str] = frozenset(
    {
        "built",
        "written",
        "authored",
        "generated",
        "created",
        "implemented",
        "developed",
        "coded",
        "produced",
        "drafted",
        "ported",
        "crafted",
        "made",
        "rewrote",
        "rewrite",
        "rewritten",
        "assembled",
        "composed",
        "synthesized",
        "refactored",
        "engineered",
        "designed",
        "wrote",
        "did",
    }
)

# Auth-like verbs that make ``with <vendor>`` credit rather than weak prep.
_WITH_CREDIT_VERBS: frozenset[str] = frozenset(_AUTH_VERBS.split("|")) | {
    "engineered",
    "synthesized",
    "refactored",
    "composed",
    "developed",
    "assisted",
    "powered",
}

_WEAK_PREP_BEFORE_RE = re.compile(
    r"(?:\b(?:via|from|per|to|using|for|of)\s+|\binstead of\s+|\brather than\s+)$"
)
_BY_BEFORE_RE = re.compile(r"\bby\s+$")
_WITH_BEFORE_RE = re.compile(r"\bwith\s+$")
_BY_DETERMINER_BEFORE_RE = re.compile(
    r"\b(?:by|with)\s+(?:the|a|an|our|its|their|this|that|these|those)\s+$"
)
_THANKS_BEFORE_RE = re.compile(r"(?:courtesy of|thanks to)\s+$")
_AUTH_VERB_IN_TEXT_RE = re.compile(rf"\b(?:{_AUTH_VERBS})\b")
_WITH_CREDIT_VERB_IN_TEXT_RE = re.compile(
    r"\b(?:" + "|".join(sorted(_WITH_CREDIT_VERBS)) + r")\b"
)

# Named agent/vendor referents for Tier-2 bare-object bylines and the
# ``generated with`` idiom. ASCII tokens are stored casefolded; the object
# slot applies a trailing word boundary so ``ai`` never eats ``airflow``/
# ``aider`` / ``aiohttp``. Residual gap: a never-before-seen vendor in title
# case after an auth verb is accepted over destroying Plan/Alembic/human
# bylines (WIDTH-55). Known AI tools belong in the closed list rather than
# relying on that residual gap.
_BYLINE_CREDIT_REFERENTS: frozenset[str] = frozenset(
    {
        *(t.casefold() for t in VENDOR_CREDIT_TOKENS),
        *(t.casefold() for t in _BYLINE_EXTRA_REFERENTS),
    }
)

# Separator-stripped forms so hyphen/dot surface variants of a multi-segment
# product id (``code-whisperer``, ``continue.dev``) still resolve to the same
# closed referent without enumerating every spelling in the source list.
_BYLINE_CREDIT_REFERENT_COMPACTS: frozenset[str] = frozenset(
    t.replace("-", "").replace(".", "") for t in _BYLINE_CREDIT_REFERENTS
)

# Whole registered backend compounds, casefolded, for Tier-3 skip.
_REGISTERED_BACKEND_IDS_NORM: frozenset[str] = frozenset(
    b.casefold() for b in _REGISTERED_BACKEND_IDS
)

# ``reviewed by|with <object>`` is not authorship in general (human review
# language and generic-agent review subjects must survive), but a byline that
# names a closed machine referent or registered backend is still credit and
# must refuse. Kept off ``_AUTH_VERBS`` / ``_TRAILER_ONLY_VERBS`` so the soft
# and core byline arms never treat bare review as authorship.
_REVIEWED_BYLINE_RE = re.compile(
    rf"\breviewed\s+(?:by|with)\s+{_BYLINE_OBJECT_WRAPPER}*(\w+)"
)


def normalize_for_match(text: str) -> str:
    """NFKC, strip marks/invisibles, fold Unicode dashes, case-fold.

    Confusables are no longer mapped letter-by-letter (SG-15): after this
    normalization, any remaining non-ASCII *letter* is refused at screen time.
    Dashes are not letters — en/em dash subjects survive. Combining marks
    (Mn nonspacing, Mc spacing-combining, Me enclosing) are stripped so
    café / Müller land as ASCII and mark-split tokens cannot evade the screen.
    Format characters (Cf) are stripped by category; U+2800 BRAILLE PATTERN
    BLANK is stripped by explicit exception. Other invisible categories are
    out of scope and not claimed.
    """
    nfkc = unicodedata.normalize("NFKC", text)
    nfd = unicodedata.normalize("NFD", nfkc)
    stripped = "".join(
        ch
        for ch in nfd
        if ch != _BRAILLE_PATTERN_BLANK
        and unicodedata.category(ch) not in {"Mn", "Mc", "Me", "Cf"}
    )
    stripped = stripped.translate(_DASH_FOLD_TABLE)
    return stripped.casefold()


def _has_non_ascii_letter(normalized: str) -> bool:
    """SG-15: residual non-ASCII letters after normalization are lookalikes."""
    for ch in normalized:
        if ord(ch) > 0x7F and unicodedata.category(ch).startswith("L"):
            return True
    return False


def _token_pattern(token: str) -> re.Pattern[str]:
    """Word-boundary match with an asymmetric trailing guard (SG-13).

    Leading ``(?<!\\w)`` keeps ``plan_cursor`` / ``fulfillment`` safe (opening
    the leading end would resurrect SG-05). Trailing ``(?![a-z])`` lets
    versioned tokens (``GPT5``, ``Grok4``, ``GPT_5``) match while
    ``grokking`` still does not.
    """
    return re.compile(rf"(?<!\w){re.escape(token)}(?![a-z])", re.IGNORECASE)


def _contains_token(haystack_normalized: str, token: str) -> bool:
    return _token_pattern(normalize_for_match(token)).search(haystack_normalized) is not None


def extract_changed_content(patch_text: str) -> str:
    """Evidence = changed content lines only (``+``/``-``, not file headers).

    File headers are detected by position (SG-24): require the trailing space
    and ``a/`` / ``b/`` / ``/dev/null``. A bare ``startswith("---")`` would
    swallow removed SQL/Lua/Haskell/Ada comment lines whose content starts
    with ``-``.
    """
    parts: list[str] = []
    for line in patch_text.splitlines():
        if _is_diff_file_header(line):
            continue
        if line.startswith("+") or line.startswith("-"):
            parts.append(line[1:])
    return "\n".join(parts)


def _is_diff_file_header(line: str) -> bool:
    if line.startswith("--- ") or line.startswith("+++ "):
        path = line[4:]
        return path.startswith("a/") or path.startswith("b/") or path.startswith("/dev/null")
    return False


def evidence_from_grounding(grounding: Iterable[str]) -> str:
    """Normalize joined changed-content from each grounding blob."""
    chunks: list[str] = []
    for blob in grounding:
        chunks.append(extract_changed_content(blob))
    return normalize_for_match("\n".join(chunks))


def truncate_at_word_boundary(text: str, budget: int) -> str:
    """Cut at a word boundary inside ``budget``; hard-cut only when no spaces.

    When ``text[budget]`` is already a space the prefix is a clean cut — do
    not back off and discard an extra word (SG-22).
    """
    if budget <= 0:
        return ""
    if len(text) <= budget:
        return text
    # Clean cut: the character at the budget index is already a separator.
    if text[budget] == " ":
        return text[:budget].rstrip()
    hard = text[:budget]
    if " " not in hard:
        return hard  # no spaces at all in the budget — hard truncate
    cut = hard.rfind(" ")
    if cut <= 0:
        return hard
    return hard[:cut].rstrip()


def _has_credit_to_attribution(normalized_subject: str) -> bool:
    """Refuse credit/credits-to only when the object is an attribution target.

    Reuses the Tier-2 determiner and generic-agent head machinery: a bare
    object (person or agent name) always refuses; a determiner-led NP refuses
    only when it is a pure generic agent phrase. Ordinary billing objects
    ("the invoice", "the customer account") survive.
    """
    spans = _spans_for_np_scan(normalized_subject)
    span_starts = [start for _tok, start, _end in spans]
    for match in _CREDIT_TO_OBJECT_RE.finditer(normalized_subject):
        obj = match.group(1)
        if obj not in _DETERMINERS:
            return True
        start_idx = bisect.bisect_left(span_starts, match.end())
        if _post_determiner_head_is_generic_agent_from_spans(spans, start_idx):
            return True
    return False


def _has_credit_form_attribution(normalized_subject: str) -> bool:
    """Refuse credit:/credits: only when the object is an attribution target.

    The colon alone is not credit: ordinary accounting labels
    (``credits: 42``, ``credits: monthly balance``) and conventional-commit
    type prefixes (``credit: add line item``) must survive. Determiner-led
    pure agent NPs refuse under the same post-determiner head test as the
    byline arm. A sole bare non-digit object after the colon is attribution
    (person or product name); a following content word marks labeled domain
    prose rather than a credit form. Bare generic agent heads refuse even
    when more words follow.
    """
    spans = _spans_for_np_scan(normalized_subject)
    span_starts = [start for _tok, start, _end in spans]
    for match in _CREDIT_FORM_OBJECT_RE.finditer(normalized_subject):
        obj = match.group(1)
        if obj not in _DETERMINERS:
            if obj.isdigit():
                continue
            if _word_is_generic_agent_head(obj):
                return True
            # Sole bare object after credit(s): is attribution; further
            # content words are type-prefix or accounting labels.
            if not re.search(r"\w", normalized_subject[match.end() :]):
                return True
            continue
        start_idx = bisect.bisect_left(span_starts, match.end())
        if _post_determiner_head_is_generic_agent_from_spans(spans, start_idx):
            return True
    return False


def _has_trailer_verb_by_credit(normalized_subject: str) -> bool:
    """Refuse <verb>-By: trailers by line-start form or mid-line object grammar.

    Line-start is unconditional: a genuine trailer begins its line and is
    never legitimate commit-subject prose. Mid-line forms judge the object
    by grammar, not vendor-token list membership and not end-of-line position:
    core authorship verbs bare-object-refuse (same rule as
    ``_has_attribution_byline``), so an unlisted product name after
    ``Authored-By:`` is credit even when a ticket id follows. Trailer-only
    verbs refuse a name-like object independently of what follows it (so
    ``Assisted-by: Jules v2`` and chained ``powered-by: generated-by: …``
    refuse). Determiner-led pure agent NPs refuse under
    ``_post_determiner_head_is_generic_agent_from_spans``.
    """
    if _TRAILER_VERB_BY_RE.search(normalized_subject):
        return True
    auth_verbs = frozenset(_AUTH_VERBS.split("|"))
    spans = _spans_for_np_scan(normalized_subject)
    span_starts = [start for _tok, start, _end in spans]
    for match in _TRAILER_VERB_BY_OBJECT_RE.finditer(normalized_subject):
        verb = match.group(1)
        obj = match.group(2)
        if obj not in _DETERMINERS:
            # Core authorship verbs: bare object is always a byline.
            if verb in auth_verbs:
                return True
            # Bare agent head is always credit (fail-safe: destroy unrecognised
            # credit).
            if _word_is_generic_agent_head(obj):
                return True
            # Name-like object: refuse unconditionally.
            return True
        start_idx = bisect.bisect_left(span_starts, match.end())
        if _post_determiner_head_is_generic_agent_from_spans(spans, start_idx):
            return True
    return False


def _word_is_named_credit_referent(word: str) -> bool:
    """True when *word* is a closed named agent/vendor/backend byline referent.

    Exact membership first, then separator-stripped compact form so
    ``code-whisperer`` and ``continue.dev`` resolve to listed product ids.
    Generic agent heads are intentionally excluded — callers that need them
    use ``_word_is_credit_byline_referent``.
    """
    if word in _BYLINE_CREDIT_REFERENTS:
        return True
    if word in _REGISTERED_BACKEND_IDS_NORM:
        return True
    compact = word.replace("-", "").replace(".", "")
    if compact and compact in _BYLINE_CREDIT_REFERENT_COMPACTS:
        return True
    return False


def _word_is_credit_byline_referent(word: str) -> bool:
    """True when *word* is a named agent/vendor byline referent.

    Keys on the object slot token (already casefolded via subject
    normalization) with exact membership in the closed referent set, plus
    generic agent heads. Trailing word boundary is inherent: the byline
    object capture is a single ``\\w+`` token, so ``ai`` never matches inside
    ``airflow`` / ``aider`` / ``aiohttp``.
    """
    if _word_is_generic_agent_head(word):
        return True
    return _word_is_named_credit_referent(word)


def _fold_product_id_at(
    spans: list[tuple[str, int, int]], i: int
) -> str | None:
    """Fold adjacent word/hyphen/dot spans into a product-id candidate.

    The byline object capture is a single ``\\w+`` token, so multi-segment
    product ids (``continue.dev``, ``code-whisperer``) need a short adjacent
    fold before closed-list lookup. Requires character adjacency so spaced
    punctuation cannot glue unrelated words.
    """
    n = len(spans)
    if i >= n or not re.fullmatch(r"\w+", spans[i][0]):
        return None
    parts = [spans[i][0]]
    seps: list[str] = []
    j = i
    while j + 2 < n:
        sep = spans[j + 1][0]
        nxt = spans[j + 2][0]
        if sep not in "-." or not re.fullmatch(r"\w+", nxt):
            break
        if spans[j][2] != spans[j + 1][1] or spans[j + 1][2] != spans[j + 2][1]:
            break
        seps.append(sep)
        parts.append(nxt)
        j += 2
    if len(parts) == 1:
        return parts[0]
    out = parts[0]
    for sep, part in zip(seps, parts[1:], strict=True):
        out += sep + part
    return out


# Object NP word window for space-separated brand prefixes after a byline
# preposition. The capture is a single ``\w+``; a short forward run lets a
# closed-list referent land after a brand prefix (``Google Bard``) without
# turning any later tool mention in the sentence into a byline. Hyphen/dot
# product-id folds stay character-adjacent via ``_fold_product_id_at``.
_OBJECT_NP_WORD_WINDOW = 3


def _advance_product_id_span(
    spans: list[tuple[str, int, int]], i: int
) -> int:
    """Return index just past a product-id fold starting at *i* (or *i* + 1)."""
    n = len(spans)
    if i >= n:
        return i
    if not re.fullmatch(r"\w+", spans[i][0]):
        return i + 1
    j = i
    while j + 2 < n:
        sep = spans[j + 1][0]
        nxt = spans[j + 2][0]
        if sep not in "-." or not re.fullmatch(r"\w+", nxt):
            break
        if spans[j][2] != spans[j + 1][1] or spans[j + 1][2] != spans[j + 2][1]:
            break
        j += 2
    return j + 1


def _token_is_object_slot_credit(
    tok: str,
    spans: list[tuple[str, int, int]],
    idx: int,
    *,
    named_only: bool,
) -> bool:
    """Closed-list membership for one object-NP word, including product-id fold."""
    if named_only:
        if _word_is_named_credit_referent(tok):
            return True
    elif _word_is_credit_byline_referent(tok):
        return True
    folded = _fold_product_id_at(spans, idx)
    if folded is not None and folded != tok and _word_is_named_credit_referent(folded):
        return True
    return False


def _object_slot_is_credit_referent(
    obj: str,
    spans: list[tuple[str, int, int]],
    obj_idx: int,
    *,
    named_only: bool = False,
) -> bool:
    """True when the byline object slot resolves to a credit referent.

    Checks the captured ``\\w+`` token first, then a hyphen/dot product-id fold
    starting at *obj_idx* for multi-segment names the capture cannot hold, then
    a bounded forward scan of space-separated words in the object NP so a brand
    prefix cannot shift a closed-list referent out of the capture slot.
    Membership of a token is the mechanism — a two-word human name has the same
    shape and survives because neither token is on the closed list.
    When *named_only* is true, generic agent heads do not count (used by the
    ``reviewed`` arm, where review of a generic assistant is not authorship).
    """
    if _token_is_object_slot_credit(obj, spans, obj_idx, named_only=named_only):
        return True
    # Capture + fold already checked; walk following words in the bounded window.
    n = len(spans)
    if obj_idx >= n:
        return False
    i = _advance_product_id_span(spans, obj_idx)
    words_after = 0
    max_after = _OBJECT_NP_WORD_WINDOW - 1
    while i < n and words_after < max_after:
        tok = spans[i][0]
        if not re.fullmatch(r"\w+", tok):
            # Punctuation ends the object NP.
            break
        if tok in _NP_CLAUSE_BOUNDARIES:
            # Preposition/subordinator ends the object NP (same as pure-agent walk).
            break
        if _token_is_object_slot_credit(tok, spans, i, named_only=named_only):
            return True
        i = _advance_product_id_span(spans, i)
        words_after += 1
    return False


def _has_generated_with_credit(normalized_subject: str) -> bool:
    """Tier 1 arm: ``generated with`` is credit only for credit-shaped objects.

    Determiner-led objects refuse when the post-determiner NP is a pure
    generic agent phrase under the same head+modifier test used by the Tier-2
    byline arm (``Generated with the assistant``, ``Generated with a model``),
    or when that NP is bare by token count (one word or fewer after the
    determiner) — the counter-case ``Generated with a tool`` is deliberately
    still refused. That bare-NP branch inspects span length only; it does not
    inspect the head noun's identity. Multi-word determiner-led non-agent NPs
    (``Generated with a timeout parameter``) therefore survive. Bare named
    agent/vendor referents refuse. Bare multi-word ordinary nouns
    (``Generated with deterministic seed handling``) survive so Tier 2 does
    not need to re-destroy them.
    """
    spans = _spans_for_np_scan(normalized_subject)
    span_starts = [start for _tok, start, _end in spans]
    for match in _GENERATED_WITH_OBJECT_RE.finditer(normalized_subject):
        obj = match.group(1)
        if obj in _DETERMINERS:
            # Post-determiner NP: pure agent heads refuse; bare (≤1 word) NPs
            # refuse by token count without reading the head's identity.
            start_idx = bisect.bisect_left(span_starts, match.end())
            if _post_determiner_head_is_generic_agent_from_spans(spans, start_idx):
                return True
            # Bare NP (single ordinary word after the determiner) is still
            # credit-shaped specificity: refuse without widening agent heads.
            if _post_determiner_np_is_bare_from_spans(spans, start_idx):
                return True
            continue
        obj_idx = bisect.bisect_left(span_starts, match.start())
        while obj_idx < len(spans) and spans[obj_idx][0] != obj:
            obj_idx += 1
        if obj_idx >= len(spans):
            obj_idx = bisect.bisect_left(span_starts, match.end() - len(obj))
        if _object_slot_is_credit_referent(obj, spans, obj_idx):
            return True
    return False


def _has_credit_idiom(normalized_subject: str, raw_subject: str) -> bool:
    """Tier 1: credit idioms are never legitimate; evidence cannot unlock."""
    if _ROBOT_EMOJI in raw_subject:
        return True
    if _COAUTHOR_RE.search(normalized_subject):
        return True
    if _has_trailer_verb_by_credit(normalized_subject):
        return True
    if _OFFLOAD_BACKEND_AT_START_RE.search(normalized_subject):
        return True
    if _has_credit_to_attribution(normalized_subject):
        return True
    if _has_credit_form_attribution(normalized_subject):
        return True
    if _has_generated_with_credit(normalized_subject):
        return True
    for phrase in _UNCONDITIONAL_PHRASES:
        if isinstance(phrase, re.Pattern):
            if phrase.search(normalized_subject):
                return True
        elif phrase in normalized_subject:
            return True
    return False


def _has_attribution_byline(normalized_subject: str) -> bool:
    """Tier 2: authorship verb + by/with + credit referent is a byline.

    ``by``/``with`` + determiner is ordinary description and must survive,
    unless the post-determiner NP ends in a generic agent head with only
    adjectival modifiers (``assistant``, ``advanced agent``, …).

    Core ``_AUTH_VERBS`` bare-object-refuse only when the object resolves to a
    named agent/vendor referent (closed list with word-boundary capture) or a
    generic agent head — not every capitalised noun. Plan provenance
    (``implementation note``), human names, and non-agent tooling (``Alembic``,
    ``Postgres``, ``Hypothesis``) therefore survive. Trailer-only verbs
    (``assisted``, ``powered``) only bare-object-refuse when the object itself
    is a generic agent head, so ``powered by Redis`` survives while
    ``Powered by an AI assistant`` still dies on the determiner+pure-agent path.

    Tokenize the subject once. Each match only walks the NP after its end
    index; re-tokenizing the whole remaining suffix per match is not required
    and is quadratic in match density times subject length.

    ``reviewed by|with`` is a separate arm: only a closed named referent or
    registered backend refuses. Generic agent NPs and ordinary review
    language stay intact because reviewing is not authorship.
    """
    spans = _spans_for_np_scan(normalized_subject)
    span_starts = [start for _tok, start, _end in spans]
    for match in _BYLINE_RE.finditer(normalized_subject):
        obj = match.group(1)
        # NP content starts at the object token (bare) or after a determiner.
        obj_idx = bisect.bisect_left(span_starts, match.start())
        while obj_idx < len(spans) and spans[obj_idx][0] != obj:
            obj_idx += 1
        if obj_idx >= len(spans):
            obj_idx = bisect.bisect_left(span_starts, match.end() - len(obj))
        if obj not in _DETERMINERS:
            # Bare object: named agent/vendor referent (including multi-segment
            # product ids), or pure agent NP (``artificial intelligences``,
            # ``neural networks``).
            if _object_slot_is_credit_referent(obj, spans, obj_idx):
                return True
            if _post_determiner_head_is_generic_agent_from_spans(spans, obj_idx):
                return True
            continue
        # Determiner absorbed the bare-object guard; inspect the NP content.
        start_idx = bisect.bisect_left(span_starts, match.end())
        if _post_determiner_head_is_generic_agent_from_spans(spans, start_idx):
            return True
    for match in _SOFT_BYLINE_RE.finditer(normalized_subject):
        obj = match.group(1)
        obj_idx = bisect.bisect_left(span_starts, match.start())
        while obj_idx < len(spans) and spans[obj_idx][0] != obj:
            obj_idx += 1
        if obj not in _DETERMINERS:
            # Soft verbs: bare agent head / pure agent NP is credit;
            # bare infra/human is not.
            if _word_is_generic_agent_head(obj):
                return True
            if _post_determiner_head_is_generic_agent_from_spans(
                spans, obj_idx, strip_trailing_adjuncts=False
            ):
                return True
            continue
        start_idx = bisect.bisect_left(span_starts, match.end())
        # Soft verbs still refuse pure agent NPs, but do not strip closed
        # trailing adjuncts — residual padding after a pure agent NP is not
        # enough to re-open the soft arm.
        if _post_determiner_head_is_generic_agent_from_spans(
            spans, start_idx, strip_trailing_adjuncts=False
        ):
            return True
    for match in _REVIEWED_BYLINE_RE.finditer(normalized_subject):
        obj = match.group(1)
        if obj in _DETERMINERS:
            # Determiner-led review language is never authorship credit.
            continue
        obj_idx = bisect.bisect_left(span_starts, match.start())
        while obj_idx < len(spans) and spans[obj_idx][0] != obj:
            obj_idx += 1
        if obj_idx >= len(spans):
            obj_idx = bisect.bisect_left(span_starts, match.end() - len(obj))
        # Named machine referent only — not generic agent heads.
        if _object_slot_is_credit_referent(obj, spans, obj_idx, named_only=True):
            return True
    return False


def _match_covered_by_registered_backend(
    normalized_subject: str, start: int, end: int
) -> re.Match[str] | None:
    """Return the registered-backend match covering ``[start, end)``, if any."""
    for backend in _REGISTERED_BACKEND_IDS_NORM:
        for form in (backend, backend.replace("-", " ")):
            for bm in re.finditer(
                rf"(?<!\w){re.escape(form)}(?![a-z])", normalized_subject
            ):
                if bm.start() <= start and end <= bm.end():
                    return bm
    return None


def _leading_tokens_after_vendor(suffix: str) -> tuple[list[str], int]:
    """Tokenize *suffix* and skip version/tier words; return (tokens, idx)."""
    tokens = re.findall(r"[a-z0-9]+(?:\.[0-9]+)*", suffix)
    idx = 0
    while idx < len(tokens):
        tok_i = tokens[idx]
        if tok_i in _MODEL_TIER_WORDS or re.fullmatch(r"\d+(?:\.\d+)*", tok_i):
            idx += 1
            continue
        break
    return tokens, idx


def _is_type_prefix_only(lead: str) -> bool:
    """True for conventional commit type prefixes (``feat:``, ``fix(api):``)."""
    return bool(re.fullmatch(r"[\w]+(?:\([\w./\-]+\))?:", lead))


def _weak_prep_is_noncredit_routing(prefix: str) -> bool:
    """True when a weak prep at the end of *prefix* is routing, not credit.

    ``Generated using Claude`` / ``Authored per grok-4.5`` stay credit because
    an auth/credit verb directly governs the prep. ``Ported the guard from
    claude-code`` is routing: a determiner-led object NP sits between the
    verb and the prep.
    """
    if not _WEAK_PREP_BEFORE_RE.search(prefix):
        return False
    # No auth/credit verb at all → pure routing.
    if not (
        _AUTH_VERB_IN_TEXT_RE.search(prefix) or _WITH_CREDIT_VERB_IN_TEXT_RE.search(prefix)
    ):
        return True
    # Auth verb present: still routing when an object NP (determiner) sits
    # between the last auth/credit verb and the trailing prep.
    last_verb_end = -1
    for cre in (_AUTH_VERB_IN_TEXT_RE, _WITH_CREDIT_VERB_IN_TEXT_RE):
        for m in cre.finditer(prefix):
            if m.end() > last_verb_end:
                last_verb_end = m.end()
    if last_verb_end < 0:
        return True
    between = prefix[last_verb_end:]
    if re.search(
        r"\b(?:the|a|an|our|its|their|this|that|these|those)\b", between
    ):
        return True
    return False


def _vendor_token_match_is_noncredit_context(
    normalized_subject: str, match: re.Match[str]
) -> bool:
    """True when a Tier-3 vendor-token hit is subject matter, not credit.

    Default is refuse (return False). Explicit non-credit positions only:

    * weak-prep routing without a leading auth verb (``via claude 3.5``)
    * ``with <vendor>`` without a with-credit verb (latency/parity prose)
    * registered backend in weak-prep routing or product-announcement lead
    * hyphen-embedded identifier segment in multi-word prose
      (``feature/wb-grok-transport-01 rebased…``)
    * vendor/product name at subject lead with non-claim continuation
      (``Llama weights are…``, ``claude-code now declares…``)

    Deliberately still refused: bare tokens, version-glued names (``Grok4``),
    ``by <vendor>`` (including ``Authored by aider``), claim-verb subjects,
    possessives, thanks/courtesy, mid-sentence unbacked mentions
    (``fix the claude costclass lane``), and sole path-like identifier
    subjects.
    """
    start, end = match.start(), match.end()
    prefix = normalized_subject[:start]
    suffix = normalized_subject[end:]
    stripped = normalized_subject.strip()

    # Sole path-like / bare token subject — always evidence-gated.
    if re.fullmatch(r"[\w./\-]+", stripped):
        return False

    # Version glued on (``Grok4``, ``GPT_5``, ``claude-style`` hyphen+letter
    # is identifier; hyphen+digit is version — handled below).
    if suffix and (suffix[0].isdigit() or suffix.startswith("_")):
        return False

    # Possessive vendor credit: ``Claude's patch``, ``grok's help``.
    if suffix.startswith("'s") or suffix.startswith("\u2019s") or suffix.startswith("'"):
        return False

    # ``thanks to`` / ``courtesy of`` before weak-prep ``to`` can match.
    if _THANKS_BEFORE_RE.search(prefix):
        return False

    # Registered backend compound (hyphen or spaced).
    backend_match = _match_covered_by_registered_backend(
        normalized_subject, start, end
    )
    if backend_match is not None:
        b_prefix = normalized_subject[: backend_match.start()]
        b_suffix = normalized_subject[backend_match.end() :]
        if _weak_prep_is_noncredit_routing(b_prefix):
            return True
        # ``with codex-cli parity in mind`` — instrumental, not authorship.
        if _WITH_BEFORE_RE.search(b_prefix) and not _WITH_CREDIT_VERB_IN_TEXT_RE.search(
            b_prefix
        ):
            return True
        lead = b_prefix.strip()
        if not lead or _is_type_prefix_only(lead):
            b_tokens, b_idx = _leading_tokens_after_vendor(b_suffix)
            if b_tokens and b_idx < len(b_tokens) and b_tokens[b_idx] not in _SUBJECT_CLAIM_VERBS:
                return True
            return False
        # ``by <backend>`` with a non-empty lead: credit only when an
        # authorship verb or subject-claim verb governs the byline. Routing
        # / assignment / schedule verbs are repository subject matter and
        # must survive for every registered backend id alike. Caller
        # grounding cannot unlock this path — the verb test is the gate.
        # Other non-empty leads stay evidence-gated (mid-sentence backend
        # mentions like ``drop the grok-remote trailer`` still refuse).
        # ``reviewed by <backend>`` is handled at Tier 2 (named-referent
        # reviewed arm) before this Tier-3 skip runs.
        if _BY_BEFORE_RE.search(b_prefix):
            if _AUTH_VERB_IN_TEXT_RE.search(b_prefix):
                return False
            if any(
                tok in _SUBJECT_CLAIM_VERBS
                for tok in re.findall(r"[a-z0-9]+(?:\.[0-9]+)*", b_prefix)
            ):
                return False
            return True
        return False

    # ``by <vendor>``: always credit at Tier 3 (auth bylines also refuse at
    # Tier 2 when the object is a closed referent).
    if _BY_BEFORE_RE.search(prefix):
        return False

    # Weak preposition routing (``via``, ``from``, ``per``, …). Auth verbs
    # only force credit when they directly govern the prep (``Generated using
    # Claude``), not when an object NP intervenes (``Ported the guard from
    # claude-code``).
    if _WEAK_PREP_BEFORE_RE.search(prefix):
        if _weak_prep_is_noncredit_routing(prefix):
            return True
        return False

    # ``with <vendor>``: credit after a with-credit verb, or possessive help
    # (``with grok's help``). Latency/parity prose survives.
    if _WITH_BEFORE_RE.search(prefix):
        with_match = None
        for m in _WITH_BEFORE_RE.finditer(prefix):
            with_match = m
        if with_match is not None:
            before_with = prefix[: with_match.start()]
            if _WITH_CREDIT_VERB_IN_TEXT_RE.search(before_with):
                return False
            if suffix.startswith("'s") or suffix.startswith("\u2019s") or (
                suffix.startswith("'") and "help" in suffix[:12]
            ):
                return False
            return True

    # Hyphen/underscore-embedded identifier segment in multi-word prose:
    # ``wb-grok-transport`` (letter after the following hyphen), not ``grok-4.5``.
    if start > 0 and normalized_subject[start - 1] in "-_/":
        rest = suffix
        if rest.startswith("-") and len(rest) > 1 and rest[1].isalpha():
            return True
        if rest.startswith("_") and len(rest) > 1 and rest[1].isalpha():
            return True

    # Hyphen + non-version tail that is not a multi-segment identifier
    # (``claude-style``): still a vendor mention — refuse.
    if suffix.startswith("-") and len(suffix) > 1 and not suffix[1].isdigit():
        return False

    tokens, idx = _leading_tokens_after_vendor(suffix)
    if idx < len(tokens) and tokens[idx] in _SUBJECT_CLAIM_VERBS:
        return False

    # Product / ecosystem announcement at subject lead only.
    lead = prefix.strip()
    if not lead or _is_type_prefix_only(lead):
        if tokens and idx < len(tokens) and tokens[idx] not in _SUBJECT_CLAIM_VERBS:
            return True

    return False


def _has_unbacked_vendor_token(normalized_subject: str, normalized_evidence: str) -> bool:
    # Subject match uses the asymmetric boundary (SG-13). Evidence match is
    # substring: changed lines may embed the token next to escapes or
    # punctuation and still corroborate it. Position-aware skips keep
    # registered backends, weak-prep model pins, and non-agent byline objects
    # from being re-destroyed after Tier 2 accepted them.
    for token in VENDOR_CREDIT_TOKENS:
        tok_n = normalize_for_match(token)
        if tok_n in normalized_evidence:
            continue
        pattern = _token_pattern(tok_n)
        for match in pattern.finditer(normalized_subject):
            if _vendor_token_match_is_noncredit_context(normalized_subject, match):
                continue
            return True
    return False


def screen_commit_subject_body(
    summary: str,
    *,
    grounding: Iterable[str] = (),
    max_body_len: int | None = None,
) -> str:
    """Return the screened subject *body* (no ``offload: `` prefix).

    Attribution and credit-idiom screening run on the untruncated first line so
    a length cap cannot manufacture a byline by severing a trailing head noun.
    Vendor-token screening runs on the truncated candidate so a token past the
    cut never rejects prose that would not have shipped it. Blank summary →
    fallback body. Credit idioms, attribution bylines, residual non-ASCII
    letters, or unbacked vendor tokens → fallback body. Otherwise the
    truncated candidate.
    """
    budget = max_body_len if max_body_len is not None else _MAX_COMMIT_SUBJECT - len(_REMOTE_TURN_SUBJECT_PREFIX)
    stripped = summary.strip()
    if not stripped:
        return _FALLBACK_BODY

    candidate = stripped.splitlines()[0]
    normalized = normalize_for_match(candidate)

    # Tier 1 — credit idioms (unconditional; evidence must not unlock).
    if _has_credit_idiom(normalized, candidate):
        return _FALLBACK_BODY

    # SG-15 — residual non-ASCII letters after normalization are lookalikes.
    if _has_non_ascii_letter(normalized):
        return _FALLBACK_BODY

    # Tier 2 — attribution grammar on the full line (unconditional).
    # Truncating first can drop a trailing non-agent head and invent a byline.
    if _has_attribution_byline(normalized):
        return _FALLBACK_BODY

    # Accept shape is known; cut to budget before the evidence-gated token pass
    # so a vendor name past the cut cannot reject text that never ships it.
    truncated = truncate_at_word_boundary(candidate, budget)
    normalized_trunc = normalize_for_match(truncated)

    # Tier 3 — bare vendor tokens (evidence-gated) on what would actually land.
    evidence = evidence_from_grounding(grounding)
    if _has_unbacked_vendor_token(normalized_trunc, evidence):
        return _FALLBACK_BODY

    return truncated


# Separator classes folded before vendor-identity / byline matching on lane ids.
# Hyphen, underscore, dot, and whitespace must screen identically so
# ``claude-code``, ``claude_code``, ``claude code``, and ``claude.code`` share
# one canonical form; the same fold lets ``written_by_claude`` match the
# multi-word credit form ``written by claude``.
_LANE_ID_SEPARATOR_RE = re.compile(r"[-_.\s]+")


def _fold_lane_id_separators(text: str) -> str:
    """Collapse hyphen, underscore, dot, and space runs to a single space."""
    return _LANE_ID_SEPARATOR_RE.sub(" ", text).strip()


# Role tails that may be glued onto a vendor without a separator
# (``claudecode``, ``codexcli``). Arbitrary alphabetic remainders
# (``grokking``) must not count — that is ordinary prose, not identity glue.
_IDENTITY_ROLE_SUFFIXES = frozenset(
    {
        "code",
        "cli",
        "remote",
        "worker",
        "agent",
        "lane",
        "host",
        "bot",
        "subagent",
        "local",
        "build",
    }
)
# Fillers that may sit after a vendor in an identity id without making the
# vendor attributive. ``review`` is a role tail in identity slugs
# (``codex-review-lane``), not work substance naming a product under edit.
_IDENTITY_SHAPE_FILLERS = _IDENTITY_ROLE_SUFFIXES | frozenset({"review"})
# Agentive English spellings of a role stem (``reviewer`` / ``builder`` /
# ``coders``). Longer first so plural forms strip cleanly before singular.
_AGENTIVE_SUFFIXES = ("ers", "ors", "er", "or")
_LANE_ID_ROUND_RE = re.compile(r"^r\d+$")


def _agentive_base_candidates(token: str) -> tuple[str, ...]:
    """Stems from stripping agentive ``-er`` / ``-or`` (and plurals).

    Silent-``e`` restoration (``coder`` → ``code``) and doubled-consonant
    undoubling (``runner`` → ``run``) are derived, not enumerated, so the
    next agentive of a known role does not require a new set member (DATA-01).
    """
    out: list[str] = []
    for suffix in _AGENTIVE_SUFFIXES:
        if len(token) <= len(suffix) or not token.endswith(suffix):
            continue
        stem = token[: -len(suffix)]
        if not stem:
            continue
        out.append(stem)
        out.append(stem + "e")
        if len(stem) >= 2 and stem[-1] == stem[-2] and stem[-1].isalpha():
            out.append(stem[:-1])
    return tuple(out)


def _token_in_role_set(token: str, role_set: frozenset[str]) -> bool:
    """True when *token* is in *role_set* or is an agentive form of a member."""
    if token in role_set:
        return True
    for base in _agentive_base_candidates(token):
        if base in role_set:
            return True
    return False


def _rest_is_role_or_role_digits(rest: str, role_set: frozenset[str]) -> bool:
    """Role tail, agentive role tail, or either plus trailing digits."""
    if rest.isdigit():
        return True
    if _token_in_role_set(rest, role_set):
        return True
    i = len(rest)
    while i > 0 and rest[i - 1].isdigit():
        i -= 1
    if 0 < i < len(rest) and _token_in_role_set(rest[:i], role_set):
        return True
    return False


def _lane_id_token_is_identity_filler(token: str) -> bool:
    """Ordinal, round tag, or role filler — not work substance."""
    if token.isdigit() or _LANE_ID_ROUND_RE.fullmatch(token) is not None:
        return True
    return _token_in_role_set(token, _IDENTITY_SHAPE_FILLERS)


def _lane_id_token_is_vendor_like(token: str) -> bool:
    """True when *token* is a vendor or vendor+role/digits glue form."""
    for vendor in VENDOR_CREDIT_TOKENS:
        tok_n = normalize_for_match(vendor)
        if token == tok_n:
            return True
        if not token.startswith(tok_n) or len(token) <= len(tok_n):
            continue
        rest = token[len(tok_n) :]
        if _rest_is_role_or_role_digits(rest, _IDENTITY_ROLE_SUFFIXES):
            return True
    return False


def _vendor_has_following_work_substance(sep_folded: str, token: str) -> bool:
    """True when *token* sits attributively before a work noun in *sep_folded*.

    Work-describing ids put the vendor before product/work substance
    (``fix-grok-remote-timeout-01``: grok modifies timeout work). Identity ids
    put the vendor with only role/version/ordinal tails
    (``claude-worker-01``, ``impl-byline-gpt5-r0730``).
    """
    tok_n = normalize_for_match(token)
    tokens = sep_folded.split()
    for i, word in enumerate(tokens):
        matched = word == tok_n
        if not matched and word.startswith(tok_n) and len(word) > len(tok_n):
            rest = word[len(tok_n) :]
            if _rest_is_role_or_role_digits(rest, _IDENTITY_ROLE_SUFFIXES):
                matched = True
        if not matched:
            continue
        for later in tokens[i + 1 :]:
            if _lane_id_token_is_identity_filler(later):
                continue
            if _lane_id_token_is_vendor_like(later):
                continue
            return True
    return False


def _lane_id_is_agent_identity_shape(lane_id: str, *, sep_folded: str) -> bool:
    """True when a vendor token in *lane_id* names the actor, not a product.

    Judge by grammatical role, not whitespace. Real lane ids are hyphenated
    slugs with no spaces, so a "no whitespace ⇒ identity" rule made the
    work-subject arm unreachable and erased product-naming ids
    (``fix-grok-remote-timeout-01``). A vendor is identity-shaped when no
    work-substance token follows it (only role/version/ordinal fillers). A
    vendor with following work substance is attributive and must not fire the
    bare-token arm. Credit idioms and attribution bylines still refuse on
    both shapes.
    """
    del lane_id  # shape is fully determined by separator-folded tokens
    for token in VENDOR_CREDIT_TOKENS:
        if not _identity_contains_vendor_token(sep_folded, token):
            continue
        if not _vendor_has_following_work_substance(sep_folded, token):
            return True
    return False


def _identity_contains_vendor_token(sep_folded: str, token: str) -> bool:
    """Vendor match on separator-folded text, including run-together glue.

    Word-boundary match covers spaced forms after fold (``claude code``).
    Run-together forms (``claudecode``) have no internal separator: accept a
    single word that is the vendor plus a known role suffix or trailing digits.
    Agentive role tails (``reviewer``, ``builder``) count via stem derivation,
    not a second enumerated set.
    """
    tok_n = normalize_for_match(token)
    if _contains_token(sep_folded, tok_n):
        return True
    for word in sep_folded.split():
        if not word.startswith(tok_n) or len(word) <= len(tok_n):
            continue
        rest = word[len(tok_n) :]
        # role / agentive role / role+digits: code01, reviewer2
        if _rest_is_role_or_role_digits(rest, _IDENTITY_ROLE_SUFFIXES):
            return True
    return False


def _lane_id_carries_credit(lane_id: str) -> bool:
    """True when a lane identifier would put credit into git history.

    Discriminates agent identity from work subject. Credit idioms and
    attribution bylines always refuse (both shapes), after separator folding so
    underscore-glued multi-word credit matches space-separated forms. Bare
    vendor tokens refuse only when the vendor names the agent/lane (identity
    shape), not when it names a product under edit in a work-describing id.
    This is not ``screen_commit_subject_body`` — that prose policy collapses
    ordinary identifiers to one fallback and erases lane identity.
    """
    normalized = normalize_for_match(lane_id)
    sep_folded = _fold_lane_id_separators(normalized)
    if _has_credit_idiom(sep_folded, lane_id):
        return True
    if _has_non_ascii_letter(normalized):
        return True
    if _has_attribution_byline(sep_folded):
        return True
    # Vendor token = agent identity only when not attributive of work substance.
    if _lane_id_is_agent_identity_shape(lane_id, sep_folded=sep_folded):
        for token in VENDOR_CREDIT_TOKENS:
            if _identity_contains_vendor_token(sep_folded, token):
                return True
    return False


def _discriminating_lane_placeholder(lane_id: str) -> str:
    """Stable stand-in that refuses credit yet keeps lanes distinguishable."""
    # surrogatepass: total over all str inputs (lone surrogates have no UTF-8 form).
    # Keeps the digest injective and byte-identical for inputs that already encode.
    digest = hashlib.sha256(lane_id.encode("utf-8", errors="surrogatepass")).hexdigest()[:12]
    return f"lane-{digest}"


# Split lane ids while retaining separators so vendor elision can rejoin cleanly.
_LANE_ID_SPLIT_KEEP_SEP_RE = re.compile(r"([-_.\s]+)")
# Authorship verbs that are credit residue after a vendor span is removed
# (``claude-authored-guard`` → residual ``authored-guard`` must not land).
_AUTH_VERB_TOKEN_SET = frozenset(_AUTH_VERBS.split("|"))


# Bound on elide-and-join re-screens. Interior elision can rejoin neighbours
# into a fresh vendor spelling; iterate until the residual stops changing.
_ELIDE_FIXPOINT_CAP = 8


def _elide_vendor_tokens_once(lane_id: str) -> str:
    """One elide-and-join pass over *lane_id* (no re-screen of the residual)."""
    text = lane_id
    # Multi-separator vendor tokens first (e.g. ``offload-backend``), longest
    # first so a longer form is not partially eaten by a shorter sibling.
    multi = [v for v in VENDOR_CREDIT_TOKENS if any(ch in v for ch in "-_.")]
    for vendor in sorted(multi, key=len, reverse=True):
        text = _token_pattern(vendor).sub("", text)

    parts = _LANE_ID_SPLIT_KEEP_SEP_RE.split(text)
    out: list[str] = []
    for i, part in enumerate(parts):
        if i % 2 == 1:
            out.append(part)
            continue
        if part and _lane_id_token_is_vendor_like(normalize_for_match(part)):
            out.append("")
        else:
            out.append(part)
    joined = "".join(out)
    collapsed = re.sub(r"([-_.\s])(?:[-_.\s])+", r"\1", joined)
    return collapsed.strip("-_. \t\n\r")


def _elide_vendor_tokens_from_lane_id(lane_id: str) -> str:
    """Remove vendor-token spans from *lane_id*; keep remaining work substance.

    Pure agent-identity ids use the digest placeholder path before this runs.
    When a vendor token sits beside real work substance, drop only the vendor
    span so the residual still discriminates between lanes and still reads as
    that work (``grok-fix-01`` → ``fix-01``). After each join, re-screen the
    residual and iterate to a fixpoint: eliding an interior span can rejoin
    neighbours into a clean vendor spelling that did not appear in the input
    (``open-gpt-ai`` → ``open-ai``). Bound the loop so a pathological residual
    cannot spin. Never reintroduce the prose subject screen here — that
    collapses identifiers to one fallback.
    """
    text = lane_id
    for _ in range(_ELIDE_FIXPOINT_CAP):
        nxt = _elide_vendor_tokens_once(text)
        if nxt == text:
            return text
        text = nxt
    return text


def _residual_segments_join_to_vendor(residual: str) -> bool:
    """True when contiguous residual segments concatenate to a vendor token.

    Elision can manufacture a vendor spelling that the single-pass token split
    never saw as one part (``open`` + ``ai`` → ``openai``). Catch that join so
    the residual falls back to the digest path instead of landing in history.
    """
    normalized = normalize_for_match(residual)
    parts = _LANE_ID_SPLIT_KEEP_SEP_RE.split(normalized)
    segs = [parts[i] for i in range(0, len(parts), 2) if parts[i]]
    if not segs:
        return False
    vendor_norms = {normalize_for_match(v) for v in VENDOR_CREDIT_TOKENS}
    n = len(segs)
    for i in range(n):
        for j in range(i + 1, n + 1):
            joined = "".join(segs[i:j])
            if joined in vendor_norms or _lane_id_token_is_vendor_like(joined):
                return True
    return False


def _lane_id_residual_carries_credit(residual: str) -> bool:
    """True when vendor-elided residual is empty or still credit-bearing.

    Authorship-verb tokens left after elision (``authored-guard``) are credit
    residue, not work substance — fall back to the digest placeholder of the
    original id so discrimination survives without landing the verb. Joined
    residual segments that spell a vendor token (manufactured by interior
    elision) are also credit residue.
    """
    if not residual:
        return True
    if _lane_id_carries_credit(residual):
        return True
    sep_folded = _fold_lane_id_separators(normalize_for_match(residual))
    if any(word in _AUTH_VERB_TOKEN_SET for word in sep_folded.split()):
        return True
    return _residual_segments_join_to_vendor(residual)


def sanitize_lane_id_for_commit_message(lane_id: str, *, max_len: int | None = None) -> str:
    """Encode an untrusted lane_id for a git commit-message sink.

    lane_id crosses a trust boundary here: a raw newline (or CR) would change
    the message's line structure and can smuggle git trailers into history.
    Collapse to a single physical line. Credit screening has two arms:

    1. Pure agent identity, credit idioms, and attribution bylines → digest
       stand-in that still discriminates between lanes.
    2. Vendor token beside work substance → elide only the vendor span; keep
       the residual work substance so the subject stays discriminating and
       free of vendor names in git history (SG-20). Authorship-verb residue
       after elision falls back to arm 1.

    This is not ``screen_commit_subject_body`` (REVBYL-B-02): the prose screen
    collapses credit-looking identifiers to one fallback and erases lane
    identity. Optionally bound length when the value participates in the
    subject.
    """
    safe = lane_id.replace("\r\n", "\n").replace("\r", "\n").split("\n", 1)[0]
    if _lane_id_carries_credit(safe):
        safe = _discriminating_lane_placeholder(safe)
    else:
        elided = _elide_vendor_tokens_from_lane_id(safe)
        if elided != safe:
            if _lane_id_residual_carries_credit(elided):
                safe = _discriminating_lane_placeholder(safe)
            else:
                safe = elided
    if max_len is not None:
        safe = safe[:max_len]
    return safe


def build_remote_turn_commit_message(
    *,
    lane_id: str,
    summary: str,
    grounding: Iterable[str] = (),
) -> str:
    """Pure builder for the remote-turn commit message.

    Neutral template only: first line of summary (truncated so the WHOLE subject
    is at most 72 chars) as the subject, or ``remote turn`` when summary is
    blank/whitespace or carries an unbacked / unconditional credit. Subject is
    accepted verbatim or replaced whole — never excised mid-string.
    """
    first_line = screen_commit_subject_body(summary, grounding=grounding)
    safe_lane = sanitize_lane_id_for_commit_message(lane_id)
    return f"{_REMOTE_TURN_SUBJECT_PREFIX}{first_line}\n\nOffload turn for lane {safe_lane}."


def read_patch_text(patch_file) -> str:
    """Decode a patch file defensively (never raise on non-UTF-8 content)."""
    from pathlib import Path

    path = Path(patch_file)
    return path.read_bytes().decode("utf-8", "replace")
