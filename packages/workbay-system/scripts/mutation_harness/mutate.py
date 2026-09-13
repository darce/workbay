"""Apply one mutation spec to a file copy inside a sandbox."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


class MutationError(ValueError):
    """Mutation could not be applied."""


def _mutation_kind(mutation: dict[str, Any]) -> str:
    kind = str(mutation.get("kind") or mutation.get("type") or "replace").lower()
    if kind in ("regex", "re"):
        return "regex"
    if kind in ("replace", "str_replace", "string", "patch"):
        return "replace"
    raise MutationError(f"unsupported mutation kind: {kind!r}")


def _mutation_count(mutation: dict[str, Any]) -> int:
    try:
        count = int(mutation.get("count", 0))
    except (TypeError, ValueError) as exc:
        raise MutationError("mutation count must be a non-negative integer") from exc
    if count < 0:
        raise MutationError("mutation count must be a non-negative integer")
    return count


def _regex_pattern(mutation: dict[str, Any]) -> re.Pattern[str]:
    pattern = mutation.get("pattern")
    if not isinstance(pattern, str):
        raise MutationError("regex mutation requires string pattern and replacement")
    flags = 0
    if mutation.get("multiline", True):
        flags |= re.MULTILINE
    if mutation.get("dotall"):
        flags |= re.DOTALL
    try:
        return re.compile(pattern, flags)
    except re.error as exc:
        raise MutationError(f"invalid regex pattern: {exc}") from exc


def locate_mutation_spans(
    text: str,
    mutation: dict[str, Any],
) -> tuple[tuple[int, int], ...]:
    """Locate exactly the matches that :func:`apply_mutation` will replace.

    This is the single implementation of mutation-kind, flag, and ``count``
    matching semantics. Coverage selection uses it too, so it cannot attribute
    a mutant to lines that the mutation application would not touch.
    """
    kind = _mutation_kind(mutation)
    count = _mutation_count(mutation)
    if kind == "regex":
        if not isinstance(mutation.get("replacement"), str):
            raise MutationError(
                "regex mutation requires string pattern and replacement"
            )
        matches = _regex_pattern(mutation).finditer(text)
        spans = (
            tuple(match.span() for _, match in zip(range(count), matches))
            if count
            else tuple(match.span() for match in matches)
        )
    else:
        old = mutation.get("old", mutation.get("find"))
        new = mutation.get("new", mutation.get("replacement"))
        if not isinstance(old, str) or not isinstance(new, str) or not old:
            raise MutationError(
                "replace mutation requires non-empty string old and string new"
            )
        found: list[tuple[int, int]] = []
        start = 0
        while count == 0 or len(found) < count:
            index = text.find(old, start)
            if index < 0:
                break
            found.append((index, index + len(old)))
            start = index + len(old)
        spans = tuple(found)
    if not spans and not mutation.get("allow_no_match"):
        if kind == "regex":
            pattern = mutation.get("pattern")
            raise MutationError(f"regex pattern matched zero times: {pattern!r}")
        old = mutation.get("old", mutation.get("find"))
        preview = old[:80] if isinstance(old, str) else old
        raise MutationError(f"substring not found: {preview!r}")
    return spans


def apply_mutation(
    file_path: Path,
    mutation: dict[str, Any],
    *,
    mutant_id: str | None = None,
) -> str:
    """Apply ``mutation`` to ``file_path`` in place; return the new text.

    Supported kinds:
    - ``regex``: ``pattern`` + ``replacement`` (``count`` optional, default 0 = all)
    - ``replace`` / ``str_replace``: exact ``old`` -> ``new`` (``count`` optional)
    - ``patch``: apply unified-diff-style line replacements via ``old`` / ``new``
      blocks (exact substring, same as replace)

    Fail-closed guards:
    - zero matches without ``allow_no_match``
    - byte-identical result without explicit ``allow_noop`` (separate opt-in;
      ``allow_no_match`` does **not** authorize a no-op edit)
    """
    if not file_path.is_file():
        raise MutationError(f"target file not found: {file_path}")
    try:
        original = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise MutationError(f"cannot read {file_path}: {exc}") from exc

    kind = _mutation_kind(mutation)
    locate_mutation_spans(original, mutation)
    if kind == "regex":
        replacement = mutation.get("replacement")
        if not isinstance(replacement, str):
            raise MutationError(
                "regex mutation requires string pattern and replacement"
            )
        new_text = _regex_pattern(mutation).sub(
            replacement, original, count=_mutation_count(mutation)
        )
    else:
        old = mutation.get("old", mutation.get("find"))
        new = mutation.get("new", mutation.get("replacement"))
        if not isinstance(old, str) or not isinstance(new, str):
            raise MutationError("replace mutation requires string old and new")
        count = _mutation_count(mutation)  # 0 = all for str.replace semantics
        if count == 0:
            new_text = original.replace(old, new)
        else:
            new_text = original.replace(old, new, count)

    # No-op edit: n >= 1 (or substring found) but file is byte-identical.
    # allow_no_match must NOT double as permission for a no-op — separate key.
    if new_text == original and not mutation.get("allow_noop"):
        who = mutant_id if mutant_id is not None else file_path.name
        raise MutationError(
            f"mutation is a no-op (file byte-identical after apply) for mutant "
            f"{who!r} at {file_path.name}; set allow_noop=true to opt in"
        )

    try:
        file_path.write_text(new_text, encoding="utf-8")
    except OSError as exc:
        raise MutationError(f"cannot write {file_path}: {exc}") from exc
    return new_text
