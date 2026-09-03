"""Apply one mutation spec to a file copy inside a sandbox."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


class MutationError(ValueError):
    """Mutation could not be applied."""


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

    kind = str(mutation.get("kind") or mutation.get("type") or "replace").lower()
    if kind in ("regex", "re"):
        pattern = mutation.get("pattern")
        replacement = mutation.get("replacement")
        if not isinstance(pattern, str) or not isinstance(replacement, str):
            raise MutationError("regex mutation requires string pattern and replacement")
        count = int(mutation.get("count", 0))
        flags = 0
        if mutation.get("multiline", True):
            flags |= re.MULTILINE
        if mutation.get("dotall"):
            flags |= re.DOTALL
        try:
            compiled = re.compile(pattern, flags)
        except re.error as exc:
            raise MutationError(f"invalid regex pattern: {exc}") from exc
        new_text, n = compiled.subn(replacement, original, count=count)
        if n == 0 and not mutation.get("allow_no_match"):
            raise MutationError(
                f"regex pattern matched zero times in {file_path.name}: {pattern!r}"
            )
    elif kind in ("replace", "str_replace", "string", "patch"):
        old = mutation.get("old", mutation.get("find"))
        new = mutation.get("new", mutation.get("replacement"))
        if not isinstance(old, str) or not isinstance(new, str):
            raise MutationError("replace mutation requires string old and new")
        count = int(mutation.get("count", 0))  # 0 = all for str.replace semantics
        if old not in original and not mutation.get("allow_no_match"):
            raise MutationError(
                f"substring not found in {file_path.name}: {old[:80]!r}"
            )
        if count == 0:
            new_text = original.replace(old, new)
        else:
            new_text = original.replace(old, new, count)
    else:
        raise MutationError(f"unsupported mutation kind: {kind!r}")

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
