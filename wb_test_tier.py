"""Repo-root pytest test-tier plugin (``WORKBAY_TEST_TIER``).

Shipped as a *plugin module* rather than only a repo-root ``conftest.py`` so it
fires regardless of pytest ``rootdir``. Package-scoped runs -- ``cd
packages/<pkg> && pytest`` (``make test``), ``make test-handoff``, and
``check-all`` -- set ``rootdir`` to the package directory because every package
``pyproject.toml`` carries ``[tool.pytest.ini_options]``. pytest's default
``confcutdir`` is the ``rootdir``, so a repo-root ``conftest.py`` sitting *above*
it is never collected and this plugin would be silently absent. Each package
``tests/conftest.py`` and the repo-root ``conftest.py`` register this module via
``pytest_plugins``; pytest imports it once (deduped by name) and its hooks run
for every session.

Contract:

- ``WORKBAY_TEST_TIER`` unset or ``full``: no selection change (existing callers
  keep every collected item). Matching modules still receive the ``e2e`` marker
  so ``-m e2e`` / ``-m "not e2e"`` work where a caller can pass options.
- ``fast``: deselect every item whose **module** is classified e2e, plus every
  item carrying the existing ``timing`` marker.
- ``e2e``: keep only e2e-classified modules. If ``WORKBAY_TEST_TIER_SUBJECTS``
  is set (comma-separated, no spaces), additionally keep only e2e modules whose
  source text contains at least one subject substring. An empty result is
  allowed (pytest exit 5) — never fall back to the full set.
- Any other value: ``pytest.UsageError`` naming the accepted values.
- ``WORKBAY_DISABLE_TEST_TIER=1``: no-op (mirrors ``WORKBAY_DISABLE_LIVE_STATE_GUARD``).

Classification is a pure, deterministic function of the module's source text
(``_classify_module_source``). The classifier tokenizes the source and ignores
comments, string literals, and f-string middles; ``subprocess`` import aliases
(``import subprocess as sp``, ``from subprocess import run``) are resolved from
the same token stream. Unparseable source falls back to the historical raw-text
regex. The plugin reads each test file at most once per session and never
imports the test module.
"""

from __future__ import annotations

import ast
import io
import os
import re
import tokenize
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

_TIER_ENV = "WORKBAY_TEST_TIER"
_SUBJECTS_ENV = "WORKBAY_TEST_TIER_SUBJECTS"
_DISABLE_ENV = "WORKBAY_DISABLE_TEST_TIER"
_ACCEPTED_TIERS = ("e2e", "fast", "full")

_SUBPROCESS_CALL = re.compile(r"subprocess\.(run|Popen|check_output|check_call|call)\(")
_MAKE_ARGV = re.compile(r"""\[\s*["']make["']""")
_SUBPROCESS_CALL_NAMES = frozenset({"run", "Popen", "check_output", "check_call", "call"})

_TOKENIZE_SOFT = frozenset(
    {
        tokenize.COMMENT,
        tokenize.NL,
        tokenize.ENCODING,
        tokenize.INDENT,
        tokenize.DEDENT,
        tokenize.ENDMARKER,
    }
)
_CALL_SKIP = frozenset(_TOKENIZE_SOFT) | {tokenize.STRING, tokenize.NEWLINE}
if hasattr(tokenize, "FSTRING_MIDDLE"):
    _CALL_SKIP = _CALL_SKIP | {tokenize.FSTRING_MIDDLE}
_MAKE_SKIP = frozenset(_TOKENIZE_SOFT) | {tokenize.NEWLINE}

# path -> (is_e2e, source_text); session-scoped (one process).
_MODULE_CACHE: dict[str, tuple[bool, str]] = {}


def _regex_classify(text: str) -> bool:
    """Historical raw-text classification; used when tokenize cannot parse *text*."""

    return bool(_SUBPROCESS_CALL.search(text) or _MAKE_ARGV.search(text))


def _tokenize_source(text: str) -> list[tokenize.TokenInfo]:
    return list(tokenize.generate_tokens(io.StringIO(text).readline))


def _subprocess_bindings(tokens: Sequence[tokenize.TokenInfo]) -> tuple[set[str], set[str]]:
    """Return ``(module_aliases, direct_call_names)`` collected from import tokens.

    The canonical name ``subprocess`` always counts as a module alias so
    ``subprocess.run(`` classifies even in snippets that omit the import.
    """

    module_names = {"subprocess"}
    func_names: set[str] = set()
    code = [tok for tok in tokens if tok.type not in _TOKENIZE_SOFT]
    i = 0
    n = len(code)
    while i < n:
        tok = code[i]
        if tok.type == tokenize.NEWLINE:
            i += 1
            continue
        if (
            tok.type == tokenize.NAME
            and tok.string == "from"
            and i + 2 < n
            and code[i + 1].type == tokenize.NAME
            and code[i + 1].string == "subprocess"
            and code[i + 2].type == tokenize.NAME
            and code[i + 2].string == "import"
        ):
            i += 3
            while i < n and code[i].type != tokenize.NEWLINE:
                cur = code[i]
                if cur.type == tokenize.OP and cur.string in {"(", ")", ",", "*"}:
                    i += 1
                    continue
                if cur.type == tokenize.NAME:
                    name = cur.string
                    i += 1
                    alias = name
                    if (
                        i + 1 < n
                        and code[i].type == tokenize.NAME
                        and code[i].string == "as"
                        and code[i + 1].type == tokenize.NAME
                    ):
                        alias = code[i + 1].string
                        i += 2
                    if name in _SUBPROCESS_CALL_NAMES:
                        func_names.add(alias)
                    continue
                i += 1
            continue
        if tok.type == tokenize.NAME and tok.string == "import":
            i += 1
            while i < n and code[i].type != tokenize.NEWLINE:
                cur = code[i]
                if cur.type == tokenize.OP and cur.string in {"(", ")", ","}:
                    i += 1
                    continue
                if cur.type == tokenize.NAME:
                    first = cur.string
                    i += 1
                    while i < n and code[i].type == tokenize.OP and code[i].string == ".":
                        i += 1
                        if i < n and code[i].type == tokenize.NAME:
                            i += 1
                    alias = None
                    if (
                        i + 1 < n
                        and code[i].type == tokenize.NAME
                        and code[i].string == "as"
                        and code[i + 1].type == tokenize.NAME
                    ):
                        alias = code[i + 1].string
                        i += 2
                    if first == "subprocess":
                        module_names.add(alias or "subprocess")
                    continue
                i += 1
            continue
        i += 1
    return module_names, func_names


def _has_subprocess_call(tokens: Sequence[tokenize.TokenInfo]) -> bool:
    module_names, func_names = _subprocess_bindings(tokens)
    reduced = [tok for tok in tokens if tok.type not in _CALL_SKIP]
    n = len(reduced)
    for i, tok in enumerate(reduced):
        if tok.type != tokenize.NAME:
            continue
        if (
            tok.string in func_names
            and i + 1 < n
            and reduced[i + 1].type == tokenize.OP
            and reduced[i + 1].string == "("
        ):
            return True
        if (
            tok.string in module_names
            and i + 3 < n
            and reduced[i + 1].type == tokenize.OP
            and reduced[i + 1].string == "."
            and reduced[i + 2].type == tokenize.NAME
            and reduced[i + 2].string in _SUBPROCESS_CALL_NAMES
            and reduced[i + 3].type == tokenize.OP
            and reduced[i + 3].string == "("
        ):
            return True
    return False


def _string_literal_value(tok: tokenize.TokenInfo) -> object:
    try:
        return ast.literal_eval(tok.string)
    except (ValueError, SyntaxError, MemoryError):
        return None


def _has_make_argv(tokens: Sequence[tokenize.TokenInfo]) -> bool:
    n = len(tokens)
    i = 0
    while i < n:
        tok = tokens[i]
        if tok.type == tokenize.OP and tok.string == "[":
            j = i + 1
            while j < n and tokens[j].type in _MAKE_SKIP:
                j += 1
            if j < n and tokens[j].type == tokenize.STRING and _string_literal_value(tokens[j]) == "make":
                return True
        i += 1
    return False


def _classify_module_source(text: str) -> bool:
    """Return True iff *text* is an e2e module (subprocess call or make argv)."""

    try:
        tokens = _tokenize_source(text)
    except (tokenize.TokenError, SyntaxError, UnicodeError):
        return _regex_classify(text)
    try:
        return _has_make_argv(tokens) or _has_subprocess_call(tokens)
    except Exception:
        return _regex_classify(text)


def _parse_tier(value: str | None) -> str:
    """Return a canonical tier; unset/empty maps to ``full``. Fail closed."""

    if value is None or value == "":
        return "full"
    if value not in _ACCEPTED_TIERS:
        accepted = ", ".join(_ACCEPTED_TIERS)
        raise pytest.UsageError(f"{_TIER_ENV}={value!r} is not one of: {accepted}")
    return value


def _parse_subjects(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(part for part in value.split(",") if part)


def _resolve_tier(environ: Mapping[str, str] | None = None) -> str:
    """Return the active tier. Disable flag forces ``full`` (keep everything)."""

    env = os.environ if environ is None else environ
    if env.get(_DISABLE_ENV):
        return "full"
    return _parse_tier(env.get(_TIER_ENV))


def _tier_keep(
    tier: str,
    subjects: Sequence[str],
    is_e2e: bool,
    has_timing: bool,
    source_text: str,
) -> bool:
    """Return whether one collected item should stay in the selected set."""

    if tier not in _ACCEPTED_TIERS:
        accepted = ", ".join(_ACCEPTED_TIERS)
        raise pytest.UsageError(f"{_TIER_ENV}={tier!r} is not one of: {accepted}")
    if tier == "full":
        return True
    if tier == "fast":
        return not is_e2e and not has_timing
    # tier == "e2e"
    if not is_e2e:
        return False
    if not subjects:
        return True
    return any(subject in source_text for subject in subjects)


def _module_record(path: Path) -> tuple[bool, str]:
    key = os.fspath(path)
    cached = _MODULE_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        text = ""
    record = (_classify_module_source(text), text)
    _MODULE_CACHE[key] = record
    return record


def _format_summary(
    tier: str,
    kept: int,
    deselected_e2e: int,
    deselected_timing: int,
    subjects: Sequence[str],
) -> str:
    line = f"{_TIER_ENV}={tier}: kept {kept}, deselected {deselected_e2e} e2e, {deselected_timing} timing"
    if subjects:
        line += f", subjects={','.join(subjects)}"
    return line


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "e2e: module classified as e2e by wb_test_tier (subprocess call or make argv)",
    )
    disabled = bool(os.environ.get(_DISABLE_ENV))
    config._wb_test_tier_disabled = disabled  # type: ignore[attr-defined]
    if disabled:
        config._wb_test_tier = "full"  # type: ignore[attr-defined]
        config._wb_test_tier_subjects = ()  # type: ignore[attr-defined]
        return
    config._wb_test_tier = _parse_tier(os.environ.get(_TIER_ENV))  # type: ignore[attr-defined]
    config._wb_test_tier_subjects = _parse_subjects(  # type: ignore[attr-defined]
        os.environ.get(_SUBJECTS_ENV)
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if getattr(config, "_wb_test_tier_disabled", False):
        return
    tier = getattr(config, "_wb_test_tier", "full")
    subjects = getattr(config, "_wb_test_tier_subjects", ())

    kept: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    deselected_e2e = 0
    deselected_timing = 0

    for item in items:
        path = getattr(item, "path", None)
        if path is None:
            is_e2e = False
            source_text = ""
        else:
            is_e2e, source_text = _module_record(Path(path))
        if is_e2e and item.get_closest_marker("e2e") is None:
            item.add_marker(pytest.mark.e2e)
        has_timing = item.get_closest_marker("timing") is not None
        if tier == "full" or _tier_keep(tier, subjects, is_e2e, has_timing, source_text):
            kept.append(item)
            continue
        deselected.append(item)
        if is_e2e:
            deselected_e2e += 1
        if has_timing:
            deselected_timing += 1

    if tier == "full":
        return

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = kept

    config._wb_test_tier_summary = _format_summary(  # type: ignore[attr-defined]
        tier, len(kept), deselected_e2e, deselected_timing, subjects
    )


def pytest_terminal_summary(
    terminalreporter: pytest.TerminalReporter,
    exitstatus: int,
    config: pytest.Config,
) -> None:
    line = getattr(config, "_wb_test_tier_summary", None)
    if line:
        terminalreporter.write_line(line)
