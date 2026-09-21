#!/usr/bin/env python3
"""PreToolUse classifier: route source discovery through codemap (implementation note C2).

``classify_source_query(payload) -> Classification(kind, reason, spans)`` is a
pure finite lexer. Kinds are ``source_query``, ``unrelated``, and
``unknown_source``. Unsupported constructs fail loud as ``unknown_source``;
enforced mode refuses them with ``unsupported_source_query``.

Hot path: no Git, no subprocess, no freshness probe, no ledger reread from
the handler when the wrapper already passed a ``ModeRead``.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Iterable, Literal

try:
    from workbay_protocol.capability_mode import ModeRead, load_codemap_mode
except ImportError:  # pragma: no cover - unknown intent when protocol is absent
    load_codemap_mode = None  # type: ignore[assignment]

    @dataclass(frozen=True, slots=True)
    class ModeRead:  # type: ignore[no-redef]
        status: str
        value: str | None
        origin: str
        intent: str


MAX_BYTES = 64 * 1024
MAX_TOKENS = 256
MAX_WRAPPER_DEPTH = 4

Kind = Literal["source_query", "unrelated", "unknown_source"]
DecisionStatus = Literal[
    "allow", "advise", "refuse_source_query", "broken_enforcement"
]

SOURCE_SUFFIXES = frozenset(
    {
        ".py",
        ".pyi",
        ".js",
        ".jsx",
        ".ts",
        ".tsx",
        ".go",
        ".rs",
        ".c",
        ".h",
        ".cc",
        ".cpp",
        ".hpp",
        ".java",
        ".sh",
        ".rb",
        ".sql",
    }
)
SOURCE_DIRS = frozenset({"src", "lib", "scripts", "packages", "tests"})
CONFIG_SUFFIXES = frozenset(
    {".toml", ".json", ".yaml", ".yml", ".ini", ".env"}
)
LOG_SUFFIXES = frozenset({".log", ".txt"})
SEARCH_COMMANDS = frozenset({"grep", "rg", "ag", "ack"})
INTERPRETERS = frozenset({"bash", "sh", "ksh", "zsh"})
PYTHON_INTERPRETERS = frozenset({"python", "python3", "pypy", "pypy3"})
LITERAL_PRODUCERS = frozenset({"printf", "echo"})
UNSUPPORTED_VERBS = frozenset(
    {"eval", "alias", "unalias", "function", "source", "."}
)
SOURCE_TYPES = frozenset(
    {
        "py",
        "python",
        "pyi",
        "js",
        "javascript",
        "jsx",
        "ts",
        "typescript",
        "tsx",
        "go",
        "rs",
        "rust",
        "c",
        "h",
        "cc",
        "cpp",
        "hpp",
        "java",
        "sh",
        "bash",
        "rb",
        "ruby",
        "sql",
    }
)
GIT_GLOBAL_VALUE = frozenset(
    {
        "-C",
        "-c",
        "--git-dir",
        "--work-tree",
        "--namespace",
        "--exec-path",
        "--super-prefix",
    }
)
_BROKEN_MODE = frozenset(
    {
        "unreadable",
        "malformed",
        "oversized",
        "invalid_value",
        "unsupported_schema",
        "policy_missing",
    }
)
_POLICY_ENV = "WORKBAY_CODEMAP_MODE_READ"

_LISTING_FLAGS = {
    "grep": frozenset(
        {"-l", "-L", "--files-with-matches", "--files-without-match"}
    ),
    "rg": frozenset(
        {"-l", "--files", "--files-with-matches", "--files-without-match"}
    ),
    "ag": frozenset(
        {"-l", "-L", "--files-with-matches", "--files-without-matches"}
    ),
    "ack": frozenset({"-l", "-L", "--files-with-matches"}),
}
_RECURSIVE_FLAGS = {
    "grep": frozenset({"-r", "-R", "--recursive"}),
    "rg": frozenset(),
    "ag": frozenset(),
    "ack": frozenset(),
}
_FIXED_FLAGS = frozenset({"-F", "--fixed-strings", "--literal"})
_GREP_VALUE_SHORT = frozenset("efmdDABC")
_RG_VALUE_SHORT = frozenset("egtfABCmjr")
_GREP_VALUE_OPTS = frozenset(
    {
        "-e",
        "-f",
        "--regexp",
        "--file",
        "--include",
        "--exclude",
        "--include-dir",
        "--exclude-dir",
        "-d",
        "--directories",
        "-m",
        "--max-count",
        "-A",
        "-B",
        "-C",
        "--label",
    }
)
_RG_VALUE_OPTS = frozenset(
    {
        "-e",
        "-f",
        "-g",
        "--glob",
        "--iglob",
        "-t",
        "--type",
        "-T",
        "--type-not",
        "-A",
        "-B",
        "-C",
        "-m",
        "--replace",
        "-r",
        "--max-depth",
        "--max-count",
        "-j",
        "--regexp",
        "--file",
    }
)


@dataclass(frozen=True, slots=True)
class Classification:
    kind: Kind
    reason: str
    spans: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GuardDecision:
    status: DecisionStatus
    reason: str
    classification: Classification


@dataclass(frozen=True, slots=True)
class _Tok:
    kind: str
    value: str
    quoted: bool = False
    expansion: bool = False


def _cls(
    kind: Kind, reason: str, spans: Iterable[str] = ()
) -> Classification:
    return Classification(kind, reason, tuple(spans))


def _suffix(name: str) -> str:
    base = name.rsplit("/", 1)[-1]
    if base == ".env":
        return ".env"
    idx = base.rfind(".")
    if idx <= 0:
        return ""
    return base[idx:].lower()


def _looks_like_dir(path: str) -> bool:
    posix = path.replace("\\", "/")
    if posix.endswith("/"):
        return True
    base = posix.rsplit("/", 1)[-1]
    if base in {".", "..", ""}:
        return True
    return _suffix(base) == ""


def _classify_path(path: str) -> str:
    posix = path.replace("\\", "/").strip()
    if posix in {".", "./", "/", ".."}:
        return "root"
    parts = [p for p in posix.split("/") if p and p != "."]
    if any(part in SOURCE_DIRS for part in parts):
        return "source"
    base = parts[-1] if parts else posix
    suffix = _suffix(base)
    if suffix in SOURCE_SUFFIXES:
        return "source"
    if len(parts) >= 2 and parts[0] == "logs" and suffix in LOG_SUFFIXES:
        return "literal_data"
    if suffix in CONFIG_SUFFIXES:
        return "literal_data"
    return "unknown"


def _is_source_glob_or_type(value: str) -> bool:
    text = value.strip().strip("'\"")
    if text.lower() in SOURCE_TYPES:
        return True
    if _suffix(text.lstrip("*")) in SOURCE_SUFFIXES:
        return True
    if any(f"*{suf}" in text or text.endswith(suf) for suf in SOURCE_SUFFIXES):
        return True
    parts = [p for p in text.replace("\\", "/").split("/") if p and p != "**"]
    return any(part in SOURCE_DIRS for part in parts)


def _is_assignment(value: str) -> bool:
    if "=" not in value:
        return False
    name, _sep, _rest = value.partition("=")
    return bool(name) and name[0].isalpha() or name[:1] == "_"


def _is_assignment_token(tok: _Tok) -> bool:
    if tok.quoted:
        return False
    name, sep, _rest = tok.value.partition("=")
    if sep != "=" or not name:
        return False
    return name[0].isalpha() or name[0] == "_"


def _basename(value: str) -> str:
    return os.path.basename(value.replace("\\", "/")) or value


def _oversized(raw: str | bytes) -> bool:
    if isinstance(raw, bytes):
        return len(raw) > MAX_BYTES
    return len(raw.encode("utf-8")) > MAX_BYTES


def _read_word(src: str, i: int) -> tuple[_Tok, int, bool]:
    n = len(src)
    buf: list[str] = []
    quoted = False
    quote: str | None = None
    expansion = False
    command_sub = False
    while i < n:
        c = src[i]
        if quote is None and c in " \t\n|&;<>":
            break
        if quote is None and c == "#":
            break
        if c == "\\" and quote != "'":
            if i + 1 >= n:
                buf.append(c)
                i += 1
                break
            nxt = src[i + 1]
            if quote == '"' and nxt not in '"\\$`\n':
                buf.append(c)
                buf.append(nxt)
            elif nxt != "\n":
                buf.append(nxt)
            i += 2
            continue
        if quote is None and c in "'\"":
            quoted = True
            quote = c
            i += 1
            continue
        if quote == "'" and c == "'":
            quote = None
            i += 1
            continue
        if quote == '"' and c == '"':
            quote = None
            i += 1
            continue
        if quote != "'" and c == "$":
            expansion = True
            if i + 1 < n and src[i + 1] == "(":
                command_sub = True
            buf.append(c)
            i += 1
            continue
        if quote != "'" and c == "`":
            expansion = True
            command_sub = True
            buf.append(c)
            i += 1
            continue
        if quote != "'" and c == "<" and i + 1 < n and src[i + 1] == "(":
            command_sub = True
        buf.append(c)
        i += 1
    if quote is not None:
        raise ValueError("unclosed quote")
    return (
        _Tok("word", "".join(buf), quoted=quoted, expansion=expansion),
        i,
        command_sub,
    )


def _read_heredoc(
    src: str, i: int, delim: str, strip_tabs: bool
) -> tuple[str, int]:
    n = len(src)
    lines: list[str] = []
    while i <= n:
        start = i
        while i < n and src[i] != "\n":
            i += 1
        line = src[start:i]
        if i < n and src[i] == "\n":
            i += 1
            had_nl = True
        else:
            had_nl = False
        compare = line.lstrip("\t") if strip_tabs else line
        if compare == delim:
            break
        lines.append(line)
        if not had_nl:
            break
    return "\n".join(lines), i


def tokenize(src: str) -> tuple[list[_Tok], bool]:
    tokens: list[_Tok] = []
    pending: list[tuple[bool, str]] = []
    n = len(src)
    i = 0
    command_sub = False
    while i < n:
        c = src[i]
        if c in " \t":
            i += 1
            continue
        if c == "\n":
            tokens.append(_Tok("newline", "\n"))
            i += 1
            for strip_tabs, delim in pending:
                body, i = _read_heredoc(src, i, delim, strip_tabs)
                tokens.append(_Tok("heredoc_body", body, quoted=True))
            pending = []
            continue
        if c == "#" and (
            not tokens or tokens[-1].kind in {"newline", "op", "heredoc_op"}
        ):
            while i < n and src[i] != "\n":
                i += 1
            continue
        if src.startswith("<<-", i) or src.startswith("<<", i):
            strip_tabs = src.startswith("<<-", i)
            i += 3 if strip_tabs else 2
            while i < n and src[i] in " \t":
                i += 1
            delim_tok, i, sub = _read_word(src, i)
            command_sub = command_sub or sub
            pending.append((strip_tabs, delim_tok.value))
            tokens.append(_Tok("heredoc_op", "<<-" if strip_tabs else "<<"))
            continue
        two = src[i : i + 2]
        if two in {"||", "&&", ">>", ">&", "|&"}:
            tokens.append(_Tok("op", "|" if two == "|&" else two))
            i += 2
            continue
        if c in "|;&<>":
            tokens.append(_Tok("op", c))
            i += 1
            continue
        tok, i, sub = _read_word(src, i)
        command_sub = command_sub or sub
        tokens.append(tok)
    for strip_tabs, delim in pending:
        body, i = _read_heredoc(src, i, delim, strip_tabs)
        tokens.append(_Tok("heredoc_body", body, quoted=True))
    return _bind_heredocs(tokens), command_sub


def _bind_heredocs(tokens: list[_Tok]) -> list[_Tok]:
    bodies = [tok for tok in tokens if tok.kind == "heredoc_body"]
    out: list[_Tok] = []
    index = 0
    for tok in tokens:
        if tok.kind == "heredoc_body":
            continue
        out.append(tok)
        if tok.kind == "heredoc_op" and index < len(bodies):
            out.append(bodies[index])
            index += 1
    return out


def _parse(tokens: list[_Tok]) -> list[list[dict[str, Any]]]:
    pipelines: list[list[dict[str, Any]]] = []
    pipe: list[dict[str, Any]] = []
    cmd: dict[str, Any] = {"words": [], "heredocs": [], "stdin": []}

    def flush_cmd() -> None:
        if cmd["words"] or cmd["heredocs"] or cmd["stdin"]:
            pipe.append(
                {
                    "words": list(cmd["words"]),
                    "heredocs": list(cmd["heredocs"]),
                    "stdin": list(cmd["stdin"]),
                }
            )
        cmd["words"] = []
        cmd["heredocs"] = []
        cmd["stdin"] = []

    def flush_pipe() -> None:
        flush_cmd()
        if pipe:
            pipelines.append(list(pipe))
            pipe.clear()

    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok.kind == "word":
            cmd["words"].append(tok)
        elif tok.kind == "heredoc_op":
            pass
        elif tok.kind == "heredoc_body":
            cmd["heredocs"].append(tok)
        elif tok.kind == "op" and tok.value == "<":
            if i + 1 < len(tokens) and tokens[i + 1].kind == "word":
                i += 1
                cmd["stdin"].append(tokens[i])
        elif tok.kind == "op" and tok.value in {">", ">>"}:
            if i + 1 < len(tokens) and tokens[i + 1].kind == "word":
                i += 1
        elif tok.kind == "op" and tok.value == "|":
            flush_cmd()
        elif (tok.kind == "op" and tok.value in {"&&", "||", ";", "&"}) or (
            tok.kind == "newline"
        ):
            flush_pipe()
        i += 1
    flush_pipe()
    return pipelines


def _expand_short(toks: list[_Tok], cmd: str) -> list[_Tok]:
    value_short = _RG_VALUE_SHORT if cmd in {"rg", "ag", "ack"} else _GREP_VALUE_SHORT
    out: list[_Tok] = []
    for tok in toks:
        v = tok.value
        if (
            tok.quoted
            or not v.startswith("-")
            or v.startswith("--")
            or len(v) <= 2
        ):
            out.append(tok)
            continue
        letters = v[1:]
        if not all(ch.isalpha() for ch in letters):
            out.append(tok)
            continue
        i = 0
        while i < len(letters):
            ch = letters[i]
            if ch in value_short:
                out.append(_Tok("word", f"-{ch}", quoted=False, expansion=False))
                rest = letters[i + 1 :]
                if rest:
                    out.append(_Tok("word", rest, quoted=False, expansion=tok.expansion))
                break
            out.append(_Tok("word", f"-{ch}", quoted=False, expansion=False))
            i += 1
    return out


def _peel_env(toks: list[_Tok]) -> list[_Tok]:
    i = 0
    while i < len(toks):
        tok = toks[i]
        if tok.quoted:
            break
        value = tok.value
        if value in {"-i", "-0", "-v"}:
            i += 1
            continue
        if value in {"-u", "-S"}:
            i += 2
            continue
        if _is_assignment_token(tok):
            i += 1
            continue
        break
    return toks[i:]


def _peel_command(toks: list[_Tok]) -> list[_Tok]:
    i = 0
    while i < len(toks) and not toks[i].quoted and toks[i].value in {"-p", "-v", "-V"}:
        i += 1
    return toks[i:]


def _reduce(results: list[Classification]) -> Classification:
    if not results:
        return _cls("unrelated", "unrelated_command")
    source = [item for item in results if item.kind == "source_query"]
    if source:
        spans: list[str] = []
        for item in source:
            spans.extend(item.spans)
        return _cls("source_query", source[0].reason, spans)
    unknown = [item for item in results if item.kind == "unknown_source"]
    if unknown:
        return unknown[0]
    for preferred in ("heredoc_data", "literal_data", "quoted_data"):
        for item in results:
            if item.reason == preferred:
                return item
    return results[0]


def _output_origin(simple: dict[str, Any], result: Classification) -> str:
    words: list[_Tok] = simple["words"]
    verb = _basename(words[0].value) if words else ""
    if simple["heredocs"]:
        if verb in INTERPRETERS or verb in PYTHON_INTERPRETERS:
            return "unknown"
        return "heredoc"
    if verb in LITERAL_PRODUCERS:
        return "literal"
    if verb == "cat":
        files = [tok.value for tok in words[1:] if not tok.value.startswith("-")]
        files.extend(tok.value for tok in simple["stdin"])
        if not files:
            return "literal" if simple["heredocs"] else "unknown"
        classes = [_classify_path(path) for path in files]
        if any(item == "source" for item in classes):
            return "source"
        if classes and all(item == "literal_data" for item in classes):
            return "literal"
        return "unknown"
    if result.kind == "source_query":
        return "literal"
    return "unknown"


def classify_search(
    cmd: str,
    argtoks: list[_Tok],
    stdin_origin: str | None,
    redirects: dict[str, Any],
    *,
    native_literal: bool = False,
) -> Classification:
    listing_flags = _LISTING_FLAGS.get(cmd, frozenset())
    recursive_flags = _RECURSIVE_FLAGS.get(cmd, frozenset())
    value_opts = _RG_VALUE_OPTS if cmd in {"rg", "ag", "ack"} else _GREP_VALUE_OPTS
    args = _expand_short(argtoks, cmd)
    listing = False
    recursive_opt = False
    fixed = native_literal
    files_only = False
    types: list[str] = []
    globs: list[str] = []
    saw_pattern = False
    path_toks: list[_Tok] = []
    positionals: list[_Tok] = []
    i = 0
    while i < len(args):
        tok = args[i]
        value = tok.value
        if value == "--" and not tok.quoted:
            rest = args[i + 1 :]
            if (
                not saw_pattern
                and not files_only
                and not positionals
                and rest
            ):
                saw_pattern = True
                path_toks.extend(rest[1:])
            else:
                path_toks.extend(rest)
            break
        if value.startswith("-") and value != "-":
            if value in listing_flags:
                listing = True
            if value == "--files":
                files_only = True
                listing = True
            if value in recursive_flags:
                recursive_opt = True
            if value in _FIXED_FLAGS:
                fixed = True
            if value in {"-t", "--type", "-T", "--type-not"}:
                if i + 1 < len(args):
                    i += 1
                    types.append(args[i].value)
            elif value.startswith("--type="):
                types.append(value.split("=", 1)[1])
            elif value in {"-g", "--glob", "--iglob"}:
                if i + 1 < len(args):
                    i += 1
                    globs.append(args[i].value)
            elif value.startswith("--glob=") or value.startswith("--iglob="):
                globs.append(value.split("=", 1)[1])
            elif value.startswith("--include="):
                globs.append(value.split("=", 1)[1])
            elif value == "--include" and i + 1 < len(args):
                i += 1
                globs.append(args[i].value)
            elif value in {"-e", "--regexp"}:
                saw_pattern = True
                i += 1
            elif value in value_opts:
                i += 1
            elif value.startswith("--directories=") and "recurse" in value:
                recursive_opt = True
            i += 1
            continue
        positionals.append(tok)
        i += 1
    if files_only:
        path_toks.extend(positionals)
    elif positionals:
        if not saw_pattern:
            path_toks.extend(positionals[1:])
        else:
            path_toks.extend(positionals)
    path_toks.extend(redirects.get("stdin") or [])
    for tok in path_toks:
        if tok.expansion:
            return _cls("unknown_source", "dynamic_path", (tok.value,))
    for collection in (types, globs):
        for item in collection:
            if "$" in item or "`" in item:
                return _cls("unknown_source", "dynamic_path", (item,))

    source_filter = any(_is_source_glob_or_type(item) for item in types + globs)
    classes = [_classify_path(tok.value) for tok in path_toks]
    spans = tuple(tok.value for tok in path_toks)

    glob_classes = [_classify_path(item) for item in globs]
    if source_filter:
        return _cls("source_query", "source_type_filter", spans or types or globs)
    if listing:
        if any(item in {"source", "root"} for item in classes + glob_classes):
            return _cls("source_query", "file_list_discovery", spans or globs)
        if not path_toks and not globs and not types:
            return _cls("source_query", "file_list_discovery", spans)
        if path_toks:
            return _cls("unknown_source", "file_list_discovery", spans)
        return _cls("unknown_source", "unknown_path", spans or globs)

    default_recursive = cmd in {"rg", "ag", "ack"}
    recursive = recursive_opt or (
        default_recursive and any(_looks_like_dir(tok.value) for tok in path_toks)
    )
    if (
        fixed
        and not listing
        and not recursive_opt
        and path_toks
        and all(item == "literal_data" for item in classes)
        and not recursive
    ):
        return _cls("unrelated", "literal_data", spans)
    if any(item == "source" for item in classes):
        return _cls("source_query", "source_path", spans)
    if path_toks and all(item == "root" for item in classes):
        return _cls("source_query", "repository_root", spans)
    if not path_toks:
        if stdin_origin in {"literal", "heredoc"}:
            reason = "heredoc_data" if stdin_origin == "heredoc" else "literal_data"
            return _cls("unrelated", reason)
        if stdin_origin == "source":
            return _cls("source_query", "source_path")
        if stdin_origin == "unknown":
            return _cls("unknown_source", "unknown_origin")
        return _cls("source_query", "omitted_path")
    return _cls("unknown_source", "unknown_path", spans)


def _classify_git(
    argv: list[_Tok], stdin_origin: str | None, simple: dict[str, Any]
) -> Classification:
    i = 1
    while i < len(argv):
        tok = argv[i]
        value = tok.value
        if value == "--" and not tok.quoted:
            break
        if value.startswith("-") and not tok.quoted:
            name = value.split("=", 1)[0]
            if name in GIT_GLOBAL_VALUE and "=" not in value:
                i += 2
                continue
            i += 1
            continue
        break
    if i >= len(argv):
        return _cls("unrelated", "unrelated_command")
    sub = argv[i].value
    if sub != "grep":
        return _cls("unrelated", "unrelated_command")
    return classify_search("grep", argv[i + 1 :], stdin_origin, simple)


def _c_flag_body(argv: list[_Tok]) -> _Tok | None:
    i = 1
    while i < len(argv):
        tok = argv[i]
        value = tok.value
        if value == "-c" and not tok.quoted:
            if i + 1 < len(argv):
                return argv[i + 1]
            return _Tok("word", "", quoted=False, expansion=True)
        if (
            value.startswith("-")
            and not value.startswith("--")
            and not tok.quoted
            and "c" in value[1:]
        ):
            if i + 1 < len(argv):
                return argv[i + 1]
            return _Tok("word", "", quoted=False, expansion=True)
        i += 1
    return None


def classify_simple(
    simple: dict[str, Any], stdin_origin: str | None, depth: int
) -> Classification:
    if depth > MAX_WRAPPER_DEPTH:
        return _cls("unknown_source", "wrapper_depth")
    words: list[_Tok] = list(simple["words"])
    while words and _is_assignment_token(words[0]):
        words = words[1:]
    if not words:
        return _cls("unrelated", "unrelated_command")
    verb = _basename(words[0].value)
    if verb in UNSUPPORTED_VERBS:
        reason = "eval" if verb == "eval" else "alias_or_function"
        if verb in {".", "source"}:
            reason = "nonliteral_script"
        return _cls("unknown_source", reason, (verb,))
    if any(
        tok.value.endswith("()") or tok.value == "()" for tok in words if not tok.quoted
    ):
        return _cls("unknown_source", "alias_or_function")
    if verb == "env":
        rest = _peel_env(words[1:])
        if depth + 1 > MAX_WRAPPER_DEPTH:
            return _cls("unknown_source", "wrapper_depth")
        return classify_simple({**simple, "words": rest}, stdin_origin, depth + 1)
    if verb == "command":
        rest = _peel_command(words[1:])
        if depth + 1 > MAX_WRAPPER_DEPTH:
            return _cls("unknown_source", "wrapper_depth")
        return classify_simple({**simple, "words": rest}, stdin_origin, depth + 1)
    if verb in INTERPRETERS:
        if simple["heredocs"]:
            return _cls("unknown_source", "opaque_heredoc", (verb,))
        body = _c_flag_body(words)
        if body is None:
            return _cls("unknown_source", "nonliteral_script", (verb,))
        if body.expansion or not body.value:
            return _cls("unknown_source", "nonliteral_script", (verb,))
        if depth + 1 > MAX_WRAPPER_DEPTH:
            return _cls("unknown_source", "wrapper_depth")
        return classify_command_string(body.value, depth + 1)
    if verb in PYTHON_INTERPRETERS:
        body = _c_flag_body(words)
        if body is not None:
            return _cls("unknown_source", "python_c_body", (verb,))
        return _cls("unrelated", "unrelated_command", (verb,))
    if verb == "git":
        return _classify_git(words, stdin_origin, simple)
    if verb in SEARCH_COMMANDS:
        heredoc_stdin = bool(simple["heredocs"]) and stdin_origin is None
        origin = stdin_origin
        redirects = {
            "stdin": list(simple["stdin"]),
            "heredoc_stdin": bool(simple["heredocs"]),
        }
        if simple["heredocs"] and origin is None:
            origin = "literal"
        if heredoc_stdin:
            origin = "literal"
        return classify_search(verb, words[1:], origin, redirects)
    if verb in LITERAL_PRODUCERS:
        return _cls("unrelated", "literal_data", (verb,))
    return _cls("unrelated", "unrelated_command", (verb,))


def classify_command_string(command: str, depth: int) -> Classification:
    if depth > MAX_WRAPPER_DEPTH:
        return _cls("unknown_source", "wrapper_depth")
    if _oversized(command):
        return _cls("unknown_source", "oversized_payload_64kib")
    if not command.strip():
        return _cls("unrelated", "unrelated_command")
    try:
        tokens, command_sub = tokenize(command)
    except ValueError:
        return _cls("unknown_source", "malformed_payload")
    word_count = sum(1 for tok in tokens if tok.kind == "word")
    if word_count > MAX_TOKENS:
        return _cls("unknown_source", "token_limit_256")
    if command_sub:
        return _cls("unknown_source", "command_substitution")
    pipelines = _parse(tokens)
    results: list[Classification] = []
    for pipeline in pipelines:
        origin: str | None = None
        pipe_results: list[Classification] = []
        for index, simple in enumerate(pipeline):
            stdin_origin = origin if index else None
            item = classify_simple(simple, stdin_origin, depth)
            pipe_results.append(item)
            origin = _output_origin(simple, item)
        results.append(_reduce(pipe_results))
    return _reduce(results)


def classify_native_grep(tool_input: dict[str, Any]) -> Classification:
    path = tool_input.get("path")
    glob = tool_input.get("glob") or tool_input.get("include")
    typ = tool_input.get("type")
    output_mode = str(tool_input.get("output_mode") or "")
    args: list[_Tok] = [_Tok("word", "-F")]
    if output_mode in {"files_with_matches", "files", "files_without_match"}:
        args.append(_Tok("word", "--files-with-matches"))
    if glob:
        args.extend([_Tok("word", "--glob"), _Tok("word", str(glob))])
    if typ:
        args.extend([_Tok("word", "--type"), _Tok("word", str(typ))])
    args.append(_Tok("word", str(tool_input.get("pattern") or "x")))
    if path not in (None, ""):
        args.append(_Tok("word", str(path)))
    return classify_search(
        "rg", args[1:] if False else args, None, {"stdin": []}, native_literal=True
    )


def classify_native_glob(tool_input: dict[str, Any]) -> Classification:
    pattern = str(tool_input.get("pattern") or "")
    path = tool_input.get("path")
    args = [_Tok("word", "--files")]
    if pattern:
        args.extend([_Tok("word", "--glob"), _Tok("word", pattern)])
    if path not in (None, ""):
        args.append(_Tok("word", str(path)))
    return classify_search("rg", args, None, {"stdin": []})


def parse_hook_payload(raw: str | bytes) -> Any:
    if isinstance(raw, bytes):
        if len(raw) > MAX_BYTES:
            return raw[: MAX_BYTES + 1]
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError:
            return "{malformed"
    if _oversized(raw):
        return raw
    text = raw.strip()
    if not text:
        return {}
    if text[0] in "{[":
        try:
            return json.loads(text)
        except ValueError:
            return text
    return text


def classify_source_query(payload: Any) -> Classification:
    if isinstance(payload, (bytes, bytearray)):
        if len(payload) > MAX_BYTES:
            return _cls("unknown_source", "oversized_payload_64kib")
        payload = parse_hook_payload(bytes(payload))
    if isinstance(payload, str):
        if _oversized(payload):
            return _cls("unknown_source", "oversized_payload_64kib")
        stripped = payload.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                payload = json.loads(stripped)
            except ValueError:
                return _cls("unknown_source", "malformed_payload")
        else:
            return classify_command_string(payload, 0)
    if not isinstance(payload, dict):
        return _cls("unknown_source", "malformed_payload")
    encoded = json.dumps(payload, sort_keys=True)
    if _oversized(encoded):
        return _cls("unknown_source", "oversized_payload_64kib")
    command = ""
    tool_input = payload.get("tool_input") or payload.get("input") or {}
    if not isinstance(tool_input, dict):
        tool_input = {}
    if "command" in tool_input:
        command = str(tool_input.get("command") or "")
    elif "command" in payload:
        command = str(payload.get("command") or "")
    if command and _oversized(command):
        return _cls("unknown_source", "oversized_payload_64kib")
    tool = str(payload.get("tool_name") or payload.get("tool") or "")
    key = tool.lower()
    try:
        if key in {"bash", "shell"}:
            return classify_command_string(command, 0)
        if key == "grep":
            return classify_native_grep(tool_input)
        if key == "glob":
            return classify_native_glob(tool_input)
        if command and not tool:
            return classify_command_string(command, 0)
        if key:
            return _cls("unrelated", "unrelated_command", (tool,))
        if command:
            return classify_command_string(command, 0)
        return _cls("unrelated", "unrelated_command")
    except Exception as exc:  # noqa: BLE001 — search/shell must fail loud
        if key in {"bash", "shell", "grep", "glob"} or command:
            return _cls("unknown_source", f"malformed_payload:{type(exc).__name__}")
        return _cls("unrelated", "unrelated_command")


def decide_guard(
    classification: Classification, mode: ModeRead | None
) -> GuardDecision:
    if classification.kind == "unrelated":
        return GuardDecision("allow", classification.reason, classification)
    if mode is None:
        return GuardDecision("advise", "unknown_intent", classification)
    if mode.status == "ok" and mode.value == "off":
        return GuardDecision("allow", "codemap_mode_off", classification)
    if mode.status in _BROKEN_MODE:
        return GuardDecision("broken_enforcement", str(mode.status), classification)
    enforced = mode.status == "ok" and mode.value == "enforced"
    if classification.kind == "unknown_source":
        if enforced:
            return GuardDecision(
                "refuse_source_query", "unsupported_source_query", classification
            )
        return GuardDecision("advise", classification.reason, classification)
    if enforced:
        return GuardDecision(
            "refuse_source_query", classification.reason, classification
        )
    return GuardDecision("advise", classification.reason, classification)


def decide_from_payload(
    payload: Any,
    mode_read: ModeRead | None = None,
    *,
    repo_root: str | os.PathLike[str] | None = None,
) -> GuardDecision:
    classification = classify_source_query(payload)
    if classification.kind == "unrelated":
        return GuardDecision("allow", classification.reason, classification)
    if mode_read is None:
        reader = load_codemap_mode
        if reader is None:
            mode_read = None
        else:
            mode_read = reader(repo_root or ".")
    return decide_guard(classification, mode_read)


def emit_decision(decision: GuardDecision) -> int:
    if decision.status == "allow":
        return 0
    if decision.status == "advise":
        json.dump(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "additionalContext": (
                        f"codemap-first: {decision.reason}. "
                        "Prefer search_graph for symbols."
                    ),
                }
            },
            sys.stdout,
        )
        sys.stdout.write("\n")
        return 0
    sys.stderr.write(f"codemap-first: {decision.status}: {decision.reason}\n")
    if decision.status == "refuse_source_query":
        sys.stderr.write(
            "Use codemap search_graph for symbols; inspect coverage "
            "before absence claims.\n"
            "Change policy: workbay codemap --available | workbay codemap --off\n"
        )
        if decision.reason == "unsupported_source_query":
            sys.stderr.write(
                f"unsupported construct: {decision.classification.reason}; "
                "use a literal supported command or codemap.\n"
            )
    return 2


def _mode_from_env() -> ModeRead | None:
    raw = os.environ.get(_POLICY_ENV)
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except ValueError:
        return ModeRead(
            status="malformed", value=None, origin="missing", intent="unknown"
        )
    if not isinstance(data, dict):
        return ModeRead(
            status="malformed", value=None, origin="missing", intent="unknown"
        )
    return ModeRead(
        status=str(data.get("status") or "malformed"),
        value=data.get("value"),
        origin=str(data.get("origin") or "missing"),
        intent=str(data.get("intent") or "unknown"),
    )


def main(argv: list[str] | None = None) -> int:
    del argv
    raw = sys.stdin.read(MAX_BYTES + 1)
    payload = parse_hook_payload(raw)
    passed = _mode_from_env()
    root = os.environ.get("CLAUDE_PROJECT_DIR") or os.environ.get(
        "GROK_WORKSPACE_ROOT"
    ) or os.getcwd()
    return emit_decision(
        decide_from_payload(payload, mode_read=passed, repo_root=root)
    )


if __name__ == "__main__":
    sys.exit(main())
