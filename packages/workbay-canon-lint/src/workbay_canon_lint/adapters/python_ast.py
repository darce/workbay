"""Check unbounded HTTP calls and suppressed errors that lose the original cause."""

from __future__ import annotations

import ast
from collections import Counter
from collections.abc import Iterator
from itertools import pairwise

_REQUESTS_CHECK = "python.requests-timeout"
_SILENT_CHECK = "python.silent-except"
_REQUESTS_FUNCTIONS = frozenset({"request", "get", "options", "head", "post", "put", "patch", "delete"})
_HTTP_FUNCTIONS = {
    "requests": _REQUESTS_FUNCTIONS,
    "httpx": _REQUESTS_FUNCTIONS | {"stream"},
    "urllib.request": frozenset({"urlopen"}),
}


def _bindings(node: ast.AST) -> Iterator[str]:
    if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
        yield node.id
    elif isinstance(node, ast.Attribute) and isinstance(node.ctx, (ast.Store, ast.Del)):
        if isinstance(node.value, ast.Name):
            yield node.value.id
    elif isinstance(node, ast.arg):
        yield node.arg
    elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        yield node.name
    elif isinstance(node, (ast.Import, ast.ImportFrom)):
        for name in node.names:
            yield name.asname or name.name.split(".", 1)[0]
    elif isinstance(node, (ast.ExceptHandler, ast.MatchAs, ast.MatchStar)) and node.name is not None:
        yield node.name
    elif isinstance(node, ast.MatchMapping) and node.rest is not None:
        yield node.rest
    elif isinstance(node, ast.TypeAlias) and isinstance(node.name, ast.Name):
        yield node.name.id
    elif isinstance(node, (ast.TypeVar, ast.TypeVarTuple, ast.ParamSpec)):
        yield node.name
    elif isinstance(node, (ast.Global, ast.Nonlocal)):
        yield from node.names


def _request_names(tree: ast.Module, bindings: Counter[str]) -> dict[str, str]:
    """An ordinary module-level import is evidence only if its name is unambiguous."""
    targets = {}
    if bindings["*"]:
        return targets
    for statement in tree.body:
        if isinstance(statement, ast.Import):
            for item in statement.names:
                alias = item.asname or item.name.split(".", 1)[0]
                if bindings[alias] == 1:
                    prefix = item.asname or item.name
                    for name in _HTTP_FUNCTIONS.get(item.name, ()):
                        targets[f"{prefix}.{name}"] = item.name
        elif isinstance(statement, ast.ImportFrom) and statement.level == 0:
            for item in statement.names:
                alias = item.asname or item.name
                if bindings[alias] != 1:
                    continue
                if statement.module == "urllib" and item.name == "request":
                    targets[f"{alias}.urlopen"] = "urllib.request"
                elif item.name in _HTTP_FUNCTIONS.get(statement.module, ()):
                    targets[alias] = statement.module
    return targets


def _qualified_name(node: ast.expr) -> str | None:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        return ".".join([node.id, *reversed(parts)])
    return None


def _unbounded(call: ast.Call, library: str) -> bool:
    for keyword in call.keywords:
        if keyword.arg == "timeout":
            value = keyword.value
            if isinstance(value, ast.Constant):
                return value.value is None
            if isinstance(value, ast.Tuple) and len(value.elts) == 2:
                return any(isinstance(item, ast.Constant) and item.value is None for item in value.elts)
            return False
    if any(keyword.arg is None for keyword in call.keywords):
        return False
    if library == "urllib.request":
        if any(isinstance(arg, ast.Starred) for arg in call.args):
            return False
        if len(call.args) >= 3:
            timeout = call.args[2]
            return isinstance(timeout, ast.Constant) and timeout.value is None
    return True


def _silent_handler(node: ast.ExceptHandler, bindings: Counter[str]) -> bool:
    broad = node.type is None or (
        isinstance(node.type, ast.Name)
        and node.type.id in {"Exception", "BaseException"}
        and bindings[node.type.id] == 0
    )
    return broad and len(node.body) == 1 and isinstance(node.body[0], ast.Pass)


def _lost_error_handlers(body: list[ast.stmt], bindings: Counter[str]) -> Iterator[ast.ExceptHandler]:
    for attempt, following in pairwise(body):
        if not isinstance(attempt, ast.Try) or attempt.orelse or attempt.finalbody:
            continue
        if len(attempt.body) != 1 or len(attempt.handlers) != 1 or not isinstance(following, ast.Return):
            continue
        assignment = attempt.body[0]
        handler = attempt.handlers[0]
        if not isinstance(assignment, ast.Assign) or len(assignment.targets) != 1:
            continue
        target = assignment.targets[0]
        if not isinstance(target, ast.Name) or not isinstance(following.value, ast.Name):
            continue
        if target.id != following.value.id or bindings[target.id] != 1:
            continue
        if isinstance(assignment.value, (ast.Call, ast.Await)) and _silent_handler(handler, bindings):
            yield handler


def scan(source: str, check_ids: frozenset[str]) -> list[tuple[str, int, str]]:
    """Return only observations in the supported static subset."""
    tree = ast.parse(source)
    nodes = list(ast.walk(tree))
    bindings = Counter(name for node in nodes for name in _bindings(node))
    targets = _request_names(tree, bindings) if _REQUESTS_CHECK in check_ids else {}
    # Match the parser's newline convention for exact evidence locations.
    lines = source.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    findings = []
    for node in nodes:
        if _REQUESTS_CHECK in check_ids and isinstance(node, ast.Call):
            library = targets.get(_qualified_name(node.func))
            if library and _unbounded(node, library):
                findings.append((_REQUESTS_CHECK, node.lineno, lines[node.lineno - 1].strip()[:240]))
        elif _SILENT_CHECK in check_ids and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for handler in _lost_error_handlers(node.body, bindings):
                findings.append((_SILENT_CHECK, handler.lineno, lines[handler.lineno - 1].strip()[:240]))
    return sorted(findings, key=lambda item: (item[1], item[0]))
