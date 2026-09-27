"""Collect cyclomatic complexity units (radon-backed)."""

from __future__ import annotations

import ast
from pathlib import Path

from crap_report.coverage_load import normalize_repo_path
from crap_report.models import MethodUnit


class RadonUnavailableError(RuntimeError):
    """Raised when radon cannot be imported for live collection."""


def collect_complexity(
    paths: list[Path],
    *,
    repo_root: Path | None = None,
) -> list[MethodUnit]:
    """Visit Python sources under *paths* and return MethodUnit rows.

    Requires the ``radon`` package. Emits **Function** units only (methods and
    nested functions); Class aggregate scores are expanded then skipped so they
    are not double-counted.
    """
    try:
        from radon.complexity import cc_visit  # type: ignore[import-untyped]
        from radon.visitors import Function as RadonFunction  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RadonUnavailableError(
            "radon is required for complexity collection; "
            "install with: pip install 'radon>=6.0.0'"
        ) from exc

    root = (repo_root or Path.cwd()).resolve()
    units: list[MethodUnit] = []
    py_files: list[Path] = []
    for p in paths:
        p = p.resolve()
        if p.is_file() and p.suffix == ".py":
            py_files.append(p)
        elif p.is_dir():
            py_files.extend(sorted(p.rglob("*.py")))

    for fp in py_files:
        try:
            source = fp.read_text(encoding="utf-8")
        except OSError:
            continue
        try:
            blocks = cc_visit(source)
        except SyntaxError:
            continue
        rel = normalize_repo_path(fp, repo_root=root)
        func_blocks = list(_iter_function_blocks(blocks, RadonFunction))
        # radon drops ClassDef nodes nested inside functions; recover them.
        func_blocks.extend(
            _recover_function_nested_class_methods(source, RadonFunction)
        )
        for block in func_blocks:
            name = getattr(block, "fullname", None) or getattr(block, "name", "<unknown>")
            line_start = int(block.lineno)
            line_end = int(getattr(block, "endline", line_start) or line_start)
            comp = int(block.complexity)
            units.append(
                MethodUnit(
                    file=rel,
                    name=str(name),
                    line_start=line_start,
                    line_end=line_end,
                    comp=comp,
                )
            )
    return units


def _is_radon_function(block, RadonFunction) -> bool:
    return isinstance(block, RadonFunction) or block.__class__.__name__ == "Function"


def _is_radon_class(block, RadonFunction) -> bool:
    if _is_radon_function(block, RadonFunction):
        return False
    return block.__class__.__name__ == "Class" or hasattr(block, "methods")


def _iter_function_blocks(
    block_list,
    RadonFunction,
    *,
    class_methods_are_siblings: bool = True,
) -> list:
    """Collect Function nodes without double-counting top-level class methods.

    ``radon.complexity.cc_visit`` flattens each top-level class into a Class
    block **and** sibling Function blocks for every method. Recursing into
    ``Class.methods`` therefore duplicates every method. Skip Class.methods
    when those siblings are present; still walk ``closures`` on Functions and
    ``inner_classes`` (whose methods are *not* emitted as siblings).

    When *class_methods_are_siblings* is False (nested / recovered Class
    blocks), methods are collected from the Class node itself.
    """
    out: list = []
    for block in block_list:
        if _is_radon_function(block, RadonFunction):
            out.append(block)
            closures = getattr(block, "closures", None) or []
            out.extend(
                _iter_function_blocks(
                    closures,
                    RadonFunction,
                    class_methods_are_siblings=False,
                )
            )
            continue
        if _is_radon_class(block, RadonFunction):
            if not class_methods_are_siblings:
                methods = getattr(block, "methods", None) or []
                out.extend(
                    _iter_function_blocks(
                        methods,
                        RadonFunction,
                        class_methods_are_siblings=False,
                    )
                )
            # Nested classes are never flattened as top-level siblings.
            for inner in getattr(block, "inner_classes", None) or []:
                out.extend(
                    _iter_function_blocks(
                        [inner],
                        RadonFunction,
                        class_methods_are_siblings=False,
                    )
                )
            continue
        # Unknown block type: keep prior best-effort behaviour (append + closures).
        out.append(block)
        closures = getattr(block, "closures", None) or []
        out.extend(
            _iter_function_blocks(
                closures,
                RadonFunction,
                class_methods_are_siblings=False,
            )
        )
    return out


def _recover_function_nested_class_methods(source: str, RadonFunction) -> list:
    """Collect methods of classes defined inside functions.

    radon's ``ComplexityVisitor.visit_FunctionDef`` only keeps nested
    *functions* (as ``closures``). Nested ``ClassDef`` nodes and their methods
    are discarded, so a high-complexity method on a function-local class would
    otherwise be invisible to the prioritizer.
    """
    try:
        from radon.visitors import ComplexityVisitor  # type: ignore[import-untyped]
    except ImportError:
        return []

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    parent_map = {
        child: parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    out: list = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        # Only ClassDefs whose nearest scope ancestor is a function — classes
        # nested under another class are reached via Class.inner_classes when
        # the outer class itself is recovered or is a top-level Class block.
        ancestor = parent_map.get(node)
        while ancestor is not None and not isinstance(
            ancestor,
            (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module),
        ):
            ancestor = parent_map.get(ancestor)
        if not isinstance(ancestor, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue

        visitor = ComplexityVisitor()
        visitor.visit(node)
        for cls in visitor.classes:
            out.extend(
                _iter_function_blocks(
                    [cls],
                    RadonFunction,
                    class_methods_are_siblings=False,
                )
            )
    return out
