"""Thin CLI adapter for overlay validators."""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from importlib import import_module

_VERB_MAIN: dict[str, tuple[str, str, bool]] = {
    # verb -> (module, attr, passes_argv)
    "check-harness-sync": (
        "workbay_system.overlay_tooling.check_harness_sync",
        "main",
        True,
    ),
    "check-skills": ("workbay_system.overlay_tooling.check_skills", "main", True),
    "lint-hoisted-paths": (
        "workbay_system.overlay_tooling.lint_hoisted_paths",
        "main",
        True,
    ),
    "generate-agent-workflows": (
        "workbay_system.overlay_tooling._generator",
        "main",
        True,
    ),
}


def _dispatch(argv: Sequence[str]) -> int:
    if not argv or argv[0] in {"-h", "--help"}:
        verbs = ", ".join(sorted(_VERB_MAIN))
        print(f"usage: workbay-overlay-tooling <verb> [args...]\nverbs: {verbs}")
        return 0 if argv and argv[0] in {"-h", "--help"} else 2
    verb = argv[0]
    spec = _VERB_MAIN.get(verb)
    if spec is None:
        print(f"workbay-overlay-tooling: unknown verb {verb!r}", file=sys.stderr)
        return 2
    module_name, attr, passes_argv = spec
    target: Callable[..., int] = getattr(import_module(module_name), attr)
    if passes_argv:
        return int(target(list(argv[1:])))
    return int(target())


def main(argv: Sequence[str] | None = None) -> int:
    return _dispatch(list(argv if argv is not None else sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
