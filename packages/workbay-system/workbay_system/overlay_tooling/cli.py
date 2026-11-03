"""Thin CLI adapter for overlay validators."""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from importlib import import_module
from pathlib import Path
from runpy import run_path

_PAYLOAD_ROOT = Path(__file__).resolve().parents[1] / "payload"
_PAYLOAD_SCRIPT_VERBS = {
    "check-workflow-facade": "check_workflow_facade.py",
    "validate-claude-settings-pin": "validate_claude_settings_pin.py",
}

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
        verbs = ", ".join(sorted((*_VERB_MAIN, *_PAYLOAD_SCRIPT_VERBS)))
        print(f"usage: workbay-overlay-tooling <verb> [args...]\nverbs: {verbs}")
        return 0 if argv and argv[0] in {"-h", "--help"} else 2
    verb = argv[0]
    if verb in _PAYLOAD_SCRIPT_VERBS:
        return _run_payload_script(verb, list(argv[1:]))
    spec = _VERB_MAIN.get(verb)
    if spec is None:
        print(f"workbay-overlay-tooling: unknown verb {verb!r}", file=sys.stderr)
        return 2
    module_name, attr, passes_argv = spec
    target: Callable[..., int] = getattr(import_module(module_name), attr)
    if passes_argv:
        return int(target(list(argv[1:])))
    return int(target())


def _root_argument(argv: Sequence[str]) -> Path:
    for index, argument in enumerate(argv):
        if argument == "--root" and index + 1 < len(argv):
            return Path(argv[index + 1])
        if argument.startswith("--root="):
            return Path(argument.partition("=")[2])
    return _PAYLOAD_ROOT


def _run_payload_script(verb: str, argv: list[str]) -> int:
    script_path = _PAYLOAD_ROOT / "scripts" / _PAYLOAD_SCRIPT_VERBS[verb]
    if not script_path.is_file():
        print(
            f"workbay-overlay-tooling: missing payload script {script_path}",
            file=sys.stderr,
        )
        return 2

    original_argv = sys.argv
    sys.argv = [str(script_path), *argv]
    try:
        namespace = run_path(str(script_path), run_name="workbay_payload_script")
        if verb == "check-workflow-facade":
            root = _root_argument(argv)
            manifest_rel_path = namespace["MANIFEST_REL_PATH"]
            target_manifest = root / manifest_rel_path
            payload_manifest = _PAYLOAD_ROOT / manifest_rel_path
            uses_payload_manifest = not target_manifest.is_file() and payload_manifest.is_file()
            if uses_payload_manifest:
                # Package consumers omit generator inputs from the target.
                # Keep checks rooted at the consumer while resolving the map
                # and skill source from the installed payload.
                namespace["MANIFEST_REL_PATH"] = payload_manifest

            check_skill_by_command_map = namespace["check_skill_by_command_map"]

            def _check_skill_by_command_map(target_root: Path) -> list[str]:
                if uses_payload_manifest or not (target_root / "skills").is_dir():
                    return check_skill_by_command_map(_PAYLOAD_ROOT)
                return check_skill_by_command_map(target_root)

            namespace["check_skill_by_command_map"] = _check_skill_by_command_map
            return int(namespace["main"]())
        return int(namespace["main"](argv))
    finally:
        sys.argv = original_argv


def main(argv: Sequence[str] | None = None) -> int:
    return _dispatch(list(argv if argv is not None else sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
