#!/usr/bin/env python3
"""Lint workflow source files for public-facade cold-start guidance.

The facade scans only under ``--root`` (default: the payload tree); files
outside that root are never considered, by design (covered by the
generator's drift gate, not this lint)."""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path


DEFAULT_ROOT = Path(__file__).resolve().parents[1]
MAX_SOURCE_BYTES = 256 * 1024
MANIFEST_REL_PATH = Path("config") / "agent-workflows" / "portable_commands.json"
MAKEFILE_FRAGMENTS_GLOB = "Makefile.d/*.mk"

# (skill, MAKE_VAR, argument_schema.name) triples whose Make variable name
# does not match a schema entry under the SHOUTING_SNAKE -> kebab-lower
# translation. Keyed on the unchanged skill field so a command-id rename
# (wb-* rail) does not invalidate the documented TASK=<task-ref> ergonomic
# divergence, and so a future command cannot inherit the pair unaudited.
# Adding to this set should be a deliberate review decision; the manifest
# test pins the membership by data.
MANIFEST_ARG_NAME_ALLOWLIST: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("branch-lifecycle", "TASK", "task-ref"),
        ("incremental-implementation", "TASK", "task-ref"),
        ("tdd", "TASK", "task-ref"),
    }
)

# Make targets that are real lifecycle recipes but are not skill-broadcast
# wrappers. SKILL_BY_COMMAND only dispatches plan-review / plan-analyze.
# Adding these keys to the map would change runtime broadcast behavior, so
# completeness checks exempt them explicitly instead.
SKILL_BROADCAST_EXEMPT_MAKE_TARGETS: frozenset[str] = frozenset(
    {
        "task-start",
        "review-run",
        "context",
        "slice-start",
    }
)

_MAKE_VAR_RE = re.compile(r"\b([A-Z][A-Z0-9_]*)=")
_MAKE_TARGET_NAME_RE = re.compile(r"^make\s+([a-zA-Z0-9_.-]+)")
ORIENTATION_BLOCK_MARKERS = (
    "On cold start:",
    "At session start",
)
RAW_ORIENTATION_PATTERNS = (
    re.compile(r"\bload_session\b"),
    re.compile(r"\bsearch_handoff\b"),
    re.compile(r"\breview_findings\b"),
    re.compile(r"\breview_runs\b"),
    re.compile(r"\bget_handoff_state\b"),
    re.compile(r"\bmcp__workbay-handoff-mcp__\b"),
    re.compile(r"\bsqlite3\s+\.task-state/handoff\.db\b"),
    re.compile(r"\bDASHBOARD\.txt\b"),
    re.compile(r"\bCURRENT_TASK\.json\b"),
    re.compile(r"\bcd\s+packages/\b"),
)
FACADE_PATTERNS = (
    re.compile(r"\bmake status\b"),
    re.compile(r"\bmake tasks\b"),
    re.compile(r"\bmake doctor\b"),
)
DEEPER_LOAD_PATTERNS = (
    re.compile(r"\bmake context\b"),
    re.compile(r"\bcat\s+DASHBOARD\.txt\b"),
)
DOCTOR_PATTERN = re.compile(r"\bmake doctor\b")
# internal: the optional non-Make ``agentic`` CLI facade was
# decided *skip* (decision id ``claude_internal_53_cli_facade_skip``).
# Until a real CLI scaffold lands, orientation blocks must not teach
# operators to call commands like ``agentic status --json`` or
# ``agentic tasks --json`` — the binary does not exist on PATH and the
# guidance silently misleads anyone who tries to run it. The verb list
# tracks the lifecycle handlers the canonical Make loop covers.
UNIMPLEMENTED_AGENTIC_CLI_PATTERN = re.compile(
    r"\bagentic\s+(?:status|tasks|task-start|task-finish|review-ready|"
    r"close-check|context|doctor|review-run|enter)\b"
)
# internal: ``make <target> --json`` is not a real Make spelling.
# Make parses ``--json`` after a target name as another target, not a
# flag, so any orientation guidance teaching that form is silently
# misleading. The honest form is ``make <target> LIFECYCLE_ARGS=--json``.
RAW_POST_TARGET_JSON_PATTERN = re.compile(
    r"\bmake\s+(?:status|tasks|task-start|review-ready|close-check|"
    r"context|doctor|handoff-close-check)\s+--json\b"
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    return parser.parse_args()


def _iter_source_files(root: Path) -> list[Path]:
    files = sorted((root / "skills").glob("*/body.md"))
    prompts_root = root / "config" / "agent-workflows" / "prompts"
    if prompts_root.is_dir():
        files.extend(sorted(prompts_root.rglob("*.md")))
    files.extend(_iter_generated_files(root))
    return files


def _generated_scan_roots(root: Path) -> list[Path]:
    """Return roots that may host generated adapters/docs.

    Production ``make check-agent-workflows`` passes
    ``--root $(WORKFLOWS_ROOT)``, which is the payload tree. The generator
    writes package-internal adapters at ``<payload>/../../`` and a producer
    router at the repo-root ``docs/workbay/generated`` mirror. Isolated
    tmp_path fixtures are not payload-shaped and stay local to ``--root``.
    """
    roots = [root]
    if root.name == "payload":
        # Sibling of payload (scratch payload-shaped trees + some layouts).
        roots.append(root.parent)
        if root.parent.name == "workbay_system" and len(root.parents) >= 2:
            roots.append(root.parents[1])
            if len(root.parents) >= 4:
                roots.append(root.parents[3])
    seen: set[Path] = set()
    unique: list[Path] = []
    for candidate in roots:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(candidate)
    return unique


def _display_rel(path: Path, root: Path) -> str:
    """Best-effort relative path for error messages, including extra scan roots."""
    resolved = path.resolve()
    for base in _generated_scan_roots(root):
        try:
            return resolved.relative_to(base.resolve()).as_posix()
        except ValueError:
            continue
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


def _iter_generated_files(root: Path) -> list[Path]:
    """Return generated workflow adapter/doc paths the lint must also scan.

    internal (finding internal): `generate_agent_workflows.py`
    renders skill bodies into `.claude/commands/*.md`, `.github/prompts/**.md`,
    and `docs/workbay/generated/*.md`. Without scanning those outputs the
    `agentic <verb>` skip recorded in implementation note can leak past the source skills
    via a manifest or template change. The output directories are listed
    explicitly (rather than globbed from `root`) so unrelated `.md` files —
    READMEs, ADRs, design docs — are not pulled into orientation lint scope.

    Scan every generator-shaped root, not just directories that already
    exist under ``--root``. Missing dirs under the payload tree must not
    skip the package-internal / repo-root mirrors.
    """
    files: list[Path] = []
    seen: set[Path] = set()
    for scan_root in _generated_scan_roots(root):
        claude_commands = scan_root / ".claude" / "commands"
        if claude_commands.is_dir():
            for path in sorted(claude_commands.glob("*.md")):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                files.append(path)
        github_prompts = scan_root / ".github" / "prompts"
        if github_prompts.is_dir():
            for path in sorted(github_prompts.rglob("*.md")):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                files.append(path)
        codex_router = scan_root / "docs" / "workbay" / "generated"
        if codex_router.is_dir():
            for path in sorted(codex_router.glob("*.md")):
                resolved = path.resolve()
                if resolved in seen:
                    continue
                seen.add(resolved)
                files.append(path)
    return files


def _should_scan_text(text: str) -> bool:
    return any(marker in text for marker in ORIENTATION_BLOCK_MARKERS)


def _orientation_blocks(text: str) -> list[tuple[int, str]]:
    lines = text.splitlines()
    blocks: list[tuple[int, str]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not any(marker in line for marker in ORIENTATION_BLOCK_MARKERS):
            index += 1
            continue
        start = index
        end = index + 1
        while end < len(lines):
            current = lines[end]
            if not current.strip():
                end += 1
                continue
            if re.match(r"^##\s+", current) or re.match(r"^\d+\.\s", current):
                break
            if not current.startswith((" ", "\t", "-", "*", "|")):
                break
            end += 1
        blocks.append((start + 1, "\n".join(lines[start:end])))
        index = end
    return blocks


def _block_has_raw_orientation(block: str) -> bool:
    return any(pattern.search(block) for pattern in RAW_ORIENTATION_PATTERNS)


def _block_has_facade(block: str) -> bool:
    return any(pattern.search(block) for pattern in FACADE_PATTERNS)


def _block_uses_raw_post_target_json(block: str) -> bool:
    """Report blocks that teach ``make <target> --json`` as a flag.

    internal: GNU Make does not accept arbitrary post-target
    flags. ``--json`` after a target name is parsed as another target
    name, not a flag, and silently does the wrong thing. The canonical
    spelling is ``make <target> LIFECYCLE_ARGS=--json``; orientation
    guidance must not normalise the broken form.
    """
    return RAW_POST_TARGET_JSON_PATTERN.search(block) is not None


def _block_uses_unimplemented_workbay_cli(block: str) -> tuple[bool, str | None]:
    """Report blocks that teach an ``agentic <verb>`` CLI form.

    internal recorded the decision to *skip* a non-Make CLI
    facade. Until a real scaffold lands the ``agentic`` binary does not
    exist on PATH, so any orientation block teaching ``agentic status``
    / ``agentic tasks`` / etc. silently misleads operators back to a
    surface they cannot run. The lint must reject those references so
    docs and generated workflow text stay honest.
    """
    match = UNIMPLEMENTED_AGENTIC_CLI_PATTERN.search(block)
    if match is None:
        return False, None
    return True, match.group(0)


def _block_violates_doctor_first(block: str) -> bool:
    """Report blocks that put `make context` or `cat DASHBOARD.txt` ahead of `make doctor`.

    internal promotes `make doctor` to the cold-start surface.
    The deeper-load commands stay valid as follow-ups but must not be
    presented as the *first* recommendation in an orientation block.
    """
    doctor_match = DOCTOR_PATTERN.search(block)
    if doctor_match is None:
        return False
    doctor_pos = doctor_match.start()
    for pattern in DEEPER_LOAD_PATTERNS:
        match = pattern.search(block)
        if match is not None and match.start() < doctor_pos:
            return True
    return False


def _translate_make_var(name: str) -> str:
    """SHOUTING_SNAKE -> kebab-lower (e.g. ``TEST_CMD`` -> ``test-cmd``)."""
    return name.lower().replace("_", "-")


def _load_manifest(root: Path) -> dict | None:
    manifest_path = root / MANIFEST_REL_PATH
    if not manifest_path.is_file():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _find_recipe_body(root: Path, target_name: str) -> str | None:
    """Return the recipe body for ``target_name`` from any Makefile.d/*.mk
    fragment, or None if the target is not defined.

    The body is collected from the colon line through the first non-recipe
    line so a `target: ## doc\n\t@cmd` shape returns "@cmd".
    """
    target_re = re.compile(
        rf"^{re.escape(target_name)}\s*:(?!=)"
    )
    for fragment in sorted(root.glob(MAKEFILE_FRAGMENTS_GLOB)):
        text = fragment.read_text(encoding="utf-8")
        lines = text.splitlines()
        for index, line in enumerate(lines):
            if not target_re.match(line):
                continue
            body_parts: list[str] = [line]
            cursor = index + 1
            while cursor < len(lines):
                current = lines[cursor]
                if current.startswith("\t"):
                    body_parts.append(current)
                    cursor += 1
                    continue
                if current.strip() == "" and cursor + 1 < len(lines) and lines[cursor + 1].startswith("\t"):
                    body_parts.append(current)
                    cursor += 1
                    continue
                break
            return "\n".join(body_parts)
    return None


def check_manifest_recipe_forwarding(root: Path) -> list[str]:
    """Cross-check ``portable_commands.json`` ``makefile_target`` invocations
    against (a) ``argument_schema`` arg names and (b) recipe ``$(VAR)``
    references. Returns a list of human-readable error strings."""
    errors: list[str] = []
    manifest = _load_manifest(root)
    if manifest is None:
        return errors
    for entry in manifest.get("commands", []):
        if not isinstance(entry, dict):
            continue
        if entry.get("tombstone") is True:
            continue
        command_id = entry.get("command_id", "<unknown>")
        target_str = entry.get("makefile_target", "")
        # Skip placeholder targets like "(in-session intake; ...)".
        if not target_str.startswith("make "):
            continue
        name_match = _MAKE_TARGET_NAME_RE.match(target_str)
        if name_match is None:
            continue
        target_name = name_match.group(1)
        schema_names = {
            arg.get("name") for arg in entry.get("argument_schema", []) if arg.get("name")
        }
        recipe_body = _find_recipe_body(root, target_name)
        skill = entry.get("skill")
        for var_name in _MAKE_VAR_RE.findall(target_str):
            translated = _translate_make_var(var_name)
            allowlisted = any(
                (skill, var_name, schema_name) in MANIFEST_ARG_NAME_ALLOWLIST
                for schema_name in schema_names
            )
            if translated not in schema_names and not allowlisted:
                errors.append(
                    f"portable_commands.json: command {command_id!r} documents "
                    f"`{var_name}=` in makefile_target but no argument_schema "
                    f"entry named {translated!r} exists. Add the schema arg, "
                    f"rename the Make var, or extend MANIFEST_ARG_NAME_ALLOWLIST."
                )
            if recipe_body is not None and f"$({var_name})" not in recipe_body:
                errors.append(
                    f"Makefile.d: recipe for `{target_name}` does not reference "
                    f"`$({var_name})`, but portable_commands.json command "
                    f"{command_id!r} documents `{var_name}=`. The Make variable "
                    f"is silently dropped — add `$(if $({var_name}),--{translated} "
                    f"'$({var_name}))')` (or equivalent) to the recipe."
                )
            if recipe_body is None:
                errors.append(
                    f"Makefile.d: portable_commands.json command "
                    f"{command_id!r} documents target `{target_name}` but no "
                    f"matching recipe was found in Makefile.d/*.mk."
                )
                # One missing-recipe error per entry is enough; do not
                # repeat for every var.
                break
    return errors


# [T23/T19] Skill bodies that mandate get_handoff_state / load_session must
# name a read_profile (or sections= shape pin). Wired into check-agent-workflows
# via this facade check so new skills inherit the discipline automatically.
_HANDOFF_READ_CALL_RE = re.compile(
    r"\b(get_handoff_state|load_session)\s*\(([^)]*)\)",
    re.DOTALL,
)


_FENCE_RE = re.compile(r"^(?:```|~~~)", re.MULTILINE)


def _mandate_code_spans(text: str) -> list[tuple[int, int]]:
    """Return (start, end) spans of code-formatted regions in *text*.

    Skill bodies mandate calls in code formatting — fenced blocks or inline
    backtick spans. Prose mentions outside code formatting (explanations,
    negative examples like "never call load_session(...) unshaped") are not
    call mandates and must not be linted (S10-A-03).
    """
    spans: list[tuple[int, int]] = []
    # Fenced code blocks (``` / ~~~ pairs).
    fences = [m.start() for m in _FENCE_RE.finditer(text)]
    for i in range(0, len(fences) - 1, 2):
        spans.append((fences[i], fences[i + 1]))
    # Inline backtick spans (single-line, non-greedy).
    for m in re.finditer(r"`[^`\n]+`", text):
        spans.append((m.start(), m.end()))
    return spans


def _in_mandate_context(spans: list[tuple[int, int]], start: int, end: int) -> bool:
    return any(s <= start and end <= e for s, e in spans)


def _handoff_read_call_is_shaped(args: str) -> bool:
    """True when args name read_profile= or pin sections= (bounded shape).

    Doc placeholders ``load_session(...)`` / ``get_handoff_state(...)`` use a
    bare ellipsis and are not call mandates — only concrete argument lists are
    linted.
    """
    stripped = args.strip()
    if stripped in {"...", "…"}:
        return True
    return bool(
        re.search(r"\bread_profile\s*=", args) or re.search(r"\bsections\s*=", args)
    )


def _load_skill_by_command(path: Path) -> dict[str, str]:
    """Parse ``SKILL_BY_COMMAND`` from skill_broadcast.py without importing it."""
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in module.body:
        target_id: str | None = None
        value: ast.AST | None = None
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target_id = node.target.id
            value = node.value
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "SKILL_BY_COMMAND":
                    target_id = target.id
                    value = node.value
                    break
        if target_id != "SKILL_BY_COMMAND" or value is None:
            continue
        parsed = ast.literal_eval(value)
        if not isinstance(parsed, dict):
            raise ValueError(f"{path}: SKILL_BY_COMMAND must be a dict")
        return {str(key): str(slug) for key, slug in parsed.items()}
    raise ValueError(f"{path}: SKILL_BY_COMMAND assignment not found")


def _live_command_for_make_target(commands: list, make_target: str) -> dict | None:
    prefix = f"make {make_target}"
    for command in commands:
        if not isinstance(command, dict) or command.get("tombstone") is True:
            continue
        target = command.get("makefile_target") or ""
        if target == prefix or target.startswith(prefix + " "):
            return command
    return None


def check_skill_by_command_map(root: Path) -> list[str]:
    """Every SKILL_BY_COMMAND key must bind to the live command's declared skill.

    The map check is in-scope only when the broadcast module is present so
    orientation-only tmp_path fixtures stay independent: a root that does not
    ship ``skill_broadcast.py`` is a partial tree (pytest fixtures write skill
    bodies or a copied manifest without the lifecycle handler), and sibling
    ``check_root`` gates already no-op when their input file is absent. A
    shipped payload still fail-closes, because ``test_as_shipped_manifest_passes``
    requires the file and then runs this check against the real map. When the
    module is present the check fail-closes on parse errors, a missing
    manifest, and live ``make <target>`` rows that have no map entry.
    """
    broadcast = (
        root / "scripts" / "workbay_lifecycle" / "handlers" / "skill_broadcast.py"
    )
    if not broadcast.is_file():
        return []
    try:
        skill_by_command = _load_skill_by_command(broadcast)
    except (SyntaxError, ValueError) as exc:
        return [f"SKILL_BY_COMMAND: {exc}"]
    manifest_path = root / MANIFEST_REL_PATH
    if not manifest_path.is_file():
        return [f"SKILL_BY_COMMAND: missing {manifest_path}"]
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"SKILL_BY_COMMAND: invalid JSON in {manifest_path}: {exc}"]
    if not isinstance(manifest, dict):
        return [f"SKILL_BY_COMMAND: {manifest_path} must contain a JSON object"]
    commands = manifest.get("commands")
    if not isinstance(commands, list):
        return [f"SKILL_BY_COMMAND: {manifest_path} commands must be a list"]
    manifest_skills = {
        command.get("skill")
        for command in commands
        if isinstance(command, dict) and command.get("skill")
    }
    skills_dir = root / "skills"
    errors: list[str] = []
    for make_target, slug in sorted(skill_by_command.items()):
        skill_yaml = skills_dir / slug / "skill.yaml"
        body_md = skills_dir / slug / "body.md"
        if not skill_yaml.is_file() or not body_md.is_file():
            errors.append(
                f"SKILL_BY_COMMAND[{make_target!r}]={slug!r} does not name an "
                f"existing skills/{slug}/{{skill.yaml,body.md}} pair."
            )
        if slug not in manifest_skills:
            errors.append(
                f"SKILL_BY_COMMAND[{make_target!r}]={slug!r} is not a skill "
                "referenced by portable_commands.json."
            )
        bound = _live_command_for_make_target(commands, make_target)
        if bound is None:
            errors.append(
                f"SKILL_BY_COMMAND[{make_target!r}] is not bound to a live "
                "makefile_target in portable_commands.json."
            )
        elif bound.get("skill") != slug:
            errors.append(
                f"SKILL_BY_COMMAND[{make_target!r}]={slug!r} does not match "
                f"command {bound.get('command_id')!r} skill {bound.get('skill')!r}."
            )
    for command in commands:
        if not isinstance(command, dict) or command.get("tombstone") is True:
            continue
        target_str = command.get("makefile_target") or ""
        if not target_str.startswith("make "):
            continue
        name_match = _MAKE_TARGET_NAME_RE.match(target_str)
        if name_match is None:
            continue
        make_target = name_match.group(1)
        if make_target in skill_by_command:
            continue
        if make_target in SKILL_BROADCAST_EXEMPT_MAKE_TARGETS:
            continue
        errors.append(
            f"SKILL_BY_COMMAND: live command {command.get('command_id')!r} "
            f"documents makefile_target {target_str!r} but {make_target!r} "
            "has no map entry. Add the binding or extend "
            "SKILL_BROADCAST_EXEMPT_MAKE_TARGETS."
        )
    return errors


def check_skill_handoff_read_profiles(root: Path) -> list[str]:
    """Fail when a skill body mandates an unshaped handoff read.

    Named error format: ``skills/<id>/body.md:<line>: unshaped <helper>(...)``
    so the check-agent-workflows gate lists the offending skill + line.
    """
    skills_dir = root / "skills"
    if not skills_dir.is_dir():
        return []
    errors: list[str] = []
    for path in sorted(skills_dir.glob("*/body.md")):
        size = path.stat().st_size
        if size > MAX_SOURCE_BYTES:
            rel = _display_rel(path, root)
            errors.append(
                f"{rel}: oversized ({size} bytes > {MAX_SOURCE_BYTES}); "
                "refuse to skip handoff-read lint."
            )
            continue
        text = path.read_text(encoding="utf-8-sig")
        code_spans = _mandate_code_spans(text)
        for match in _HANDOFF_READ_CALL_RE.finditer(text):
            helper = match.group(1)
            args = match.group(2).strip()
            if _handoff_read_call_is_shaped(args):
                continue
            if not _in_mandate_context(code_spans, match.start(), match.end()):
                # Prose / negative-example mention, not a call mandate.
                continue
            line_no = text.count("\n", 0, match.start()) + 1
            rel = _display_rel(path, root)
            errors.append(
                f"{rel}:{line_no}: unshaped {helper}({args}) — "
                "payload skill bodies mandating get_handoff_state/load_session "
                "must name a read_profile= (or pin sections=). [T19/T23]"
            )
    return errors


def check_root(root: Path) -> list[str]:
    errors: list[str] = []
    for path in _iter_source_files(root):
        size = path.stat().st_size
        if size > MAX_SOURCE_BYTES:
            rel = _display_rel(path, root)
            errors.append(
                f"{rel}: oversized ({size} bytes > {MAX_SOURCE_BYTES}); "
                "refuse to skip orientation lint."
            )
            continue
        text = path.read_text(encoding="utf-8-sig")
        if not _should_scan_text(text):
            continue
        for line_no, block in _orientation_blocks(text):
            if "<!-- diagnostic-only -->" in block:
                continue
            if _block_has_raw_orientation(block) and not _block_has_facade(block):
                rel = _display_rel(path, root)
                errors.append(
                    f"{rel}:{line_no}: session-start guidance that uses raw MCP orientation must route through `make status` or `make tasks` first."
                )
            if _block_violates_doctor_first(block):
                rel = _display_rel(path, root)
                errors.append(
                    f"{rel}:{line_no}: cold-start guidance must put `make doctor` before `make context` or `cat DASHBOARD.txt`."
                )
            if _block_uses_raw_post_target_json(block):
                rel = _display_rel(path, root)
                errors.append(
                    f"{rel}:{line_no}: orientation guidance uses `make <target> --json` — Make parses `--json` after a target as another target, not a flag. Use `make <target> LIFECYCLE_ARGS=--json` instead."
                )
            cli_hit, cli_form = _block_uses_unimplemented_workbay_cli(block)
            if cli_hit:
                rel = _display_rel(path, root)
                errors.append(
                    f"{rel}:{line_no}: orientation guidance teaches `{cli_form}` "
                    "but no `agentic <verb>` CLI exists on PATH "
                    "(internal recorded `claude_internal_53_cli_facade_skip`). "
                    "Use the canonical Make form (e.g. `make status LIFECYCLE_ARGS=--json`) "
                    "or land a real CLI scaffold before reintroducing this surface."
                )
    errors.extend(check_manifest_recipe_forwarding(root))
    errors.extend(check_skill_handoff_read_profiles(root))
    errors.extend(check_skill_by_command_map(root))
    return errors


def main() -> int:
    args = _parse_args()
    errors = check_root(args.root)
    if errors:
        print("workflow facade check failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    print("workflow facade check: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())