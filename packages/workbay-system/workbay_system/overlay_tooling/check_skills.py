#!/usr/bin/env python3
"""Validate skill anatomy and wiring for `.claude/skills/*/SKILL.md`."""

from __future__ import annotations

import ast
from collections import Counter
import re
import sys
from pathlib import Path

try:
    import yaml
except ModuleNotFoundError as exc:
    yaml = None
    _YAML_IMPORT_ERROR = exc
else:
    _YAML_IMPORT_ERROR = None

try:
    from workbay_system.overlay_tooling.overlay_resolver import (
        BrokenOverlayError,
        OverlayResolverError,
        detect_overlay_mode,
        resolve_surface,
    )
except ModuleNotFoundError:
    from .overlay_resolver import (
        BrokenOverlayError,
        OverlayResolverError,
        detect_overlay_mode,
        resolve_surface,
    )

from workbay_system.overlay_tooling._paths import PACKAGE_ROOT as REPO_ROOT

# implementation note step 1: canonical source-of-truth is the neutral layout
# at <package>/skills/<slug>/{skill.yaml, body.md}. The Claude-namespaced
# .claude/skills/<slug>/SKILL.md path is a generated artifact in target
# repos, not source.
SKILLS_ROOT = REPO_ROOT / "skills"
ROUTING_FILE = REPO_ROOT / "docs" / "workbay" / "maps" / "mcp-tool-routing.yaml"

# Structured validation is delegated to workbay_protocol.SkillManifest;
# this list only enumerates the fields check_skills.py performs *additional*
# local checks on (e.g. cross-references against make targets and known
# MCP tools). 'disable-model-invocation' is no longer canonical — it
# lives under generator.claude_overrides — so it's not in this list.
REQUIRED_FIELDS = (
    "name",
    "description",
    "scope",
    "mode",
    "context_budget",
    "makefile_target",
    "mcp_tools",
    "tdd_gate",
)
VALID_SCOPES = {"harness", "project"}
REQUIRED_SECTIONS = (
    "## Overview",
    "## Trigger",
    "## Goal",
    "## Canonical Policy",
    "## Core Process",
    "## Common Rationalizations",
    "## Red Flags",
    "## Recovery",
    "## Convergence Criteria",
    "## See Also",
)
VALID_MODES = {"advisory", "execution"}


# MCP server API files. Two layouts supported:
#   - workbay-system embedded inside a consuming monorepo (target repo
#     containing packages/mcp-workbay-handoff/, packages/mcp-workbay-orchestrator/);
#     REPO_ROOT walks up to the consuming-monorepo root.
#   - workbay (this monorepo): the MCP servers are
#     siblings under packages/mcp-workbay-handoff/ and
#     packages/mcp-workbay-orchestrator/. Look for whichever resolves.
def _resolve_server_api_files(repo_root: Path, packages_root: Path) -> dict[str, Path]:
    candidates_by_server: dict[str, tuple[Path, ...]] = {
        "workbay-handoff-mcp": (
            packages_root
            / "mcp-workbay-handoff"
            / "src"
            / "workbay_handoff_mcp"
            / "api.py",
            repo_root
            / "packages"
            / "mcp-workbay-handoff"
            / "src"
            / "workbay_handoff_mcp"
            / "api.py",
        ),
        "workbay-orchestrator-mcp": (
            packages_root
            / "mcp-workbay-orchestrator"
            / "src"
            / "workbay_orchestrator_mcp"
            / "api.py",
            repo_root
            / "packages"
            / "mcp-workbay-orchestrator"
            / "src"
            / "workbay_orchestrator_mcp"
            / "api.py",
        ),
    }
    resolved: dict[str, Path] = {}
    for server, candidates in candidates_by_server.items():
        for candidate in candidates:
            if candidate.is_file():
                resolved[server] = candidate
                break
    return resolved


MAKEFILE_RE = re.compile(r"^([A-Za-z0-9_.-]+):")


class SkillCheckError(Exception):
    pass


def _load_skill_files(skill_dir: Path) -> tuple[dict, str]:
    """Read the neutral canonical layout: skill.yaml + body.md.

    Returns ``(structured_metadata, prose_body)``. Raises
    ``SkillCheckError`` with the precise problem if either file is
    missing or unparseable.
    """
    skill_yaml = skill_dir / "skill.yaml"
    body_md = skill_dir / "body.md"
    if not skill_yaml.is_file():
        raise SkillCheckError(f"missing skill.yaml at {skill_yaml}")
    if not body_md.is_file():
        raise SkillCheckError(f"missing body.md at {body_md}")
    structured = yaml.safe_load(skill_yaml.read_text()) or {}
    if not isinstance(structured, dict):
        raise SkillCheckError(f"{skill_yaml}: must parse to a mapping")
    return structured, body_md.read_text()


def _extract_literal_tool_names(api_path: Path) -> set[str]:
    module = ast.parse(api_path.read_text(), filename=str(api_path))
    for node in module.body:
        names_to_collect: list[str] = []
        value: ast.AST | None = None
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {
                    "TOOL_DESCRIPTIONS",
                    "LEGACY_TOOL_DESCRIPTIONS",
                }:
                    names_to_collect.append(target.id)
                    value = node.value
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id in {
                "TOOL_DESCRIPTIONS",
                "LEGACY_TOOL_DESCRIPTIONS",
            }:
                names_to_collect.append(node.target.id)
                value = node.value

        if not names_to_collect:
            continue
        if not isinstance(value, ast.Dict):
            raise SkillCheckError(
                f"{api_path} {', '.join(names_to_collect)} is not a dict literal"
            )
        names: set[str] = set()
        for key in value.keys:
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                raise SkillCheckError(
                    f"{api_path} {', '.join(names_to_collect)} contains a non-literal key"
                )
            names.add(key.value)
        return names
    raise SkillCheckError(
        f"{api_path} does not define TOOL_DESCRIPTIONS or LEGACY_TOOL_DESCRIPTIONS"
    )


def _load_known_tools(server_api_files: dict[str, Path]) -> set[str]:
    if not ROUTING_FILE.is_file():
        raise SkillCheckError(
            f"mcp-tool-routing.yaml not found at {ROUTING_FILE}; pass --target at a "
            "repository containing docs/workbay/maps/mcp-tool-routing.yaml"
        )
    routing = yaml.safe_load(ROUTING_FILE.read_text()) or {}
    if not isinstance(routing, dict):
        raise SkillCheckError("mcp-tool-routing.yaml must parse to a mapping")

    server_names: set[str] = set()
    always = routing.get("always", [])
    if isinstance(always, list):
        server_names.update(str(item) for item in always)

    on_demand = routing.get("on_demand", {})
    if isinstance(on_demand, dict):
        server_names.update(str(name) for name in on_demand)

    known_tools: set[str] = set()
    for server_name in sorted(server_names):
        api_path = server_api_files.get(server_name)
        if api_path is None:
            # External services such as context7 or computer-use do not ship a
            # local API manifest in this repo, so they cannot contribute local
            # skill-wiring entries here.
            continue
        known_tools.update(_extract_literal_tool_names(api_path))
    return known_tools


def _load_make_targets() -> set[str]:
    """Collect Make targets from any Makefile/mk fragments present.

    Tolerant of missing files: workbay-system embedded in a consumer
    monorepo has both; workbay's package directory
    has neither (those live in target repos). Skills with
    ``makefile_target: null`` always pass; skills referencing a
    non-null target validate against whatever targets we found.
    """
    targets: set[str] = set()
    candidates = [REPO_ROOT / "Makefile", *(REPO_ROOT / "mk").glob("*.mk")]
    for path in candidates:
        if not path.is_file():
            continue
        for line in path.read_text().splitlines():
            match = MAKEFILE_RE.match(line)
            if match:
                targets.add(match.group(1))
    return targets


def _validate_frontmatter(
    frontmatter: dict, make_targets: set[str], known_tools: set[str]
) -> list[str]:
    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in frontmatter:
            errors.append(f"missing required frontmatter field `{field}`")

    mode = frontmatter.get("mode")
    if mode not in VALID_MODES:
        errors.append(f"`mode` must be one of {sorted(VALID_MODES)}, got {mode!r}")

    scope = frontmatter.get("scope")
    if scope not in VALID_SCOPES:
        errors.append(
            f"`scope` must be one of {sorted(VALID_SCOPES)}, got {scope!r}. "
            "Use 'harness' for project-agnostic skills installed by workbay-bootstrap, "
            "'project' for skills that must stay in the target repo."
        )

    # Cross-repo contract: validate against workbay_protocol.SkillManifest
    # when available. Failure to install the protocol package is not
    # fatal — local validators continue to enforce the rest. This keeps
    # the harness usable during partial migrations.
    try:
        from workbay_protocol import SkillManifest  # type: ignore[import-not-found]
    except ImportError:
        pass
    else:
        try:
            SkillManifest.model_validate(frontmatter)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"workbay_protocol.SkillManifest validation: {exc}")

    context_budget = frontmatter.get("context_budget")
    if not isinstance(context_budget, int) or context_budget <= 0:
        errors.append("`context_budget` must be a positive integer")

    if not isinstance(frontmatter.get("tdd_gate"), bool):
        errors.append("`tdd_gate` must be a boolean")

    target = frontmatter.get("makefile_target")
    if target is not None:
        if not isinstance(target, str) or not target.strip():
            errors.append("`makefile_target` must be null or a non-empty string")
        elif make_targets and target not in make_targets:
            # Cross-reference only when a Makefile is present in this
            # repo. In the workbay-system source-of-truth context (no
            # Makefile), the cross-reference is enforced by the
            # consumer-repo CI where both the skills and the Makefile
            # live together.
            errors.append(f"`makefile_target` references unknown target `{target}`")

    mcp_tools = frontmatter.get("mcp_tools")
    if not isinstance(mcp_tools, list):
        errors.append("`mcp_tools` must be a list")
    else:
        for tool_name in mcp_tools:
            if not isinstance(tool_name, str) or not tool_name.strip():
                errors.append("`mcp_tools` entries must be non-empty strings")
                continue
            if tool_name not in known_tools:
                errors.append(f"`mcp_tools` references unknown MCP tool `{tool_name}`")

    for text_field in ("name", "description"):
        value = frontmatter.get(text_field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"`{text_field}` must be a non-empty string")

    return errors


def _validate_sections(body: str) -> list[str]:
    return [
        f"missing required section `{section}`"
        for section in REQUIRED_SECTIONS
        if section not in body
    ]


def _resolve_skill_dirs(
    repo_root: Path, skills_root: Path
) -> tuple[list[Path], list[str]]:
    """Return the per-slug directories that contain skill.yaml + body.md.

    Overlay mode is classified by the shared ``detect_overlay_mode`` detector,
    not a raw ``.workbay-overlay.json`` filename probe:
    - source-tree or canonical bootstrap mode: glob ``<skills_root>/*/skill.yaml``.
      Skills are not a canonical-ledger surface (they live in the generated
      plugin tree), so an absent skills ledger entry is never drift.
    - legacy mapping overlay: delegate to ``resolve_surface`` so
      shared/local/overlapping accounting still works. Each entry's
      ``effective_path`` is treated as the slug dir and must contain
      both ``skill.yaml`` and ``body.md``.
    An ambiguous user-owned dual-manifest fails closed via ``detect_overlay_mode``.
    """
    try:
        mode = detect_overlay_mode(repo_root)
    except OverlayResolverError as exc:
        return [], [f"infrastructure error: {exc}"]

    if mode != "legacy":
        return sorted(p.parent for p in skills_root.glob("*/skill.yaml")), []

    try:
        resolved_entries = resolve_surface("skills", repo_root)
    except BrokenOverlayError as exc:
        return [], [f"BrokenOverlayError: {exc}"]
    except OverlayResolverError as exc:
        return [], [f"infrastructure error: {exc}"]

    resolved_dirs = sorted(
        entry.effective_path
        for entry in resolved_entries
        if (entry.effective_path / "skill.yaml").is_file()
    )
    return resolved_dirs, []


def _format_success_message(*, repo_root: Path, skills_root: Path) -> str:
    skill_dirs, _overlay_failures = _resolve_skill_dirs(repo_root, skills_root)
    try:
        mode = detect_overlay_mode(repo_root)
    except OverlayResolverError:
        mode = "source_tree"
    # Only a legacy mapping overlay carries shared/local/overlapping accounting;
    # canonical bootstrap and source-tree modes glob a flat skills_root.
    if mode != "legacy":
        return f"check-skills: OK ({len(skill_dirs)} skills)"

    counts: Counter[str] = Counter(
        entry.source
        for entry in resolve_surface("skills", repo_root)
        if (entry.effective_path / "skill.yaml").is_file()
    )
    return (
        "check-skills: OK "
        f"({len(skill_dirs)} skills; shared={counts['shared']} local={counts['local']} overlapping={counts['overlapping']})"
    )


def check_skills(
    *,
    repo_root: Path = REPO_ROOT,
    packages_root: Path | None = None,
    skills_root: Path | None = None,
    routing_file: Path | None = None,
) -> tuple[list[str], int]:
    if _YAML_IMPORT_ERROR is not None:
        return ["infrastructure error: PyYAML is required to load skill frontmatter"], 1

    packages_root = packages_root or repo_root.parent
    skills_root = skills_root or (repo_root / "skills")
    routing_file = routing_file or (
        repo_root / "docs" / "workbay" / "maps" / "mcp-tool-routing.yaml"
    )
    server_api_files = _resolve_server_api_files(repo_root, packages_root)

    global REPO_ROOT, SKILLS_ROOT, ROUTING_FILE
    original_repo_root, original_skills_root, original_routing_file = (
        REPO_ROOT,
        SKILLS_ROOT,
        ROUTING_FILE,
    )
    REPO_ROOT, SKILLS_ROOT, ROUTING_FILE = repo_root, skills_root, routing_file

    try:
        known_tools = _load_known_tools(server_api_files)
        make_targets = _load_make_targets()
    except SkillCheckError as exc:
        return [f"infrastructure error: {exc}"], 1
    finally:
        REPO_ROOT, SKILLS_ROOT, ROUTING_FILE = (
            original_repo_root,
            original_skills_root,
            original_routing_file,
        )

    failures: list[str] = []
    skill_dirs, overlay_failures = _resolve_skill_dirs(repo_root, skills_root)
    failures.extend(overlay_failures)
    for skill_dir in skill_dirs:
        try:
            structured, body = _load_skill_files(skill_dir)
        except SkillCheckError as exc:
            failures.append(f"{skill_dir.relative_to(repo_root)}: {exc}")
            continue

        for error in _validate_frontmatter(structured, make_targets, known_tools):
            failures.append(f"{skill_dir.relative_to(repo_root)}: {error}")
        for error in _validate_sections(body):
            failures.append(f"{skill_dir.relative_to(repo_root)}: {error}")

    return failures, 0 if not failures else 1


def main(argv: list[str] | None = None) -> int:
    from workbay_system.overlay_tooling._paths import parse_overlay_tooling_argv

    try:
        ctx, _remaining = parse_overlay_tooling_argv(
            list(argv or []), require_fixture_sources=False
        )
    except ValueError as exc:
        print(f"check-skills: infrastructure error: {exc}", file=sys.stderr)
        return 1

    repo_root = ctx.target_root
    skills_root = repo_root / "skills"
    routing_file = repo_root / "docs" / "workbay" / "maps" / "mcp-tool-routing.yaml"

    failures, exit_code = check_skills(
        repo_root=repo_root,
        packages_root=ctx.packages_root,
        skills_root=skills_root,
        routing_file=routing_file,
    )
    if exit_code == 1 and failures and failures[0].startswith("infrastructure error:"):
        print(f"check-skills: {failures[0]}", file=sys.stderr)
        return 1

    if failures:
        print("check-skills: FAILED", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(_format_success_message(repo_root=repo_root, skills_root=skills_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
