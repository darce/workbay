#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[4]
DEFAULT_MANIFEST_DIR = REPO_ROOT / "config" / "lane-orchestration"
MANIFEST_DIR = DEFAULT_MANIFEST_DIR

REQUIRED_TOP_LEVEL_KEYS = ("task_ref", "merge_order", "lanes", "downstream")
REQUIRED_LANE_KEYS = ("branch", "worktree_path", "owned_paths", "test_commands")


def _resolve_manifest_dir(*, orchestrator_root: str | None = None, manifest_dir: str | Path | None = None) -> Path:
    if manifest_dir is not None:
        return Path(manifest_dir).expanduser().resolve()
    if MANIFEST_DIR != DEFAULT_MANIFEST_DIR:
        return MANIFEST_DIR
    if orchestrator_root:
        return (Path(orchestrator_root).expanduser().resolve() / "config" / "lane-orchestration").resolve()
    return MANIFEST_DIR


def manifest_dir(*, orchestrator_root: str | None = None, manifest_dir: str | Path | None = None) -> Path:
    return _resolve_manifest_dir(orchestrator_root=orchestrator_root, manifest_dir=manifest_dir)


def list_task_refs(*, orchestrator_root: str | None = None, manifest_dir: str | Path | None = None) -> list[str]:
    resolved_dir = _resolve_manifest_dir(orchestrator_root=orchestrator_root, manifest_dir=manifest_dir)
    if not resolved_dir.exists():
        return []
    return sorted(path.stem for path in resolved_dir.glob("*.json"))


def _require_key(container: dict[str, Any], key: str, *, path: Path, context: str) -> None:
    if key not in container:
        raise RuntimeError(f"lane manifest missing required {context} key '{key}': {path}")


def _derive_commit_paths(owned_paths: list[str]) -> list[str]:
    derived: list[str] = []
    for path in owned_paths:
        value = str(path).strip()
        if not value:
            continue
        if value.endswith("/**"):
            value = value[:-3]
        derived.append(value.rstrip("/") if value != "/" else value)
    return list(dict.fromkeys(item for item in derived if item))


def _normalize_owned_path(path: str) -> str:
    value = str(path).strip()
    if not value:
        return ""
    while value.endswith("/**") or value.endswith("/*"):
        value = value.rsplit("/", 1)[0]
    return value.rstrip("/")


def _candidate_runtime_roots(
    lane: dict[str, Any],
    *,
    orchestrator_root: str,
) -> list[Path]:
    root = Path(orchestrator_root).expanduser().resolve()
    candidates: list[Path] = []

    def add_candidate(relative_path: str) -> None:
        normalized = _normalize_owned_path(relative_path)
        if not normalized:
            return
        full_path = (root / normalized).resolve()
        probe = full_path if full_path.is_dir() else full_path.parent
        for candidate in [probe, *probe.parents]:
            if candidate == root.parent:
                break
            if candidate == root or root in candidate.parents:
                if (candidate / "composer.json").is_file() or (candidate / "package.json").is_file():
                    candidates.append(candidate)
                    break

    app_root = lane.get("app_root")
    if isinstance(app_root, str) and app_root.strip():
        add_candidate(app_root)

    owned_paths = lane.get("owned_paths", [])
    if isinstance(owned_paths, list):
        for owned_path in owned_paths:
            add_candidate(str(owned_path))

    tooling_paths = lane.get("tooling_paths", [])
    if isinstance(tooling_paths, list):
        for tooling_path in tooling_paths:
            tooling_str = str(tooling_path).strip()
            if tooling_str:
                add_candidate(str(Path(tooling_str).parent))

    unique: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate not in seen:
            unique.append(candidate)
            seen.add(candidate)
    return unique


def _derive_runtime_preflight(
    lane: dict[str, Any],
    *,
    orchestrator_root: str,
) -> dict[str, Any]:
    roots = _candidate_runtime_roots(lane, orchestrator_root=orchestrator_root)
    if not roots:
        return {
            "capability_tags": [],
            "preflight_commands": [],
            "preflight_failure_summary": None,
            "preflight_failure_details": None,
        }

    root = Path(orchestrator_root).expanduser().resolve()
    capability_tags: list[str] = []
    commands: list[str] = []
    for runtime_root in roots:
        relative_root = str(runtime_root.relative_to(root))
        if (runtime_root / "composer.json").is_file():
            capability_tags.append("php-tooling-ready")
            commands.append(f"cd {relative_root} && test -f vendor/autoload.php")
        if (runtime_root / "package.json").is_file():
            capability_tags.append("node-tooling-ready")
            commands.append(f"cd {relative_root} && test -d node_modules")

    unique_tags = list(dict.fromkeys(tag for tag in capability_tags if tag))
    unique_commands = list(dict.fromkeys(command for command in commands if command))
    if not unique_commands:
        return {
            "capability_tags": unique_tags,
            "preflight_commands": [],
            "preflight_failure_summary": None,
            "preflight_failure_details": None,
        }

    return {
        "capability_tags": unique_tags,
        "preflight_commands": unique_commands,
        "preflight_failure_summary": "lane runtime preflight failed; lane-local dependencies are not ready.",
        "preflight_failure_details": (
            "This lane references package-managed application roots. Lane bootstrap should provision "
            "lane-local Composer vendor directories and any required Node dependencies before worker "
            "execution. If these prerequisites are still missing, fix bootstrap or install dependencies "
            "before dispatching a model turn."
        ),
    }


def _derive_routing_from_owned_paths(lanes: dict[str, Any]) -> list[tuple[str, str]]:
    derived: list[tuple[str, str]] = []
    for lane_id, lane in lanes.items():
        if not isinstance(lane_id, str) or not isinstance(lane, dict):
            continue
        owned_paths = lane.get("owned_paths", [])
        if not isinstance(owned_paths, list):
            continue
        for raw in owned_paths:
            value = str(raw).strip()
            if not value:
                continue
            if value.endswith("/**"):
                value = value[:-3].rstrip("/") + "/"
            derived.append((value, lane_id))
    return list(dict.fromkeys(derived))


def validate_manifest(data: dict[str, Any], path: Path) -> dict[str, Any]:
    for key in REQUIRED_TOP_LEVEL_KEYS:
        _require_key(data, key, path=path, context="top-level")

    merge_order = data.get("merge_order")
    lanes = data.get("lanes")
    downstream = data.get("downstream")
    if not isinstance(merge_order, list):
        raise RuntimeError(f"lane manifest merge_order must be a list: {path}")
    if not isinstance(lanes, dict):
        raise RuntimeError(f"lane manifest lanes must be an object: {path}")
    if not isinstance(downstream, dict):
        raise RuntimeError(f"lane manifest downstream must be an object: {path}")
    task_plan_path = data.get("task_plan_path")
    heading_to_lane = data.get("heading_to_lane")
    plan_routing_hints = data.get("plan_routing_hints")
    if task_plan_path is not None and (not isinstance(task_plan_path, str) or not task_plan_path.strip()):
        raise RuntimeError(f"lane manifest task_plan_path must be a non-empty string when present: {path}")
    if heading_to_lane is not None and not isinstance(heading_to_lane, dict):
        raise RuntimeError(f"lane manifest heading_to_lane must be an object when present: {path}")
    if plan_routing_hints is not None and not isinstance(plan_routing_hints, list):
        raise RuntimeError(f"lane manifest plan_routing_hints must be a list when present: {path}")

    lane_ids = set(lane_id for lane_id in lanes.keys() if isinstance(lane_id, str))
    unknown_merge_order = [lane_id for lane_id in merge_order if isinstance(lane_id, str) and lane_id not in lane_ids]
    if unknown_merge_order:
        raise RuntimeError(f"lane manifest merge_order references unknown lane(s) {unknown_merge_order}: {path}")

    for lane_id, lane in lanes.items():
        if not isinstance(lane_id, str) or not isinstance(lane, dict):
            raise RuntimeError(f"lane manifest lanes entries must be string->object pairs: {path}")
        for key in REQUIRED_LANE_KEYS:
            _require_key(lane, key, path=path, context=f"lane '{lane_id}'")
        if not isinstance(lane.get("branch"), str) or not str(lane.get("branch")).strip():
            raise RuntimeError(f"lane '{lane_id}' must define a non-empty branch: {path}")
        if not isinstance(lane.get("worktree_path"), str) or not str(lane.get("worktree_path")).strip():
            raise RuntimeError(f"lane '{lane_id}' must define a non-empty worktree_path: {path}")
        if not isinstance(lane.get("owned_paths"), list):
            raise RuntimeError(f"lane '{lane_id}' owned_paths must be a list: {path}")
        if not isinstance(lane.get("test_commands"), list):
            raise RuntimeError(f"lane '{lane_id}' test_commands must be a list: {path}")
        if "commit_paths" in lane and not isinstance(lane.get("commit_paths"), list):
            raise RuntimeError(f"lane '{lane_id}' commit_paths must be a list when present: {path}")
        if "tooling_paths" in lane and not isinstance(lane.get("tooling_paths"), list):
            raise RuntimeError(f"lane '{lane_id}' tooling_paths must be a list when present: {path}")
        if "capability_tags" in lane and not isinstance(lane.get("capability_tags"), list):
            raise RuntimeError(f"lane '{lane_id}' capability_tags must be a list when present: {path}")
        if "preflight_commands" in lane and not isinstance(lane.get("preflight_commands"), list):
            raise RuntimeError(f"lane '{lane_id}' preflight_commands must be a list when present: {path}")
        if "preferred_model" in lane and (
            not isinstance(lane.get("preferred_model"), str) or not str(lane.get("preferred_model")).strip()
        ):
            raise RuntimeError(f"lane '{lane_id}' preferred_model must be a non-empty string when present: {path}")
        if "preferred_backend" in lane and (
            not isinstance(lane.get("preferred_backend"), str) or not str(lane.get("preferred_backend")).strip()
        ):
            raise RuntimeError(f"lane '{lane_id}' preferred_backend must be a non-empty string when present: {path}")
        if "preferred_reasoning_effort" in lane:
            from _env import CODEX_REASONING_EFFORTS

            effort_val = lane.get("preferred_reasoning_effort")
            if not isinstance(effort_val, str) or effort_val.strip().lower() not in CODEX_REASONING_EFFORTS:
                raise RuntimeError(
                    f"lane '{lane_id}' preferred_reasoning_effort must be one of {CODEX_REASONING_EFFORTS} when present: {path}"
                )
        if "preflight_failure_summary" in lane and (
            not isinstance(lane.get("preflight_failure_summary"), str)
            or not str(lane.get("preflight_failure_summary")).strip()
        ):
            raise RuntimeError(
                f"lane '{lane_id}' preflight_failure_summary must be a non-empty string when present: {path}"
            )
        if "preflight_failure_details" in lane and (
            not isinstance(lane.get("preflight_failure_details"), str)
            or not str(lane.get("preflight_failure_details")).strip()
        ):
            raise RuntimeError(
                f"lane '{lane_id}' preflight_failure_details must be a non-empty string when present: {path}"
            )
        if "guidance_fallbacks" in lane and not isinstance(lane.get("guidance_fallbacks"), list):
            raise RuntimeError(f"lane '{lane_id}' guidance_fallbacks must be a list when present: {path}")

    for lane_id, dependents in downstream.items():
        if lane_id not in lane_ids:
            raise RuntimeError(f"lane manifest downstream references unknown source lane '{lane_id}': {path}")
        if not isinstance(dependents, list):
            raise RuntimeError(f"lane manifest downstream for lane '{lane_id}' must be a list: {path}")
        unknown_dependents = [dep for dep in dependents if isinstance(dep, str) and dep not in lane_ids]
        if unknown_dependents:
            raise RuntimeError(
                f"lane manifest downstream for lane '{lane_id}' references unknown lane(s) {unknown_dependents}: {path}"
            )

    if isinstance(heading_to_lane, dict):
        for heading, lane_id in heading_to_lane.items():
            if not isinstance(heading, str) or not heading.strip():
                raise RuntimeError(f"lane manifest heading_to_lane keys must be non-empty strings: {path}")
            if not isinstance(lane_id, str) or lane_id not in lane_ids:
                raise RuntimeError(f"lane manifest heading_to_lane references unknown lane '{lane_id}': {path}")

    if isinstance(plan_routing_hints, list):
        for index, hint in enumerate(plan_routing_hints):
            if not isinstance(hint, dict):
                raise RuntimeError(f"lane manifest plan_routing_hints entries must be objects: {path}")
            lane_id = hint.get("lane")
            if not isinstance(lane_id, str) or lane_id not in lane_ids:
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}] references unknown lane '{lane_id}': {path}"
                )
            heading = hint.get("heading")
            text_prefix = hint.get("text_prefix")
            contains = hint.get("contains")
            if heading is not None and (not isinstance(heading, str) or not heading.strip()):
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}].heading must be a non-empty string when present: {path}"
                )
            if text_prefix is not None and (not isinstance(text_prefix, str) or not text_prefix.strip()):
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}].text_prefix must be a non-empty string when present: {path}"
                )
            if contains is not None and (not isinstance(contains, str) or not contains.strip()):
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}].contains must be a non-empty string when present: {path}"
                )
            if heading is None and text_prefix is None and contains is None:
                raise RuntimeError(
                    f"lane manifest plan_routing_hints[{index}] must define at least one matcher field: {path}"
                )

    return data


def load_manifest(
    task_ref: str,
    *,
    orchestrator_root: str | None = None,
    manifest_dir: str | Path | None = None,
) -> dict[str, Any]:
    path = _resolve_manifest_dir(orchestrator_root=orchestrator_root, manifest_dir=manifest_dir) / f"{task_ref}.json"
    if not path.exists():
        raise FileNotFoundError(f"lane manifest not found for task {task_ref}: {path}")
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise RuntimeError(f"lane manifest must be a JSON object: {path}")
    return validate_manifest(data, path)


def save_manifest(
    manifest: dict[str, Any],
    *,
    orchestrator_root: str | None = None,
    manifest_dir: str | Path | None = None,
) -> Path:
    """Validate and persist a lane manifest under config/lane-orchestration/."""
    resolved_dir = _resolve_manifest_dir(orchestrator_root=orchestrator_root, manifest_dir=manifest_dir)
    task_ref = str(manifest.get("task_ref") or "").strip()
    if not task_ref:
        raise RuntimeError("lane manifest must include task_ref before save")
    path = resolved_dir / f"{task_ref}.json"
    validated = validate_manifest(manifest, path)
    resolved_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(validated, indent=2) + "\n", encoding="utf-8")
    return path


def list_lanes(
    task_ref: str, *, orchestrator_root: str | None = None, manifest_dir: str | Path | None = None
) -> list[str]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root, manifest_dir=manifest_dir)
    lanes = manifest.get("lanes", {})
    if not isinstance(lanes, dict):
        return []
    return sorted(lanes.keys())


def _lane_manifest(
    task_ref: str,
    lane_id: str,
    *,
    orchestrator_root: str | None = None,
    manifest_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    lanes = load_manifest(task_ref, orchestrator_root=orchestrator_root, manifest_dir=manifest_dir).get("lanes", {})
    if not isinstance(lanes, dict):
        return None
    lane = lanes.get(lane_id)
    return lane if isinstance(lane, dict) else None


def expand_path_template(template: str, *, orchestrator_root: str) -> str:
    return template.replace("{orchestrator_root}", orchestrator_root)


def get_lane_config(task_ref: str, lane_id: str, *, orchestrator_root: str | None = None) -> dict[str, Any] | None:
    lane = _lane_manifest(task_ref, lane_id, orchestrator_root=orchestrator_root)
    if lane is None:
        return None
    result = dict(lane)
    owned_paths = [str(item) for item in result.get("owned_paths", []) if str(item).strip()]
    commit_paths = result.get("commit_paths")
    if not isinstance(commit_paths, list) or not commit_paths:
        result["commit_paths"] = _derive_commit_paths(owned_paths)
    result.setdefault("tooling_paths", [])
    result.setdefault("capability_tags", [])
    result.setdefault("preflight_commands", [])
    result.setdefault("guidance_fallbacks", [])
    result.setdefault("preferred_model", None)
    result.setdefault("preferred_backend", None)
    result.setdefault("preferred_reasoning_effort", None)
    result.setdefault("token_burn_threshold", 2_000_000)
    result.setdefault("model_context_window", 128_000)
    if orchestrator_root:
        derived_preflight = _derive_runtime_preflight(result, orchestrator_root=orchestrator_root)
        if not result.get("capability_tags"):
            result["capability_tags"] = derived_preflight["capability_tags"]
        if not result.get("preflight_commands"):
            result["preflight_commands"] = derived_preflight["preflight_commands"]
        if not result.get("preflight_failure_summary") and derived_preflight["preflight_failure_summary"]:
            result["preflight_failure_summary"] = derived_preflight["preflight_failure_summary"]
        if not result.get("preflight_failure_details") and derived_preflight["preflight_failure_details"]:
            result["preflight_failure_details"] = derived_preflight["preflight_failure_details"]
    if orchestrator_root and isinstance(result.get("worktree_path"), str):
        result["worktree_path"] = expand_path_template(result["worktree_path"], orchestrator_root=orchestrator_root)
    return result


def infer_lane_from_branch(branch: str, task_ref: str | None = None, *, orchestrator_root: str | None = None) -> str:
    if not branch:
        return ""

    task_refs = [task_ref] if task_ref else list_task_refs(orchestrator_root=orchestrator_root)
    matches: list[str] = []
    for candidate_task in task_refs:
        if not candidate_task:
            continue
        manifest = load_manifest(candidate_task, orchestrator_root=orchestrator_root)
        lanes = manifest.get("lanes", {})
        if not isinstance(lanes, dict):
            continue
        for lane_id, lane in lanes.items():
            if not isinstance(lane, dict):
                continue
            if lane.get("branch") == branch:
                matches.append(lane_id)
    unique = sorted(set(matches))
    if len(unique) == 1:
        return unique[0]
    return ""


def infer_task_from_branch_or_worktree(
    branch: str,
    *,
    worktree_path: str | None = None,
    orchestrator_root: str | None = None,
) -> str:
    candidates: list[str] = []
    normalized_worktree = str(Path(worktree_path).expanduser().resolve()) if worktree_path else ""

    for candidate_task in list_task_refs(orchestrator_root=orchestrator_root):
        manifest = load_manifest(candidate_task, orchestrator_root=orchestrator_root)
        lanes = manifest.get("lanes", {})
        if not isinstance(lanes, dict):
            continue
        for _lane_id, lane in lanes.items():
            if not isinstance(lane, dict):
                continue
            lane_branch = str(lane.get("branch", "")).strip()
            lane_worktree = str(lane.get("worktree_path", "")).strip()
            if orchestrator_root and lane_worktree:
                lane_worktree = expand_path_template(lane_worktree, orchestrator_root=orchestrator_root)
            lane_worktree_resolved = str(Path(lane_worktree).expanduser().resolve()) if lane_worktree else ""
            if branch and lane_branch == branch:
                candidates.append(candidate_task)
            elif normalized_worktree and lane_worktree_resolved == normalized_worktree:
                candidates.append(candidate_task)

    unique = sorted(set(candidates))
    if len(unique) == 1:
        return unique[0]
    return ""


def route_patterns(task_ref: str, *, orchestrator_root: str | None = None) -> list[tuple[str, str]]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    routes = manifest.get("routing", [])
    lanes = manifest.get("lanes", {})
    if not isinstance(routes, list) or not routes:
        return _derive_routing_from_owned_paths(lanes if isinstance(lanes, dict) else {})
    patterns: list[tuple[str, str]] = []
    for route in routes:
        if not isinstance(route, dict):
            continue
        prefix = route.get("prefix")
        lane = route.get("lane")
        if isinstance(prefix, str) and isinstance(lane, str):
            patterns.append((prefix, lane))
    return patterns


def lane_route_hints(task_ref: str, *, orchestrator_root: str | None = None) -> dict[str, tuple[str, ...]]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    lanes = manifest.get("lanes", {})
    if not isinstance(lanes, dict):
        return {}

    hints: dict[str, tuple[str, ...]] = {}
    for lane_id, lane in lanes.items():
        if not isinstance(lane_id, str) or not isinstance(lane, dict):
            continue
        values: list[str] = [lane_id]
        title = lane.get("title")
        branch = lane.get("branch")
        worktree_path = lane.get("worktree_path")
        route_hints_value = lane.get("route_hints", [])
        if isinstance(title, str) and title.strip():
            values.append(title)
            values.append(title.lower())
        if isinstance(branch, str) and branch.strip():
            values.append(branch)
        if isinstance(worktree_path, str) and worktree_path.strip():
            values.append(worktree_path)
            values.append(Path(worktree_path).name)
        if isinstance(route_hints_value, list):
            values.extend(str(item) for item in route_hints_value if str(item).strip())
        normalized = tuple(dict.fromkeys(value for value in values if value and value.strip()))
        hints[lane_id] = normalized
    return hints


def merge_order(task_ref: str, *, orchestrator_root: str | None = None) -> list[str]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    order = manifest.get("merge_order", [])
    if not isinstance(order, list):
        return []
    return [lane for lane in order if isinstance(lane, str)]


def downstream_lanes(task_ref: str, lane_id: str, *, orchestrator_root: str | None = None) -> list[str]:
    """Return the declared downstream dependents for *lane_id*, or ``[]``."""
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    downstream = manifest.get("downstream", {})
    if not isinstance(downstream, dict):
        return []
    deps = downstream.get(lane_id, [])
    if not isinstance(deps, list):
        return []
    return [d for d in deps if isinstance(d, str)]


def guidance_fallbacks(task_ref: str, lane_id: str, *, orchestrator_root: str | None = None) -> list[dict[str, Any]]:
    lane = _lane_manifest(task_ref, lane_id, orchestrator_root=orchestrator_root)
    if lane is None:
        return []
    fallbacks = lane.get("guidance_fallbacks", [])
    if not isinstance(fallbacks, list):
        return []
    return [row for row in fallbacks if isinstance(row, dict)]


def task_plan_path(task_ref: str, *, orchestrator_root: str | None = None) -> str:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    raw = manifest.get("task_plan_path")
    if not isinstance(raw, str) or not raw.strip():
        return ""
    path = Path(raw)
    if path.is_absolute():
        return str(path)
    base = Path(orchestrator_root) if orchestrator_root else REPO_ROOT
    return str((base / path).expanduser())


def heading_to_lane(task_ref: str, *, orchestrator_root: str | None = None) -> dict[str, str]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    value = manifest.get("heading_to_lane")
    if not isinstance(value, dict):
        return {}
    result: dict[str, str] = {}
    for heading, lane_id in value.items():
        if isinstance(heading, str) and heading.strip() and isinstance(lane_id, str) and lane_id.strip():
            result[heading] = lane_id
    return result


def plan_routing_hints(task_ref: str, *, orchestrator_root: str | None = None) -> list[dict[str, str]]:
    manifest = load_manifest(task_ref, orchestrator_root=orchestrator_root)
    value = manifest.get("plan_routing_hints")
    if not isinstance(value, list):
        return []
    rows: list[dict[str, str]] = []
    for hint in value:
        if not isinstance(hint, dict):
            continue
        row: dict[str, str] = {}
        for key in ("heading", "text_prefix", "contains", "lane"):
            cell = hint.get(key)
            if isinstance(cell, str) and cell.strip():
                row[key] = cell
        if "lane" in row:
            rows.append(row)
    return rows
