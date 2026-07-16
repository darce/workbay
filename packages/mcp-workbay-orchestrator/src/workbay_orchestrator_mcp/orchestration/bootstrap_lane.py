#!/usr/bin/env python3
"""Bootstrap lane dependencies: link or install composer/npm for worktree lanes."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from grok_lane_config import DEFAULT_GROK_MODEL, materialize_grok_lane_config
from lane_manifest import get_lane_config

# implementation note S3 [OBS-08]: named missing-manifest cause (never bare exit-code-1).
# Callers that swallow stdout still surface this string via RuntimeError.
MISSING_LANE_MANIFEST_HINT = "run materialize_offload_lane_manifest"


def format_missing_lane_manifest_error(task_ref: str, lane_id: str) -> str:
    """Typed missing-manifest message for bootstrap + dispatch/pass results."""
    return f"no manifest for {lane_id}; {MISSING_LANE_MANIFEST_HINT} (No lane config found for {task_ref} / {lane_id})"


def _default_grok_model() -> str:
    """The single canonical default Composer slug.

    Imported from the grok config module so the config-env attribution identity
    and the adapter's argv/prompt identity cannot drift out of one source
    (harm-002).
    """
    return DEFAULT_GROK_MODEL


def _grok_default_agent(model: str) -> str:
    """Canonical Composer identity for WORKBAY_HANDOFF_DEFAULT_AGENT.

    Derived from the lane's EFFECTIVE model (not a hardcoded default) so the
    config-env attribution matches the adapter's prompt-suffix identity even
    under a dispatch-time model override — both normalize the same slug via the
    S1 enum, so the two identities stay harmonized (s5-a-006 / s6-a-003). No
    hand-written label literal (that was the drift risk in harm-002).
    """
    try:
        from workbay_handoff_mcp.enums import normalize_model_identity, normalize_model_label

        label = normalize_model_label(model)
        return normalize_model_identity(label, None) or label or model
    except Exception:
        return model


def _grok_mcp_servers(worktree_root: Path) -> list[dict[str, object]]:
    """The two WorkBay MCP servers to register in the lane-scoped grok config.

    Command/args mirror the console-script launch (the consumer's live oracle
    confirms the exact form); the load-bearing guarantee is the attribution env
    injected by ``build_grok_lane_config``, not the launch string.
    """
    root = str(worktree_root)
    return [
        {
            "name": "workbay-handoff-mcp",
            "command": "mcp-workbay-handoff",
            "args": ["--workspace-root", root, "serve-stdio"],
        },
        {
            "name": "workbay-orchestrator-mcp",
            "command": "mcp-workbay-orchestrator",
            "args": ["--workspace-root", root, "serve-stdio"],
        },
    ]


def ensure_grok_lane_config(worktree_path: str | Path, model: str | None = None) -> None:
    effective_model = model or _default_grok_model()
    worktree_root = Path(worktree_path).resolve()
    materialize_grok_lane_config(
        worktree_root,
        model=effective_model,
        fork_secondary_model=effective_model,
        default_agent=_grok_default_agent(effective_model),
        servers=_grok_mcp_servers(worktree_root),
    )


_materialize_grok_lane_config = ensure_grok_lane_config


def _normalize_owned_path(path: str) -> str:
    value = str(path).strip()
    if not value:
        return ""
    while value.endswith("/**") or value.endswith("/*"):
        value = value.rsplit("/", 1)[0]
    return value.rstrip("/")


def _candidate_app_roots(
    lane_cfg: dict[str, object],
    *,
    worktree_path: str | Path,
) -> list[Path]:
    worktree_root = Path(worktree_path).resolve()
    candidates: list[Path] = []

    def add_candidate(relative_path: str) -> None:
        normalized = _normalize_owned_path(relative_path)
        if not normalized:
            return
        full_path = (worktree_root / normalized).resolve()
        probe = full_path if full_path.is_dir() else full_path.parent
        if probe == worktree_root.parent and not probe.exists():
            return

        search_path = probe
        while search_path != worktree_root and worktree_root not in search_path.parents:
            search_path = search_path.parent

        for candidate in [search_path, *search_path.parents]:
            if candidate == worktree_root.parent:
                break
            if candidate == worktree_root or worktree_root in candidate.parents:
                if (candidate / "composer.json").is_file() or (candidate / "package.json").is_file():
                    candidates.append(candidate)

    owned_paths = lane_cfg.get("owned_paths")
    for relative_path in owned_paths if isinstance(owned_paths, list) else []:
        add_candidate(str(relative_path))

    app_root = lane_cfg.get("app_root")
    if isinstance(app_root, str) and app_root.strip():
        add_candidate(app_root)

    tooling_paths = lane_cfg.get("tooling_paths")
    if isinstance(tooling_paths, list):
        for tooling_path in tooling_paths:
            tooling_str = str(tooling_path).strip()
            if tooling_str:
                add_candidate(str(Path(tooling_str).parent))

    unique_candidates: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate not in seen:
            unique_candidates.append(candidate)
            seen.add(candidate)
    return unique_candidates


def _bootstrap(
    orchestrator_root: str | Path,
    task_ref: str,
    lane_id: str,
    worktree_path: str | Path,
    backend: str | None = None,
    model: str | None = None,
) -> int:
    try:
        lane_cfg = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    except FileNotFoundError:
        # Whole-task manifest file missing — same operator fix as a missing lane entry.
        print(format_missing_lane_manifest_error(task_ref, lane_id))
        return 1
    if not lane_cfg:
        # implementation note S3 [OBS-08]: named cause (stdout may still be swallowed by callers).
        print(format_missing_lane_manifest_error(task_ref, lane_id))
        return 1

    if "owned_paths" not in lane_cfg:
        print(f"Warning: 'owned_paths' not found in lane config for {lane_id}.")

    # D3/D4: materialize the worktree-scoped grok config BEFORE the app-root
    # early return. A python-only grok lane has no composer/npm app root, so a
    # materialization placed inside/after the app-root loop would silently never
    # run for exactly the lanes that need the Composer-only guarantee.
    if backend == "grok-cli":
        ensure_grok_lane_config(worktree_path, model)

    app_roots = _candidate_app_roots(lane_cfg, worktree_path=worktree_path)
    if not app_roots:
        return 0

    worktree_root = Path(worktree_path).resolve()

    for full_path in app_roots:
        relative_path = str(full_path.relative_to(worktree_root))

        # Composer Bootstrap
        if (full_path / "composer.json").is_file():
            print(f"Bootstrapping Composer dependencies in {relative_path}...")
            root_vendor = Path(orchestrator_root) / relative_path / "vendor"
            lane_vendor = full_path / "vendor"
            vendor_points_outside_lane = lane_vendor.is_symlink() and full_path not in lane_vendor.resolve().parents
            if vendor_points_outside_lane:
                print(f"  Replacing shared vendor symlink with lane-local vendor: {lane_vendor.resolve()}")
                lane_vendor.unlink()

            if not lane_vendor.exists() and root_vendor.is_dir():
                print(f"  Copying vendor from orchestrator root: {root_vendor}")
                shutil.copytree(root_vendor, lane_vendor)
            elif not lane_vendor.exists():
                print(f"  Running composer install in {relative_path}")
                result = subprocess.run(
                    ["composer", "install", "--no-interaction", "--no-progress"],
                    cwd=full_path,
                )
                if result.returncode != 0:
                    print(f"Error: composer install failed with code {result.returncode}")
                    return result.returncode
            else:
                print(f"  Vendor already present in {relative_path}")

        # JS/Node Bootstrap
        if (full_path / "package.json").is_file():
            print(f"Bootstrapping NPM dependencies in {relative_path}...")
            root_nm = Path(orchestrator_root) / relative_path / "node_modules"
            lane_nm = full_path / "node_modules"
            node_modules_points_outside_lane = lane_nm.is_symlink() and full_path not in lane_nm.resolve().parents
            if node_modules_points_outside_lane:
                print(f"  Replacing shared node_modules symlink with lane-local node_modules: {lane_nm.resolve()}")
                lane_nm.unlink()

            if not lane_nm.exists() and root_nm.is_dir():
                print(f"  Copying node_modules from orchestrator root: {root_nm}")
                shutil.copytree(root_nm, lane_nm)
            elif not lane_nm.exists():
                print(f"  Running npm install in {relative_path}")
                result = subprocess.run(
                    ["npm", "install", "--no-audit", "--no-fund"],
                    cwd=full_path,
                )
                if result.returncode != 0:
                    print(f"Error: npm install failed with code {result.returncode}")
                    return result.returncode
            else:
                print(f"  Node modules already present in {relative_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser("Bootstrap lane dependencies")
    parser.add_argument("--orchestrator-root", required=True)
    parser.add_argument("--task-ref", required=True)
    parser.add_argument("--lane-id", required=True)
    parser.add_argument("--worktree-path", required=True)
    parser.add_argument("--backend", default=None)
    parser.add_argument(
        "--model", default=None, help="Effective lane model (drives grok Composer attribution identity)."
    )
    args = parser.parse_args()

    return _bootstrap(
        orchestrator_root=args.orchestrator_root,
        task_ref=args.task_ref,
        lane_id=args.lane_id,
        worktree_path=args.worktree_path,
        backend=args.backend,
        model=args.model,
    )


if __name__ == "__main__":
    sys.exit(main())
