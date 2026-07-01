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

from lane_manifest import get_lane_config


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
) -> int:
    lane_cfg = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    if not lane_cfg:
        print(f"No lane config found for {task_ref} / {lane_id}")
        return 1

    if "owned_paths" not in lane_cfg:
        print(f"Warning: 'owned_paths' not found in lane config for {lane_id}.")

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
    args = parser.parse_args()

    return _bootstrap(
        orchestrator_root=args.orchestrator_root,
        task_ref=args.task_ref,
        lane_id=args.lane_id,
        worktree_path=args.worktree_path,
    )


if __name__ == "__main__":
    sys.exit(main())
