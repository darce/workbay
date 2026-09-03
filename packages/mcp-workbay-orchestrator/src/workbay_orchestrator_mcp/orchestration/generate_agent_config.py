#!/usr/bin/env python3
"""Generate agent configuration from lane manifest.

Usage:
    python3 scripts/mcp/generate_agent_config.py --task-ref 7.0/my-task --lane-id frontend
"""

import argparse
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

try:
    from lane_manifest import get_lane_config
except ImportError:
    print("Error: Could not import lane_manifest. Run from repo root or ensure scripts/mcp is in PYTHONPATH.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Generate agent config from lane manifest.")
    parser.add_argument(
        "--orchestrator-root",
        default=".",
        help="Orchestrator workspace root used to resolve config/lane-orchestration manifests.",
    )
    parser.add_argument("--task-ref", required=True, help="Task reference (e.g. 7.0/my-task)")
    parser.add_argument("--lane-id", required=True, help="Lane ID (e.g. frontend)")
    parser.add_argument("--output", type=Path, help="Optional output path (e.g. .agent/config.json)")
    args = parser.parse_args()

    orchestrator_root = Path(args.orchestrator_root).expanduser().resolve()
    cfg = get_lane_config(args.task_ref, args.lane_id, orchestrator_root=str(orchestrator_root))
    if not cfg:
        print(f"Error: Lane '{args.lane_id}' not found in manifest for task '{args.task_ref}'.")
        sys.exit(1)

    # Generate the agent config
    agent_config = {
        "backend": cfg.get("preferred_backend") or "codex-subagent",
        "model": cfg.get("preferred_model"),
        "reasoning_effort": cfg.get("reasoning_effort") or "auto",
        "task_ref": args.task_ref,
        "lane_id": args.lane_id,
        "metadata": {
            "title": cfg.get("title"),
            "objective": cfg.get("objective"),
            "branch": cfg.get("branch"),
        },
    }

    output_json = json.dumps(agent_config, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output_json)
        print(f"Config successfully generated at {args.output}")
    else:
        print(output_json)


if __name__ == "__main__":
    main()
