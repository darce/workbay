"""Regenerate JSON Schema artifacts from Pydantic models.

Run from the package root:
    python scripts/generate_schemas.py

Outputs ``schemas/*.json``. Commit the regenerated files alongside model
changes so non-Python consumers stay in sync without installing the
package.
"""

from __future__ import annotations

import json
from pathlib import Path

from workbay_protocol.bootstrap import (
    BootstrapManifest,
    PluginEffectiveLock,
    PluginMcpServerPatch,
    PluginOverrideLock,
    PluginOverrideManifest,
)
from workbay_protocol.compaction import StructuredSummary
from workbay_protocol.handoff import ActiveTask, HandoffState, TaskPlanRef
from workbay_protocol.hooks import (
    PostToolUseEvent,
    PreToolUseEvent,
    SessionStartEvent,
    StopEvent,
    UserPromptSubmitEvent,
)
from workbay_protocol.skills import SkillManifest


def main() -> None:
    out_dir = Path(__file__).resolve().parent.parent / "schemas"
    out_dir.mkdir(exist_ok=True)
    artifacts: dict[str, type] = {
        "handoff-state": HandoffState,
        "active-task": ActiveTask,
        "task-plan-ref": TaskPlanRef,
        "compaction-summary": StructuredSummary,
        "skill-manifest": SkillManifest,
        "bootstrap-manifest": BootstrapManifest,
        "plugin-override-manifest": PluginOverrideManifest,
        "plugin-override-lock": PluginOverrideLock,
        "plugin-effective-lock": PluginEffectiveLock,
        "plugin-mcp-server-patch": PluginMcpServerPatch,
        "hook-session-start": SessionStartEvent,
        "hook-user-prompt-submit": UserPromptSubmitEvent,
        "hook-pre-tool-use": PreToolUseEvent,
        "hook-post-tool-use": PostToolUseEvent,
        "hook-stop": StopEvent,
    }
    for name, model in artifacts.items():
        schema = model.model_json_schema()
        path = out_dir / f"{name}.json"
        path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")
        print(f"wrote {path.relative_to(out_dir.parent)}")


if __name__ == "__main__":
    main()
