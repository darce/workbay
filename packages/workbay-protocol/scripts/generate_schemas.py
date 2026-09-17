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
from typing import Any

from workbay_protocol.bootstrap import (
    BootstrapManifest,
    PluginEffectiveLock,
    PluginMcpServerPatch,
    PluginOverrideLock,
    PluginOverrideManifest,
)
from workbay_protocol.compaction import StructuredSummary
from workbay_protocol.convergence import (
    CandidateDisposition,
    LaneContextPacket,
    MergeCapability,
    ReviewAttemptOutcomeV2,
    ShipCleanupPostcondition,
    WorkerOutcomeV2,
)
from workbay_protocol.handoff import ActiveTask, HandoffState, TaskPlanRef
from workbay_protocol.hooks import (
    PostToolUseEvent,
    PreToolUseEvent,
    SessionStartEvent,
    StopEvent,
    UserPromptSubmitEvent,
)
from workbay_protocol.skills import SkillManifest

JSON_SCHEMA_DIALECT_2020_12 = "https://json-schema.org/draft/2020-12/schema"

SCHEMA_ARTIFACTS: dict[str, type] = {
    "handoff-state": HandoffState,
    "active-task": ActiveTask,
    "task-plan-ref": TaskPlanRef,
    "compaction-summary": StructuredSummary,
    "worker-outcome-v2": WorkerOutcomeV2,
    "review-attempt-outcome-v2": ReviewAttemptOutcomeV2,
    "candidate-disposition": CandidateDisposition,
    "merge-capability": MergeCapability,
    "lane-context-packet": LaneContextPacket,
    "ship-cleanup-postcondition": ShipCleanupPostcondition,
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


def apply_json_schema_dialect(schema: dict[str, Any]) -> dict[str, Any]:
    """Declare Draft 2020-12 on a generated top-level artifact."""
    artifact = dict(schema)
    artifact["$schema"] = JSON_SCHEMA_DIALECT_2020_12
    return artifact


def schema_artifact(model: type) -> dict[str, Any]:
    # Must-hold model_validators live on the models as json_schema_extra
    # if/then clauses so this dump stays structural for non-Python consumers.
    return apply_json_schema_dialect(model.model_json_schema())


def main() -> None:
    out_dir = Path(__file__).resolve().parent.parent / "schemas"
    out_dir.mkdir(exist_ok=True)
    for name, model in SCHEMA_ARTIFACTS.items():
        path = out_dir / f"{name}.json"
        path.write_text(
            json.dumps(schema_artifact(model), indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {path.relative_to(out_dir.parent)}")


if __name__ == "__main__":
    main()
