"""workbay-protocol: typed cross-repo contracts for the WorkBay system.

Pydantic v2 models that consumer packages (mcp-workbay-handoff,
mcp-workbay-orchestrator, workbay-bootstrap, workbay-system) import to
guarantee wire-level compatibility across out-of-process boundaries.
"""

from __future__ import annotations

from . import branch_naming as branch_naming  # re-exported submodule
from . import cli_contract as cli_contract  # re-exported submodule
from . import review_impact as review_impact  # re-exported submodule
from .bootstrap import BootstrapManifest, OverlayConfigEntry, OverlaySurface
from .branch_naming import (
    TASK_REF_RE,
    derive_task_ref_candidates,
    extract_plan_id,
    format_suggested_branch_name,
)
from .brand import (
    BRAND_NAME,
    BRAND_SLUG,
    ORG,
    REPO,
    REPO_HTTPS_URL,
    REPO_URL,
)
from .cli_contract import (
    POLICY_KINDS,
    UNAVAILABLE_KINDS,
    ExitCode,
    JsonArgumentParser,
    emit_json,
    exit_code_for,
    run_json_command,
)
from .compaction import DecisionRef, StructuredSummary, TurnRange
from .convergence import (
    CandidateControlState,
    CandidateDisposition,
    CandidateEvent,
    LaneContextPacket,
    MergeCapability,
    ReviewAttemptOutcomeV2,
    ShipCleanupPostcondition,
    ShipStage,
    TransitionAdmission,
    TransitionRefusedError,
    WorkerOutcomeV2,
    admit_candidate_transition,
)
from .env_aliases import resolve_env_alias
from .handoff import (
    ActiveTask,
    HandoffState,
    HandoffStatus,
    TargetWorktree,
    TaskPlanRef,
    TaskPlanResolution,
    TaskRef,
)
from .hooks import (
    PostToolUseEvent,
    PreToolUseEvent,
    SessionStartEvent,
    StopEvent,
    UserPromptSubmitEvent,
)
from .paths import (
    BOOTSTRAP_MANIFEST_NAME,
    CONTRACTS_DIR,
    DOCS_MIRROR_DIR,
    HARNESS_CONTRACT_RELPATH,
    INSTRUCTIONS_RELPATH,
    LEGACY_AGENTIC_OVERLAY_MANIFEST_NAME,
    LEGACY_WORKBAY_OVERLAY_MANIFEST_NAME,
    MANIFEST_NAME_PRECEDENCE,
    RULES_DIR,
    RUNTIME_ROOT_DIRNAME,
    TEMPLATES_DIR,
    docs_mirror_path,
    runtime_root_path,
)
from .skills import SkillManifest, SkillScope
from .version import version_of

__version__ = version_of("workbay-protocol", anchor=__file__)

__all__ = [
    "ActiveTask",
    "BOOTSTRAP_MANIFEST_NAME",
    "BRAND_NAME",
    "BRAND_SLUG",
    "BootstrapManifest",
    "CONTRACTS_DIR",
    "CandidateControlState",
    "CandidateDisposition",
    "CandidateEvent",
    "DOCS_MIRROR_DIR",
    "DecisionRef",
    "ExitCode",
    "HARNESS_CONTRACT_RELPATH",
    "HandoffState",
    "HandoffStatus",
    "INSTRUCTIONS_RELPATH",
    "JsonArgumentParser",
    "LaneContextPacket",
    "LEGACY_AGENTIC_OVERLAY_MANIFEST_NAME",
    "LEGACY_WORKBAY_OVERLAY_MANIFEST_NAME",
    "MANIFEST_NAME_PRECEDENCE",
    "MergeCapability",
    "ORG",
    "OverlayConfigEntry",
    "OverlaySurface",
    "POLICY_KINDS",
    "PostToolUseEvent",
    "PreToolUseEvent",
    "REPO",
    "REPO_HTTPS_URL",
    "REPO_URL",
    "ReviewAttemptOutcomeV2",
    "RULES_DIR",
    "TEMPLATES_DIR",
    "RUNTIME_ROOT_DIRNAME",
    "SessionStartEvent",
    "ShipCleanupPostcondition",
    "ShipStage",
    "SkillManifest",
    "SkillScope",
    "StopEvent",
    "StructuredSummary",
    "TASK_REF_RE",
    "TargetWorktree",
    "TaskPlanRef",
    "TaskPlanResolution",
    "TaskRef",
    "TransitionAdmission",
    "TransitionRefusedError",
    "TurnRange",
    "UNAVAILABLE_KINDS",
    "UserPromptSubmitEvent",
    "WorkerOutcomeV2",
    "__version__",
    "admit_candidate_transition",
    "branch_naming",
    "cli_contract",
    "derive_task_ref_candidates",
    "docs_mirror_path",
    "emit_json",
    "exit_code_for",
    "extract_plan_id",
    "format_suggested_branch_name",
    "resolve_env_alias",
    "review_impact",
    "run_json_command",
    "runtime_root_path",
]
