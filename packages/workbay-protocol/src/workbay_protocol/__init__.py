"""workbay-protocol: typed cross-repo contracts for the WorkBay system.

Pydantic v2 models that consumer packages (mcp-workbay-handoff,
mcp-workbay-orchestrator, workbay-bootstrap, workbay-system) import to
guarantee wire-level compatibility across out-of-process boundaries.
"""

from __future__ import annotations

from . import branch_naming as branch_naming  # re-exported submodule
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
    PYPI_URL,
    REPO,
    REPO_HTTPS_URL,
    REPO_URL,
)
from .compaction import DecisionRef, StructuredSummary, TurnRange
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
    docs_mirror_path,
    runtime_root_path,
)
from .skills import SkillManifest, SkillScope

__version__ = "0.2.0"

__all__ = [
    "ActiveTask",
    "BOOTSTRAP_MANIFEST_NAME",
    "BRAND_NAME",
    "BRAND_SLUG",
    "BootstrapManifest",
    "CONTRACTS_DIR",
    "DOCS_MIRROR_DIR",
    "DecisionRef",
    "HARNESS_CONTRACT_RELPATH",
    "HandoffState",
    "HandoffStatus",
    "INSTRUCTIONS_RELPATH",
    "LEGACY_AGENTIC_OVERLAY_MANIFEST_NAME",
    "LEGACY_WORKBAY_OVERLAY_MANIFEST_NAME",
    "MANIFEST_NAME_PRECEDENCE",
    "ORG",
    "OverlayConfigEntry",
    "OverlaySurface",
    "PYPI_URL",
    "PostToolUseEvent",
    "PreToolUseEvent",
    "REPO",
    "REPO_HTTPS_URL",
    "REPO_URL",
    "RULES_DIR",
    "RUNTIME_ROOT_DIRNAME",
    "SessionStartEvent",
    "SkillManifest",
    "SkillScope",
    "StopEvent",
    "StructuredSummary",
    "TASK_REF_RE",
    "TargetWorktree",
    "TaskPlanRef",
    "TaskPlanResolution",
    "TaskRef",
    "TurnRange",
    "UserPromptSubmitEvent",
    "__version__",
    "branch_naming",
    "derive_task_ref_candidates",
    "docs_mirror_path",
    "extract_plan_id",
    "format_suggested_branch_name",
    "resolve_env_alias",
    "runtime_root_path",
]
