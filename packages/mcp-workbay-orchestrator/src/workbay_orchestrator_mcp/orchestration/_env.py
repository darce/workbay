from __future__ import annotations

import logging
import os
import re
from pathlib import Path

PYENV_VERSION_PATTERN = re.compile(r"\bPYENV_VERSION=([A-Za-z0-9._-]+)")
CODEX_REASONING_EFFORTS = ("low", "medium", "high", "xhigh")
EFFORT_LADDER = ("low", "medium", "high", "xhigh")
WORKER_REASONING_EFFORT_CHOICES = ("inherit", "auto", *CODEX_REASONING_EFFORTS)
logger = logging.getLogger(__name__)


def _escalate_effort(current: str) -> str | None:
    """Return the next higher effort level, or None if already at the ceiling."""
    normalized = str(current or "").strip().lower()
    try:
        idx = EFFORT_LADDER.index(normalized)
    except ValueError:
        return None
    if idx + 1 >= len(EFFORT_LADDER):
        return None
    return EFFORT_LADDER[idx + 1]


def extract_pyenv_version(commands: list[str]) -> str | None:
    for command in commands:
        match = PYENV_VERSION_PATTERN.search(str(command))
        if match:
            return match.group(1)
    return None


def _lane_runtime_profile(
    orchestrator_root: Path,
    *,
    task_ref: str | None,
    lane_id: str | None,
) -> tuple[str | None, list[str]]:
    if not task_ref or not lane_id:
        return None, []
    try:
        from workbay_orchestrator_mcp.orchestration.lane_manifest import get_lane_config

        lane = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root))
    except (ImportError, FileNotFoundError, KeyError, ValueError) as exc:
        logger.warning("_lane_runtime_profile: could not load lane config: %s", exc)
        return None, []
    if not isinstance(lane, dict):
        return None, []

    pyenv_version = extract_pyenv_version([str(command) for command in lane.get("test_commands", [])])

    extra_paths: list[str] = []
    if pyenv_version:
        pyenv_root = Path.home() / ".pyenv"
        venv_bin = pyenv_root / "versions" / pyenv_version / "bin"
        if venv_bin.exists():
            extra_paths.append(str(venv_bin))
        for candidate in (pyenv_root / "shims", pyenv_root / "bin"):
            if candidate.exists():
                extra_paths.append(str(candidate))
    return pyenv_version, extra_paths


def pythonpath_env(
    orchestrator_root: Path,
    *,
    task_ref: str | None = None,
    lane_id: str | None = None,
) -> dict[str, str]:
    """Return an env dict with local bridge paths, writable temp, and lane runtime hints."""
    env = os.environ.copy()
    pythonpath_parts = [
        str(orchestrator_root / "packages" / "workbay-codex-bridge" / "src"),
    ]
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = ":".join([*pythonpath_parts, existing]) if existing else ":".join(pythonpath_parts)

    temp_root = orchestrator_root / ".task-state" / "tmp"
    if lane_id:
        temp_root = temp_root / lane_id
    temp_root.mkdir(parents=True, exist_ok=True)
    temp_text = str(temp_root)
    env["TMPDIR"] = temp_text
    env["TMP"] = temp_text
    env["TEMP"] = temp_text

    pyenv_version, extra_paths = _lane_runtime_profile(
        orchestrator_root,
        task_ref=task_ref,
        lane_id=lane_id,
    )
    if pyenv_version:
        env["PYENV_VERSION"] = pyenv_version
    if extra_paths:
        current_path = env.get("PATH", "")
        env["PATH"] = ":".join([*extra_paths, current_path]) if current_path else ":".join(extra_paths)
    return env


# Backend family detection helpers used by apply_backend_runtime_hints.
_CODEX_BACKENDS = frozenset({"codex-cli", "codex-subagent"})
_CLAUDE_BACKENDS = frozenset({"claude-code"})
_CODEX_MODEL_PREFIXES = ("CODEX", "GPT", "O1", "O3", "O4")
_CLAUDE_MODEL_PREFIXES = ("CLAUDE", "ANTHROPIC")


def _is_codex_backend(backend: str | None, model: str | None) -> bool:
    if backend in _CODEX_BACKENDS:
        return True
    if not backend and model:
        upper = str(model).upper()
        return any(prefix in upper for prefix in _CODEX_MODEL_PREFIXES)
    return False


def _is_claude_backend(backend: str | None, model: str | None) -> bool:
    if backend in _CLAUDE_BACKENDS:
        return True
    if not backend and model:
        upper = str(model).upper()
        return any(prefix in upper for prefix in _CLAUDE_MODEL_PREFIXES)
    return False


def apply_backend_runtime_hints(
    env: dict[str, str],
    *,
    reasoning_effort: str | None = None,
    model: str | None = None,
    session_mode: str | None = None,
    backend: str | None = None,
) -> dict[str, str]:
    """Apply backend-specific runtime hints to an existing environment mapping."""
    is_codex = _is_codex_backend(backend, model)
    is_claude = _is_claude_backend(backend, model)

    if model and is_codex:
        env.setdefault("CODEX_MODEL", model)
    if model and is_claude:
        env.setdefault("ANTHROPIC_MODEL", model)

    normalized_effort = str(reasoning_effort or "").strip().lower()
    if normalized_effort in CODEX_REASONING_EFFORTS and (is_codex or is_claude):
        if is_codex:
            env["CODEX_REASONING_EFFORT"] = normalized_effort
        if is_claude:
            env["ANTHROPIC_REASONING_EFFORT"] = normalized_effort

    normalized_session_mode = str(session_mode or "").strip().lower()
    if normalized_session_mode == "shared_lane" and is_codex:
        env["CODEX_SUBAGENT_BRIDGE_SESSION_MODE"] = "shared"
    elif normalized_session_mode == "fresh_turn":
        env.pop("CODEX_SUBAGENT_BRIDGE_SESSION_MODE", None)
    return env


# ---------------------------------------------------------------------------
# Shared auto-reasoning effort resolution
# ---------------------------------------------------------------------------

# Markers matched against owned_paths and test_commands only (NOT objectives,
# which can contain repo-specific domain terms that leak across all lanes).
_AUTO_HIGH_PATH_MARKERS = (
    "mcp-workbay-handoff",
    "db/",
    "migrations",
    "scripts/mcp",
)

# Markers matched against lane_id (structural identity of the lane).
_AUTO_HIGH_LANE_ID_MARKERS = (
    "backend",
    "api",
    "infra",
)

_AUTO_MEDIUM_PATH_MARKERS = (
    "api/",
    "composer phpunit",
    "controller",
    "js/admin",
    "npm run test",
    "phpunit",
    "src/",
    "vitest",
)

_AUTO_MEDIUM_LANE_ID_MARKERS = (
    "frontend",
    "ui",
    "proxy",
)


def resolve_auto_reasoning_effort(
    *,
    orchestrator_root: Path,
    task_ref: str,
    lane_id: str,
    requested: str,
    cycle: int,
    prompt_override: str | None,
    previous_run_exhausted: bool = False,
) -> tuple[str | None, list[str]]:
    """Shared reasoning-effort resolver used by all backend adapters.

    Priority: explicit override > manifest preferred_reasoning_effort > auto scoring.
    """
    normalized = str(requested or "inherit").strip().lower()
    if normalized in {"inherit", ""}:
        return None, ["inherit existing Codex/default reasoning effort"]
    if normalized != "auto":
        return normalized, [f"explicit override: {normalized}"]

    # Load lane manifest
    try:
        from lane_manifest import get_lane_config

        lane = get_lane_config(task_ref, lane_id, orchestrator_root=str(orchestrator_root)) or {}
    except Exception:
        lane = {}

    # Check manifest-level preference first
    manifest_effort = str(lane.get("preferred_reasoning_effort") or "").strip().lower()
    if manifest_effort in CODEX_REASONING_EFFORTS:
        return manifest_effort, [f"manifest preferred_reasoning_effort: {manifest_effort}"]

    # Auto-scoring based on lane structure (not domain objectives)
    score = 0
    reasons: list[str] = []
    if cycle > 0:
        score += 2
        reasons.append("follow-up review/fix cycle")
    if prompt_override:
        score += 2
        reasons.append("fix prompt active")

    lid = lane_id.lower()
    owned_paths = [str(item).strip().lower() for item in lane.get("owned_paths", []) if str(item).strip()]
    test_commands = [str(item).strip().lower() for item in lane.get("test_commands", []) if str(item).strip()]
    path_haystack = "\n".join([*owned_paths, *test_commands])

    if any(marker in lid for marker in _AUTO_HIGH_LANE_ID_MARKERS):
        score += 2
        reasons.append("backend/infra lane id")
    elif any(marker in path_haystack for marker in _AUTO_HIGH_PATH_MARKERS):
        score += 2
        reasons.append("backend/infra owned paths")
    elif any(marker in lid for marker in _AUTO_MEDIUM_LANE_ID_MARKERS):
        score += 1
        reasons.append("application-layer lane id")
    elif any(marker in path_haystack for marker in _AUTO_MEDIUM_PATH_MARKERS):
        score += 1
        reasons.append("application-layer owned paths")

    docs_only = bool(owned_paths) and all(path.startswith("docs/") for path in owned_paths)
    if docs_only:
        score -= 1
        reasons.append("docs-only scope")

    if score >= 2:
        effort = "high"
    elif score <= 0:
        effort = "low"
    else:
        effort = "medium"

    if previous_run_exhausted:
        escalated = _escalate_effort(effort)
        if escalated is not None:
            reasons.append(f"escalated after exhaustion: {effort} -> {escalated}")
            effort = escalated

    return effort, reasons or [f"auto-selected {effort}"]
