"""Effort default resolution for both lane and wave dispatch."""

from workbay_protocol.reasoning_effort import validate_reasoning_effort

from workbay_orchestrator_mcp.orchestration.backend_registry import BACKENDS
from workbay_orchestrator_mcp.orchestration.openrouter_lane_config import OPENROUTER_MODEL_ALLOWED_EFFORTS

# Models whose omitted effort must resolve to ``max`` rather than the shared
# ``high`` default. Luna is a reasoning-first model: run below max and it
# degrades quietly, producing lane output that reads plausible and has to be
# rescued or re-run later. The operator directive is that every Luna lane runs
# at max, and a default that silently disagrees is the failure mode -- the
# dispatch succeeds, so nothing surfaces until the work is already wrong.
#
# Omission resolves to max, and concrete lower efforts are refused by
# resolve_dispatch_effort below. Selection modes (auto/inherit) remain
# execution-time choices and are not concrete dispatch efforts.
MAX_EFFORT_MODELS = frozenset({"gpt-5.6-luna", "gpt-6-luna"})

DEFAULT_EFFORT = "high"
_MODEL_EFFORT_CAPABILITY_TABLES = (OPENROUTER_MODEL_ALLOWED_EFFORTS,)


class LunaEffortPolicyError(ValueError):
    """Raised when a max-effort model is given a concrete lower effort."""


def _normalize_model(model: str | None) -> str:
    return str(model or "").strip().lower()


def default_effort_for_model(backend_id: str, model: str) -> str | None:
    """Preserve backend defaults; never substitute an explicit effort."""
    backend = BACKENDS.get(backend_id)
    selected_model = model or (backend.allowed_model if backend is not None else None)

    if backend is not None and backend.model_tiers is not None:
        # A curated tier row owns this model's default. An unentitled or
        # uncurated slug yields None instead of fabricating an override.
        row = backend.model_tiers.get(selected_model)
        return row.default_effort if row is not None and row.entitled else None

    if backend is not None and backend.allowed_models is not None:
        # Per-model advertised effort sets are capability metadata. A matching
        # allow-list selects the relevant metadata table without keying policy
        # on the backend's name. Empty/missing effort metadata means omit the
        # override; only an advertised shared default may be supplied.
        for capabilities in _MODEL_EFFORT_CAPABILITY_TABLES:
            if set(capabilities).intersection(backend.allowed_models):
                advertised = capabilities.get(selected_model)
                return DEFAULT_EFFORT if advertised and DEFAULT_EFFORT in advertised else None

    if _normalize_model(model) in MAX_EFFORT_MODELS:
        return "max"
    return DEFAULT_EFFORT


def resolve_dispatch_effort(backend_id: str, model: str, effort: str | None) -> str | None:
    """Resolve omitted effort and enforce max-effort model policy."""
    validated = validate_reasoning_effort(effort)
    if _normalize_model(model) in MAX_EFFORT_MODELS and validated in {"low", "medium", "high", "xhigh"}:
        raise LunaEffortPolicyError(
            f"model {model!r} requires effort 'max'; explicit effort {validated!r} is refused [OBS-08]"
        )
    return default_effort_for_model(backend_id, model) if validated is None else validated
