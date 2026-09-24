"""Effort default resolution for both lane and wave dispatch."""

from workbay_protocol.reasoning_effort import validate_reasoning_effort

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


class LunaEffortPolicyError(ValueError):
    """Raised when a max-effort model is given a concrete lower effort."""


def _normalize_model(model: str | None) -> str:
    return str(model or "").strip().lower()


def default_effort_for_model(backend_id: str, model: str) -> str | None:
    """Preserve backend defaults; never substitute an explicit effort."""
    if _normalize_model(model) in MAX_EFFORT_MODELS:
        return "max"
    if backend_id == "codex-remote":
        # Codex defaults come from the curated tier row, not the shared
        # ``high``: a blanket default here is a second writer of a value the
        # tier table already owns, and the two disagree (senior/sol is xhigh).
        # An unentitled or uncurated slug yields None so argv omits the
        # override rather than silently picking a tier nobody chose.
        from .codex_lane_config import CODEX_MODEL_TIERS, DEFAULT_CODEX_MODEL

        row = CODEX_MODEL_TIERS.get(model or DEFAULT_CODEX_MODEL)
        return row.default_effort if row is not None and row.entitled else None
    if backend_id == "openrouter-remote":
        from .openrouter_lane_config import DEFAULT_OPENROUTER_MODEL, OPENROUTER_MODEL_ALLOWED_EFFORTS

        advertised = OPENROUTER_MODEL_ALLOWED_EFFORTS.get(model or DEFAULT_OPENROUTER_MODEL)
        return DEFAULT_EFFORT if advertised and DEFAULT_EFFORT in advertised else None
    return DEFAULT_EFFORT


def resolve_dispatch_effort(backend_id: str, model: str, effort: str | None) -> str | None:
    """Resolve omitted effort and enforce max-effort model policy."""
    validated = validate_reasoning_effort(effort)
    if _normalize_model(model) in MAX_EFFORT_MODELS and validated in {"low", "medium", "high", "xhigh"}:
        raise LunaEffortPolicyError(
            f"model {model!r} requires effort 'max'; explicit effort {validated!r} is refused [OBS-08]"
        )
    return default_effort_for_model(backend_id, model) if validated is None else validated
