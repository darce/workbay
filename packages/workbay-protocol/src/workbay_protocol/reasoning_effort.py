"""Shared model-routing data and effort vocabulary for WorkBay consumers.

Concrete effort and selection mode are separate concepts. MAX is an explicit
Codex effort, not an alias for xhigh. Automatic scoring remains capped at xhigh
until an operator explicitly selects max (directly or through a manifest).

Model identifiers and routing defaults live here because protocol is a common
dependency of handoff, the orchestrator, and installed lifecycle scripts.
These are WorkBay's routing preferences, not a provider capability catalogue:
the dispatching backend must still validate an explicitly requested model.
"""


class ModelId:
    """Model identities used by WorkBay routing and compatibility metadata."""

    CODEX_LEGACY_SENIOR = "gpt-5.6-sol"
    CODEX_SENIOR = "gpt-6.1-sol"
    CODEX_LEGACY_JUNIOR = "gpt-5.6-luna"
    CODEX_JUNIOR = "gpt-6-luna"
    CODEX_UNENTITLED = "gpt-5.3-sparx"
    CODEX_SUMMARY = "gpt-6.1-terra"
    CODEX_GPT_54 = "gpt-5.4"
    CODEX_GPT_5 = "gpt-5"
    CODEX_O3 = "o3"
    CODEX_O4_MINI = "o4-mini"

    GROK_CURRENT = "grok-4.6"
    GROK_RETIRED = "grok-4.5"
    GROK_LEGACY = "grok-4"
    GROK_BUILD = "grok-build"

    CURSOR_GROK_45 = "cursor-grok-4.5"
    CURSOR_GROK_45_LOW = "cursor-grok-4.5-low"
    CURSOR_GROK_45_LOW_FAST = "cursor-grok-4.5-low-fast"
    CURSOR_GROK_45_MEDIUM = "cursor-grok-4.5-medium"
    CURSOR_GROK_45_MEDIUM_FAST = "cursor-grok-4.5-medium-fast"
    CURSOR_GROK_45_HIGH = "cursor-grok-4.5-high"
    CURSOR_GROK_45_HIGH_FAST = "cursor-grok-4.5-high-fast"
    CURSOR_GROK_46 = "cursor-grok-4.6"
    CURSOR_GROK_46_LOW = "cursor-grok-4.6-low"
    CURSOR_GROK_46_MEDIUM = "cursor-grok-4.6-medium"
    CURSOR_GROK_46_HIGH = "cursor-grok-4.6-high"
    CURSOR_GROK_46_HIGH_FAST = "cursor-grok-4.6-high-fast"
    CURSOR_GROK_46_XHIGH = "cursor-grok-4.6-xhigh"
    CURSOR_KIMI = "kimi-k3"
    CURSOR_KIMI_LOW = "kimi-k3-low"
    CURSOR_KIMI_HIGH = "kimi-k3-high"
    CURSOR_KIMI_MAX = "kimi-k3-max"

    CLAUDE_OPUS = "claude-opus-5-5"
    CLAUDE_OPUS_DOTTED = "claude-opus-5.5"
    CLAUDE_OPUS_SHORT = "opus-5.5"
    CLAUDE_SONNET = "claude-sonnet-5-5"
    CLAUDE_FABLE = "claude-fable-5-1"
    CLAUDE_LEGACY_HAIKU = "claude-haiku-4-5-20251001"
    CLAUDE_HAIKU = "claude-haiku-5-5"
    CLAUDE_SONNET_4 = "claude-sonnet-4"
    CLAUDE_OPUS_4 = "claude-opus-4"
    MUSE = "muse-wonder-1.0"

    OPENROUTER_MIMO = "xiaomi/mimo-v2.6-pro"
    OPENROUTER_GLM = "z-ai/glm-5.3-flash"
    OPENROUTER_DEEPSEEK = "deepseek/deepseek-v4-flash"
    OPENROUTER_QWEN_FLASH = "qwen/qwen3.8-flash"
    OPENROUTER_QWEN_MAX = "qwen/qwen3.8-max"
    OPENROUTER_QWEN_27B = "qwen/qwen3.8-27b"

    EMBEDDING = "gte-base-en-v1.5"


# Keys describe the use of a model, rather than duplicating its identifier in
# callers. Change a preference here; explicit per-call routes still take
# precedence. Distinct uses can deliberately select different models.
MODEL_ROUTING_DEFAULTS: dict[str, str] = {
    "codex": ModelId.CODEX_JUNIOR,
    "codex_tier_senior": ModelId.CODEX_SENIOR,
    "codex_tier_junior": ModelId.CODEX_LEGACY_JUNIOR,
    "review_role": ModelId.CODEX_LEGACY_JUNIOR,
    "grok": ModelId.GROK_CURRENT,
    "cursor": ModelId.CURSOR_GROK_45_HIGH_FAST,
    "claude": ModelId.CLAUDE_SONNET,
    "muse": ModelId.MUSE,
    "openrouter": ModelId.OPENROUTER_MIMO,
    "plan_review": ModelId.CODEX_SENIOR,
    "preflight_probe": ModelId.CODEX_LEGACY_SENIOR,
    "fleet_summary": ModelId.CODEX_SUMMARY,
}

CODEX_REASONING_EFFORTS = ("low", "medium", "high", "xhigh", "max")
CONCRETE_EFFORTS = frozenset(CODEX_REASONING_EFFORTS)
EFFORT_MODES = ("inherit", "auto")
WORKER_REASONING_EFFORT_CHOICES = (*EFFORT_MODES, *CODEX_REASONING_EFFORTS)
AUTO_EFFORT_LADDER = CODEX_REASONING_EFFORTS[:-1]
# Other backend profiles retain their existing capabilities.
STANDARD_WORKER_REASONING_EFFORT_CHOICES = (*EFFORT_MODES, *AUTO_EFFORT_LADDER)


def validate_reasoning_effort(value: object, *, allow_modes: bool = True) -> str | None:
    """Narrow an untrusted input to an exact protocol value without rewriting it."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("reasoning effort must be a string")
    if value != value.strip().lower():
        raise ValueError(
            f"effort {value!r} is not canonical; use lowercase with no surrounding whitespace"
        )
    allowed = (
        WORKER_REASONING_EFFORT_CHOICES if allow_modes else CODEX_REASONING_EFFORTS
    )
    if value not in allowed:
        raise ValueError(
            f"effort {value!r} not in {sorted(allowed)}; refusing to ship a substituted effort"
        )
    return value
