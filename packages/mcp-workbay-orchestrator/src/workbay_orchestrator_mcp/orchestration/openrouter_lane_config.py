"""OpenRouter remote-lane pins (implementation note S4).

``openrouter-remote`` drives the *codex* CLI on the OCI VM against OpenRouter's
OpenAI-compatible transport. The model is ``z-ai/glm-5.3-flash`` (Z.ai GLM 5.3
Flash) — **not** an OpenAI model; OpenRouter is merely OpenAI-transport
compatible, which is why the codex binary, path prepend and sandbox pins come
from :mod:`codex_lane_config` while the provider, credential port and
allow-list live here.

Deliberately absent (implementation note S4):

* **No** ``list_models_argv`` — OpenRouter's catalogue must never feed the
  allow-list (SECD-05). The curated set below is the whole authorization set:
  five hand-curated slugs, not one. Membership requires advertised
  ``structured_outputs`` because ``openrouter-remote`` drives the codex binary
  with ``--output-schema``; a slug without that capability fails at dispatch
  rather than at validation.
* **No spend-cap constant** — the $ cap lives at OpenRouter and is read from
  ``GET /api/v1/key`` at probe time (DATA-14). The only local number is the
  policy threshold :data:`OPENROUTER_MIN_REMAINING_USD`.
"""

from __future__ import annotations

import os

# Operator's slug (VERIFY-1). The tracked default of the curated allow-list;
# the set below also admits four hand-curated siblings.
# The stealth pin retired 2026-08-29; this is its OpenRouter-published successor.
TRACKED_OPENROUTER_MODEL = "z-ai/glm-5.3-flash"
# Five hand-curated slugs. Membership requires advertised structured_outputs
# (openrouter-remote drives codex with --output-schema). Catalogue siblings
# such as qwen/qwen3.8-2.4t-a95b are real and still refused.
OPENROUTER_ALLOWED_MODELS: frozenset[str] = frozenset(
    {
        TRACKED_OPENROUTER_MODEL,
        "deepseek/deepseek-v4-flash",
        "qwen/qwen3.8-flash",
        "qwen/qwen3.8-max",
        "qwen/qwen3.8-27b",
    }
)
WORKBAY_OPENROUTER_MODEL_ENV = "WORKBAY_OPENROUTER_MODEL"

# Per-model advertised efforts (S4-M-05). An empty set means the model exposes
# no reasoning_effort knob; argv omits the knob rather than silently shipping a
# substituted value. backend_spec validates that every advertised token is
# supported by the remote codex transport.
OPENROUTER_MODEL_ALLOWED_EFFORTS: dict[str, frozenset[str]] = {
    "z-ai/glm-5.3-flash": frozenset({"max"}),
    "deepseek/deepseek-v4-flash": frozenset({"xhigh", "high"}),
    "qwen/qwen3.8-max": frozenset({"xhigh", "high", "medium", "low"}),
    "qwen/qwen3.8-27b": frozenset({"xhigh", "medium", "low"}),
    "qwen/qwen3.8-flash": frozenset(),
}

# The efforts reachable by at least one allow-listed model (union, never an
# intersection). Pair validation must use OPENROUTER_MODEL_ALLOWED_EFFORTS.
OPENROUTER_ALLOWED_EFFORTS: frozenset[str] = frozenset(
    effort for efforts in OPENROUTER_MODEL_ALLOWED_EFFORTS.values() for effort in efforts
)

# Credential port (env_file kind): the VM sources OPENROUTER_ENV_FILE to export
# WORKBAY_OPENROUTER_API_KEY; codex reads it through model_providers.*.env_key.
WORKBAY_OPENROUTER_API_KEY_ENV = "WORKBAY_OPENROUTER_API_KEY"
OPENROUTER_ENV_FILE = "~/.config/openrouter/env"

# Bare remote-lane wall clock. This is intentionally owned here rather than
# reading the other CLI transport's default: the two bounds may be tuned
# independently even though both currently resolve to 300 seconds.
OPENROUTER_REMOTE_LANE_TIMEOUT_S = 300
OPENROUTER_TIMEOUT_CAP = 1500

# Provider shaping for codex ``-c model_providers.openrouter.*`` (VERIFY-1:
# nested keys work; wire_api must be ``responses``).
OPENROUTER_PROVIDER_ID = "openrouter"
OPENROUTER_PROVIDER_NAME = "OpenRouter"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_WIRE_API = "responses"

# Key-info endpoint used by the auth probe's authenticated step (VERIFY-3
# pinned the shape: ``{"data": {"limit", "limit_remaining", "usage", ...}}``).
# Zero model spend per probe. ``limit`` / ``limit_remaining`` are the key's
# typed spend cap, not the account wallet — that is ``OPENROUTER_CREDITS_URL``.
OPENROUTER_KEY_INFO_URL = "https://openrouter.ai/api/v1/key"
# Account credit pool (``{"data": {"total_credits", "total_usage"}}``). This is
# the balance a 402 names as ``limit_source: "openrouter_credits"``.
OPENROUTER_CREDITS_URL = "https://openrouter.ai/api/v1/credits"
# Policy threshold (USD): a key whose spendable pool is below this is
# refused as budget-exhausted (exit 15). Single source — the rendered probe
# receives this value from here. Applied to the credit pool
# (``total_credits - total_usage``) and, for the key-info stage, to
# ``limit_remaining``; do not retune the number here to paper over a
# wrong quantity.
OPENROUTER_MIN_REMAINING_USD = 1.0


def resolve_tracked_or_env_openrouter_model() -> str:
    """Env override if set, else the tracked pin. Does not read settings.local.json."""
    raw = (os.environ.get(WORKBAY_OPENROUTER_MODEL_ENV) or "").strip()
    return raw or TRACKED_OPENROUTER_MODEL


DEFAULT_OPENROUTER_MODEL = resolve_tracked_or_env_openrouter_model()
