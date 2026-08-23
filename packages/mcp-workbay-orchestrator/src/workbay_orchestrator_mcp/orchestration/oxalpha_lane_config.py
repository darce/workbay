"""0xAlpha remote-lane pins (implementation note S4).

``0xalpha-remote`` drives the *codex* CLI on the OCI VM against OpenRouter's
OpenAI-compatible transport. The model is ``stealth/ox-alpha`` — **not** an
OpenAI model; 0xAlpha is merely OpenAI-transport compatible, which is why the
codex binary, path prepend and sandbox pins come from :mod:`codex_lane_config`
while the provider, credential port and allow-list live here.

Module name cannot start with a digit, hence ``oxalpha_``.

Deliberately absent (implementation note S4):

* **No** ``list_models_argv`` — OpenRouter's catalogue must never feed the
  allow-list (SECD-05). The curated set below is the whole authorization set.
* **No spend-cap constant** — the $ cap lives at OpenRouter and is read from
  ``GET /api/v1/key`` at probe time (DATA-14). The only local number is the
  policy threshold :data:`OXALPHA_MIN_REMAINING_USD`.
"""

from __future__ import annotations

import os

# Operator's slug (VERIFY-1). The ONLY member of the curated allow-list.
TRACKED_0XALPHA_MODEL = "stealth/ox-alpha"
OXALPHA_ALLOWED_MODELS: frozenset[str] = frozenset({TRACKED_0XALPHA_MODEL})
WORKBAY_0XALPHA_MODEL_ENV = "WORKBAY_0XALPHA_MODEL"

# Effort allow-list (S4-M-05). The model advertises {max, high, low}
# (VERIFY-1b); this set is that advertisement ∩ backend_spec.REMOTE_EFFORTS.
# `max` is unreachable because REMOTE_EFFORTS has no such value. `medium` and
# `xhigh` are refused because the transport accepts them SILENTLY (observed
# live 2026-08-23: codex exec returned rc 0 / "OK" for low, medium, high,
# xhigh and max alike), and we must not ship an effort the model never
# advertised (SECD-05). Keep this set sorted-stable; tests pin the wording.
OXALPHA_ALLOWED_EFFORTS: frozenset[str] = frozenset({"low", "high"})

# Credential port (env_file kind): the VM sources OXALPHA_ENV_FILE to export
# WORKBAY_0XALPHA_API_KEY; codex reads it through model_providers.*.env_key.
WORKBAY_0XALPHA_API_KEY_ENV = "WORKBAY_0XALPHA_API_KEY"
OXALPHA_ENV_FILE = "~/.config/0xalpha/env"

# Provider shaping for codex ``-c model_providers.0xalpha.*`` (VERIFY-1:
# nested keys work; wire_api must be ``responses``).
OXALPHA_PROVIDER_ID = "0xalpha"
OXALPHA_PROVIDER_NAME = "0xAlpha"
OXALPHA_BASE_URL = "https://openrouter.ai/api/v1"
OXALPHA_WIRE_API = "responses"

# Key-info endpoint used by the auth probe's authenticated step (VERIFY-3
# pinned the shape: ``{"data": {"limit", "limit_remaining", "usage", ...}}``).
# Zero model spend per probe.
OXALPHA_KEY_INFO_URL = "https://openrouter.ai/api/v1/key"
# Policy threshold (USD): a key whose ``limit_remaining`` is below this is
# refused as budget-exhausted (exit 15). Single source — the rendered probe
# receives this value from here.
OXALPHA_MIN_REMAINING_USD = 1.0


def resolve_tracked_or_env_0xalpha_model() -> str:
    """Env override if set, else the tracked pin. Does not read settings.local.json."""
    raw = (os.environ.get(WORKBAY_0XALPHA_MODEL_ENV) or "").strip()
    return raw or TRACKED_0XALPHA_MODEL


DEFAULT_0XALPHA_MODEL = resolve_tracked_or_env_0xalpha_model()
