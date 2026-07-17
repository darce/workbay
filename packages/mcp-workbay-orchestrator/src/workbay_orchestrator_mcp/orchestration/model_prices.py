"""Static operator-maintained USD price table for turn-metrics cost derivation.

implementation note S1: cost is computed at **read time** in ``get_turn_metrics_summary``
— never stored per row — so editing this table re-prices history. Keys are
``"<backend>::<model>"`` matching the summary's ``by_backend_model`` key shape.

Prices are USD per million tokens (mtok). Values are approximate operator
anchors; update when vendor list prices change. Missing keys → row is
``unpriced`` (never silently zero).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ModelPrice:
    """Per-million-token USD rates for a single backend::model key."""

    input_per_mtok: float
    output_per_mtok: float


#: ``backend::model`` → USD rates. Keep keys exact against producer labels.
PRICES: dict[str, ModelPrice] = {
    # xAI grok-cli default pin (DEFAULT_GROK_MODEL); approximate public rates.
    "grok-cli::grok-4.5": ModelPrice(input_per_mtok=3.0, output_per_mtok=15.0),
    "grok-cli::grok-4": ModelPrice(input_per_mtok=3.0, output_per_mtok=15.0),
    # Codex CLI / OpenAI family (common offload backends).
    "codex-cli::gpt-5.4": ModelPrice(input_per_mtok=2.5, output_per_mtok=10.0),
    "codex-cli::gpt-5": ModelPrice(input_per_mtok=2.5, output_per_mtok=10.0),
    "codex-cli::o3": ModelPrice(input_per_mtok=10.0, output_per_mtok=40.0),
    "codex-cli::o4-mini": ModelPrice(input_per_mtok=1.1, output_per_mtok=4.4),
    # Claude Code host backends.
    "claude-code::claude-sonnet-4": ModelPrice(input_per_mtok=3.0, output_per_mtok=15.0),
    "claude-code::claude-opus-4": ModelPrice(input_per_mtok=15.0, output_per_mtok=75.0),
}


def price_key(backend: str | None, model: str | None) -> str:
    """Build the same ``backend::model`` key used by turn-metrics summaries."""
    backend_part = (backend or "").strip() or "unknown"
    model_part = (model or "").strip() or "default"
    return f"{backend_part}::{model_part}"


def lookup_price(backend: str | None, model: str | None) -> ModelPrice | None:
    """Return the static price for ``backend::model``, or None if unpriced."""
    return PRICES.get(price_key(backend, model))
