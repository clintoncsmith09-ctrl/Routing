"""Tier / environment pointer resolution.

Tiers are resolved purely from environment variables. No model name or
provider ID is hardcoded anywhere in this package — the only knowledge of
concrete models lives in the deployment environment's env vars.
"""

from __future__ import annotations

import os

from .model import Tier

TIER_ENV_VARS: dict[Tier, str] = {
    "standard": "TIER_1_MODEL_POINTER",
    "advanced": "TIER_2_MODEL_POINTER",
    "frontier": "TIER_3_MODEL_POINTER",
}


class TierConfigError(RuntimeError):
    """Raised when a required tier env pointer is missing or empty."""


def resolve_model_pointer(tier: Tier) -> str:
    """Return the model pointer for a tier from its env var.

    The pointer may be any opaque string (a model id, a provider-scoped id,
    etc.) — the router treats it as an opaque token to hand to dispatch.
    """
    env_var = TIER_ENV_VARS[tier]
    value = os.environ.get(env_var, "").strip()
    if not value:
        raise TierConfigError(
            f"Missing required environment variable {env_var!r} for tier {tier!r}."
        )
    return value


def all_pointers() -> dict[Tier, str]:
    """Resolve pointers for all tiers (useful for tests / introspection)."""
    return {tier: resolve_model_pointer(tier) for tier in Tier.__args__}  # type: ignore[attr-defined]
