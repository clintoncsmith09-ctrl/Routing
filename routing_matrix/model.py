"""Core data types for the Routing Matrix."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional

Tier = Literal["standard", "advanced", "frontier"]


@dataclass
class Task:
    """An incoming task to be routed to a capability tier.

    Fields:
        prompt:          Free-form task description.
        task_type:       Optional short label such as "crud" or "refactor".
        escalate:        If True, route directly to the frontier tier.
        failure_context: Optional context carried into the rationale when escalating.
    """

    prompt: str
    task_type: Optional[str] = None
    escalate: bool = False
    failure_context: Optional[str] = None


@dataclass
class RoutingDecision:
    """The result of routing a task to a tier."""

    tier: Tier
    model_pointer: str
    rationale: str
    degraded: bool
    estimated_cost: float


@dataclass(frozen=True)
class CostModel:
    """Per-tier cost weights used to estimate routing cost.

    A small, self-contained heuristic. Kept here so the numbers are
    configuration, not magic values scattered through routing logic.
    """

    standard: float = 0.0
    advanced: float = 0.0
    frontier: float = 0.0

    @classmethod
    def lowest(cls) -> "CostModel":
        # Deliberately returns "negligible" costs for the classification
        # path (Tier 1 / heuristic only), independent of model pointers.
        return cls(standard=0.0, advanced=0.0, frontier=0.0)

    def cost_for(self, tier: Tier) -> float:
        return getattr(self, tier)


DEFAULT_COST_MODEL = CostModel(standard=0.0001, advanced=0.001, frontier=0.01)
