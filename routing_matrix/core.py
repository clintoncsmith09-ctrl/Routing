"""Core routing logic for the Routing Matrix.

Decides which capability tier a task should be routed to, resolves the
corresponding model pointer from env vars, estimates cost, and logs every
decision as structured JSON.
"""

from __future__ import annotations

from .classifier import CLASSIFIER, Classification
from .logging import DecisionLogger, DECISION_LOGGER
from .model import DEFAULT_COST_MODEL, CostModel, RoutingDecision, Task, Tier
from .tiers import resolve_model_pointer


def _classify(task: Task) -> Classification:
    """Run complexity classification.

    Complexity classification runs only on a Tier-1 model or a non-LLM
    heuristic — never on Tier 2/3. Here it is always the fast, non-LLM
    heuristic so classification cost stays negligible at any call volume.
    """
    return CLASSIFIER.classify(task)


def route(
    task: Task,
    degraded: bool,
    *,
    cost_model: CostModel = DEFAULT_COST_MODEL,
    decision_logger: DecisionLogger | None = None,
) -> RoutingDecision:
    """Route a task to a capability tier and return the decision.

    Args:
        task: The task to route.
        degraded: Caller-supplied flag. When True, never return the frontier
            tier (cap at advanced). Never looked up or stored internally.
        cost_model: Tuning for estimated cost (defaults are sensible).
        decision_logger: Overridable structured logger (tests use this).

    Returns:
        A RoutingDecision naming the tier, resolved model pointer, rationale,
        the degraded flag, and an estimated cost.
    """
    logger = decision_logger if decision_logger is not None else DECISION_LOGGER

    escalated = bool(task.escalate)

    # Escalation takes precedence: straight to frontier, with context.
    if escalated:
        tier: Tier = "frontier"
        context = task.failure_context
        rationale = (
            f"Escalated by caller. "
            f"failure_context={context!r} (note: provided as {type(context).__name__})."
        )
    else:
        classification = _classify(task)
        tier = classification.tier  # type: ignore[assignment]

        if classification.tier == "standard":
            rationale = f"Classified standard via heuristic: {classification.signal}."
        else:
            rationale = (
                f"Classified {classification.tier} via complexity heuristic "
                f"(score={classification.score}): {classification.signal}."
            )

    # Degraded cap: never frontier, regardless of classification/escalation.
    if degraded and tier == "frontier":
        tier = "advanced"
        rationale = (
            f"{rationale} -> CAPPED to advanced because degraded=True "
            f"(frontier unavailable; cap at advanced)."
        )

    model_pointer = resolve_model_pointer(tier)
    estimated_cost = cost_model.cost_for(tier)

    decision = RoutingDecision(
        tier=tier,
        model_pointer=model_pointer,
        rationale=rationale,
        degraded=degraded,
        estimated_cost=estimated_cost,
    )

    # Structured JSON logging of the full decision.
    logger.log_decision(
        {
            "task": {
                "prompt": task.prompt,
                "task_type": task.task_type,
                "escalate": task.escalate,
                "failure_context": task.failure_context,
            },
            "tier": decision.tier,
            "model_pointer": decision.model_pointer,
            "rationale": decision.rationale,
            "degraded": decision.degraded,
            "estimated_cost": decision.estimated_cost,
        }
    )

    return decision
