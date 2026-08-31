"""Deterministic cost projection used by usage reservation.

The orchestrator must know a projected cost for a task BEFORE it calls
``routing_matrix.route()`` (because the ``degraded`` flag that route() receives
is computed from reserving that projected cost against the account store). This
module provides a *pure, deterministic* projection that mirrors the router's
non-LLM tier decision, so it never needs the router or any I/O.

It intentionally duplicates the router's tier heuristic (it is a projection,
not the routing decision). The authoritative tier always comes from
``routing_matrix.route()``; this projection only sizes the reservation.
"""

from __future__ import annotations

from routing_matrix.classifier import CLASSIFIER
from routing_matrix.model import DEFAULT_COST_MODEL

FRONTIER_COST = DEFAULT_COST_MODEL.frontier  # 0.01
ADVANCED_COST = DEFAULT_COST_MODEL.advanced  # 0.001
STANDARD_COST = DEFAULT_COST_MODEL.standard  # 0.0001


def project_cost(task: dict, *, degraded: bool = False) -> float:
    """Project the routing cost for a task, ignoring the ``degraded`` cap.

    We project the *non-degraded* tier cost (an upper bound), so the reservation
    is conservative: if reserving the upper bound trips degraded, route() caps
    to advanced and ``settle()`` later refunds the difference. If we instead
    projected already-capped costs we could under-reserve and let concurrent
    tasks slip past the threshold.

    Args:
        task: dict form of a task (prompt/task_type/escalate/failure_context).
        degraded: unused — reserved for clarity; projection is on the
            non-degraded tier so the reservation is an upper bound.

    Returns:
        The projected estimated cost for this task.
    """
    if bool(task.get("escalate")):
        return FRONTIER_COST
    tier = CLASSIFIER.classify(_to_task(task)).tier
    return {
        "standard": STANDARD_COST,
        "advanced": ADVANCED_COST,
        "frontier": FRONTIER_COST,
    }[tier]


def _to_task(task: dict):
    # Local import keeps this projection decoupled from the router's core.
    from routing_matrix import Task

    return Task(
        prompt=task.get("prompt", ""),
        task_type=task.get("task_type"),
        escalate=bool(task.get("escalate")),
        failure_context=task.get("failure_context"),
    )
