"""Master Orchestrator core: the downstream consumer of the Routing Matrix.

The orchestrator is the caller that owns the account-level usage state the
router deliberately does not hold. Given a task it:

1. calls `routing_matrix.route(task, degraded=degraded)` for the tier /
   model_pointer / rationale / estimated_cost;
2. records usage against the caller-owned account ledger (in-memory);
3. builds a structured execution plan, including the provider-adapter
   dispatch step via `REGISTRY.dispatch(provider_name, model_pointer)`;
4. logs the whole decision as a single structured-JSON line.

It reuses the router's public API only and never modifies the router package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from routing_matrix import Task, route
from routing_matrix.providers import REGISTRY, parse_provider

from .logging import DECISION_LOGGER, DecisionLogger
from .usage import DEFAULT_ACCOUNT, DEFAULT_LEDGER, UsageLedger


@dataclass
class OrchestrationResult:
    """The outcome of orchestrating one task."""

    task: Task
    account_id: str
    tier: str
    model_pointer: str
    rationale: str
    degraded: bool
    estimated_cost: float
    plan: dict
    usage: dict


@dataclass
class Orchestrator:
    """Stateless-by-design orchestrator; usage state lives in an injected
    UsageLedger (which it owns, accounting for calls per account)."""

    usage_ledger: UsageLedger = field(default_factory=lambda: DEFAULT_LEDGER)
    decision_logger: DecisionLogger = field(default_factory=lambda: DECISION_LOGGER)

    def orchestrate(
        self,
        task: Task,
        account_id: Optional[str] = None,
        degraded: bool = False,
        *,
        provider_name: Optional[str] = None,
    ) -> OrchestrationResult:
        """Route a task, account for its usage, build a plan, and log it.

        Args:
            task: The task to orchestrate (reuses routing_matrix.Task).
            account_id: Account to charge usage to (defaults to DEFAULT_ACCOUNT).
            degraded: Caller-supplied flag forwarded to the router (caps at
                advanced — never frontier).
            provider_name: Optional provider name for dispatch; otherwise the
                registry's default provider is used.

        Returns:
            An OrchestrationResult with the routed tier/model_pointer, the
            structured execution plan, and the post-call usage snapshot.
        """
        account_id = account_id or DEFAULT_ACCOUNT

        # 1. Router makes the decision (the router stays stateless).
        decision = route(task, degraded=degraded)

        # 2. Record usage against the caller-owned account ledger.
        self.usage_ledger.record(account_id, decision.tier, decision.estimated_cost)
        usage = self.usage_ledger.snapshot(account_id)

        # 3. Execution plan: dispatch via the provider adapter registry.
        provider = parse_provider(provider_name)
        dispatch = REGISTRY.dispatch(provider, decision.model_pointer)
        plan = {
            "routed_tier": decision.tier,
            "model_pointer": decision.model_pointer,
            "rationale": decision.rationale,
            "degraded": decision.degraded,
            "estimated_cost": decision.estimated_cost,
            "dispatch_step": {
                "provider": provider,
                "result": dispatch,
            },
            "usage": usage,
        }

        result = OrchestrationResult(
            task=task,
            account_id=account_id,
            tier=decision.tier,
            model_pointer=decision.model_pointer,
            rationale=decision.rationale,
            degraded=decision.degraded,
            estimated_cost=decision.estimated_cost,
            plan=plan,
            usage=usage,
        )

        # 4. Structured JSON logging of the orchestration decision.
        self.decision_logger.log_decision(
            {
                "task": {
                    "prompt": task.prompt,
                    "task_type": task.task_type,
                    "escalate": task.escalate,
                },
                "decision": {
                    "tier": decision.tier,
                    "model_pointer": decision.model_pointer,
                    "rationale": decision.rationale,
                    "degraded": decision.degraded,
                    "estimated_cost": decision.estimated_cost,
                },
                "account_id": account_id,
                "usage": usage,
                "plan": plan,
            }
        )

        return result


# Default module-level orchestrator for the simplest call path.
DEFAULT_ORCHESTRATOR = Orchestrator()


def orchestrate(
    task: Task,
    account_id: Optional[str] = None,
    degraded: bool = False,
    *,
    orchestrator: Optional[Orchestrator] = None,
    provider_name: Optional[str] = None,
) -> OrchestrationResult:
    """Convenience wrapper around Orchestrator.orchestrate().

    Uses a shared default orchestrator (and so a shared in-memory usage
    ledger) unless an explicit `orchestrator` is supplied.
    """
    impl = orchestrator if orchestrator is not None else DEFAULT_ORCHESTRATOR
    return impl.orchestrate(
        task,
        account_id=account_id,
        degraded=degraded,
        provider_name=provider_name,
    )
