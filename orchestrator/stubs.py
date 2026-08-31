"""Stub interfaces for later, separate phases of the platform.

The Master Orchestrator hands off to these subsystems but does NOT implement
them. Each is a clearly-marked stub: the orchestrator calls it, gets a
placeholder result, and continues. A real implementation lands in its own
phase without the orchestrator changing its call sites.

Subsystems (all later-phase, out of scope for the orchestration phase):
    - Execution Engine   (actually executes the routed model call)
    - Deployment Factory (provisions/deploys the artifact)
    - Royalty Ledger     (records royalty obligations)
    - Payment Router     (routes/collects payments)
"""

from __future__ import annotations

# Sentinel: every stub result is tagged so it can never be mistaken for a real
# subsystem result.
STUB_PHASE = "later-phase-stub"


def _stub_result(name: str) -> dict:
    return {
        "stub": name,
        "phase": STUB_PHASE,
        "status": "not-implemented",
        "note": "Placeholder — real subsystems land in later phases.",
    }


class ExecutionEngine:
    """Later-phase stub: actually running the model call."""

    name = "ExecutionEngine"

    def execute(self, model_pointer: str, task: dict) -> dict:
        return _stub_result(self.name)


class DeploymentFactory:
    """Later-phase stub: provisioning/deploying the artifact."""

    name = "DeploymentFactory"

    def deploy(self, task_id: str, artifact: dict) -> dict:
        return _stub_result(self.name)


class RoyaltyLedger:
    """Later-phase stub: recording royalty obligations."""

    name = "RoyaltyLedger"

    def record(self, account_id: str, task_id: str, royalty: float) -> dict:
        return _stub_result(self.name)


class PaymentRouter:
    """Later-phase stub: routing/collecting payments."""

    name = "PaymentRouter"

    def route(self, account_id: str, amount: float) -> dict:
        return _stub_result(self.name)


# Friendly accessors used by the engine: each returns a clearly-marked stub.
def execution_engine() -> ExecutionEngine:
    return ExecutionEngine()


def deployment_factory() -> DeploymentFactory:
    return DeploymentFactory()


def royalty_ledger() -> RoyaltyLedger:
    return RoyaltyLedger()


def payment_router() -> PaymentRouter:
    return PaymentRouter()
