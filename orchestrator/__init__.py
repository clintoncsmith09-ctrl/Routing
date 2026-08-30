"""Master Orchestrator — the downstream consumer of the Routing Matrix.

The orchestrator is the caller that owns the account-level usage state the
router deliberately does not hold. It routes each task, records usage against
an in-memory per-account ledger, builds an execution plan (including the
provider-adapter dispatch step), and logs every decision as structured JSON.

Public interface:
    from orchestrator import orchestrate, OrchestrationResult, UsageLedger
"""

from .core import (
    DEFAULT_ORCHESTRATOR,
    OrchestrationResult,
    Orchestrator,
    orchestrate,
)
from .logging import DecisionLogger, JsonFormatter, LOGGER
from .usage import (
    DEFAULT_ACCOUNT,
    DEFAULT_LEDGER,
    AccountLedger,
    UsageLedger,
)

__all__ = [
    "orchestrate",
    "OrchestrationResult",
    "Orchestrator",
    "DEFAULT_ORCHESTRATOR",
    "UsageLedger",
    "AccountLedger",
    "DEFAULT_ACCOUNT",
    "DEFAULT_LEDGER",
    "DecisionLogger",
    "JsonFormatter",
    "LOGGER",
]
