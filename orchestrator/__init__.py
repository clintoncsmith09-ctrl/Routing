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
# ---- Durable Master Orchestrator layer (REAL Temporal) ----
from .workflow import (  # noqa: E402
    TaskInput,
    TaskWorkflow,
    TASK_QUEUE,
    create_worker,
)
from .store import (  # noqa: E402
    Reservation,
    SqliteAtomicStore,
)
from .api import (  # noqa: E402
    AccountRequired,
    OrchestratorAPI,
    RateLimitExceeded as APIRateLimitExceeded,
    TaskNotFound,
    TenantIsolationViolation,
)
from .costing import project_cost  # noqa: E402

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
    # Durable layer
    "TaskInput",
    "TaskWorkflow",
    "TASK_QUEUE",
    "create_worker",
    "Reservation",
    "SqliteAtomicStore",
    "OrchestratorAPI",
    "AccountRequired",
    "TaskNotFound",
    "TenantIsolationViolation",
    "APIRateLimitExceeded",
    "project_cost",
]
