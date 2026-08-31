"""Durable Master Orchestrator: Temporal workflow + activities.

This is the REAL Temporal durability layer. Every task is a durable
``TaskWorkflow`` instance. The architecture constraint is strict:

- **Workflow** (``@workflow.defn`` / ``@workflow.run``) contains ONLY
  deterministic orchestration — deciding which activities to run and in what
  order, and reading their returned values. It does no I/O, no ledger
  read/write, no ``routing_matrix.route()`` call, no dispatch inline.
- **Activities** (``@activity.defn``) perform ALL I/O: the atomic usage-store
  reads/writes, the ``route()`` call, the circuit-breaker consult/record, the
  provider dispatch + later-phase stub hand-offs, transport rate limiting, and
  structured logging.

A restart resumes an in-flight task exactly where it left off because Temporal
replays a workflow's event history and re-runs only the deterministic code;
completed activity results are replayed from history, never re-executed.

The workflow is tested for replay-safety in ``test_robustness.py`` using
``temporalio.worker.Replayer``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

# The workflow sandbox only needs to *reference* these; importing them at module
# load is deterministic (no I/O at import time). Activities (which run outside
# the sandbox) use them freely.
with workflow.unsafe.imports_passed_through():
    from routing_matrix import Task, route
    from routing_matrix.providers import REGISTRY, parse_provider
    from .costing import project_cost
    from .logging import LOGGER as JSON_LOGGER
    from .stubs import deployment_factory, execution_engine, payment_router, royalty_ledger
    from .trace import with_correlation_fields

TASK_QUEUE = "master-orchestrator"

# Activity execution policy: bounded retries + a start-to-close timeout so no
# step ever hangs or vanishes.
RETRY_POLICY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(milliseconds=200),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=1),
)
STEP_TIMEOUT = timedelta(seconds=30)

# Task lifecycle states exposed through the polling facade.
STATUS_ACCEPTED = "ACCEPTED"
STATUS_RUNNING = "RUNNING"
STATUS_COMPLETED = "COMPLETED"
STATUS_FAILED = "FAILED"


# --------------------------------------------------------------------------- #
# Dependency container (wired by create_worker / the API)
# --------------------------------------------------------------------------- #


class _Deps:
    def __init__(self) -> None:
        self.store = None
        self.circuit_breaker = None
        self.rate_limiter = None


_DEPS = _Deps()


# --------------------------------------------------------------------------- #
# Activity inputs / outputs (JSON-safe dataclasses)
# --------------------------------------------------------------------------- #


@dataclass
class TaskInput:
    """JSON-safe dict form of a routing_matrix.Task (keeps activity args simple)."""

    prompt: str
    task_type: Optional[str] = None
    escalate: bool = False
    failure_context: Optional[str] = None

    def to_task(self) -> Task:
        return Task(
            prompt=self.prompt,
            task_type=self.task_type,
            escalate=self.escalate,
            failure_context=self.failure_context,
        )


@dataclass
class ReservationOut:
    account_id: str
    degraded: bool
    projected_cost: float
    balance: float


@dataclass
class DegradedOut:
    degraded: bool  # effective (per-account OR circuit-breaker)


@dataclass
class RouteOut:
    tier: str
    model_pointer: str
    rationale: str
    degraded: bool
    estimated_cost: float


# --------------------------------------------------------------------------- #
# Activities — ALL I/O lives here, never in workflow code
# --------------------------------------------------------------------------- #


@activity.defn
async def check_rate_limit(args: dict) -> bool:
    """Transport-level rate limiting: consumes one token per submission."""
    limiter = _DEPS.rate_limiter
    if limiter is None:
        return True
    return limiter.allow(args["account_id"])


@activity.defn
async def reserve_usage(args: dict) -> ReservationOut:
    """Atomic account-usage reservation. Computes the per-account degraded flag.

    This is the ONE atomic check+update on the external store that the
    architecture requires. ``routing_matrix.route()`` is never consulted here.
    """
    store = _DEPS.store
    account_id = args["account_id"]
    projected = args["projected_cost"]
    res = store.reserve(account_id, projected)
    JSON_LOGGER.info(
        "usage_reserved",
        extra=with_correlation_fields(
            args.get("correlation_id"),
            {
                "account_id": account_id,
                "projected_cost": projected,
                "degraded": res.degraded,
                "balance": res.balance,
            },
        ),
    )
    return ReservationOut(
        account_id=res.account_id,
        degraded=res.degraded,
        projected_cost=res.projected_cost,
        balance=res.balance,
    )


@activity.defn
async def circuit_breaker_effective(args: dict) -> DegradedOut:
    """Consult the global spend circuit breaker; OR over per-account degraded.

    When the breaker is tripped the whole platform is forced non-frontier,
    regardless of individual account headroom."""
    breaker = _DEPS.circuit_breaker
    per_account = bool(args["per_account_degraded"])
    if breaker is None:
        return DegradedOut(degraded=per_account)
    effective = breaker.effective_degraded(per_account)
    JSON_LOGGER.info(
        "circuit_breaker_effective",
        extra=with_correlation_fields(
            args.get("correlation_id"),
            {
                "per_account_degraded": per_account,
                "tripped": breaker.tripped,
                "effective_degraded": effective,
            },
        ),
    )
    return DegradedOut(degraded=effective)


@activity.defn
async def route_task(args: dict) -> RouteOut:
    """The only place ``routing_matrix.route()`` is called. Never inline."""
    ti = TaskInput(**args["task"])
    degraded = bool(args["degraded"])
    decision = route(ti.to_task(), degraded=degraded)
    JSON_LOGGER.info(
        "routed",
        extra=with_correlation_fields(
            args.get("correlation_id"),
            {
                "tier": decision.tier,
                "model_pointer": decision.model_pointer,
                "degraded": decision.degraded,
                "estimated_cost": decision.estimated_cost,
            },
        ),
    )
    return RouteOut(
        tier=decision.tier,
        model_pointer=decision.model_pointer,
        rationale=decision.rationale,
        degraded=decision.degraded,
        estimated_cost=decision.estimated_cost,
    )


@activity.defn
async def settle_usage(args: dict) -> float:
    """Reconcile the projected reservation to the actual routed cost (atomic)."""
    store = _DEPS.store
    account_id = args["account_id"]
    reserved = args["reserved_cost"]
    actual = args["actual_cost"]
    balance = store.settle(account_id, reserved, actual)
    JSON_LOGGER.info(
        "usage_settled",
        extra=with_correlation_fields(
            args.get("correlation_id"),
            {
                "account_id": account_id,
                "reserved": reserved,
                "actual": actual,
                "balance": balance,
            },
        ),
    )
    return balance


@activity.defn
async def circuit_breaker_record(args: dict) -> bool:
    """Accumulate one task's cost into the aggregate spend; returns whether
    frontier is still allowed."""
    breaker = _DEPS.circuit_breaker
    if breaker is not None:
        breaker.record(args["cost"])
    if breaker is None:
        return True
    return breaker.frontier_allowed()


@activity.defn
async def dispatch_task(args: dict) -> dict:
    """Provider dispatch + later-phase stub hand-offs. Also the dead-letter seam.

    ``args["simulate_failure"]`` is a test hook: when ``"dispatch"`` it always
    fails so retry-exhaustion -> FAILED can be exercised end-to-end.
    """
    if args.get("simulate_failure") == "dispatch":
        raise RuntimeError("simulated dispatch failure for dead-letter test")

    provider = parse_provider(args.get("provider_name"))
    dispatch = REGISTRY.dispatch(provider, args["model_pointer"])

    # Later-phase subsystems are called as stubs (never implemented here).
    task_id = args["task_id"]
    plan = {"routed_tier": args["tier"], "model_pointer": args["model_pointer"]}
    head_off = {
        "execution": execution_engine().execute(args["model_pointer"], args["task"]),
        "deployment": deployment_factory().deploy(task_id, plan),
        "royalty": royalty_ledger().record(args["account_id"], task_id, 0.0),
        "payment": payment_router().route(args["account_id"], args["estimated_cost"]),
    }
    JSON_LOGGER.info(
        "dispatched",
        extra=with_correlation_fields(
            args.get("correlation_id"),
            {"provider": provider, "dispatch": dispatch, "task_id": task_id},
        ),
    )
    return {"provider": provider, "dispatch": dispatch, "head_off": head_off}


@activity.defn
async def mark_status(args: dict) -> None:
    """Durable task-status write to the external store (tenant-scoped)."""
    store = _DEPS.store
    store.set_task_status(
        args["task_id"],
        args["status"],
        error=args.get("error"),
        tier=args.get("tier"),
        model_pointer=args.get("model_pointer"),
        estimated_cost=args.get("estimated_cost"),
    )
    JSON_LOGGER.info(
        "task_status",
        extra=with_correlation_fields(
            args.get("correlation_id"),
            {"task_id": args["task_id"], "status": args["status"], "error": args.get("error")},
        ),
    )


@activity.defn
async def log_event(args: dict) -> None:
    """Structured log with the correlation id attached (used for tracing)."""
    payload = with_correlation_fields(
        args.get("correlation_id"), json.loads(args.get("payload", "{}"))
    )
    JSON_LOGGER.info(args.get("event", "orchestrator_event"), extra=payload)


ALL_ACTIVITIES = [
    check_rate_limit,
    reserve_usage,
    circuit_breaker_effective,
    route_task,
    settle_usage,
    circuit_breaker_record,
    dispatch_task,
    mark_status,
    log_event,
]


# --------------------------------------------------------------------------- #
# Workflow — deterministic orchestration only
# --------------------------------------------------------------------------- #


@workflow.defn
class TaskWorkflow:
    """Durable lifecycle for exactly one task.

    The body is deterministic control flow over activities. Every I/O side
    effect happens in an activity, so Temporal can replay this workflow from
    its event history after a restart and resume exactly where it left off.
    """

    @workflow.run
    async def run(
        self,
        task: TaskInput,
        account_id: str,
        idempotency_key: Optional[str],
        correlation_id: str,
        provider_name: Optional[str] = None,
        simulate_failure: Optional[str] = None,
    ) -> dict:
        wf_id = workflow.info().workflow_id

        try:
            await self._activity(mark_status, dict(
                task_id=wf_id, status=STATUS_RUNNING, correlation_id=correlation_id,
            ))

            # 1. Transport-level rate limiting (independent of cost degradation).
            await self._activity(check_rate_limit, dict(
                account_id=account_id, correlation_id=correlation_id,
            ))

            # 2. Atomic account-usage reservation -> per-account degraded flag.
            projected = project_cost(task.__dict__)
            reservation = await self._activity(reserve_usage, dict(
                account_id=account_id,
                projected_cost=projected,
                correlation_id=correlation_id,
            ))

            # 3. Global circuit breaker ORs over per-account degraded.
            eff = await self._activity(circuit_breaker_effective, dict(
                per_account_degraded=reservation.degraded,
                correlation_id=correlation_id,
            ))

            # 4. Route (I/O lives in the activity; the router is never called here).
            routed = await self._activity(route_task, dict(
                task=task.__dict__,
                degraded=eff.degraded,
                correlation_id=correlation_id,
            ))

            # 5. Reconcile reservation to actual routed cost (atomic).
            await self._activity(settle_usage, dict(
                account_id=account_id,
                reserved_cost=reservation.projected_cost,
                actual_cost=routed.estimated_cost,
                correlation_id=correlation_id,
            ))

            # 6. Aggregate spend into the global breaker.
            await self._activity(circuit_breaker_record, dict(
                cost=routed.estimated_cost, correlation_id=correlation_id,
            ))

            # 7. Dispatch + later-phase stub hand-off.
            await self._activity(dispatch_task, dict(
                task_id=wf_id,
                account_id=account_id,
                provider_name=provider_name,
                model_pointer=routed.model_pointer,
                tier=routed.tier,
                estimated_cost=routed.estimated_cost,
                task=task.__dict__,
                simulate_failure=simulate_failure,
                correlation_id=correlation_id,
            ))

            # 8. Durable completion.
            await self._activity(mark_status, dict(
                task_id=wf_id,
                status=STATUS_COMPLETED,
                tier=routed.tier,
                model_pointer=routed.model_pointer,
                estimated_cost=routed.estimated_cost,
                correlation_id=correlation_id,
            ))

            return {
                "task_id": wf_id,
                "account_id": account_id,
                "tier": routed.tier,
                "model_pointer": routed.model_pointer,
                "rationale": routed.rationale,
                "degraded": routed.degraded,
                "estimated_cost": routed.estimated_cost,
                "correlation_id": correlation_id,
                "status": STATUS_COMPLETED,
            }

        except Exception as exc:
            # Dead-letter: bound retries are exhausted (or an unexpected fault);
            # the task lands in a definitive FAILED state with a logged reason —
            # it never hangs and never vanishes.
            parts: list[str] = []
            cur: BaseException | None = exc
            while cur is not None:
                parts.append(f"{type(cur).__name__}: {cur}")
                cur = getattr(cur, "cause", None)
            reason = " <- ".join(parts)
            await self._activity(mark_status, dict(
                task_id=wf_id,
                status=STATUS_FAILED,
                error=reason,
                correlation_id=correlation_id,
            ))
            workflow.logger.error("task_failed task_id=%s reason=%s", wf_id, reason)
            raise ApplicationError(f"Task {wf_id} failed: {reason}") from exc

    async def _activity(self, fn, args: dict):
        """Run one activity under the bounded-retry + timeout policy."""
        return await workflow.execute_activity(
            fn,
            args,
            retry_policy=RETRY_POLICY,
            start_to_close_timeout=STEP_TIMEOUT,
            schedule_to_start_timeout=timedelta(seconds=10),
        )


# --------------------------------------------------------------------------- #
# Worker factory — wires the dependency container + registers workflow/activities
# --------------------------------------------------------------------------- #


def create_worker(
    client,
    task_queue: str = TASK_QUEUE,
    *,
    store,
    circuit_breaker=None,
    rate_limiter=None,
):
    """Build a Temporal Worker bound to the given dependencies.

    The caller owns the store/breaker/limiter lifecycle; this just binds them
    into the activity container so every activity sees the same shared state.
    """
    _DEPS.store = store
    _DEPS.circuit_breaker = circuit_breaker
    _DEPS.rate_limiter = rate_limiter
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[TaskWorkflow],
        activities=ALL_ACTIVITIES,
    )
