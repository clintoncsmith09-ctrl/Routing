"""Async, idempotent library facade over the durable TaskWorkflow.

No HTTP server — this is a programmatic entry point:

- ``await api.submit(task, account_id, idempotency_key=None) -> task_id`` —
  resolves the request to an account, rate-limits at the transport level,
  claims an idempotency key (durably), and starts a Temporal ``TaskWorkflow``.
  It returns the task id immediately without waiting for the workflow to finish.
- ``await api.get_status(task_id, account_id) -> dict`` — polling facade. Reads
  the durable task row from the external store; every lookup is tenant-scoped
  (the account id is required and must match the task's owning account).

Idempotency is durable: a duplicate ``idempotency_key`` returns the EXISTING
task id (and never starts a second workflow), because registration happens
atomically in the external store before the workflow is started.
"""

from __future__ import annotations

import uuid
from typing import Optional

from temporalio.client import Client

from .rate_limit import TokenBucketLimiter
from .store import SqliteAtomicStore
from .trace import new_correlation_id
from .workflow import TaskInput, TaskWorkflow


class Error(Exception):
    """Base orchestrator facade error."""


class AccountRequired(Error):
    """Raised when a request is not resolved to an account."""


class TaskNotFound(Error):
    """Raised when a task cannot be found for the given account."""


class TenantIsolationViolation(Error):
    """Raised when a caller attempts to reach a task owned by another account."""


class RateLimitExceeded(Error):
    """Raised when transport-level rate limiting denies a submission."""


class OrchestratorAPI:
    """Library facade that submits tasks as durable Temporal workflows.

    Args:
        client: Temporal client (from a ``WorkflowEnvironment`` in tests, or a
            connection to a real Temporal server in production).
        task_queue: The Temporal task queue the workers poll.
        store: External atomic store implementing Usage/Idempotency/TaskStore.
        rate_limiter: Optional transport-level token-bucket limiter.
    """

    def __init__(
        self,
        client: Client,
        task_queue: str = "master-orchestrator",
        *,
        store: SqliteAtomicStore,
        rate_limiter: Optional[TokenBucketLimiter] = None,
    ) -> None:
        self._client = client
        self._task_queue = task_queue
        self._store = store
        self._rate_limiter = rate_limiter

    # -- helpers -------------------------------------------------------------- #

    def _resolve_account(self, account_id: Optional[str]) -> str:
        """Requirement 1: every request resolved to an account before any
        downstream call. account_id is the key all cost/state/logging attach to."""
        if not account_id:
            raise AccountRequired("account_id is required")
        return account_id

    # -- submission ----------------------------------------------------------- #

    async def submit(
        self,
        task,
        account_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        *,
        provider_name: Optional[str] = None,
        simulate_failure: Optional[str] = None,
    ) -> str:
        """Submit a task as a durable workflow; returns its task id immediately.

        Idempotency: a duplicate ``idempotency_key`` returns the existing task
        id and does NOT start a new workflow.
        """
        account_id = self._resolve_account(account_id)

        # Transport-level rate limiting (independent of cost degradation).
        if self._rate_limiter is not None and not self._rate_limiter.allow(account_id):
            raise RateLimitExceeded(
                f"rate limit exceeded for account {account_id!r}"
            )

        correlation_id = new_correlation_id()
        task_id = uuid.uuid4().hex

        # Durable idempotency claim (atomic). Duplicate -> existing task, never new.
        if idempotency_key is not None:
            existing, created = self._store.register_idempotency(idempotency_key, task_id)
            if not created:
                return existing  # return the EXISTING task; do not start a new one

        self._store.create_task(task_id, account_id, idempotency_key, correlation_id)

        # Start the durable workflow. SDK note: this SDK's `start_workflow` accepts at
        # most ONE positional argument after the workflow and does NOT allow it
        # combined with the keyword-only `args=[...]` sequence (it raises
        # "Cannot have arg and args"). So ALL workflow parameters travel in the
        # `args=[...]` list, in the same order as TaskWorkflow.run's parameters.
        # This returns as soon as the task is scheduled — the caller never waits
        # for completion.
        await self._client.start_workflow(
            TaskWorkflow,
            args=[
                TaskInput(
                    prompt=task.prompt,
                    task_type=task.task_type,
                    escalate=task.escalate,
                    failure_context=getattr(task, "failure_context", None),
                ),
                account_id,
                idempotency_key,
                correlation_id,
                provider_name,
                simulate_failure,
            ],
            id=task_id,
            task_queue=self._task_queue,
        )
        return task_id

    # -- polling -------------------------------------------------------------- #

    async def get_status(self, task_id: str, account_id: Optional[str]) -> dict:
        """Poll a task's durable status. Every lookup is tenant-scoped."""
        account_id = self._resolve_account(account_id)
        task = self._store.get_task(task_id, account_id)
        if task is None:
            # Either the task does not exist, or it is owned by another account
            # (tenant isolation: one account cannot reach another's state).
            owner = self._store.task_owner(task_id)
            if owner is not None:
                raise TenantIsolationViolation(
                    f"task {task_id!r} is not owned by account {account_id!r}"
                )
            raise TaskNotFound(f"no task {task_id!r} for account {account_id!r}")
        return task
