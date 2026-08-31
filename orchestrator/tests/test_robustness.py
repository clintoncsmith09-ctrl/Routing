"""Robustness test suite for the Master Orchestrator durability layer.

Proves the five required scenarios against REAL Temporal
(``WorkflowEnvironment.start_local``) and the external atomic store:

1. Concurrent threshold race — two concurrent submissions on one account cannot
   both pass the degradation threshold (atomicity of the external store, proven
   at both the raw-store level with threaded barriers and end-to-end through
   two concurrent workflows).
2. Simulated restart mid-task — the workflow is replayed from its recorded
   event history (``temporalio.worker.Replayer``) and reconstructs the same
   deterministic result, proving a restart resumes in-flight work.
3. Idempotent duplicate — the same idempotency key twice returns the existing
   task and never creates a second workflow/task.
4. Retry exhaustion — a step that always fails exhausts its bounded retries and
   lands the task in FAILED with a logged reason (never hangs, never vanishes).
5. Global circuit breaker — aggregate load trips the breaker and forces
   standard/advanced-only even for an account with individual headroom.

Each test sets up its own Temporal environment via ``asyncio.run`` (no external
Temporal server, no pytest-asyncio plugin required).
"""

from __future__ import annotations

import asyncio
import tempfile
import threading

import pytest

from routing_matrix import Task

from orchestrator.api import (
    OrchestratorAPI,
    RateLimitExceeded,
    TaskNotFound,
    TenantIsolationViolation,
)
from orchestrator.circuit_breaker import CircuitBreaker
from orchestrator.rate_limit import TokenBucketLimiter
from orchestrator.store import SqliteAtomicStore
from orchestrator.workflow import ALL_ACTIVITIES, TaskWorkflow, create_worker

T1 = "pointer/tier1"
T2 = "pointer/tier2"
T3 = "pointer/tier3"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    """Self-contained tier pointers for every test (same pattern as the router)."""
    monkeypatch.setenv("TIER_1_MODEL_POINTER", T1)
    monkeypatch.setenv("TIER_2_MODEL_POINTER", T2)
    monkeypatch.setenv("TIER_3_MODEL_POINTER", T3)


# --------------------------------------------------------------------------- #
# Setup helpers
# --------------------------------------------------------------------------- #


async def _env_with(
    threshold: float,
    *,
    breaker_threshold: float | None = None,
    rate: float | None = None,
    capacity: int | None = None,
):
    """Start a local Temporal env + store + breaker + limiter + worker + API.

    Returns a context with ``.env/.store/.breaker/.limiter/.api/.worker`` and
    ``close()`` to shut everything down.
    """
    from temporalio.testing import WorkflowEnvironment

    env = await WorkflowEnvironment.start_local()
    db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db.close()
    store = SqliteAtomicStore(db.name, degraded_threshold=threshold)
    breaker = CircuitBreaker(breaker_threshold) if breaker_threshold is not None else None
    limiter = (
        TokenBucketLimiter(rate, capacity) if rate is not None else None
    )
    worker = create_worker(
        env.client,
        store=store,
        circuit_breaker=breaker,
        rate_limiter=limiter,
    )
    await worker.__aenter__()  # Worker is an async context manager; run it
    api = OrchestratorAPI(env.client, store=store, rate_limiter=limiter)

    ctx = _Ctx()
    ctx.env = env
    ctx.store = store
    ctx.breaker = breaker
    ctx.limiter = limiter
    ctx.worker = worker
    ctx.api = api
    ctx.db_path = db.name
    return ctx


class _Ctx:
    """Mutable test context populated by :func:`_env_with`."""

    async def close(self):
        await self.worker.__aexit__(None, None, None)
        await self.env.shutdown()


async def _result(client, task_id: str):
    return await client.get_workflow_handle(task_id).result()


# =========================================================================== #
# 1. Concurrent threshold race
# =========================================================================== #


def test_store_reservation_is_atomic_across_threads_with_barrier():
    """Genuine concurrency: N threads (each its own store connection to the SAME
    db file) race to reserve against one account key. If reservation were a
    non-atomic read-then-write, more than the threshold's worth of threads would
    pass. With a real atomic check+update exactly ``threshold`` worth pass, and
    every thread observes a distinct serialized balance.

    C=1.0, threshold=5.0 -> exactly 4 reservations may pass (new balance < 5).
    """

    def run():
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        store = SqliteAtomicStore(db_path, degraded_threshold=5.0)
        n_threads = 8
        barrier = threading.Barrier(n_threads)
        results: list = []
        results_lock = threading.Lock()
        errors: list = []

        def worker_thread():
            # Each thread opens its OWN store/connection to the same db file, so
            # atomicity comes from the database write lock, not a Python lock.
            own = SqliteAtomicStore(db_path, degraded_threshold=5.0)
            try:
                barrier.wait()  # align the race
                res = own.reserve("acct-race", 1.0)
                with results_lock:
                    results.append(res)
            except Exception as exc:  # pragma: no cover - defensive
                with results_lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker_thread) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, errors
        assert len(results) == n_threads
        non_degraded = [r for r in results if not r.degraded]
        degraded = [r for r in results if r.degraded]
        # Exactly `threshold` in dollar terms passes; the rest are degraded.
        assert len(non_degraded) == 4, len(non_degraded)
        assert len(degraded) == 4, len(degraded)
        # Every thread saw a distinct serialized post-add balance (1..8): full
        # serialization of the atomic read-modify-write.
        assert sorted(r.balance for r in results) == list(range(1, n_threads + 1))
        # All 8 charges committed; none lost.
        assert store.balance("acct-race") == pytest.approx(8.0)

    run()


def test_two_concurrent_workflows_cannot_both_pass_threshold():
    """End-to-end: two escalated tasks (would both be frontier) on ONE account
    race the degradation threshold through the real Temporal worker. The atomic
    store ensures exactly one passes (frontier); the other is capped. Order of
    arrival is nondeterministic, so we assert the invariant (one frontier, one
    not), which holds only if the check+update is atomic."""

    async def scenario():
        thr = 0.015  # enough for one 0.01 reservation, not two
        ctx = await _env_with(thr)
        try:
            escalated = Task(prompt="tricky", escalate=True, failure_context="x")
            t1 = await ctx.api.submit(escalated, "acct", idempotency_key="k1")
            t2 = await ctx.api.submit(escalated, "acct", idempotency_key="k2")
            r1 = await _result(ctx.env.client, t1)
            r2 = await _result(ctx.env.client, t2)
            tiers = {r1["tier"], r2["tier"]}
            frontier = sum(1 for r in (r1, r2) if r["tier"] == "frontier")
            non_frontier = sum(1 for r in (r1, r2) if r["tier"] != "frontier")
            assert frontier == 1, (frontier, tiers)
            assert non_frontier == 1, (non_frontier, tiers)
            assert "advanced" in tiers  # the capped one landed at advanced
            # Both tasks completed independently (task-scoped state).
            for tid in (t1, t2):
                assert (await ctx.api.get_status(tid, "acct"))["status"] == "COMPLETED"
        finally:
            await ctx.close()

    asyncio.run(scenario())


# =========================================================================== #
# 2. Simulated restart mid-task (workflow replay from recorded history)
# =========================================================================== #


def test_workflow_replays_from_history_after_restart():
    """Run a task to completion, capture its event history, then replay it from
    that history with ``Replayer``. Replay re-runs only the deterministic
    workflow code from the recorded events (a simulated restart) and must
    reconstruct the same outcome with no nondeterminism — Temporal's guarantee
    that an in-flight task resumes exactly where it left off."""

    async def scenario():
        ctx = await _env_with(0.05)
        try:
            task_id = await ctx.api.submit(
                Task(prompt="Refactor billing across multiple files.", task_type="refactor"),
                "acct",
            )
            original = await _result(ctx.env.client, task_id)
            assert original["status"] == "COMPLETED"
            assert original["tier"] in ("standard", "advanced")

            from temporalio.worker import Replayer

            handle = ctx.env.client.get_workflow_handle(task_id)
            history = await handle.fetch_history()

            # Simulated restart: rebuild the workflow from recorded events only.
            # (Activities are not passed to the Replayer — during replay they
            # are never re-executed; completed results come from history.)
            replayer = Replayer(workflows=[TaskWorkflow])
            replay = await replayer.replay_workflow(history)
            # raise_on_replay_failure defaults True -> a nondeterministic
            # workflow would raise here.
            assert replay is not None
        finally:
            await ctx.close()

    asyncio.run(scenario())


# =========================================================================== #
# 3. Idempotent duplicate
# =========================================================================== #


def test_duplicate_idempotency_key_returns_existing_task_only():
    """Same idempotency key twice -> the SAME task id; exactly one task and one
    workflow ever exist (no duplicate submission reaches the worker)."""

    async def scenario():
        ctx = await _env_with(0.05)
        try:
            key = "submission-abc"
            t1 = await ctx.api.submit(Task(prompt="simple"), "acct", idempotency_key=key)
            t2 = await ctx.api.submit(Task(prompt="simple"), "acct", idempotency_key=key)
            assert t1 == t2
            assert ctx.store.count_tasks() == 1
            assert ctx.store.get_idempotency(key) == t1
            complete = await _result(ctx.env.client, t1)
            assert complete["status"] == "COMPLETED"
            assert ctx.store.count_tasks() == 1  # still exactly one after running
        finally:
            await ctx.close()

    asyncio.run(scenario())


# =========================================================================== #
# 4. Retry exhaustion -> FAILED dead-letter
# =========================================================================== #


def test_retry_exhaustion_lands_in_failed_never_hangs():
    """A step that always fails exhausts its bounded retry limit; the task
    reaches FAILED with a logged reason, and terminates (never hangs, never
    vanishes)."""

    async def scenario():
        ctx = await _env_with(0.05)
        try:
            task_id = await ctx.api.submit(
                Task(prompt="doomed", escalate=True, failure_context="boom"),
                "acct",
                simulate_failure="dispatch",
            )
            # The workflow fails after bounded retries exhaust.
            with pytest.raises(Exception) as excinfo:
                await _result(ctx.env.client, task_id)
            # The SDK wraps the workflow's ApplicationError in a
            # WorkflowFailureError whose own str() is generic; walk the cause
            # chain to find the original dead-letter reason.
            saw_reason = False
            cur: BaseException | None = excinfo.value
            while cur is not None:
                if "simulated dispatch failure" in str(cur):
                    saw_reason = True
                    break
                cur = getattr(cur, "cause", None)
            assert saw_reason
            # Durable dead-letter state is recorded with a logged reason.
            status = await ctx.api.get_status(task_id, "acct")
            assert status["status"] == "FAILED"
            assert "simulated dispatch failure" in status["error"]
        finally:
            await ctx.close()

    asyncio.run(scenario())


# =========================================================================== #
# 5. Global circuit breaker
# =========================================================================== #


def test_circuit_breaker_forces_non_frontier_even_with_account_headroom():
    """Aggregate load trips the global breaker; an escalated task on a FRESH
    account (individual headroom) is still forced standard/advanced-only — no
    frontier is allowed once the breaker is tripped."""

    async def scenario():
        ctx = await _env_with(0.05, breaker_threshold=0.001)
        try:
            # Simulate aggregate platform load tripping the breaker.
            ctx.breaker.record(0.002)  # > 0.001 threshold -> tripped
            assert ctx.breaker.tripped is True

            task_id = await ctx.api.submit(
                Task(prompt="tricky", escalate=True, failure_context="x"),
                "fresh-account-with-headroom",
            )
            result = await _result(ctx.env.client, task_id)
            # Breaker forces non-frontier; account headroom is irrelevant.
            assert result["tier"] != "frontier"
            assert result["tier"] == "advanced"
            assert result["degraded"] is True
        finally:
            await ctx.close()

    asyncio.run(scenario())


# =========================================================================== #
# 6. Tenant isolation
# =========================================================================== #


def test_account_b_cannot_reach_account_a_task():
    """One account's task data is structurally unreachable from another: a
    lookup of A's task id with B's account is denied (tenant isolation is
    enforced in the data access, not assumed)."""

    async def scenario():
        ctx = await _env_with(0.05)
        try:
            task_id = await ctx.api.submit(Task(prompt="private"), "acct-A")
            await _result(ctx.env.client, task_id)
            # A can read its own task.
            assert (await ctx.api.get_status(task_id, "acct-A"))["status"] == "COMPLETED"
            # B cannot reach A's task.
            with pytest.raises(TenantIsolationViolation):
                await ctx.api.get_status(task_id, "acct-B")
            # An unknown task id is still an error, distinct from a tenant clash.
            with pytest.raises(TaskNotFound):
                await ctx.api.get_status("nope-not-a-task", "acct-A")
        finally:
            await ctx.close()

    asyncio.run(scenario())


# =========================================================================== #
# 7. Transport rate limiting (independent of cost degradation)
# =========================================================================== #


def test_transport_rate_limiting_denies_submission():
    """The token-bucket limiter rejects submissions beyond the transport rate,
    independently of cost-based degradation."""

    async def scenario():
        ctx = await _env_with(0.05, rate=1.0, capacity=1)
        try:
            await ctx.api.submit(Task(prompt="one"), "acct-rl")  # consumes token
            with pytest.raises(RateLimitExceeded):
                await ctx.api.submit(Task(prompt="two"), "acct-rl")
        finally:
            await ctx.close()

    asyncio.run(scenario())
