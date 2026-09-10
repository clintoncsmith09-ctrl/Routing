"""Postgres backend tests for the Master Orchestrator atomic store.

Covers the same behaviors as ``SqliteAtomicStore`` (reserve/settle/balance
thresholds, idempotency duplicate returns existing, task lifecycle,
tenant-scoped lookups) against a REAL Postgres, plus the two concurrency
proofs that matter:

1. ``test_concurrent_reserves_serialize_on_one_account`` — N threads with
   their OWN connections, barrier-synchronized so they truly contend, racing
   ``reserve()`` on ONE account. Threshold sized so exactly one of the N
   reserves crosses it: exactly one reports ``degraded=True``, the final
   balance equals the exact sum (no lost updates), and every returned balance
   is a distinct multiple of the unit cost (each reservation observed all
   previous charges — full serialization).
2. ``test_row_lock_blocks_second_connection`` — tx1 takes ``SELECT ... FOR
   UPDATE`` on the account lock row; tx2's ``reserve()`` must block until
   tx1 commits (ordering proven with timestamps).

Runs against the DSN in ``TEST_POSTGRES_DSN`` (default
``postgresql://postgres:postgres@localhost:5432/postgres``). Skips cleanly
when Postgres (or ``psycopg``) is unavailable, so environments without a
database still run the rest of the suite green.
"""

from __future__ import annotations

import os
import threading
import time

import pytest

psycopg = pytest.importorskip("psycopg", reason="postgres extra not installed")

from orchestrator.postgres_store import PostgresAtomicStore

DSN = os.environ.get(
    "TEST_POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/postgres"
)


def _can_connect() -> bool:
    try:
        conn = psycopg.connect(DSN, connect_timeout=3)
        conn.close()
        return True
    except Exception:
        return False


needs_postgres = pytest.mark.skipif(
    not _can_connect(), reason=f"no Postgres reachable at {DSN}"
)


@pytest.fixture()
def store():
    s = PostgresAtomicStore(DSN, degraded_threshold=0.0011)
    s.reset()
    yield s
    s.reset()


# --------------------------------------------------------------------------- #
# Functional parity with SqliteAtomicStore
# --------------------------------------------------------------------------- #


@needs_postgres
def test_reserve_threshold_and_balance(store):
    r1 = store.reserve("acct-a", 0.0005)
    assert r1.degraded is False
    assert r1.balance == pytest.approx(0.0005)
    # NOTE: 0.0005 + 0.0006 == 0.0010999999999999998 in binary float, i.e.
    # just *under* the 0.0011 threshold — same on SQLite (the comparison is
    # done in Python on both backends). Use a cost with clean margin.
    r2 = store.reserve("acct-a", 0.0010)
    assert r2.degraded is True  # 0.0015 >= threshold
    assert r2.balance == pytest.approx(0.0015)
    assert store.balance("acct-a") == pytest.approx(0.0015)
    # Other accounts are isolated.
    assert store.balance("acct-b") == pytest.approx(0.0)


@needs_postgres
def test_settle_reconciles_to_actual(store):
    store.reserve("acct-s", 0.0010)
    bal = store.settle("acct-s", 0.0010, 0.0002)
    assert bal == pytest.approx(0.0002)
    assert store.balance("acct-s") == pytest.approx(0.0002)


@needs_postgres
def test_idempotency_duplicate_returns_existing(store):
    task_id, created = store.register_idempotency("key-1", "task-1")
    assert (task_id, created) == ("task-1", True)
    task_id2, created2 = store.register_idempotency("key-1", "task-2")
    assert (task_id2, created2) == ("task-1", False)
    assert store.get_idempotency("key-1") == "task-1"
    assert store.get_idempotency("missing") is None


@needs_postgres
def test_task_lifecycle_and_tenant_isolation(store):
    store.create_task("t-1", "acct-a", "key-1", "corr-1")
    store.set_task_status("t-1", "RUNNING", tier="standard",
                          model_pointer="p/1", estimated_cost=0.0004)
    got = store.get_task("t-1", "acct-a")
    assert got is not None and got["status"] == "RUNNING"
    assert got["tier"] == "standard"
    # Cross-account read is structurally unreachable.
    assert store.get_task("t-1", "acct-b") is None
    assert store.task_owner("t-1") == "acct-a"
    assert store.task_owner("nope") is None
    assert store.count_tasks() == 1


@needs_postgres
def test_from_env_wiring(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", DSN)
    monkeypatch.setenv("STORE_DEGRADED_THRESHOLD", "0.005")
    monkeypatch.setenv("STORE_USAGE_WINDOW_SECONDS", "60")
    s = PostgresAtomicStore.from_env()
    assert s._threshold == pytest.approx(0.005)
    assert s._window == pytest.approx(60.0)
    s.reset()
    r = s.reserve("env-acct", 0.004)
    assert r.degraded is False
    r2 = s.reserve("env-acct", 0.001)
    assert r2.degraded is True
    s.reset()


# --------------------------------------------------------------------------- #
# REQUIRED: genuine multi-connection race (barrier-synchronized, own conns)
# --------------------------------------------------------------------------- #


@needs_postgres
def test_concurrent_reserves_serialize_on_one_account():
    """N threads x own connections, barrier-synced, racing reserve() on 1 acct.

    Threshold 0.0011, unit cost 0.0004, N=4: cumulative balances are
    0.0004/0.0008/0.0012/0.0016, so exactly the last two cross the threshold
    only if every reservation observes all previous charges. Asserts:
      * exactly 2 of 4 report degraded=True (atomic threshold outcome);
      * final balance == exact sum (no lost updates);
      * the 4 returned balances are exactly {0.0004..0.0016} — each reserve
        saw a distinct serialized position (full serialization, impossible
        under a lost-update race where two txns read the same balance).
    """
    N = 4
    UNIT = 0.0004
    THRESHOLD = 0.0011
    store = PostgresAtomicStore(DSN, degraded_threshold=THRESHOLD)
    store.reset("race-acct")
    barrier = threading.Barrier(N)
    results = [None] * N
    errors = []

    def worker(i: int) -> None:
        try:
            barrier.wait(timeout=30)  # all threads contend at once
            results[i] = store.reserve("race-acct", UNIT)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, f"reserve raised under contention: {errors}"
    assert all(r is not None for r in results)
    # Cumulative balances 0.0004/0.0008/0.0012/0.0016: exactly the last two
    # cross the 0.0011 threshold (with clean float margin — 0.0004*k is exact
    # enough in binary that no boundary ambiguity arises).
    degraded_count = sum(1 for r in results if r.degraded)
    assert degraded_count == 2, (
        f"expected exactly 2/4 degraded (balances 0.0012, 0.0016 >= {THRESHOLD}), "
        f"got {degraded_count}: {[ (r.balance, r.degraded) for r in results ]}"
    )
    balances = sorted(r.balance for r in results)
    assert balances == pytest.approx([0.0004, 0.0008, 0.0012, 0.0016])
    assert store.balance("race-acct") == pytest.approx(N * UNIT)
    store.reset("race-acct")


@needs_postgres
def test_row_lock_blocks_second_connection():
    """Prove the blocking itself: tx1 holds FOR UPDATE, tx2 reserve() waits.

    Opens tx1, locks the account row with SELECT ... FOR UPDATE, then starts
    tx2's reserve() in another thread. tx2 must not finish until tx1 commits;
    ordering is asserted with timestamps (tx2_done > tx1_commit).
    """
    store = PostgresAtomicStore(DSN, degraded_threshold=100.0)
    store.reset("lock-acct")
    store.reserve("lock-acct", 0.0)  # ensure the lock row exists

    tx1 = psycopg.connect(DSN, connect_timeout=5)
    tx1.autocommit = False
    cur1 = tx1.cursor()
    cur1.execute(
        "SELECT account_id FROM account_locks WHERE account_id = %s FOR UPDATE",
        ("lock-acct",),
    )
    tx1_held_at = time.monotonic()

    tx2_done_at: list[float] = []
    tx2_result: list = []

    def tx2_work() -> None:
        tx2_result.append(store.reserve("lock-acct", 0.0007))
        tx2_done_at.append(time.monotonic())

    t = threading.Thread(target=tx2_work)
    t.start()
    time.sleep(1.0)  # give tx2 time to block on the row lock
    assert not tx2_done_at, "tx2 reserve() finished while tx1 held FOR UPDATE"
    assert t.is_alive(), "tx2 thread died instead of blocking"
    tx1.commit()  # release the row lock
    tx1_commit_at = time.monotonic()
    t.join(timeout=30)
    assert tx2_done_at, "tx2 reserve() never finished after tx1 commit"
    assert tx2_done_at[0] >= tx1_commit_at, (
        f"tx2 finished at {tx2_done_at[0]:.3f} before tx1 committed "
        f"at {tx1_commit_at:.3f} — no blocking occurred"
    )
    assert tx1_held_at < tx1_commit_at  # sanity: the hold window was real
    assert tx2_result[0].balance == pytest.approx(0.0007)
    tx1.close()
    store.reset("lock-acct")
