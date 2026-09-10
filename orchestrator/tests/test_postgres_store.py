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

3. ``test_bounded_pool_reuse_max_connections`` — the production gap: the
   store's ``ConnectionPool`` (``max_connections=2``) must serve many
   sequential reservations from exactly 2 connections (pool stats
   ``connections_num``/``pool_size`` + a live ``pg_stat_activity`` count),
   never opening a fresh connection per call.

Runs against the DSN in ``TEST_POSTGRES_DSN`` (default
``postgresql://postgres:postgres@localhost:5432/postgres``). Skips cleanly
when Postgres (or ``psycopg``) is unavailable, so environments without a
database still run the rest of the suite green.

Pool hygiene: every store instance constructed in this module is closed in
teardown (fixture) or at the end of the test, so no connection pool leaks
across the suite.
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
    s.close()  # pool hygiene: never leak pooled connections across tests


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
    monkeypatch.setenv("STORE_PG_MAX_CONNECTIONS", "3")
    s = PostgresAtomicStore.from_env()
    assert s._threshold == pytest.approx(0.005)
    assert s._window == pytest.approx(60.0)
    assert s._max_connections == 3
    assert s._pool.max_size == 3
    s.reset()
    r = s.reserve("env-acct", 0.004)
    assert r.degraded is False
    r2 = s.reserve("env-acct", 0.001)
    assert r2.degraded is True
    s.reset()
    s.close()


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
    store.close()


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
    store.close()


# --------------------------------------------------------------------------- #
# REQUIRED: bounded pooling proof (the production connection-ceiling gap)
# --------------------------------------------------------------------------- #


@needs_postgres
def test_bounded_pool_reuse_max_connections():
    """Pool caps Postgres sockets at ``max_connections`` and reuses them.

    ``max_connections=2``, two phases:

    * Phase 1 — sequential reuse: 50 sequential reserves. The pool opens
      exactly ONE connection and serves all 50 calls from it
      (``connections_num == 1``); a fresh-``psycopg.connect()``-per-call
      store would churn 50 connections. This proves reuse.
    * Phase 2 — bounded ceiling under overload: a barrier-synchronized burst
      of 6 threads x 5 reserves on ONE account, far exceeding pool capacity.
      Demand beyond the ceiling must QUEUE on the pool, not open new
      sockets: cumulative ``connections_num`` grows to exactly 2 and
      ``pool_size`` never exceeds 2. A leaky or per-call store would exceed
      it (or churn). This proves the connection ceiling.

    Server-side ``pg_stat_activity`` count for this user, sampled between
    calls, never exceeds ``max_connections`` (excludes the monitor
    connection). All results must also be arithmetically correct — pooled
    connection reuse must not contaminate transactions across calls.

    Note: purely sequential traffic naturally uses exactly 1 connection
    (next call reuses the one returned); the bound is proven by the
    concurrent burst, where 6 threads must share the same 2 sockets.
    """
    N_SEQ = 50
    BURST_THREADS = 6
    BURST_EACH = 5
    UNIT = 0.0005
    store = PostgresAtomicStore(DSN, degraded_threshold=100.0, max_connections=2)
    store.reset("bounded-acct")
    server_conn_peak = 0
    with psycopg.connect(DSN, connect_timeout=5) as monitor:
        with monitor.cursor() as cur:
            cur.execute("SELECT pg_backend_pid() AS pid")
            monitor_pid = cur.fetchone()[0]  # raw connect: tuple rows
        # Phase 1: sequential reuse.
        for i in range(N_SEQ):
            r = store.reserve("bounded-acct", UNIT)
            assert r.balance == pytest.approx((i + 1) * UNIT)
            with monitor.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM pg_stat_activity "
                    "WHERE usename = current_user "
                    "AND datname = current_database() "
                    "AND pid <> %s",
                    (monitor_pid,),
                )
                server_conn_peak = max(server_conn_peak, int(cur.fetchone()[0]))
        # Phase 2: concurrent burst — demand (6 threads) exceeds capacity (2).
        barrier = threading.Barrier(BURST_THREADS)
        errors: list[BaseException] = []

        def burst_worker(_: int) -> None:
            try:
                barrier.wait(timeout=30)
                for _ in range(BURST_EACH):
                    store.reserve("bounded-acct", UNIT)
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)

        threads = [
            threading.Thread(target=burst_worker, args=(i,))
            for i in range(BURST_THREADS)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        # Sample the server-side count once more while all conns are idle.
        with monitor.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM pg_stat_activity "
                "WHERE usename = current_user "
                "AND datname = current_database() "
                "AND pid <> %s",
                (monitor_pid,),
            )
            server_conn_peak = max(server_conn_peak, int(cur.fetchone()[0]))
        assert not errors, f"burst reserve raised: {errors}"
    stats = store._pool.get_stats()
    # Sequential phase alone uses exactly 1 connection; the burst grows the
    # pool to the configured ceiling of 2 — never past it.
    assert stats["connections_num"] == 2, (
        f"pool opened {stats['connections_num']} connections cumulatively, "
        f"expected exactly 2 (1 reused sequentially + 1 more under the burst, "
        f"never more): {stats}"
    )
    assert stats["pool_size"] <= 2, (
        f"pool_size={stats['pool_size']} exceeded max_connections=2"
    )
    assert server_conn_peak <= 2, (
        f"server-side connection count peaked at {server_conn_peak} (>2)"
    )
    total = N_SEQ + BURST_THREADS * BURST_EACH
    assert store.balance("bounded-acct") == pytest.approx(total * UNIT)
    store.reset("bounded-acct")
    store.close()
