"""Postgres-backed atomic store for the Master Orchestrator.

Production implementation of the three store protocols defined in
``orchestrator/store.py`` (``UsageStore``, ``IdempotencyStore``,
``TaskStore``) with the same semantics as ``SqliteAtomicStore``.

Concurrency model
-----------------
``reserve()`` performs prune -> SUM -> degraded computation -> INSERT inside a
single transaction, serialized per account by a row-level lock: it first
locks the account's row in ``account_locks`` with ``SELECT ... FOR UPDATE``
(creating the row with ``INSERT ... ON CONFLICT DO NOTHING`` when the account
is seen for the first time). Two concurrent reservations for one account
therefore serialize — the second sees the first one's charge, exactly like
SQLite's ``BEGIN IMMEDIATE`` write lock, but scoped to the account row instead
of the whole database file.

Connection pooling
------------------
Instead of opening a fresh ``psycopg.connect()`` per call, every store method
acquires a connection from a bounded ``psycopg_pool.ConnectionPool`` for the
duration of its transaction and returns it (context manager) — a ceiling on
concurrent DB sockets regardless of traffic. The pool grows on demand
(``min_size=0``) up to ``max_size = max_connections``, so a test-scale or
low-traffic deployment opens only as many connections as are actually needed,
while production traffic is capped at the configured bound. Connections are
thread-safe to share across the orchestrator's worker threads (the pool serves
concurrent ``connection()`` borrowers with distinct connections; per-account
serialization still comes from the row locks, never from client-side
locking).

Schema mirrors ``DEPLOYMENT.md`` §1.2 (which mirrors ``_SCHEMA`` in
``orchestrator/store.py``) plus one extra ``account_locks`` table holding the
per-account lock rows. The extra table is an internal locking detail; the
published contract (``reserve()``/``settle()``/``balance()`` and the three
tables' column names) is unchanged.

Wiring (per the runbook's ``STORE_*`` env knobs)::

    store = PostgresAtomicStore.from_env()  # DATABASE_URL, STORE_* ...

Requires the optional ``postgres`` extra (``psycopg[binary]`` +
``psycopg-pool``); the base install stays temporalio-only.
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from typing import Iterator, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
except ImportError as _psycopg_import_error:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]
    ConnectionPool = None  # type: ignore[assignment]
    _PSYCOPG_IMPORT_ERROR = _psycopg_import_error
else:
    _PSYCOPG_IMPORT_ERROR = None

from orchestrator.store import Reservation

_SCHEMA = """
CREATE TABLE IF NOT EXISTS usage_charges (
    id         BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    amount     DOUBLE PRECISION NOT NULL,
    ts         DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_account ON usage_charges(account_id);
CREATE TABLE IF NOT EXISTS idempotency (
    idempotency_key TEXT PRIMARY KEY,
    task_id         TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    task_id        TEXT PRIMARY KEY,
    account_id     TEXT NOT NULL,
    idempotency_key TEXT,
    correlation_id TEXT,
    status         TEXT NOT NULL,
    error          TEXT,
    tier           TEXT,
    model_pointer  TEXT,
    estimated_cost DOUBLE PRECISION,
    created_at     DOUBLE PRECISION,
    updated_at     DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS account_locks (
    account_id TEXT PRIMARY KEY
);
"""

#: Default bound on the Postgres connection pool (``max_size``).
DEFAULT_MAX_CONNECTIONS = 10


def _require_psycopg() -> None:
    if psycopg is None or ConnectionPool is None:
        raise ImportError(
            "PostgresAtomicStore requires the optional 'postgres' extra "
            "(pip install routing-matrix[postgres]). "
            f"Original import error: {_PSYCOPG_IMPORT_ERROR}"
        )


class PostgresAtomicStore:
    """Durable atomic store backed by Postgres.

    Every read-modify-write runs in a single transaction serialized per
    account by ``SELECT ... FOR UPDATE`` on the account's lock row — genuine
    cross-thread/cross-process atomicity, not a Python ``threading.Lock``.

    Connections come from a bounded ``psycopg_pool.ConnectionPool``
    (``max_size = max_connections``): each method holds one pooled connection
    for the duration of its transaction and returns it on exit, so concurrent
    traffic can never exceed the configured number of DB sockets.

    Args:
        dsn: Postgres DSN, e.g.
            ``postgresql://user:pass@localhost:5432/routing_matrix``.
        degraded_threshold: A reservation whose resulting rolling balance is
            ``>=`` this value marks the account ``degraded`` (capped routing).
        usage_window_seconds: Rolling-window width; charges older than this
            are pruned when computing a balance.
        max_connections: Upper bound on open Postgres connections in the
            pool. The pool grows on demand up to this ceiling.
    """

    def __init__(
        self,
        dsn: str,
        *,
        degraded_threshold: float = 0.0011,
        usage_window_seconds: float = 3600.0,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
    ) -> None:
        _require_psycopg()
        self._dsn = dsn
        self._threshold = float(degraded_threshold)
        self._window = float(usage_window_seconds)
        self._max_connections = int(max_connections)
        if self._max_connections < 1:
            raise ValueError("max_connections must be >= 1")
        if psycopg is None or ConnectionPool is None:  # pragma: no cover
            raise AssertionError("unreachable after _require_psycopg")
        # Bounded, lazily-growing pool. min_size=0: no connections are
        # pre-created — the first transaction opens one, later concurrency
        # grows it toward max_size, and idle ones are reused (putconn).
        # kwargs are forwarded to psycopg.connect() per connection.
        self._pool = ConnectionPool(
            dsn,
            min_size=0,
            max_size=self._max_connections,
            open=False,
            kwargs={"row_factory": dict_row, "connect_timeout": 30},
            name="routing-matrix-pg",
            timeout=30.0,
        )
        self._pool.open()
        self._setup()

    # -- construction helpers ------------------------------------------------- #

    @classmethod
    def from_env(cls, dsn: Optional[str] = None, **overrides) -> "PostgresAtomicStore":
        """Build from the runbook's ``STORE_*`` env knobs (``DEPLOYMENT.md`` C5).

        ``DATABASE_URL`` supplies the DSN; ``STORE_DEGRADED_THRESHOLD``,
        ``STORE_USAGE_WINDOW_SECONDS`` and ``STORE_PG_MAX_CONNECTIONS`` supply
        the tuning knobs. Explicit arguments win over the environment.
        """
        resolved_dsn = dsn or os.environ.get("DATABASE_URL", "")
        if not resolved_dsn:
            raise ValueError(
                "Postgres DSN required: pass dsn= or set DATABASE_URL"
            )
        kwargs = {
            "degraded_threshold": float(
                overrides.get(
                    "degraded_threshold",
                    os.environ.get("STORE_DEGRADED_THRESHOLD", 0.0011),
                )
            ),
            "usage_window_seconds": float(
                overrides.get(
                    "usage_window_seconds",
                    os.environ.get("STORE_USAGE_WINDOW_SECONDS", 3600.0),
                )
            ),
            "max_connections": int(
                overrides.get(
                    "max_connections",
                    os.environ.get("STORE_PG_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS),
                )
            ),
        }
        return cls(resolved_dsn, **kwargs)

    @contextmanager
    def _connection(self) -> Iterator["psycopg.Connection"]:
        """Acquire a pooled connection; commit/rollback + release on exit.

        Wraps ``psycopg_pool.ConnectionPool.connection()``, whose context
        manager returns the connection to the pool on exit and applies
        psycopg's ``with conn`` behaviour (commit on success, rollback on
        error). The explicit ``rollback()`` here runs before release, so a
        failed transaction never re-enters the pool — a connection with an
        aborted transaction would poison the next borrower ("current
        transaction is aborted").
        """
        assert psycopg is not None and self._pool is not None  # pragma: no cover
        with self._pool.connection() as conn:
            try:
                yield conn
            except BaseException:
                try:
                    conn.rollback()
                except Exception:  # pragma: no cover
                    # Connection is broken; the pool discards it on putconn.
                    pass
                raise

    def _setup(self) -> None:
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA)
            conn.commit()

    def _lock_account(self, cur, account_id: str) -> None:
        """Serialize this account's read-modify-write on its lock row.

        Must be called inside an open transaction, before the balance read.
        """
        cur.execute(
            "INSERT INTO account_locks(account_id) VALUES (%s) "
            "ON CONFLICT (account_id) DO NOTHING",
            (account_id,),
        )
        cur.execute(
            "SELECT account_id FROM account_locks WHERE account_id = %s FOR UPDATE",
            (account_id,),
        )

    # -- UsageStore ----------------------------------------------------------- #

    def reserve(self, account_id: str, projected_cost: float) -> Reservation:
        """Atomically check + update one account's rolling usage.

        Same contract as ``SqliteAtomicStore.reserve``: reads the current
        rolling balance and writes the new charge in one transaction, so
        concurrent workflows cannot both slip past the degradation threshold
        undetected. The returned ``degraded`` flag is the value the
        orchestrator passes into ``routing_matrix.route()``.
        """
        now = time.time()
        with self._connection() as conn:
            with conn.cursor() as cur:
                self._lock_account(cur, account_id)
                cur.execute(
                    "DELETE FROM usage_charges WHERE account_id = %s AND ts < %s",
                    (account_id, now - self._window),
                )
                cur.execute(
                    "SELECT COALESCE(SUM(amount), 0.0) AS balance "
                    "FROM usage_charges WHERE account_id = %s",
                    (account_id,),
                )
                balance = float(cur.fetchone()["balance"])
                new_balance = balance + projected_cost
                degraded = new_balance >= self._threshold
                cur.execute(
                    "INSERT INTO usage_charges(account_id, amount, ts) "
                    "VALUES (%s, %s, %s)",
                    (account_id, projected_cost, now),
                )
            conn.commit()
            return Reservation(
                account_id=account_id,
                degraded=degraded,
                projected_cost=projected_cost,
                balance=new_balance,
            )

    def settle(
        self, account_id: str, reserved_cost: float, actual_cost: float
    ) -> float:
        """Atomically reconcile a reservation to the actual routed cost.

        Replaces the most recent unsreconciled reservation of ``reserved_cost``
        with the actual cost and returns the resulting rolling balance.
        """
        now = time.time()
        with self._connection() as conn:
            with conn.cursor() as cur:
                self._lock_account(cur, account_id)
                cur.execute(
                    "DELETE FROM usage_charges WHERE account_id = %s AND ts < %s",
                    (account_id, now - self._window),
                )
                # Find the most recent charge equal to the reserved cost (this
                # workflow's own reservation) and revalue it to the actual cost.
                cur.execute(
                    "SELECT id FROM usage_charges "
                    "WHERE account_id = %s AND amount = %s "
                    "ORDER BY id DESC LIMIT 1",
                    (account_id, reserved_cost),
                )
                row = cur.fetchone()
                if row is not None:
                    cur.execute(
                        "UPDATE usage_charges SET amount = %s WHERE id = %s",
                        (actual_cost, row["id"]),
                    )
                cur.execute(
                    "SELECT COALESCE(SUM(amount), 0.0) AS balance "
                    "FROM usage_charges WHERE account_id = %s",
                    (account_id,),
                )
                balance = float(cur.fetchone()["balance"])
            conn.commit()
            return balance

    def balance(self, account_id: str) -> float:
        now = time.time()
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM usage_charges WHERE account_id = %s AND ts < %s",
                    (account_id, now - self._window),
                )
                cur.execute(
                    "SELECT COALESCE(SUM(amount), 0.0) AS balance "
                    "FROM usage_charges WHERE account_id = %s",
                    (account_id,),
                )
                balance = float(cur.fetchone()["balance"])
            conn.commit()
            return balance

    # -- IdempotencyStore ----------------------------------------------------- #

    def register_idempotency(
        self, idempotency_key: str, task_id: str
    ) -> tuple[str, bool]:
        """Atomically claim an idempotency key.

        Returns ``(task_id, True)`` when this call created the mapping, or
        ``(existing_task_id, False)`` when the key was already present — a
        duplicate submission must return the existing task, never a new one.
        """
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO idempotency(idempotency_key, task_id) "
                    "VALUES (%s, %s) ON CONFLICT (idempotency_key) DO NOTHING "
                    "RETURNING task_id",
                    (idempotency_key, task_id),
                )
                row = cur.fetchone()
                if row is not None:
                    conn.commit()
                    return row["task_id"], True
                cur.execute(
                    "SELECT task_id FROM idempotency WHERE idempotency_key = %s",
                    (idempotency_key,),
                )
                existing = cur.fetchone()
            conn.commit()
            # DO NOTHING fired, so the row must exist (concurrent claim won).
            assert existing is not None
            return existing["task_id"], False

    def get_idempotency(self, idempotency_key: str) -> Optional[str]:
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT task_id FROM idempotency WHERE idempotency_key = %s",
                    (idempotency_key,),
                )
                row = cur.fetchone()
                return row["task_id"] if row else None

    # -- TaskStore ------------------------------------------------------------ #

    def create_task(
        self,
        task_id: str,
        account_id: str,
        idempotency_key: Optional[str],
        correlation_id: str,
    ) -> None:
        now = time.time()
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO tasks"
                    "(task_id, account_id, idempotency_key, correlation_id, "
                    "status, created_at, updated_at)"
                    " VALUES (%s, %s, %s, %s, 'ACCEPTED', %s, %s)"
                    "ON CONFLICT (task_id) DO UPDATE SET "
                    "account_id = EXCLUDED.account_id, "
                    "idempotency_key = EXCLUDED.idempotency_key, "
                    "correlation_id = EXCLUDED.correlation_id, "
                    "status = 'ACCEPTED', created_at = EXCLUDED.created_at, "
                    "updated_at = EXCLUDED.updated_at",
                    (
                        task_id,
                        account_id,
                        idempotency_key,
                        correlation_id,
                        now,
                        now,
                    ),
                )
            conn.commit()

    def set_task_status(
        self,
        task_id: str,
        status: str,
        *,
        error: Optional[str] = None,
        tier: Optional[str] = None,
        model_pointer: Optional[str] = None,
        estimated_cost: Optional[float] = None,
    ) -> None:
        now = time.time()
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET status = %s, error = %s, tier = %s, "
                    "model_pointer = %s, "
                    "estimated_cost = COALESCE(%s, estimated_cost), "
                    "updated_at = %s WHERE task_id = %s",
                    (
                        status,
                        error,
                        tier,
                        model_pointer,
                        estimated_cost,
                        now,
                        task_id,
                    ),
                )
            conn.commit()

    def get_task(self, task_id: str, account_id: str) -> Optional[dict]:
        """Tenant-scoped lookup. Returns ``None`` when the task does not belong
        to ``account_id`` — one account cannot reach another's task state."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM tasks WHERE task_id = %s", (task_id,)
                )
                row = cur.fetchone()
                if row is None:
                    return None
                data = dict(row)
                if data.get("account_id") != account_id:
                    # Tenant isolation: structurally unreachable from another
                    # account.
                    return None
                return data

    def task_owner(self, task_id: str) -> Optional[str]:
        """Return the owning account id, or ``None`` if the task does not exist."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT account_id FROM tasks WHERE task_id = %s", (task_id,)
                )
                row = cur.fetchone()
                return row["account_id"] if row else None

    def count_tasks(self) -> int:
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM tasks")
                return int(cur.fetchone()["n"])

    # -- test/admin helpers ---------------------------------------------------- #

    def reset(self, account_id: Optional[str] = None) -> None:
        """Clear usage/idempotency/tasks (optionally scoped to one account)."""
        with self._connection() as conn:
            with conn.cursor() as cur:
                if account_id is None:
                    cur.execute("DELETE FROM usage_charges")
                    cur.execute("DELETE FROM idempotency")
                    cur.execute("DELETE FROM tasks")
                    cur.execute("DELETE FROM account_locks")
                else:
                    cur.execute(
                        "DELETE FROM usage_charges WHERE account_id = %s",
                        (account_id,),
                    )
                    cur.execute(
                        "DELETE FROM tasks WHERE account_id = %s", (account_id,)
                    )
            conn.commit()

    def close(self) -> None:
        """Close the connection pool; subsequent calls raise ``PoolClosed``.

        Idle connections are closed immediately; connections currently checked
        out (a transaction in flight on another thread) are closed as they are
        returned. ``close()`` is idempotent.
        """
        if self._pool is not None:
            self._pool.close()

    def __enter__(self) -> "PostgresAtomicStore":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()