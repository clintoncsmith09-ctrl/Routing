"""External, atomically-updated store for the Master Orchestrator.

This is the replacement for the old in-memory ``UsageLedger``. Account-level
usage (and idempotency + task metadata) lives in an externally persisted store
that many concurrent task-workflows for one account read and write. It can
never live inside a single workflow instance or in-process dict, because
concurrent workflows on the same account must observe each other's charges.

Design
------
This module defines narrow interface protocols (``UsageStore``,
``IdempotencyStore``, ``TaskStore``) and one concrete implementation,
``SqliteAtomicStore``, which satisfies all three. SQLite is used here purely as
a real, locally-persisted atomic primitive: every read-modify-write happens in
a single ``BEGIN IMMEDIATE`` transaction, which takes an exclusive write lock
on the database. That guarantees atomic check+update across threads *and*
across processes, which is exactly the property a Postgres row-lock or a Redis
``INCR``/WATCH provides in production.

How it maps to the constraint
-----------------------------
- ``reserve()`` is the ONE atomic operation the spec requires: it atomically
  reads the account's rolling-usage balance, computes the ``degraded`` flag
  from whether this task's projected cost would push the balance at/over the
  threshold, and writes the charge — all in a single transaction. Two
  concurrent tasks on one account therefore cannot both pass the degradation
  threshold undetected: the second transaction sees the first one's charge
  because both serialize on the database write lock.
- ``settle()`` later reconciles the projected (pre-route) cost with the actual
  (post-route) cost in the same atomic manner.
- Idempotency registration and task metadata are stored in the same database
  so the whole facade is durable.

Swapping the backing store
--------------------------
To run on Postgres/Redis in production, implement ``UsageStore``,
``IdempotencyStore`` and ``TaskStore`` against that backend and pass the object
into the worker/API. The workflow and activities never care which concrete
store backs the interface:
- Postgres: ``reserve()`` becomes ``INSERT ... ON CONFLICT`` / a ``SELECT ...
  FOR UPDATE`` row lock on the account row followed by an update — row-level
  locking serializes concurrent reservations for one account.
- Redis: keep the rolling balance + threshold check in a Lua script executed
  with ``EVAL`` (atomic by construction), or use ``INCRBYFLOAT`` + a
  ``WATCH``/``MULTI`` transaction for the degraded computation.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Protocol


# --------------------------------------------------------------------------- #
# Interfaces (the swap seam)
# --------------------------------------------------------------------------- #


@dataclass
class Reservation:
    """Outcome of atomically reserving one projected cost against an account."""

    account_id: str
    degraded: bool  # whether the *reserved* balance is at/over the threshold
    projected_cost: float  # the cost that was reserved
    balance: float  # account rolling-usage balance AFTER the reservation


class UsageStore(Protocol):
    """Atomic per-account rolling usage, externally persisted."""

    def reserve(self, account_id: str, projected_cost: float) -> Reservation: ...
    def settle(self, account_id: str, reserved_cost: float, actual_cost: float) -> float: ...
    def balance(self, account_id: str) -> float: ...


class IdempotencyStore(Protocol):
    """Durable idempotency-key -> task_id mapping (atomic claim)."""

    def register_idempotency(self, idempotency_key: str, task_id: str) -> tuple[str, bool]: ...
    def get_idempotency(self, idempotency_key: str) -> Optional[str]: ...


class TaskStore(Protocol):
    """Durable task metadata used by the polling facade (tenant-scoped)."""

    def create_task(
        self,
        task_id: str,
        account_id: str,
        idempotency_key: Optional[str],
        correlation_id: str,
    ) -> None: ...
    def set_task_status(
        self,
        task_id: str,
        status: str,
        *,
        error: Optional[str] = None,
        tier: Optional[str] = None,
        model_pointer: Optional[str] = None,
        estimated_cost: Optional[float] = None,
    ) -> None: ...
    def get_task(self, task_id: str, account_id: str) -> Optional[dict]: ...
    def task_owner(self, task_id: str) -> Optional[str]:
        """Account that owns ``task_id``, or ``None`` if it does not exist.

        Used by the API to distinguish "task not found" from "task exists but
        is owned by another account" (tenant isolation) without ever returning
        another account's task data.
        """
        ...
    def count_tasks(self) -> int: ...


# --------------------------------------------------------------------------- #
# Concrete: SQLite-backed atomic store
# --------------------------------------------------------------------------- #

_SCHEMA = """
CREATE TABLE IF NOT EXISTS usage_charges (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    amount     REAL NOT NULL,
    ts         REAL NOT NULL
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
    estimated_cost REAL,
    created_at     REAL,
    updated_at     REAL
);
"""


class SqliteAtomicStore:
    """Durable atomic store backed by a SQLite database file.

    Every write operation runs in a single ``BEGIN IMMEDIATE`` transaction so
    that concurrent reservation/registration calls serialize on the file
    write-lock — genuine cross-thread/cross-process atomicity, not a Python
    ``threading.Lock`` around separate reads and writes.

    Args:
        db_path: Path to the SQLite file (persistent across restarts).
        degraded_threshold: A reservation whose resulting rolling balance is
            ``>=`` this value marks the account ``degraded`` (capped routing).
        usage_window_seconds: Rolling-window width; charges older than this
            are pruned when computing a balance.
    """

    def __init__(
        self,
        db_path: str,
        *,
        degraded_threshold: float = 0.0011,
        usage_window_seconds: float = 3600.0,
    ) -> None:
        self._db_path = db_path
        self._threshold = float(degraded_threshold)
        self._window = float(usage_window_seconds)
        self._setup()

    # -- connection helpers --------------------------------------------------- #

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _setup(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # -- UsageStore ----------------------------------------------------------- #

    def reserve(self, account_id: str, projected_cost: float) -> Reservation:
        """Atomically check + update one account's rolling usage.

        This is the single atomic operation the architecture requires: it reads
        the current rolling balance and writes the new charge in one
        transaction, so concurrent workflows cannot both slip past the
        degradation threshold undetected. The returned ``degraded`` flag is the
        value the orchestrator passes into ``routing_matrix.route()``.
        """
        now = time.time()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._prune(conn, account_id, now)
            cur = conn.execute(
                "SELECT COALESCE(SUM(amount), 0.0) FROM usage_charges WHERE account_id = ?",
                (account_id,),
            )
            balance = float(cur.fetchone()[0])
            new_balance = balance + projected_cost
            degraded = new_balance >= self._threshold
            conn.execute(
                "INSERT INTO usage_charges(account_id, amount, ts) VALUES (?, ?, ?)",
                (account_id, projected_cost, now),
            )
            conn.commit()
            return Reservation(
                account_id=account_id,
                degraded=degraded,
                projected_cost=projected_cost,
                balance=new_balance,
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def settle(self, account_id: str, reserved_cost: float, actual_cost: float) -> float:
        """Atomically reconcile a reservation to the actual routed cost.

        Replaces the most recent unsreconciled reservation of ``reserved_cost``
        with the actual cost and returns the resulting rolling balance.
        """
        now = time.time()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._prune(conn, account_id, now)
            # Find the most recent charge equal to the reserved cost (this
            # workflow's own reservation) and revalue it to the actual cost.
            cur = conn.execute(
                "SELECT id FROM usage_charges WHERE account_id = ? AND amount = ? "
                "ORDER BY id DESC LIMIT 1",
                (account_id, reserved_cost),
            )
            row = cur.fetchone()
            if row is not None:
                conn.execute(
                    "UPDATE usage_charges SET amount = ? WHERE id = ?",
                    (actual_cost, row["id"]),
                )
            cur = conn.execute(
                "SELECT COALESCE(SUM(amount), 0.0) FROM usage_charges WHERE account_id = ?",
                (account_id,),
            )
            balance = float(cur.fetchone()[0])
            conn.commit()
            return balance
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def balance(self, account_id: str) -> float:
        now = time.time()
        conn = self._connect()
        try:
            self._prune(conn, account_id, now)
            cur = conn.execute(
                "SELECT COALESCE(SUM(amount), 0.0) FROM usage_charges WHERE account_id = ?",
                (account_id,),
            )
            return float(cur.fetchone()[0])
        finally:
            conn.close()

    def _prune(self, conn: sqlite3.Connection, account_id: str, now: float) -> None:
        conn.execute(
            "DELETE FROM usage_charges WHERE account_id = ? AND ts < ?",
            (account_id, now - self._window),
        )

    # -- IdempotencyStore ----------------------------------------------------- #

    def register_idempotency(self, idempotency_key: str, task_id: str) -> tuple[str, bool]:
        """Atomically claim an idempotency key.

        Returns ``(task_id, True)`` when this call created the mapping, or
        ``(existing_task_id, False)`` when the key was already present — a
        duplicate submission must return the existing task, never a new one.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "SELECT task_id FROM idempotency WHERE idempotency_key = ?",
                (idempotency_key,),
            )
            row = cur.fetchone()
            if row is not None:
                conn.commit()
                return row["task_id"], False
            conn.execute(
                "INSERT INTO idempotency(idempotency_key, task_id) VALUES (?, ?)",
                (idempotency_key, task_id),
            )
            conn.commit()
            return task_id, True
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_idempotency(self, idempotency_key: str) -> Optional[str]:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT task_id FROM idempotency WHERE idempotency_key = ?",
                (idempotency_key,),
            )
            row = cur.fetchone()
            return row["task_id"] if row else None
        finally:
            conn.close()

    # -- TaskStore ------------------------------------------------------------ #

    def create_task(
        self,
        task_id: str,
        account_id: str,
        idempotency_key: Optional[str],
        correlation_id: str,
    ) -> None:
        now = time.time()
        conn = self._connect()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO tasks"
                "(task_id, account_id, idempotency_key, correlation_id, status, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, 'ACCEPTED', ?, ?)",
                (task_id, account_id, idempotency_key, correlation_id, now, now),
            )
            conn.commit()
        finally:
            conn.close()

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
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE tasks SET status = ?, error = ?, tier = ?, model_pointer = ?, "
                "estimated_cost = COALESCE(?, estimated_cost), updated_at = ? WHERE task_id = ?",
                (status, error, tier, model_pointer, estimated_cost, now, task_id),
            )
            conn.commit()
        finally:
            conn.close()

    def get_task(self, task_id: str, account_id: str) -> Optional[dict]:
        """Tenant-scoped lookup. Returns ``None`` when the task does not belong
        to ``account_id`` — one account cannot reach another's task state."""
        conn = self._connect()
        try:
            cur = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
            row = cur.fetchone()
            if row is None:
                return None
            data = dict(row)
            if data.get("account_id") != account_id:
                # Tenant isolation: structurally unreachable from another account.
                return None
            return data
        finally:
            conn.close()

    def task_owner(self, task_id: str) -> Optional[str]:
        """Return the owning account id, or ``None`` if the task does not exist."""
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT account_id FROM tasks WHERE task_id = ?", (task_id,)
            )
            row = cur.fetchone()
            return row["account_id"] if row else None
        finally:
            conn.close()

    def count_tasks(self) -> int:
        conn = self._connect()
        try:
            cur = conn.execute("SELECT COUNT(*) AS n FROM tasks")
            return int(cur.fetchone()["n"])
        finally:
            conn.close()

    # -- test/admin helpers ---------------------------------------------------- #

    def reset(self, account_id: Optional[str] = None) -> None:
        """Clear usage/idempotency/tasks (optionally scoped to one account)."""
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            if account_id is None:
                conn.execute("DELETE FROM usage_charges")
                conn.execute("DELETE FROM idempotency")
                conn.execute("DELETE FROM tasks")
            else:
                conn.execute(
                    "DELETE FROM usage_charges WHERE account_id = ?", (account_id,)
                )
                conn.execute(
                    "DELETE FROM tasks WHERE account_id = ?", (account_id,)
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def close(self) -> None:
        """No-op for sqlite (connections are short-lived); kept for parity."""
