# Master Orchestrator — Production Deployment Runbook

Grounded in the committed source at commit `4ddd662` (`main`). Every claim
below cites the file and line it comes from. If code and this doc ever
disagree, the code wins — update this doc.

Target: run the **Master Orchestrator** (the durable Temporal layer) in
production: a real Temporal server, one or more long-running Python Worker
processes, an external atomic store, and the async API facade. The `route()` /
Routing Matrix library itself is stateless and needs nothing but environment
variables (§1.3).

> Note: `routing_matrix/README.md` does not exist at this commit; the router's
> README is the repository-root `README.md`. Its env-var table is at
> `README.md:39-53`.

Sections are labelled by requirement tier:

1. **Required to run at all (production)**
2. **Required only for multi-worker / high availability**
3. **Stub / later-phase integration**
4. **Checklist** — owner must provision vs. owner must configure/decide
5. **Running the test suite**

---

## 1. Required to run at all (production)

There is no "dev mode" that becomes production by flipping a flag. The test
suite proves durability with `temporalio.testing.WorkflowEnvironment`
(`orchestrator/tests/test_robustness.py:75-77` — `WorkflowEnvironment.start_local()`),
which is an **in-process server** and cannot serve production traffic. You must
provision the four things in this section before the orchestrator can run.

### 1.1 A real Temporal server + a long-running Worker + a Client

**What to provision**

| Piece | What it is | Where cited |
|---|---|---|
| Temporal server (or Temporal Cloud namespace) | The durable workflow engine that persists every task's event history | `Client` connection required by `OrchestratorAPI` (`orchestrator/api.py:23,62-74`); workflow history is replayed after restart (`orchestrator/workflow.py:15-21`) |
| Temporal namespace | SDK default is `"default"` (`Client.connect(..., namespace="default")`, temporalio 1.32.0). A self-hosted dev server auto-creates `"default"`; **a custom namespace must be created manually** on the server / in Cloud before first use | `orchestrator/api.py:62-74`; `orchestrator/workflow.py:472-493` (task queue); SDK `Client.connect` signature |
| Task queue | **`master-orchestrator`** — this name is hardcoded as `TASK_QUEUE` and as the API default. The Client/API and the Worker **must agree on it** or workflows never get picked up | `TASK_QUEUE = "master-orchestrator"` — `orchestrator/workflow.py:46`; `create_worker(..., task_queue: str = TASK_QUEUE)` — `orchestrator/workflow.py:472-474`; `OrchestratorAPI(..., task_queue: str = "master-orchestrator")` — `orchestrator/api.py:64-65` |
| Long-running Worker process(es) | A process that polls the task queue and executes workflows + activities. Wired via `create_worker(client, store=..., circuit_breaker=..., rate_limiter=...)` — the returned `Worker` is an async context manager; run it to completion (`async with worker: await asyncio.Future()`) | `create_worker` — `orchestrator/workflow.py:472-493`; the worker registers `TaskWorkflow` + the 9 activities in `ALL_ACTIVITIES` (`orchestrator/workflow.py:319-329`); usage pattern in tests: `orchestrator/tests/test_robustness.py:85-91` |
| A Temporal `Client` for the Worker **and** for the API process | The API starts workflows via `client.start_workflow(...)` (`orchestrator/api.py:126-143`); the Worker needs its own client | `orchestrator/api.py:126-143`, `orchestrator/workflow.py:488-492` |

**Client + Worker environment (env vars — named exactly)**

The temporalio SDK (1.32.0, the version installed in this repo's `.venv`)
reads client configuration from environment variables (verified against the
installed `temporalio/envconfig.py`). The operator's launcher constructs the
client with `Client.connect(...)` and these env vars must be set in **both**
the API process and every Worker process:

| Env var | Purpose | Notes |
|---|---|---|
| `TEMPORAL_ADDRESS` | `host:port` of the Temporal frontend (self-hosted) or Cloud endpoint | e.g. `localhost:7233` for dev; `<namespace>.tmprl.cloud:7233` for Cloud |
| `TEMPORAL_NAMESPACE` | Namespace name | Default `"default"` if unset (SDK `Client.connect` default). Must match what you provisioned |
| `TEMPORAL_TLS` / `TEMPORAL_USE_TLS` | Enable TLS for the gRPC channel | `"true"` when talking to Cloud or a TLS-terminated server |
| `TEMPORAL_API_KEY` | API key → `Authorization: Bearer` header | Required for Temporal Cloud; API keys are a separate Cloud namespace-level credential, not an mTLS cert |
| `TEMPORAL_CLIENT_CERT` / `TEMPORAL_CLIENT_KEY` | Client certificate + private key for mTLS | Cloud requires these (or the `*_DATA` / `*_PATH` variants). `TEMPORAL_TLS_SERVER_CA_CERT_PATH`/`_DATA`, `TEMPORAL_TLS_SERVER_NAME`, `TEMPORAL_TLS_DISABLE_HOST_VERIFICATION` also honored |
| `TEMPORAL_CONFIG_FILE` | Optional TOML file with the same client settings (address/namespace/api_key/tls/grpc_meta) | Alternative to individual env vars |

Version pin: install **`temporalio==1.32.0`** in the deployment environment.
It is the version this repo is tested against (`.venv`), but note
`pyproject.toml` does **not** declare it — `pyproject.toml:13-15` only lists
`pytest>=7.0` under `test`. The deployment must add the temporalio dependency
itself.

**One Worker per process.** `create_worker` writes the shared dependency
container `_DEPS` at module scope (`orchestrator/workflow.py:70-77,485-487`),
so a single Python process should run exactly one Worker. Run more capacity
as more processes (that is also when §2 kicks in).

### 1.2 The external atomic store

Account-level usage, idempotency keys, and task metadata **must** live in an
externally persisted, atomically-updated store accessed only via activities —
never in-process, never scoped to one workflow instance
(`orchestrator/store.py:1-7`, architecture constraint in `WORKFLOW.md`).

**What ships today:** `SqliteAtomicStore` (`orchestrator/store.py:150`), a
real, locally-persisted atomic primitive: every read-modify-write runs in one
`BEGIN IMMEDIATE` transaction, an exclusive write lock (`orchestrator/store.py:14-18,
153-157, 208`). It satisfies the three interface protocols
(`UsageStore` — `orchestrator/store.py:71-76`; `IdempotencyStore` —
`orchestrator/store.py:79-83`; `TaskStore` — `orchestrator/store.py:86-115`).
The workflow and API never care which concrete store backs the interface —
the swap seam is described at `orchestrator/store.py:34-45`.

**Production:** swap in a Postgres- or Redis-backed implementation of the same
three protocols and pass the object into `create_worker(...)` and
`OrchestratorAPI(...)` (they take `store=...` / `store: SqliteAtomicStore` —
`orchestrator/workflow.py:476-487`, `orchestrator/api.py:62-73`). The repo
does **not** currently ship a Postgres/Redis implementation, so the operator's
production glue must implement the protocols and construct the object from
the env vars below.

**Postgres — exact table requirements** (mirror `_SCHEMA`,
`orchestrator/store.py:122-147`):

```sql
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
```

(varchar vs. text and `REAL` vs. `DOUBLE PRECISION` are equivalent for this
purpose; keep column names exactly as above since the SQL is constructed by
name.)

**`reserve()` on Postgres** — must be the *one atomic check+update* that the
architecture requires (`orchestrator/store.py:196-232` in SQLite: prune →
`SUM(amount)` → `degraded = new_balance >= threshold` → `INSERT` in one
transaction). On Postgres use row-level locking per `orchestrator/store.py:40-42`:

```sql
BEGIN;  -- or one statement with ON CONFLICT
SELECT balance FROM account_usage WHERE account_id = ? FOR UPDATE;  -- serializes concurrent reservations per account
-- compute new_balance, degraded = (new_balance >= threshold)
UPDATE account_usage SET balance = new_balance WHERE account_id = ?;
INSERT INTO usage_charges(account_id, amount, ts) VALUES (?, projected_cost, now);
COMMIT;
```

(`account_usage(account_id TEXT PRIMARY KEY, balance DOUBLE PRECISION)` is an
additional balance row the SQLite implementation derives on the fly; the
published contract is `reserve()`/`settle()`/`balance()` — `orchestrator/store.py:74-76`.
`settle()` revalues the most recent charge equal to the reserved cost and
returns the new balance — `orchestrator/store.py:234-269`.)

**Redis — required keys + atomic semantics.** Keep the check-and-update in a
Lua script executed with `EVAL` (atomic by construction), or `INCRBYFLOAT` +
`WATCH`/`MULTI` (`orchestrator/store.py:43-45`):

| Key | Type | Contents | Used for |
|---|---|---|---|
| `usage_charges:<account_id>` | Sorted set | member = `cost:<uuid>`, score = timestamp | rolling-window charges; prune with `ZREMRANGEBYSCORE` older than `now - window` |
| `idempotency:<key>` | String | `task_id` | `SET` with the "claim" semantics: return `(existing, False)` if already present, else `(new, True)` — mirrors `register_idempotency` (`orchestrator/store.py:292-320`); `GET idempotency:<key>` for `get_idempotency` (`orchestrator/store.py:322-332`) |
| `task:<task_id>` | Hash | `account_id, idempotency_key, correlation_id, status, error, tier, model_pointer, estimated_cost, created_at, updated_at` | durable task row (`orchestrator/store.py:336-405`) |

`reserve()` in Lua: `ZREMRANGEBYSCORE` old charges → `ZADD` the new charge →
`ZSUM`/`ZSCORE` aggregate → `degraded = balance >= threshold` → return
`(account_id, degraded, projected_cost, balance)`. Exactly one reservation
observes the previous one because EVAL runs the script atomically.

**Store env vars the operator must provide** (the production launcher reads
these; the repo code itself does not read them today):

| Env var | Shape | Meaning |
|---|---|---|
| `DATABASE_URL` | Postgres DSN, e.g. `postgresql://user:pass@host:5432/routing_matrix` | Backing DB for the production store |
| `REDIS_URL` | `redis://host:6379/0` (or `rediss://` for TLS) | Backing Redis if using the Redis implementation |
| `STORE_DEGRADED_THRESHOLD` | float, default **`0.0011`** | A reservation whose resulting rolling balance is `>=` this marks the account degraded (capped routing) — constructor default `orchestrator/store.py:170` |
| `STORE_USAGE_WINDOW_SECONDS` | float, default **`3600.0`** | Rolling-window width; charges older than this are pruned — constructor default `orchestrator/store.py:171` |

(The last two are constructor params of `SqliteAtomicStore` today —
`orchestrator/store.py:166-176` — so the production Postgres/Redis store must
expose the same knobs and the launcher must map the env vars onto them.)

### 1.3 Router env vars the Worker needs

Tier model pointers are resolved **purely from env vars** — no model name is
hardcoded (`routing_matrix/tiers.py:1-6`). The `route_task` activity
(`orchestrator/workflow.py:195-219`) calls `routing_matrix.route()` (the only
place it is called — `orchestrator/workflow.py:196-200`), which resolves the
pointer via `resolve_model_pointer(tier)` (`routing_matrix/core.py:78`). A
missing/empty pointer raises `TierConfigError` (`routing_matrix/tiers.py:21-37`)
and the task fails. **These env vars must be set in every Worker process**
(activities run in the worker):

| Env var | Tier | Cited |
|---|---|---|
| `TIER_1_MODEL_POINTER` | standard | `routing_matrix/tiers.py:14-18`; README table `README.md:39-53` |
| `TIER_2_MODEL_POINTER` | advanced | same |
| `TIER_3_MODEL_POINTER` | frontier | same |

The value is an opaque string (model id, provider-scoped id) handed verbatim
to dispatch — `routing_matrix/tiers.py:25-37`.

**Provider selection:**

| Env var | Shape | Cited |
|---|---|---|
| `RM_DEFAULT_PROVIDER` | string, default **`"echo"`** | name of the provider used when the submission does not pick one — `routing_matrix/providers.py:74-83`; parsed by `parse_provider` in the `dispatch_task` activity — `orchestrator/workflow.py:267-268` |

The `echo` provider is registered by default (`routing_matrix/providers.py:86-87`)
and just echoes the model pointer (`routing_matrix/providers.py:29-43`) — it
performs no real call. To dispatch to a real provider, register an adapter
implementing `ProviderAdapter` (`routing_matrix/providers.py:18-25`) and set
`RM_DEFAULT_PROVIDER` (or pass `provider_name` per submission through
`OrchestratorAPI.submit(..., provider_name=...)` — `orchestrator/api.py:92-93`).

---

## 2. Required only for multi-worker / high availability

Everything in §1 runs with **one** Worker process and one API process. The
following items are per-process in-memory state; they become incorrect the
moment a second worker (or a second API instance) exists. None of this is
needed for a single-host MVP.

### 2.1 Rate limiter state must become shared

`TokenBucketLimiter` (`orchestrator/rate_limit.py:21`) keeps buckets in
process-local dicts guarded by a `threading.Lock`
(`orchestrator/rate_limit.py:30-37`). Two processes = two independent token
budgets per account.

It is consulted at **two** points, so confirm which ones apply to your
deployment:

- API process, before the workflow is started: `OrchestratorAPI.submit`
  checks `self._rate_limiter.allow(account_id)` — `orchestrator/api.py:103-106`.
- Worker process, as the first workflow step: the `check_rate_limit` activity
  — `orchestrator/workflow.py:130-136` (`allow` at `orchestrator/rate_limit.py:48-59`).

Operational consequences:
- With the current in-memory limiter, **each submission consumes one token in
  the API process and one in the worker process** (2 tokens total). Verify the
  token math against your chosen rate before tuning.
- Multi-worker requires a shared backing (e.g. a Redis token bucket, and the
  activity/`submit` both reading the same keys). Env shape suggestion:
  `REDIS_URL` from §1.2 plus `RATE_LIMIT_RATE` / `RATE_LIMIT_CAPACITY`
  (constructor args `orchestrator/rate_limit.py:30-31`, not read from env by
  the repo today).

### 2.2 Circuit breaker aggregate cost must become shared/durable

`CircuitBreaker` (`orchestrator/circuit_breaker.py:22`) keeps aggregate cost
and the trip flag in process-local fields under a `threading.Lock`
(`orchestrator/circuit_breaker.py:35-37`). `record()` accumulates one routed
task's cost and, once `aggregate_cost >= rate_threshold`, trips **and stays
tripped** (`orchestrator/circuit_breaker.py:54-59`); `reset()` is a
test/admin helper only (`orchestrator/circuit_breaker.py:74-78`).

There is also no persistence: **a worker restart resets the aggregate to
zero** and the trip flag to False (state lives only in the process). For
multi-worker/HA, back the aggregate with a Redis counter or a DB row so all
workers accumulate into one number that survives restarts (see `circuit_breaker_effective`
and `circuit_breaker_record` activities — `orchestrator/workflow.py:170-192,245-254`).
Env shape suggestion: `CIRCUIT_BREAKER_RATE_THRESHOLD` (float; constructor
arg `orchestrator/circuit_breaker.py:31`, not read from env by the repo today).

Trip semantics when you do this: `effective_degraded()` ORs the breaker over
per-account degradation — `orchestrator/circuit_breaker.py:65-72` — and the
tripped breaker forces `route()` non-frontier platform-wide
(`TRIPPED_ALLOWED_TIERS = ("standard", "advanced")`, `orchestrator/circuit_breaker.py:17-19`).

### 2.3 SQLite is a single-worker/MVP store

`SqliteAtomicStore` uses `BEGIN IMMEDIATE`, an **exclusive database write
lock**, for every write (`orchestrator/store.py:14-18,153-157,208`). It is
correct for one worker (and genuinely atomic across threads/processes on one
machine), but every write serializes on the file lock, so it does not scale
to multi-worker; it also keeps state on one disk. Use §1.2's Postgres/Redis
store as soon as there is more than one worker. A production single-node
deployment should strongly prefer Postgres/Redis anyway for durability of
idempotency/task rows (the sqlite file is the single point of failure for
account usage accounting).

---

## 3. Stub / later-phase integration

The `dispatch_task` activity hands off to four later-phase subsystems
(`orchestrator/stubs.py:1-13`) and continues — **they return placeholders
now**, so nothing needs provisioning for them:

- Execution Engine — `execution_engine().execute(model_pointer, task)`
  (`orchestrator/stubs.py:31-37,68-69`)
- Deployment Factory — `deployment_factory().deploy(task_id, plan)`
  (`orchestrator/stubs.py:40-46,72`)
- Royalty Ledger — `royalty_ledger().record(account_id, task_id, 0.0)`
  (`orchestrator/stubs.py:49-56,76-77`)
- Payment Router — `payment_router().route(account_id, estimated_cost)`
  (`orchestrator/stubs.py:58-64,80-81`)

Every stub result is tagged `"phase": "later-phase-stub"` /
`"status": "not-implemented"` (`orchestrator/stubs.py:19-28`) so it can never
be mistaken for a real result. They are invoked inside `dispatch_task` at
`orchestrator/workflow.py:272-278` and surfaced in the dispatch payload under
`"head_off"` (`orchestrator/workflow.py:286`). The orchestrator's call sites
will not change when real implementations land (`orchestrator/stubs.py:4-6`).

Also note: `dispatch_task` has a `simulate_failure` test hook
(`orchestrator/workflow.py:264-265`) — never set it in production
(`OrchestratorAPI.submit(..., simulate_failure=...)` is a test seam,
`orchestrator/api.py:92-93`).

---

## 4. Checklist

### 4.1 Owner must provision (infrastructure)

| # | WHAT | WHY (file/line) | VALUE SHAPE |
|---|---|---|---|
| P1 | A real Temporal server, or a Temporal Cloud account + namespace | Test suite's `WorkflowEnvironment` is in-process; production needs a real server (`orchestrator/tests/test_robustness.py:75-77`). The `Client` connects to it (`orchestrator/api.py:23`). | Self-hosted: Temporal 1.x cluster; Cloud: namespace created in console |
| P2 | Temporal namespace (or rely on `default`) | `Client.connect` uses `namespace="default"` unless overridden; a custom namespace must exist before first use; worker + API must share it | Name string; if custom, create it server-side (self-hosted `temporal operator namespace` / Cloud console); set `TEMPORAL_NAMESPACE` |
| P3 | TLS / auth material for the Temporal connection | Cloud and secure self-hosted setups require TLS + API key or mTLS certs | `TEMPORAL_API_KEY` or `TEMPORAL_CLIENT_CERT`/`TEMPORAL_CLIENT_KEY` (+ CA), `TEMPORAL_TLS=true` |
| P4 | Postgres (recommended) or Redis instance for the external atomic store | Account usage/idempotency/tasks must be externally persisted and atomically updated (`orchestrator/store.py:1-7,34-45`) | Postgres DB with the exact tables from §1.2; or Redis with the key layout + Lua `EVAL` semantics from §1.2 |
| P5 | (Multi-worker only) shared backing for rate limiter + circuit breaker | Both are in-memory per-process (§2.1, §2.2) | Same Redis, or DB rows/counters, shared by all worker/API processes |

### 4.2 Owner must configure / decide

| # | WHAT | WHY (file/line) | EXACT ENV VAR / CONFIG VALUE |
|---|---|---|---|
| C1 | Task queue name | Hardcoded `TASK_QUEUE = "master-orchestrator"`; worker and API must match (`orchestrator/workflow.py:46,472-474`; `orchestrator/api.py:64-65`) | `"master-orchestrator"` (or change both call sites consistently) |
| C2 | Temporal address / namespace / TLS / auth env vars for Client + Worker processes | `orchestrator/workflow.py:472-493` (Worker); `orchestrator/api.py:126-143` (API) | `TEMPORAL_ADDRESS=<host:port>`, `TEMPORAL_NAMESPACE` (default `default`), `TEMPORAL_TLS=true` when applicable, `TEMPORAL_API_KEY` / mTLS vars (§1.1) |
| C3 | Model pointer per tier | `resolve_model_pointer` reads env per tier; missing → `TierConfigError` and task failure (`routing_matrix/tiers.py:14-37`; resolved at `routing_matrix/core.py:78`; used by `route_task` activity `orchestrator/workflow.py:195-219`) | `TIER_1_MODEL_POINTER=<opaque>`, `TIER_2_MODEL_POINTER=<opaque>`, `TIER_3_MODEL_POINTER=<opaque>` — all three set in every Worker process |
| C4 | Default provider | `RM_DEFAULT_PROVIDER` selects the dispatch provider; default `echo` is a placeholder (`routing_matrix/providers.py:74-87`); consumed at `orchestrator/workflow.py:267-268` | `RM_DEFAULT_PROVIDER=<registered provider name>` (default `echo`); register real adapters in code (`routing_matrix/providers.py:18-25,56-58`) |
| C5 | Store backing + tuning | `SqliteAtomicStore` defaults `degraded_threshold=0.0011`, `usage_window_seconds=3600` (`orchestrator/store.py:166-176`); production store must expose the same knobs (§1.2) | `DATABASE_URL=<postgres DSN>` **or** `REDIS_URL=<redis URL>`; `STORE_DEGRADED_THRESHOLD` (default `0.0011`), `STORE_USAGE_WINDOW_SECONDS` (default `3600.0`) |
| C6 | (Single-worker MVP only) SQLite file path if you run `SqliteAtomicStore` | Constructor takes `db_path` (`orchestrator/store.py:166-172`); SQLite is fine for one worker only (§2.3) | e.g. `SQLITE_PATH=/var/lib/routing_matrix/store.db` (repo does not read a default — launcher must pass it) |
| C7 | Global circuit breaker threshold | Trips the whole platform non-frontier when aggregate cost crosses it and stays tripped (`orchestrator/circuit_breaker.py:31-37,54-59`); consulted per task (`orchestrator/workflow.py:170-192`) | `CIRCUIT_BREAKER_RATE_THRESHOLD=<float>` (constructor arg; pick a platform spend budget) |
| C8 | Rate limiter rate + burst | `TokenBucketLimiter(rate, capacity)` (`orchestrator/rate_limit.py:30-31`); enforced in API (`orchestrator/api.py:103-106`) and first workflow step (`orchestrator/workflow.py:130-136`) — note each submission consumes 2 tokens today (§2.1) | `RATE_LIMIT_RATE=<tokens/sec>`, `RATE_LIMIT_CAPACITY=<int>` |
| C9 | Retry/timeout policy | Bounded retries: `maximum_attempts=3`, 30 s start-to-close, 10 s schedule-to-start (`orchestrator/workflow.py:50-56,456-464`); exhaustion → `FAILED` dead-letter (`orchestrator/workflow.py:437-454`) | Constants in code today — decide whether to keep or tune |

---

## 5. Running the test suite

```bash
cd /home/team/shared/routing_matrix
.venv/bin/python -m pytest -q       # or: python -m pytest -q
```

- Run from the repository root (picks up `tests/` and `orchestrator/tests/`).
- The suite uses the **Temporal test server** via
  `temporalio.testing.WorkflowEnvironment.start_local()`
  (`orchestrator/tests/test_robustness.py:75-77`) — **no external services
  needed** (no Temporal server, no DB, no Redis; tier pointers are injected by
  the fixture at `orchestrator/tests/test_robustness.py:50-55`).
- Verified at this commit: **23 passed** (router + orchestrator + robustness
  suites, including the five required scenarios: concurrent threshold race,
  simulated restart/replay via `Replayer`, idempotent duplicate, retry
  exhaustion to FAILED, and the global circuit breaker).
- Requires the `.venv` (or equivalent) with `temporalio==1.32.0` installed.