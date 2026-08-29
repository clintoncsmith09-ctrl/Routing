# Routing Matrix

A standalone, stateless Python package that routes each incoming task to a
capability tier (`standard` / `advanced` / `frontier`) by complexity and cost,
returning a model pointer, a rationale, and an estimated cost.

It is a **library only** — no orchestration, execution, deployment, or
account/billing logic. It holds no persistent account or usage state.

## Public interface

```python
from routing_matrix import route, Task, RoutingDecision

decision = route(
    Task(prompt="Add a CRUD endpoint for users.", task_type="crud"),
    degraded=False,
)
# decision.tier          -> "standard"
# decision.model_pointer -> value of TIER_1_MODEL_POINTER
# decision.rationale     -> human-readable reason
# decision.degraded      -> False
# decision.estimated_cost-> float
```

### `Task`

| field            | type                | default  | meaning                              |
|------------------|---------------------|----------|--------------------------------------|
| `prompt`         | `str`               | required | task description                     |
| `task_type`      | `str \| None`       | `None`   | optional short label (e.g. `crud`)   |
| `escalate`       | `bool`              | `False`  | route directly to frontier           |
| `failure_context`| `str \| None`       | `None`   | context surfaced in rationale on escalation |

### `RoutingDecision`

`route()` returns a `RoutingDecision` with fields `tier`
(`Literal["standard","advanced","frontier"]`), `model_pointer`,
`rationale`, `degraded`, `estimated_cost`.

## Environment variables

Tiers are resolved purely from environment pointers — **no model/provider
name is hardcoded in the routing logic.**

| var                   | tier      |
|-----------------------|-----------|
| `TIER_1_MODEL_POINTER`| standard  |
| `TIER_2_MODEL_POINTER`| advanced  |
| `TIER_3_MODEL_POINTER`| frontier  |

The value is opaque: the router treats it as an opaque token to hand to the
provider dispatch layer. Missing env vars raise `TierConfigError`.

### Behaviour rules

- **Complexity classification** runs only via a fast, non-LLM heuristic
  (keyword/structural signals from `prompt` + `task_type`). It never calls
  Tier 2/3, and classification cost stays negligible at any call volume.
- **`escalate=True`** → route directly to frontier and include
  `failure_context` in the rationale (even when it is `None`).
- **`degraded=True`** (caller-supplied; never looked up or stored) → never
  frontier, cap at advanced — even for escalated tasks.
- Every call logs the full decision as a **structured JSON** line via the
  standard `logging` module (no `print()`).

## Adding a provider

Providers plug in via a registry/adapter layer in `routing_matrix/providers.py`
so a new provider can be added without touching routing logic.

1. Create a class implementing the `ProviderAdapter` protocol (at minimum a
   `name` and a `dispatch(model_pointer) -> dict` method).
2. Register it:

   ```python
   from routing_matrix.providers import REGISTRY

   def make_my_provider():
       return MyProvider()

   REGISTRY.register("myprovider", make_my_provider)
   ```

3. Optionally point dispatch at it via the `RM_DEFAULT_PROVIDER` env var.

The MVP performs no real external call — the dispatch hook is enough. The
router only ever resolves a model pointer and hands it to dispatch; it never
decides a provider based on the tier.

## Running the tests

```bash
cd routing_matrix
python -m venv .venv && . .venv/bin/activate && pip install pytest
python -m pytest tests/ -v
```

Interface contract: `from routing_matrix import route, Task, RoutingDecision`
works once the package is importable (install with `pip install -e .` or run
from the package root).
