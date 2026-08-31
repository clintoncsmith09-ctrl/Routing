"""Global spend circuit breaker.

Independent, platform-wide safety: tracks aggregate cost across ALL accounts
(the orchestrator's own view, separate from any one account's usage). If the
aggregate burn ever crosses a configured rate threshold, the breaker trips and
stays tripped, forcing the entire platform into standard/advanced-only (never
frontier) regardless of individual account headroom.

This is deliberately separate from per-account accounting. Per-account state
lives in `orchestrator.accounting`; the breaker is global.
"""

from __future__ import annotations

import threading

# Tiers the breaker allows when tripped: standard/advanced only.
TRIPPED_ALLOWED_TIERS = ("standard", "advanced")
FRONTIER = "frontier"


class CircuitBreaker:
    """Aggregate-cost circuit breaker.

    Args:
        rate_threshold: cumulative aggregate cost at/beyond which the breaker
            trips (configurable to keep it testable / configurable per the
            spec's "testable/configurable via a rate threshold").
    """

    def __init__(self, rate_threshold: float) -> None:
        if rate_threshold is None or rate_threshold < 0:
            raise ValueError("rate_threshold must be a non-negative number")
        self._rate_threshold = float(rate_threshold)
        self._lock = threading.Lock()
        self._aggregate_cost = 0.0
        self._tripped = False

    @property
    def rate_threshold(self) -> float:
        return self._rate_threshold

    @property
    def aggregate_cost(self) -> float:
        with self._lock:
            return self._aggregate_cost

    @property
    def tripped(self) -> bool:
        """True once the aggregate burn has crossed the rate threshold."""
        with self._lock:
            return self._tripped

    def record(self, cost: float) -> None:
        """Accumulate one routed task's cost into the aggregate. Atomic."""
        with self._lock:
            self._aggregate_cost += cost
            if self._aggregate_cost >= self._rate_threshold:
                self._tripped = True

    def frontier_allowed(self) -> bool:
        """Whether a frontier route is allowed right now."""
        return not self.tripped

    def effective_degraded(self, already_degraded: bool = False) -> bool:
        """Degraded flag to hand to ``route()`` — forced True when tripped.

        Args:
            already_degraded: per-account degradation already decided. The
                breaker ORs over it: either one forces non-frontier routing.
        """
        return already_degraded or self.tripped

    def reset(self) -> None:
        """Reset aggregate cost and the trip flag (test/admin helper)."""
        with self._lock:
            self._aggregate_cost = 0.0
            self._tripped = False
