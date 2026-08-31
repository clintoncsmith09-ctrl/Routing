"""Transport-level rate limiting, independent of cost-based degradation.

A simple token-bucket limiter per account. This governs HOW MANY submissions
an account may make over time — a different axis from the ``degraded`` flag
(which caps *tier* based on cost). Keeping the two separate is explicit here:
the limiter is never consulted when computing ``degraded``.

Thread-safe for concurrent submissions.
"""

from __future__ import annotations

import threading
import time


class RateLimitExceeded(Exception):
    """Raised when a submission would exceed the account's rate limit."""


class TokenBucketLimiter:
    """Per-account token bucket.

    Args:
        rate: tokens refilled per second.
        capacity: maximum bucket size (burst). Each submission consumes one
            token; if the bucket has none the submission is denied.
    """

    def __init__(self, rate: float, capacity: int) -> None:
        if rate <= 0 or capacity <= 0:
            raise ValueError("rate and capacity must be positive")
        self._rate = float(rate)
        self._capacity = float(capacity)
        self._lock = threading.Lock()
        self._buckets: dict[str, float] = {}
        self._updated: dict[str, float] = {}

    def _tokens(self, account_id: str, now: float) -> float:
        tokens = self._buckets.get(account_id, self._capacity)
        last = self._updated.get(account_id, now)
        elapsed = max(0.0, now - last)
        tokens = min(self._capacity, tokens + elapsed * self._rate)
        self._buckets[account_id] = tokens
        self._updated[account_id] = now
        return tokens

    def allow(self, account_id: str, now: float | None = None) -> bool:
        """Consume one token if available; returns True when allowed.

        Returns False (does not consume) when the bucket is empty.
        """
        now = time.monotonic() if now is None else now
        with self._lock:
            tokens = self._tokens(account_id, now)
            if tokens < 1.0:
                return False
            self._buckets[account_id] = tokens - 1.0
            return True

    def acquire(self, account_id: str, now: float | None = None) -> None:
        """Like :meth:`allow` but raises :class:`RateLimitExceeded` on deny."""
        if not self.allow(account_id, now=now):
            raise RateLimitExceeded(
                f"rate limit exceeded for account {account_id!r}"
            )

    def reset(self, account_id: str | None = None) -> None:
        with self._lock:
            if account_id is None:
                self._buckets.clear()
                self._updated.clear()
            else:
                self._buckets.pop(account_id, None)
                self._updated.pop(account_id, None)
