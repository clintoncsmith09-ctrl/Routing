"""Trace propagation: correlation IDs for the orchestrator.

A correlation ID is generated at task submission and attached to every
routing call and every log line for that task. It lets an operator / support
person correlate the async lifecycle of one task across all its steps,
activities and log lines without having to reconstruct the chain by hand.

This module is deliberately tiny and stateless apart from the generated
strings.
"""

from __future__ import annotations

import threading
import uuid


def new_correlation_id() -> str:
    """Return a fresh opaque correlation id for one task submission."""
    return uuid.uuid4().hex


# Thread-local helper so code travelling through the same thread (e.g. an
# async facade's worker thread) can read the correlation id without passing it
# through every signature. It is optional sugar — the AsyncSubmit genuinely
# holds the correlation id on the task state and passes it explicitly into the
# log calls; this context makes that propagation observable/consistent.
_context = threading.local()


def set_current_correlation_id(correlation_id: str | None) -> None:
    """Attach a correlation id to the current thread's context (optional)."""
    _context.correlation_id = correlation_id


def current_correlation_id() -> str | None:
    """Read the thread-local correlation id, if any."""
    return getattr(_context, "correlation_id", None)


def with_correlation_fields(correlation_id: str | None, extra: dict) -> dict:
    """Merge a correlation id into a structured-log payload (mutation-safe).

    Returns a new dict with ``correlation_id`` set when a non-empty value is
    given, preserving the original ``extra`` data.
    """
    out = dict(extra)
    if correlation_id:
        out["correlation_id"] = correlation_id
    return out
