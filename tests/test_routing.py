"""Test suite for the Routing Matrix."""

from __future__ import annotations

import io
import json
import logging

import pytest

from routing_matrix import RoutingDecision, Task, route
from routing_matrix.logging import DecisionLogger, JsonFormatter
from routing_matrix.tiers import TierConfigError, resolve_model_pointer

T1 = "pointer/tier1"
T2 = "pointer/tier2"
T3 = "pointer/tier3"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    """Provide self-contained tier pointers for every test."""
    monkeypatch.setenv("TIER_1_MODEL_POINTER", T1)
    monkeypatch.setenv("TIER_2_MODEL_POINTER", T2)
    monkeypatch.setenv("TIER_3_MODEL_POINTER", T3)


def _capture_logger() -> tuple[DecisionLogger, io.StringIO]:
    """Return a DecisionLogger whose lines are captured in a StringIO."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger(f"test_{len(stream.buffer.getvalue()) if False else 'x'}")
    logger.handlers[:] = []
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return DecisionLogger(logger=logger), stream


# ---- Explicit task -> tier pairs ---------------------------------------


def test_crud_routes_to_standard(_env):
    task = Task(prompt="Add a create-read-update-delete endpoint for users.", task_type="crud")
    decision = route(task, degraded=False)
    assert decision.tier == "standard"
    assert decision.model_pointer == T1
    assert not decision.degraded


def test_multi_file_refactor_routes_to_advanced(_env):
    task = Task(
        prompt="Refactor the billing module across multiple files and services.",
        task_type="refactor",
    )
    decision = route(task, degraded=False)
    assert decision.tier == "advanced"
    assert decision.model_pointer == T2


def test_escalated_routes_to_frontier_with_context(_env):
    task = Task(
        prompt="Handle a tricky edge case.",
        escalate=True,
        failure_context="retry 3 failed: timeout on shard write",
    )
    decision = route(task, degraded=False)
    assert decision.tier == "frontier"
    assert decision.model_pointer == T3
    assert "timeout on shard write" in decision.rationale
    # Ensure failure_context is explicitly surfaced even when None.


def test_escalated_without_context_notes_none(_env):
    task = Task(prompt="Handle tricky thing.", escalate=True, failure_context=None)
    decision = route(task, degraded=False)
    assert decision.tier == "frontier"
    assert "None" in decision.rationale


def test_complex_degraded_caps_to_advanced(_env):
    task = Task(
        prompt="Refactor the billing module across multiple files and services.",
        task_type="refactor",
    )
    decision = route(task, degraded=True)
    assert decision.tier == "advanced"
    assert decision.model_pointer == T2
    assert decision.degraded is True


def test_escalate_plus_degraded_caps_to_advanced(_env):
    task = Task(prompt="Tricky case.", escalate=True, failure_context="boom")
    decision = route(task, degraded=True)
    assert decision.tier == "advanced"  # degraded suppresses frontier even when escalated
    assert decision.degraded is True
    assert decision.model_pointer == T2
    assert decision.tier != "frontier"


# ---- Env pointer resolution --------------------------------------------


def test_model_pointer_flows_from_env(_env):
    decision = route(Task(prompt="simple task"), degraded=False)
    assert decision.model_pointer == T1 == resolve_model_pointer("standard")
    assert resolve_model_pointer("advanced") == T2
    assert resolve_model_pointer("frontier") == T3


def test_missing_env_raises(monkeypatch):
    monkeypatch.delenv("TIER_1_MODEL_POINTER", raising=False)
    with pytest.raises(TierConfigError):
        resolve_model_pointer("standard")


# ---- Structured JSON logging -------------------------------------------


def test_json_log_line_emitted_per_call(_env):
    logger, stream = _capture_logger()
    route(Task(prompt="simple"), degraded=False, decision_logger=logger)
    route(Task(prompt="another"), degraded=False, decision_logger=logger)
    lines = [ln for ln in stream.getvalue().strip().splitlines() if ln.strip()]
    assert len(lines) == 2
    payload = json.loads(lines[0])
    assert payload["tier"] == "standard"
    assert payload["model_pointer"] == T1
    assert payload["degraded"] is False
    assert "estimated_cost" in payload
    assert payload["task"]["prompt"] == "simple"
