"""Test suite for the Master Orchestrator.

Runs against the real Routing Matrix package. Tier pointers come from env
vars, set here so the suite is self-contained (same pattern as the router's
tests).
"""

from __future__ import annotations

import io
import json
import logging

import pytest

from routing_matrix import Task

from orchestrator import Orchestrator, UsageLedger, orchestrate
from orchestrator.logging import DecisionLogger, JsonFormatter

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
    logger = logging.getLogger("test_orchestrator")
    logger.handlers[:] = []
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return DecisionLogger(logger=logger), stream


def test_orchestrate_returns_plan_with_routed_tier_and_model_pointer():
    ledger = UsageLedger()
    orch = Orchestrator(usage_ledger=ledger)
    task = Task(prompt="Add a create-read-update-delete endpoint for users.", task_type="crud")
    result = orchestrate(task, account_id="acct-1", orchestrator=orch)

    assert result.tier == "standard"
    assert result.model_pointer == T1
    assert result.plan["routed_tier"] == "standard"
    assert result.plan["model_pointer"] == T1
    # Dispatch step wired through the provider adapter registry.
    assert result.plan["dispatch_step"]["result"]["provider"] == "echo"
    assert result.plan["dispatch_step"]["result"]["status"] == "provisioned"


def test_usage_accumulates_across_calls_for_same_account():
    ledger = UsageLedger()
    orch = Orchestrator(usage_ledger=ledger)

    simple = Task(prompt="simple task")
    complex = Task(prompt="Refactor the billing module across multiple files.", task_type="refactor")

    orchestrate(simple, account_id="acct-1", orchestrator=orch)     # standard
    orchestrate(complex, account_id="acct-1", orchestrator=orch)    # advanced

    a = ledger.snapshot("acct-1")
    assert a["call_count"] == 2
    # standard (0.0001) + advanced (0.001) from the router's default cost model.
    assert a["total_cost"] == pytest.approx(0.0011)
    assert a["tiers_used"] == {"standard": 1, "advanced": 1}
    # The post-call usage snapshot on the result reflects the account totals.
    simple = Task(prompt="simple task")
    # (re-route to observe accrual on the returned result usage snapshot)
    result = orchestrate(simple, account_id="acct-1", orchestrator=orch)
    assert result.usage["call_count"] == 3
    assert result.usage["total_cost"] == pytest.approx(0.0012)


def test_usage_is_keyed_per_account():
    ledger = UsageLedger()
    orch = Orchestrator(usage_ledger=ledger)

    orchestrate(Task(prompt="simple task"), account_id="acct-A", orchestrator=orch)
    orchestrate(Task(prompt="simple task"), account_id="acct-A", orchestrator=orch)
    orchestrate(Task(prompt="simple task"), account_id="acct-B", orchestrator=orch)

    assert ledger.snapshot("acct-A")["call_count"] == 2
    assert ledger.snapshot("acct-B")["call_count"] == 1
    # Accounts don't share state.
    assert ledger.snapshot("acct-B")["total_cost"] != ledger.snapshot("acct-A")["total_cost"]


def test_degraded_true_caps_to_non_frontier():
    ledger = UsageLedger()
    orch = Orchestrator(usage_ledger=ledger)
    task = Task(
        prompt="Refactor the billing module across multiple files and services.",
        task_type="refactor",
    )
    result = orchestrate(task, account_id="acct-1", degraded=True, orchestrator=orch)

    assert result.degraded is True
    assert result.tier != "frontier"
    assert result.tier == "advanced"
    assert result.model_pointer == T2
    assert result.plan["degraded"] is True
    # Even an escalated task is capped to advanced when degraded.
    escalated = Task(prompt="tricky case", escalate=True, failure_context="boom")
    r2 = orchestrate(escalated, account_id="acct-1", degraded=True, orchestrator=orch)
    assert r2.tier == "advanced"
    assert r2.tier != "frontier"


def test_escalated_task_routes_to_frontier_and_plan_reflects_it():
    ledger = UsageLedger()
    orch = Orchestrator(usage_ledger=ledger)
    task = Task(prompt="Handle a tricky edge case.", escalate=True, failure_context="timeout on shard write")
    result = orchestrate(task, account_id="acct-1", orchestrator=orch)

    assert result.tier == "frontier"
    assert result.model_pointer == T3
    assert result.plan["routed_tier"] == "frontier"
    assert result.plan["dispatch_step"]["result"]["model_pointer"] == T3


def test_json_log_line_emitted_per_orchestrate_call():
    ledger = UsageLedger()
    logger, stream = _capture_logger()
    orch = Orchestrator(usage_ledger=ledger, decision_logger=logger)

    orchestrate(Task(prompt="simple"), account_id="acct-1", orchestrator=orch)
    orchestrate(Task(prompt="another"), account_id="acct-2", orchestrator=orch)

    lines = [ln for ln in stream.getvalue().strip().splitlines() if ln.strip()]
    assert len(lines) == 2
    payload = json.loads(lines[0])
    assert payload["event"] == "orchestration_decision"
    assert payload["decision"]["tier"] == "standard"
    assert payload["account_id"] == "acct-1"
    assert payload["usage"]["call_count"] == 1
    assert payload["task"]["prompt"] == "simple"
