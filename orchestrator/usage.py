"""Account-level usage state owned by the orchestrator.

The Routing Matrix router is deliberately stateless: it resolves a tier and a
model pointer and returns a decision, holding no persistent account or usage
state. That account-level state belongs here, in the orchestrator, as simple
in-process (in-memory) state keyed by account_id.

Nothing here is persisted or shared across restarts — usage accumulates only
for the lifetime of the process (or of the ledger instance).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AccountLedger:
    """Per-account usage accumulator.

    Tracks accumulated cost, call count, and the tiers used (with a count per
    tier). `record()` updates all three in one step.
    """

    account_id: str
    total_cost: float = 0.0
    call_count: int = 0
    tiers_used: dict[str, int] = field(default_factory=dict)

    def record(self, tier: str, cost: float) -> None:
        """Accumulate one routed call for this account."""
        self.total_cost += cost
        self.call_count += 1
        self.tiers_used[tier] = self.tiers_used.get(tier, 0) + 1

    def snapshot(self) -> dict:
        """Stable, JSON-serialisable view of this account's usage."""
        return {
            "account_id": self.account_id,
            "total_cost": self.total_cost,
            "call_count": self.call_count,
            "tiers_used": dict(self.tiers_used),
        }


class UsageLedger:
    """In-memory ledger keyed by account_id -> AccountLedger.

    This is the caller-owned account state the router deliberately does not
    hold. It is process-local only — no persistence, no DB.
    """

    def __init__(self) -> None:
        self._accounts: dict[str, AccountLedger] = {}

    def account(self, account_id: str) -> AccountLedger:
        """Get (creating if needed) the ledger for an account."""
        if account_id not in self._accounts:
            self._accounts[account_id] = AccountLedger(account_id=account_id)
        return self._accounts[account_id]

    def record(self, account_id: str, tier: str, cost: float) -> AccountLedger:
        """Record one routed call against an account and return its ledger."""
        return self.account(account_id).record(tier, cost)

    def snapshot(self, account_id: str) -> dict:
        """Snapshot of one account's usage (empty zero-state if unknown)."""
        if account_id not in self._accounts:
            return AccountLedger(account_id=account_id).snapshot()
        return self._accounts[account_id].snapshot()

    @property
    def accounts(self) -> dict[str, AccountLedger]:
        """Raw mapping of all accounts (read-only by convention)."""
        return dict(self._accounts)


# Account id used when the caller does not supply one.
DEFAULT_ACCOUNT = "default"

# The process-wide default ledger — used by `orchestrate()` unless a caller
# injects its own (e.g. for isolated tests).
DEFAULT_LEDGER = UsageLedger()
