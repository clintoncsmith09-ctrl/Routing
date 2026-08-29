"""Provider dispatch adapter layer.

A small registry/adapter pattern so a new provider can be added without
touching routing logic. The router only resolves a *model pointer* and hands
it to `dispatch()`; dispatch looks up a provider by an opaque pointer/name
and returns a provider handle.

No actual provider call is made in this MVP — the hook for dispatch is
enough. The router never decides the tier here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Protocol


class ProviderAdapter(Protocol):
    """Minimal surface a provider adapter must expose for dispatch."""

    name: str

    def dispatch(self, model_pointer: str) -> dict:  # pragma: no cover - protocol
        """Stub: return a dispatch payload for the given model pointer."""
        ...


@dataclass
class EchoProvider:
    """Default provider that just echoes the model pointer.

    This is the only built-in provider; it performs no real call. A real
    provider would implement `dispatch()` to hand off to the external model.
    """

    name: str = "echo"

    def dispatch(self, model_pointer: str) -> dict:
        return {
            "provider": self.name,
            "model_pointer": model_pointer,
            "status": "provisioned",
        }


ProviderFactory = Callable[[], ProviderAdapter]


@dataclass
class ProviderRegistry:
    """Registry mapping provider names to lazily created provider adapters."""

    _factories: dict[str, ProviderFactory] = field(default_factory=dict)
    _instances: dict[str, ProviderAdapter] = field(default_factory=dict)

    def register(self, name: str, factory: ProviderFactory) -> None:
        """Register a provider factory under a name."""
        self._factories[name] = factory

    def get(self, name: str) -> ProviderAdapter:
        """Get (creating if needed) the provider adapter for `name`."""
        if name not in self._instances:
            if name not in self._factories:
                raise KeyError(f"No provider registered for {name!r}")
            self._instances[name] = self._factories[name]()
        return self._instances[name]

    def dispatch(self, provider_name: str, model_pointer: str) -> dict:
        """Dispatch a model pointer through a named provider adapter."""
        provider = self.get(provider_name)
        return provider.dispatch(model_pointer)


def _default_provider() -> str:
    """Name of the provider to use when the model pointer carries none.

    Model pointers are opaque; the router does not parse them. This helper
    just returns the registry's default provider name, keyed by an optional
    env override so deployments can switch it without code changes.
    """
    import os

    return os.environ.get("RM_DEFAULT_PROVIDER", "echo").strip() or "echo"


REGISTRY = ProviderRegistry()
REGISTRY.register("echo", EchoProvider)


def parse_provider(provider_name: str | None) -> str:
    """Resolve the provider name used for a dispatch."""
    return (provider_name or _default_provider()).strip()
