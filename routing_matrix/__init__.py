"""Routing Matrix — routes tasks to capability tiers by complexity and cost.

Public interface:
    from routing_matrix import route, Task, RoutingDecision
"""

from .core import route
from .model import RoutingDecision, Task, Tier
from .tiers import resolve_model_pointer

__all__ = ["route", "Task", "RoutingDecision", "Tier", "resolve_model_pointer"]
__version__ = "0.1.0"
