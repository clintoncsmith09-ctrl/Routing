"""Non-LLM heuristic complexity classification.

Complexity is classified with a pure, synchronous, keyword/structural
heuristic so that classification cost stays negligible and *no* LLM call
(Tier 1 or otherwise) is ever needed. The router never asks Tier 2/3 to
decide the tier.
"""

from __future__ import annotations

from dataclasses import dataclass

from .model import Task, Tier

# Signals that push a task toward the advanced tier. Kept deliberately
# conservative: only strong, unambiguous signals escalate beyond standard.
_ADVANCED_KEYWORDS = (
    "refactor",
    "multiple files",
    "multi-file",
    "cross-file",
    "architecture",
    "migration",
    "integration",
    "refactoring",
    "concurrent",
    "distributed",
    "refactor.",
)

# Structural multipliers: each occurrence of these contributes to the score.
_ADVANCED_STRUCTURAL = (
    "multiple",
    "several",
    "across",
    "files",
    "modules",
    "services",
    "async",
    "thread",
    "database",
    "migrate",
)

# Task-type labels that are inherently advanced.
_ADVANCED_TASK_TYPES = {
    "refactor",
    "refactoring",
    "refactor",
    "migration",
    "architecture",
    "integration",
    "multi-file",
}


@dataclass(frozen=True)
class Classification:
    """Result of heuristic classification."""

    tier: Tier
    score: int
    signal: str

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.signal


class ComplexityClassifier:
    """Pure, deterministic complexity classifier for routing decisions.

    The classifier interface is a single `classify(task: Task) ->
    Classification` method so it could in principle be swapped for a Tier-1
    LLM classifier later without touching routing logic — but the default
    stays a fast, non-LLM heuristic as required.
    """

    def classify(self, task: Task) -> Classification:
        prompt_l = (task.prompt or "").lower()
        task_type_l = (task.task_type or "").lower()

        score = 0
        signals: list[str] = []

        for kw in _ADVANCED_KEYWORDS:
            if kw in prompt_l:
                score += 2
                signals.append(kw)

        for token in _ADVANCED_STRUCTURAL:
            if token in prompt_l:
                score += 1
                signals.append(token)

        if task_type_l in _ADVANCED_TASK_TYPES:
            score += 2
            signals.append(f"task_type={task.task_type}")

        # A plain, short prompt strongly implies standard work.
        if not signals and len(prompt_l.split()) <= 20:
            return Classification(tier="standard", score=0, signal="simple, short task")

        if score >= 3:
            return Classification(
                tier="advanced",
                score=score,
                signal="; ".join(dict.fromkeys(signals)),
            )

        return Classification(
            tier="standard",
            score=score,
            signal="; ".join(dict.fromkeys(signals)) if signals else "low complexity",
        )


CLASSIFIER = ComplexityClassifier()
