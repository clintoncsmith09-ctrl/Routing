"""Structured JSON logging for orchestration decisions.

Every `orchestrate()` call logs one JSON line describing the decision, the
account it was charged to, the usage totals, and the execution plan. Uses the
standard `logging` module and mirrors the router's JSON logging style.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Log formatter that emits each record as one JSON object.

    Mirrors `routing_matrix.logging.JsonFormatter`. Structured fields passed
    as `extra={"extra_fields": {...}}` are merged into the record payload.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "logger": record.name,
            "level": record.levelname,
            "event": record.getMessage(),
        }
        extras = getattr(record, "extra_fields", None)
        if isinstance(extras, dict):
            payload.update(extras)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def _configure_default_logger() -> logging.Logger:
    logger = logging.getLogger("orchestrator")
    if not logger.handlers:  # avoid duplicate handlers on re-configure
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


# Default structured logger for the orchestrator.
LOGGER = _configure_default_logger()


class DecisionLogger:
    """Small wrapper so tests / callers can inject a logger or capture lines."""

    def __init__(self, logger: logging.Logger | None = None):
        self._logger = logger if logger is not None else LOGGER

    def log_decision(self, decision_payload: dict) -> None:
        self._logger.info("orchestration_decision", extra={"extra_fields": decision_payload})


DECISION_LOGGER = DecisionLogger()
