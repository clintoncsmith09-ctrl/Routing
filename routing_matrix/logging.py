"""Structured JSON logging for routing decisions.

Every `route()` call logs the full decision as a single JSON line via the
standard `logging` module. No `print()` is used anywhere in the package.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Log formatter that emits each record as one JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "logger": record.name,
            "level": record.levelname,
            "event": record.getMessage(),
        }
        # Attach any structured extra fields passed to logger.info(..., extra={...})
        extras = getattr(record, "extra_fields", None)
        if isinstance(extras, dict):
            payload.update(extras)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def _configure_default_logger() -> logging.Logger:
    logger = logging.getLogger("routing_matrix")
    if not logger.handlers:  # avoid duplicate handlers on re-configure
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


# Default structured logger for the package.
LOGGER = _configure_default_logger()


class DecisionLogger:
    """Small wrapper so tests / callers can inject a logger or capture lines."""

    def __init__(self, logger: logging.Logger | None = None):
        self._logger = logger if logger is not None else LOGGER

    def log_decision(self, decision_payload: dict) -> None:
        self._logger.info("routing_decision", extra={"extra_fields": decision_payload})


DECISION_LOGGER = DecisionLogger()
