"""
logging_config.py — Structured JSON logging for the orchestrator library.

WHY STRUCTURED LOGS
--------------------
Plain text logs are hard to query at scale.  JSON logs can be shipped
to any log aggregation system (Loki, Datadog, CloudWatch, Elastic) and
queried with structured filters like  task_id="abc" AND status="failure".

Every log record emitted by the library includes:
    - timestamp      ISO-8601 UTC
    - level          DEBUG / INFO / WARNING / ERROR / CRITICAL
    - logger         dotted module name (e.g. kafka_orchestrator.retry)
    - message        human-readable description
    - **kwargs       any extra={...} fields passed at the call site

USAGE
-----
Call configure_logging() once at application startup, before creating
any orchestrator components.  If you already have a logging setup,
you can skip this and just ensure the "kafka_orchestrator" logger
has appropriate handlers attached.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects.

    Extra fields passed via logging.info("msg", extra={...}) are merged
    into the top-level JSON object, making them trivially queryable.
    """

    # Fields that the LogRecord always carries — we don't want these
    # polluting the structured output as they're either redundant or noise.
    _SKIP = frozenset({
        "args", "created", "exc_info", "exc_text", "filename", "funcName",
        "levelno", "lineno", "module", "msecs", "msg", "name", "pathname",
        "process", "processName", "relativeCreated", "stack_info",
        "taskName", "thread", "threadName",
    })

    def format(self, record: logging.LogRecord) -> str:
        # Render the exception string *before* we build the dict so we
        # can include it as a plain string field rather than raw exc_info.
        if record.exc_info:
            record.exc_text = self.formatException(record.exc_info)

        output = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
        }

        # Merge any structured extra fields passed by the caller
        for key, value in record.__dict__.items():
            if key not in self._SKIP:
                output[key] = value

        # Append formatted exception if present
        if record.exc_text:
            output["exception"] = record.exc_text

        return json.dumps(output, default=str)


def configure_logging(level: str = "INFO") -> None:
    """
    Configure the 'kafka_orchestrator' logger with JSON output to stdout.

    Args:
        level: One of DEBUG, INFO, WARNING, ERROR, CRITICAL.
               Defaults to INFO.  Pass DEBUG to see offset commits and
               checkpoint writes during development.

    Call once at application startup::

        from kafka_orchestrator.logging_config import configure_logging
        configure_logging(level="INFO")
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())

    lib_logger = logging.getLogger("kafka_orchestrator")
    lib_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    lib_logger.addHandler(handler)
    # Don't propagate to the root logger — avoids double-logging when the
    # application also has a root handler configured.
    lib_logger.propagate = False

    lib_logger.info(
        "kafka_orchestrator logging configured",
        extra={"level": level},
    )