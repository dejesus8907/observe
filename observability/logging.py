"""Structured logging using structlog.

The logging layer exists to enforce consistency, not to dump pretty JSON and
pretend the platform is observable. The old version configured structlog, but it
was too bare: no request/workflow binding helpers, no call-site detail, and no
safe reset path for repeated test configuration.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

_CONFIGURED = False


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    """Configure structlog with the given log level and format.

    The function is idempotent enough for tests and CLI entry points. Repeated
    configuration should not keep stacking handlers or mutate behavior in weird
    ways.
    """
    global _CONFIGURED
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root = logging.getLogger()

    # Blow away inherited handlers so repeated initialization does not duplicate
    # every message. That problem looks harmless until tests and workers turn it
    # into unreadable garbage.
    for handler in list(root.handlers):
        root.removeHandler(handler)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=numeric_level,
    )

    renderer: Any
    if fmt == "console":
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.CallsiteParameterAdder(
            {
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            }
        ),
        structlog.processors.ExceptionRenderer(),
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    _CONFIGURED = True


def bind_context(**values: Any) -> None:
    """Bind context variables that will be merged into subsequent log lines."""
    structlog.contextvars.bind_contextvars(**values)


def clear_context(*keys: str) -> None:
    """Clear specific context keys or wipe the full bound context."""
    if keys:
        structlog.contextvars.unbind_contextvars(*keys)
    else:
        structlog.contextvars.clear_contextvars()


def get_logger(name: str | None = None, **initial_values: Any) -> structlog.BoundLogger:
    """Return a bound structlog logger with optional initial context values."""
    if not _CONFIGURED:
        configure_logging()
    logger = structlog.get_logger(name)
    if initial_values:
        logger = logger.bind(**initial_values)
    return logger  # type: ignore[return-value]
