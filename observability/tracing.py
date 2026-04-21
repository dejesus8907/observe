"""OpenTelemetry-based tracing helpers for NetObserv."""

from __future__ import annotations

import contextlib
from collections.abc import Generator
from functools import lru_cache
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Span, Status, StatusCode

_provider_initialized = False


def initialize_tracing(
    service_name: str = "netobserv",
    endpoint: str | None = None,
) -> TracerProvider:
    """Initialize the global tracer provider once.

    The current project does not yet ship a production exporter stack, so the
    endpoint is accepted for forward compatibility but console export remains the
    default behavior.
    """
    del endpoint
    global _provider_initialized
    provider = trace.get_tracer_provider()
    if _provider_initialized and isinstance(provider, TracerProvider):
        return provider

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    _provider_initialized = True
    get_tracer.cache_clear()
    return provider


class Tracer:
    """Thin wrapper around OpenTelemetry tracer."""

    def __init__(self, name: str = "netobserv") -> None:
        self._tracer = trace.get_tracer(name)

    @contextlib.contextmanager
    def span(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ) -> Generator[Span, None, None]:
        """Context manager that creates and manages a trace span."""
        with self._tracer.start_as_current_span(name) as span:
            if attributes:
                for key, value in attributes.items():
                    span.set_attribute(key, str(value))
            try:
                yield span
                span.set_status(Status(StatusCode.OK))
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    def get_current_span(self) -> Span:
        return trace.get_current_span()


@lru_cache
def get_tracer(name: str = "netobserv") -> Tracer:
    """Return the process tracer singleton for the given instrumentation name."""
    return Tracer(name=name)
