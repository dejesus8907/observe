from __future__ import annotations

from netobserv.observability.tracing import Tracer


def test_tracer_span_marks_attributes() -> None:
    tracer = Tracer("netobserv-test-tracing")
    with tracer.span("demo", {"attempt": 1, "values": [1, 2, 3]}):
        tracer.annotate_current_span(host="r1")


def test_async_tracer_decorator_compiles() -> None:
    tracer = Tracer("netobserv-test-tracing-async")

    @tracer.trace_async("work")
    async def work() -> int:
        return 1

    assert work.__name__ == "work"
