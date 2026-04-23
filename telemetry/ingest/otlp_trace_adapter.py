"""OTLP trace adapter — normalizes OTLP span data into TelemetryEnvelope.

Accepts spans in OTLP-compatible dict format (what a gRPC/HTTP receiver
would produce after protobuf deserialization) and outputs normalized
TelemetryEnvelope objects with SpanRecord payloads and TargetRef identity.

The actual gRPC server is not part of this module. This handles the
normalization: OTLP resource/scope/span attributes → TargetRef fields,
OTLP span → SpanRecord, and enrichment of the envelope with request
identity.

OTLP span structure (simplified):
    ResourceSpans[]:
        Resource: {attributes: [{key, value}]}
        ScopeSpans[]:
            Scope: {name, version}
            Spans[]:
                trace_id, span_id, parent_span_id
                name (operation)
                kind
                start_time_unix_nano, end_time_unix_nano
                attributes, events, links, status
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from netobserv.telemetry.envelope import TelemetryEnvelope
from netobserv.telemetry.enums import (
    Severity,
    SignalType,
    SourceType,
    SpanKind,
    SpanStatusCode,
)
from netobserv.telemetry.identity import RequestRef, TargetRef
from netobserv.telemetry.resource_normalizer import ResourceNormalizer
from netobserv.telemetry.signals import SpanEvent, SpanLink, SpanRecord


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# Shared normalizer instance
_normalizer = ResourceNormalizer()


# OTLP span kind int → SpanKind enum
_SPAN_KIND_MAP: dict[int, SpanKind] = {
    0: SpanKind.UNSPECIFIED,
    1: SpanKind.INTERNAL,
    2: SpanKind.SERVER,
    3: SpanKind.CLIENT,
    4: SpanKind.PRODUCER,
    5: SpanKind.CONSUMER,
}

# OTLP status code int → SpanStatusCode
_STATUS_MAP: dict[int, SpanStatusCode] = {
    0: SpanStatusCode.UNSET,
    1: SpanStatusCode.OK,
    2: SpanStatusCode.ERROR,
}


class OtlpTraceAdapter:
    """Normalizes OTLP-style span data into TelemetryEnvelopes.

    Usage:
        adapter = OtlpTraceAdapter()
        envelopes = adapter.process_resource_spans(otlp_data)
        for env in envelopes:
            trace_store.store_span(env)
    """

    def process_resource_spans(
        self, resource_spans_list: list[dict[str, Any]]
    ) -> list[TelemetryEnvelope]:
        """Process a batch of OTLP ResourceSpans → TelemetryEnvelopes.

        Args:
            resource_spans_list: List of ResourceSpans dicts, each containing
                resource attributes and scope_spans.

        Returns:
            Flat list of TelemetryEnvelopes, one per span.
        """
        envelopes: list[TelemetryEnvelope] = []
        for resource_spans in resource_spans_list:
            resource = resource_spans.get("resource", {})
            resource_attrs = self._extract_attributes(
                resource.get("attributes", [])
            )
            target = self._build_target_ref(resource_attrs)

            for scope_spans in resource_spans.get("scope_spans", []):
                for span_data in scope_spans.get("spans", []):
                    env = self._span_to_envelope(span_data, target, resource_attrs)
                    if env:
                        envelopes.append(env)
        return envelopes

    def process_spans(
        self,
        spans: list[dict[str, Any]],
        resource_attrs: dict[str, str] | None = None,
    ) -> list[TelemetryEnvelope]:
        """Process a flat list of span dicts (convenience for testing).

        Args:
            spans: List of span dicts with trace_id, span_id, etc.
            resource_attrs: Optional shared resource attributes.
        """
        resource_attrs = resource_attrs or {}
        target = self._build_target_ref(resource_attrs)
        envelopes: list[TelemetryEnvelope] = []
        for span_data in spans:
            env = self._span_to_envelope(span_data, target, resource_attrs)
            if env:
                envelopes.append(env)
        return envelopes

    def _span_to_envelope(
        self,
        span: dict[str, Any],
        base_target: TargetRef,
        resource_attrs: dict[str, str],
    ) -> TelemetryEnvelope | None:
        """Convert one OTLP span dict to a TelemetryEnvelope."""
        trace_id = str(span.get("trace_id", ""))
        span_id = str(span.get("span_id", ""))
        if not trace_id or not span_id:
            return None

        parent_span_id = span.get("parent_span_id") or None
        if parent_span_id == "":
            parent_span_id = None

        # Timestamps
        start_ns = span.get("start_time_unix_nano", 0)
        end_ns = span.get("end_time_unix_nano", 0)
        start_time = self._nanos_to_datetime(start_ns) if start_ns else None
        end_time = self._nanos_to_datetime(end_ns) if end_ns else None
        duration_ms = (end_ns - start_ns) / 1_000_000 if start_ns and end_ns else 0.0

        # Span attributes
        span_attrs = self._extract_attributes(span.get("attributes", []))

        # Determine service name (span attr overrides resource attr)
        service_name = (
            span_attrs.get("service.name")
            or resource_attrs.get("service.name")
            or base_target.service
            or ""
        )

        # Kind and status
        kind = _SPAN_KIND_MAP.get(span.get("kind", 0), SpanKind.UNSPECIFIED)
        status_data = span.get("status", {})
        status_code = _STATUS_MAP.get(
            status_data.get("code", 0), SpanStatusCode.UNSET
        )
        status_message = str(status_data.get("message", ""))

        # Events
        events = [
            SpanEvent(
                name=e.get("name", ""),
                timestamp=self._nanos_to_datetime(e.get("time_unix_nano", 0)),
                attrs=self._extract_attributes(e.get("attributes", [])),
            )
            for e in span.get("events", [])
        ]

        # Links
        links = [
            SpanLink(
                trace_id=str(lnk.get("trace_id", "")),
                span_id=str(lnk.get("span_id", "")),
                attrs=self._extract_attributes(lnk.get("attributes", [])),
            )
            for lnk in span.get("links", [])
        ]

        # Build SpanRecord
        record = SpanRecord(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            operation_name=str(span.get("name", "")),
            service_name=service_name,
            kind=kind,
            status=status_code,
            status_message=status_message,
            start_time=start_time,
            end_time=end_time,
            duration_ms=duration_ms,
            attrs=span_attrs,
            events=events,
            links=links,
        )

        # Build envelope target (merge span-level attrs into base via normalizer)
        target = base_target
        if service_name and service_name != base_target.service:
            kwargs = {f.name: getattr(base_target, f.name)
                      for f in base_target.__dataclass_fields__.values()}
            kwargs["service"] = service_name
            target = TargetRef(**kwargs)

        # Enrich from span-level attributes via normalizer
        if span_attrs:
            span_target = _normalizer.normalize(span_attrs)
            target = target.merge(span_target)

        # Build request ref
        request = RequestRef(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            operation_id=record.operation_name or None,
        )

        # Severity from status
        severity = Severity.ERROR if status_code == SpanStatusCode.ERROR else Severity.UNSPECIFIED

        return TelemetryEnvelope(
            signal_type=SignalType.SPAN,
            source_type=SourceType.OTLP,
            target=target,
            request=request,
            observed_at=start_time or _utc_now(),
            received_at=_utc_now(),
            severity=severity,
            payload=record,
            ingestion_pipeline="otlp_trace",
        )

    def _build_target_ref(self, resource_attrs: dict[str, str]) -> TargetRef:
        """Build a TargetRef from OTLP resource attributes using the normalizer."""
        result = _normalizer.normalize_with_details(resource_attrs)
        return result.target

    @staticmethod
    def _extract_attributes(attrs_list: list[dict[str, Any]] | dict[str, Any]) -> dict[str, str]:
        """Convert OTLP attribute list to flat dict."""
        return _normalizer.flatten_otlp_attributes(attrs_list)

    @staticmethod
    def _nanos_to_datetime(nanos: int) -> datetime | None:
        if not nanos:
            return None
        seconds = nanos / 1_000_000_000
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
