"""Telemetry HTTP API — FastAPI routes for all observability signals.

Endpoints:
    GET  /traces/{trace_id}              — get a trace by ID
    GET  /traces?service=X&limit=N       — list traces for a service
    GET  /metrics/golden-signals/{svc}   — golden signals for a service
    GET  /metrics/series?name=X          — list metric series
    GET  /metrics/anomalies              — detect current anomalies
    GET  /logs/search?q=X&service=Y      — search logs
    GET  /logs/trace/{trace_id}          — logs linked to a trace
    GET  /profiles/{service}             — profiles for a service
    GET  /profiles/hotspots/{service}    — CPU hotspots for a service
    GET  /services                       — list all services
    GET  /services/{name}/dependencies   — service dependencies
    GET  /services/{name}/impact         — impact radius
    POST /rca/investigate/service/{name} — run RCA for a service
    POST /rca/investigate/trace/{id}     — run RCA for a trace
    GET  /slos                           — list all SLOs
    GET  /slos/{name}                    — evaluate a single SLO
    GET  /slos/breached                  — list breached SLOs
    GET  /alerts                         — list active alerts
    GET  /alerts/all                     — list all alerts (including resolved)
    GET  /health                         — platform health and stats

    POST /correlation/metric-to-trace    — jump from metric to related traces
    POST /correlation/trace-to-network   — check network path for a trace
    POST /correlation/trace-to-profile   — find profiles for a trace
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, FastAPI, Query
from pydantic import BaseModel

from netobserv.telemetry.query_service import TelemetryQueryService
from netobserv.telemetry.slo_engine import SLOEngine
from netobserv.telemetry.alert_engine import AlertEngine


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# --- Request/Response models ---

class TraceListParams(BaseModel):
    service: str = ""
    limit: int = 20


class LogSearchParams(BaseModel):
    q: str = ""
    service: str = ""
    severity: str = ""
    limit: int = 50


class MetricToTraceRequest(BaseModel):
    metric_name: str
    service: str = ""
    limit: int = 5


class TraceToNetworkRequest(BaseModel):
    trace_id: str


class TraceToProfileRequest(BaseModel):
    trace_id: str
    profile_type: str = "cpu"


# --- Router factory ---

def create_telemetry_router(
    query_service: TelemetryQueryService,
    slo_engine: SLOEngine | None = None,
    alert_engine: AlertEngine | None = None,
) -> APIRouter:
    """Create a FastAPI router with all telemetry endpoints."""
    router = APIRouter()

    # ================================================================
    # Traces
    # ================================================================

    @router.get("/traces/{trace_id}")
    def get_trace(trace_id: str) -> dict[str, Any]:
        result = query_service.get_trace(trace_id)
        return {"success": result.success, "data": result.data, "error": result.error}

    @router.get("/traces")
    def list_traces(service: str = "", limit: int = 20) -> dict[str, Any]:
        if service:
            result = query_service.traces_for_service(service, limit=limit)
        else:
            result = query_service.traces_in_window(
                _utc_now() - __import__("datetime").timedelta(hours=1), _utc_now(), limit=limit
            )
        return {"success": result.success, "data": result.data, "count": result.count}

    # ================================================================
    # Metrics
    # ================================================================

    @router.get("/metrics/golden-signals/{service}")
    def golden_signals(service: str, window: float = 60.0) -> dict[str, Any]:
        result = query_service.golden_signals(service, window_seconds=window)
        return {"success": result.success, "data": result.data}

    @router.get("/metrics/series")
    def metric_series(name: str = "", service: str = "") -> dict[str, Any]:
        result = query_service.metric_series(name, service=service)
        return {"success": result.success, "data": result.data, "count": result.count}

    @router.get("/metrics/anomalies")
    def detect_anomalies() -> dict[str, Any]:
        result = query_service.detect_anomalies()
        return {"success": result.success, "data": result.data, "count": result.count}

    # ================================================================
    # Logs
    # ================================================================

    @router.get("/logs/search")
    def search_logs(q: str = "", service: str = "",
                    severity: str = "", limit: int = 50) -> dict[str, Any]:
        result = query_service.search_logs(
            body_contains=q, service=service, severity=severity, limit=limit
        )
        return {"success": result.success, "data": result.data, "count": result.count}

    @router.get("/logs/trace/{trace_id}")
    def logs_for_trace(trace_id: str) -> dict[str, Any]:
        result = query_service.logs_for_trace(trace_id)
        return {"success": result.success, "data": result.data, "count": result.count}

    # ================================================================
    # Profiles
    # ================================================================

    @router.get("/profiles/{service}")
    def profiles_for_service(service: str, profile_type: str = "cpu",
                              limit: int = 10) -> dict[str, Any]:
        result = query_service.profiles_for_service(service, profile_type, limit)
        return {"success": result.success, "data": result.data, "count": result.count}

    @router.get("/profiles/hotspots/{service}")
    def hotspots(service: str, profile_type: str = "cpu") -> dict[str, Any]:
        result = query_service.hotspots(service, profile_type)
        return {"success": result.success, "data": result.data, "count": result.count}

    # ================================================================
    # Services / Topology
    # ================================================================

    @router.get("/services")
    def all_services() -> dict[str, Any]:
        result = query_service.all_services()
        return {"success": result.success, "data": result.data, "count": result.count}

    @router.get("/services/{service}/dependencies")
    def service_dependencies(service: str) -> dict[str, Any]:
        result = query_service.service_dependencies(service)
        return {"success": result.success, "data": result.data, "count": result.count}

    @router.get("/services/{service}/impact")
    def service_impact(service: str) -> dict[str, Any]:
        result = query_service.service_impact(service)
        return {"success": result.success, "data": result.data, "count": result.count}

    # ================================================================
    # RCA
    # ================================================================

    @router.post("/rca/investigate/service/{service}")
    def investigate_service(service: str) -> dict[str, Any]:
        result = query_service.investigate_service(service)
        return {"success": result.success, "data": result.data}

    @router.post("/rca/investigate/trace/{trace_id}")
    def investigate_trace(trace_id: str) -> dict[str, Any]:
        result = query_service.investigate_trace(trace_id)
        return {"success": result.success, "data": result.data}

    # ================================================================
    # Correlation jumps
    # ================================================================

    @router.post("/correlation/metric-to-trace")
    def jump_metric_to_trace(req: MetricToTraceRequest) -> dict[str, Any]:
        result = query_service.jump_metric_to_trace(
            req.metric_name, service=req.service, limit=req.limit
        )
        return {"success": result.success, "data": result.data, "count": result.count}

    @router.post("/correlation/trace-to-network")
    def jump_trace_to_network(req: TraceToNetworkRequest) -> dict[str, Any]:
        result = query_service.jump_trace_to_network(req.trace_id)
        return {"success": result.success, "data": result.data}

    @router.post("/correlation/trace-to-profile")
    def jump_trace_to_profile(req: TraceToProfileRequest) -> dict[str, Any]:
        result = query_service.jump_trace_to_profile(
            req.trace_id, profile_type=req.profile_type
        )
        return {"success": result.success, "data": result.data, "count": result.count}

    # ================================================================
    # SLOs
    # ================================================================

    if slo_engine:
        @router.get("/slos")
        def list_slos() -> dict[str, Any]:
            slos = slo_engine.list_slos()
            return {"data": [
                {"name": s.name, "service": s.service, "target": s.target_pct,
                 "window_days": s.window_days}
                for s in slos
            ], "count": len(slos)}

        @router.get("/slos/breached")
        def breached_slos() -> dict[str, Any]:
            breached = slo_engine.breached_slos()
            return {"data": [
                {"name": s.slo_name, "service": s.service, "severity": s.severity.value,
                 "budget_remaining_pct": s.budget_remaining_pct,
                 "burn_rate_5m": s.burn_rate_5m, "summary": s.summary}
                for s in breached
            ], "count": len(breached)}

        @router.get("/slos/{slo_name}")
        def evaluate_slo(slo_name: str) -> dict[str, Any]:
            status = slo_engine.evaluate(slo_name)
            return {"data": {
                "slo_name": status.slo_name,
                "service": status.service,
                "target": status.target,
                "current_sli": status.current_sli,
                "budget_remaining_pct": status.budget_remaining_pct,
                "severity": status.severity.value,
                "burn_rate_5m": status.burn_rate_5m,
                "burn_rate_1h": status.burn_rate_1h,
                "is_breached": status.is_breached,
                "summary": status.summary,
            }}

    # ================================================================
    # Alerts
    # ================================================================

    if alert_engine:
        @router.get("/alerts")
        def active_alerts() -> dict[str, Any]:
            alerts = alert_engine.get_active_alerts()
            return {"data": [
                {"rule": a.rule_name, "state": a.state.value,
                 "severity": a.severity.value, "description": a.description,
                 "labels": a.labels, "fire_count": a.fire_count}
                for a in alerts
            ], "count": len(alerts)}

        @router.get("/alerts/all")
        def all_alerts() -> dict[str, Any]:
            alerts = alert_engine.get_all_alerts()
            return {"data": [
                {"rule": a.rule_name, "state": a.state.value,
                 "severity": a.severity.value, "description": a.description,
                 "labels": a.labels, "fire_count": a.fire_count}
                for a in alerts
            ], "count": len(alerts)}

    # ================================================================
    # Health / Stats
    # ================================================================

    @router.get("/health")
    def health() -> dict[str, Any]:
        result = query_service.platform_stats()
        return {
            "status": "healthy",
            "stats": result.data,
            "slo_count": len(slo_engine.list_slos()) if slo_engine else 0,
            "active_alerts": len(alert_engine.get_active_alerts()) if alert_engine else 0,
        }

    return router


def create_telemetry_app(
    query_service: TelemetryQueryService,
    slo_engine: SLOEngine | None = None,
    alert_engine: AlertEngine | None = None,
) -> FastAPI:
    """Create a complete FastAPI application with telemetry routes."""
    app = FastAPI(title="NetObserv Telemetry API", version="1.0.0")
    router = create_telemetry_router(query_service, slo_engine, alert_engine)
    app.include_router(router, prefix="/api/v1")
    return app
