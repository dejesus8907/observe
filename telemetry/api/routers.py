"""Telemetry HTTP API routers.

FastAPI routers exposing the TelemetryQueryService, AlertManager,
and SLOStore over HTTP. Each router is a thin wrapper — all logic
lives in the query service and domain modules.

Mount into a FastAPI app:
    from netobserv.telemetry.api.routers import build_telemetry_router
    app = FastAPI()
    router = build_telemetry_router(query_service, alert_manager, slo_store)
    app.include_router(router, prefix="/api/v1")
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from netobserv.telemetry.query_service import TelemetryQueryService
from netobserv.telemetry.alerting import AlertManager
from netobserv.telemetry.slo import SLOStore


# ============================================================================
# Response models
# ============================================================================

class APIResponse(BaseModel):
    success: bool = True
    data: Any = None
    error: str = ""
    count: int = 0


# ============================================================================
# Traces router
# ============================================================================

def build_traces_router(qs: TelemetryQueryService) -> APIRouter:
    router = APIRouter(prefix="/traces", tags=["traces"])

    @router.get("/{trace_id}")
    def get_trace(trace_id: str) -> APIResponse:
        result = qs.get_trace(trace_id)
        if not result.success:
            raise HTTPException(status_code=404, detail=result.error)
        return APIResponse(data=result.data, count=result.count)

    @router.get("")
    def list_traces(
        service: str = Query(default="", description="Filter by service"),
        limit: int = Query(default=20, le=100),
    ) -> APIResponse:
        if service:
            result = qs.traces_for_service(service, limit=limit)
        else:
            result = qs.traces_for_service("", limit=limit)
        return APIResponse(data=result.data, count=result.count)

    @router.get("/{trace_id}/logs")
    def trace_logs(trace_id: str) -> APIResponse:
        result = qs.jump_trace_to_logs(trace_id)
        return APIResponse(data=result.data, count=result.count)

    @router.get("/{trace_id}/network")
    def trace_network(trace_id: str) -> APIResponse:
        result = qs.jump_trace_to_network(trace_id)
        return APIResponse(data=result.data)

    @router.get("/{trace_id}/profile")
    def trace_profile(
        trace_id: str,
        profile_type: str = Query(default="cpu"),
    ) -> APIResponse:
        result = qs.jump_trace_to_profile(trace_id, profile_type)
        return APIResponse(data=result.data, count=result.count)

    return router


# ============================================================================
# Metrics router
# ============================================================================

def build_metrics_router(qs: TelemetryQueryService) -> APIRouter:
    router = APIRouter(prefix="/metrics", tags=["metrics"])

    @router.get("/golden-signals/{service}")
    def golden_signals(
        service: str,
        window: float = Query(default=60.0, description="Window in seconds"),
    ) -> APIResponse:
        result = qs.golden_signals(service, window_seconds=window)
        return APIResponse(data=result.data)

    @router.get("/series/{metric_name}")
    def metric_series(
        metric_name: str,
        service: str = Query(default=""),
    ) -> APIResponse:
        result = qs.metric_series(metric_name, service=service)
        return APIResponse(data=result.data, count=result.count)

    @router.get("/anomalies")
    def detect_anomalies() -> APIResponse:
        result = qs.detect_anomalies()
        return APIResponse(data=result.data, count=result.count)

    @router.get("/jump/{metric_name}/traces")
    def metric_to_traces(
        metric_name: str,
        service: str = Query(default=""),
        limit: int = Query(default=5, le=20),
    ) -> APIResponse:
        result = qs.jump_metric_to_trace(metric_name, service=service, limit=limit)
        return APIResponse(data=result.data, count=result.count)

    return router


# ============================================================================
# Logs router
# ============================================================================

def build_logs_router(qs: TelemetryQueryService) -> APIRouter:
    router = APIRouter(prefix="/logs", tags=["logs"])

    @router.get("/search")
    def search_logs(
        q: str = Query(default="", description="Body text search"),
        service: str = Query(default=""),
        severity: str = Query(default=""),
        limit: int = Query(default=50, le=200),
    ) -> APIResponse:
        result = qs.search_logs(body_contains=q, service=service,
                                severity=severity, limit=limit)
        return APIResponse(data=result.data, count=result.count)

    @router.get("/trace/{trace_id}")
    def logs_for_trace(trace_id: str) -> APIResponse:
        result = qs.logs_for_trace(trace_id)
        return APIResponse(data=result.data, count=result.count)

    return router


# ============================================================================
# Profiles router
# ============================================================================

def build_profiles_router(qs: TelemetryQueryService) -> APIRouter:
    router = APIRouter(prefix="/profiles", tags=["profiles"])

    @router.get("/{service}")
    def profiles_for_service(
        service: str,
        profile_type: str = Query(default="cpu"),
        limit: int = Query(default=10, le=50),
    ) -> APIResponse:
        result = qs.profiles_for_service(service, profile_type=profile_type, limit=limit)
        return APIResponse(data=result.data, count=result.count)

    @router.get("/{service}/hotspots")
    def hotspots(
        service: str,
        profile_type: str = Query(default="cpu"),
    ) -> APIResponse:
        result = qs.hotspots(service, profile_type=profile_type)
        return APIResponse(data=result.data, count=result.count)

    return router


# ============================================================================
# Services router
# ============================================================================

def build_services_router(qs: TelemetryQueryService) -> APIRouter:
    router = APIRouter(prefix="/services", tags=["services"])

    @router.get("")
    def list_services() -> APIResponse:
        result = qs.all_services()
        return APIResponse(data=result.data, count=result.count)

    @router.get("/{service}/dependencies")
    def service_dependencies(service: str) -> APIResponse:
        result = qs.service_dependencies(service)
        return APIResponse(data=result.data, count=result.count)

    @router.get("/{service}/impact")
    def service_impact(service: str) -> APIResponse:
        result = qs.service_impact(service)
        return APIResponse(data=result.data, count=result.count)

    @router.get("/{service}/golden-signals")
    def service_golden_signals(
        service: str,
        window: float = Query(default=60.0),
    ) -> APIResponse:
        result = qs.golden_signals(service, window_seconds=window)
        return APIResponse(data=result.data)

    return router


# ============================================================================
# RCA router
# ============================================================================

def build_rca_router(qs: TelemetryQueryService) -> APIRouter:
    router = APIRouter(prefix="/rca", tags=["rca"])

    @router.get("/service/{service}")
    def investigate_service(service: str) -> APIResponse:
        result = qs.investigate_service(service)
        return APIResponse(data=result.data)

    @router.get("/trace/{trace_id}")
    def investigate_trace(trace_id: str) -> APIResponse:
        result = qs.investigate_trace(trace_id)
        return APIResponse(data=result.data)

    return router


# ============================================================================
# Alerts router
# ============================================================================

def build_alerts_router(alert_manager: AlertManager) -> APIRouter:
    router = APIRouter(prefix="/alerts", tags=["alerts"])

    @router.get("")
    def list_alerts(
        state: str = Query(default="", description="Filter: firing, active, all"),
    ) -> APIResponse:
        if state == "firing":
            alerts = alert_manager.firing_alerts()
        elif state == "active":
            alerts = alert_manager.active_alerts()
        else:
            alerts = alert_manager.all_alerts()
        data = [{
            "alert_id": a.alert_id,
            "rule_id": a.rule_id,
            "name": a.name,
            "severity": a.severity.value,
            "state": a.state.value,
            "service": a.service,
            "description": a.description,
            "value": a.value,
            "fired_at": a.fired_at.isoformat() if a.fired_at else None,
            "duration_seconds": a.duration_seconds,
        } for a in alerts]
        return APIResponse(data=data, count=len(data))

    @router.get("/stats")
    def alert_stats() -> APIResponse:
        return APIResponse(data=alert_manager.stats)

    @router.post("/silence/{rule_id}")
    def silence_rule(rule_id: str) -> APIResponse:
        alert_manager.silence_rule(rule_id)
        return APIResponse(data={"silenced": rule_id})

    @router.post("/unsilence/{rule_id}")
    def unsilence_rule(rule_id: str) -> APIResponse:
        alert_manager.unsilence_rule(rule_id)
        return APIResponse(data={"unsilenced": rule_id})

    return router


# ============================================================================
# SLO router
# ============================================================================

def build_slo_router(slo_store: SLOStore, metric_store: Any = None) -> APIRouter:
    router = APIRouter(prefix="/slos", tags=["slos"])

    @router.get("")
    def list_slos(service: str = Query(default="")) -> APIResponse:
        defs = slo_store.list_definitions(service=service or None)
        data = [{
            "slo_id": d.slo_id,
            "service": d.service,
            "sli_type": d.sli_type.value,
            "target": d.target,
            "window_days": d.window_days,
            "description": d.description,
        } for d in defs]
        return APIResponse(data=data, count=len(data))

    @router.get("/{slo_id}")
    def evaluate_slo(slo_id: str) -> APIResponse:
        result = slo_store.evaluate(slo_id, metric_store)
        return APIResponse(data={
            "slo_id": result.slo_id,
            "service": result.service,
            "status": result.status.value,
            "target": result.target,
            "error_budget_remaining": result.error_budget_remaining,
            "error_budget_consumed_percent": result.error_budget_consumed_percent,
            "fast_burn_alert": result.fast_burn_alert,
            "slow_burn_alert": result.slow_burn_alert,
            "burn_rates": [{
                "window": br.window_label,
                "burn_rate": br.burn_rate,
                "good": br.good_count,
                "bad": br.bad_count,
                "total": br.total_count,
            } for br in result.burn_rates if br.sufficient_data],
            "explanation": result.explanation,
        })

    @router.get("/evaluate/all")
    def evaluate_all_slos() -> APIResponse:
        results = slo_store.evaluate_all(metric_store)
        data = [{
            "slo_id": r.slo_id,
            "service": r.service,
            "status": r.status.value,
            "error_budget_consumed_percent": r.error_budget_consumed_percent,
            "worst_burn_rate": r.worst_burn_rate,
        } for r in results]
        return APIResponse(data=data, count=len(data))

    return router


# ============================================================================
# Platform stats router
# ============================================================================

def build_platform_router(qs: TelemetryQueryService) -> APIRouter:
    router = APIRouter(prefix="/platform", tags=["platform"])

    @router.get("/stats")
    def platform_stats() -> APIResponse:
        result = qs.platform_stats()
        return APIResponse(data=result.data)

    return router


# ============================================================================
# Composite router builder
# ============================================================================

def build_telemetry_router(
    query_service: TelemetryQueryService,
    alert_manager: AlertManager | None = None,
    slo_store: SLOStore | None = None,
    metric_store: Any = None,
) -> APIRouter:
    """Build the complete telemetry API router.

    Usage:
        app = FastAPI()
        router = build_telemetry_router(query_service, alert_manager, slo_store)
        app.include_router(router, prefix="/api/v1")
    """
    root = APIRouter()
    root.include_router(build_traces_router(query_service))
    root.include_router(build_metrics_router(query_service))
    root.include_router(build_logs_router(query_service))
    root.include_router(build_profiles_router(query_service))
    root.include_router(build_services_router(query_service))
    root.include_router(build_rca_router(query_service))
    root.include_router(build_platform_router(query_service))

    if alert_manager:
        root.include_router(build_alerts_router(alert_manager))
    if slo_store:
        root.include_router(build_slo_router(slo_store, metric_store))

    return root
