"""FastAPI application factory and startup wiring.

This file now reflects the strengthened runtime primitives rather than acting as
thin decoration over the rest of the platform. Startup initializes logging,
optional tracing, database tables, and a small amount of app state used by the
health endpoints.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from netobserv.api.routers import discovery, health, replay, runtime, snapshots, sync, topology, validation
from netobserv.config.settings import get_settings
from netobserv.observability.logging import configure_logging, get_logger
from netobserv.observability.tracing import _build_provider
from netobserv.runtime.bootstrap import build_runtime_components
from netobserv.streaming import correlation_api, truth_api
from netobserv.streaming import router as streaming_router
from netobserv.streaming.bootstrap import build_streaming_components
from netobserv.storage.database import close_db, configure_storage, init_db

logger = get_logger("api.app")


@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    """Initialize platform resources on startup and mark readiness truthfully."""
    settings = get_settings()
    configure_logging(level=settings.log_level, fmt=settings.log_format)

    if settings.tracing_enabled:
        _build_provider(service_name=settings.app_name.lower())

    app.state.startup_complete = False
    app.state.startup_error = None

    try:
        configure_storage(settings)
        await init_db()
        if settings.runtime_enabled:
            runtime_components = build_runtime_components(settings)
            app.state.runtime_repository = runtime_components.repository
            app.state.runtime_dispatcher = runtime_components.dispatcher
            app.state.runtime_recovery = runtime_components.recovery
            app.state.runtime_scheduler = runtime_components.scheduler
            app.state.runtime_worker = runtime_components.worker
            app.state.runtime_service = runtime_components.service
        if settings.streaming_enabled:
            streaming_components = build_streaming_components(settings)
            app.state.streaming_repository = streaming_components.repository
            app.state.streaming_engine = streaming_components.engine
            app.state.streaming_service = streaming_components.service
            app.state.correlation_repository = streaming_components.correlation_repository
            await app.state.streaming_service.start()
        app.state.startup_complete = True
        logger.info(
            "API startup complete",
            app_name=settings.app_name,
            version=settings.app_version,
            metrics_enabled=settings.metrics_enabled,
            tracing_enabled=settings.tracing_enabled,
            runtime_enabled=settings.runtime_enabled,
            streaming_enabled=settings.streaming_enabled,
        )
        yield
    except Exception as exc:  # pragma: no cover - startup failure path
        app.state.startup_error = str(exc)
        logger.error("API startup failed", error=str(exc), exc_info=True)
        raise
    finally:
        streaming_service = getattr(app.state, 'streaming_service', None)
        if streaming_service is not None:
            await streaming_service.stop()
        await close_db()
        logger.info("API shutdown complete")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        description="Advanced Network Discovery and Topology Mapping Observability Engine",
        version=settings.app_version,
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
    )

    if settings.cors_enabled:
        allow_origins = settings.cors_allow_origins
        allow_credentials = settings.cors_allow_credentials
        if "*" in allow_origins and allow_credentials:
            logger.warning(
                "Invalid CORS posture detected; disabling credentials because wildcard origins are configured"
            )
            allow_credentials = False
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allow_origins,
            allow_credentials=allow_credentials,
            allow_methods=settings.cors_allow_methods,
            allow_headers=settings.cors_allow_headers,
        )

    if settings.metrics_enabled:
        app.mount("/metrics", make_asgi_app())

    app.include_router(health.router, prefix="/api", tags=["health"])
    app.include_router(discovery.router, prefix="/api/discovery", tags=["discovery"])
    app.include_router(snapshots.router, prefix="/api/snapshots", tags=["snapshots"])
    app.include_router(topology.router, prefix="/api/topology", tags=["topology"])
    app.include_router(validation.router, prefix="/api/validation", tags=["validation"])
    app.include_router(sync.router, prefix="/api/sync", tags=["sync"])
    app.include_router(replay.router, prefix="/api", tags=["replay"])
    app.include_router(runtime.router, prefix="/api/runtime", tags=["runtime"])
    app.include_router(correlation_api.router, prefix="/api/streaming/correlation", tags=["correlation"])
    app.include_router(truth_api.router, prefix="/api/streaming", tags=["streaming_truth"])
    app.include_router(streaming_router.router, prefix="/api/streaming", tags=["streaming"])

    return app
