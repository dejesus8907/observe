"""Streaming engine API endpoints — REST + WebSocket.

REST endpoints:
  GET  /api/streaming/status          -- engine health, session counts, bus stats
  GET  /api/streaming/events          -- recent ChangeEvents (query params: kind, device_id, severity, limit)
  GET  /api/streaming/topology        -- live topology summary
  GET  /api/streaming/topology/degraded  -- current degraded edges
  GET  /api/streaming/topology/neighbors/{device_id}  -- live neighbors
  POST /api/streaming/devices/register   -- register a device for streaming

WebSocket:
  WS   /api/streaming/ws              -- real-time stream of ChangeEvents and TopologyDeltas

WebSocket protocol
------------------
Client connects. Server sends all subsequent ChangeEvents and TopologyDeltas as
JSON objects with a "type" field:
  {"type": "change_event", "event_id": "...", "kind": "interface_down", ...}
  {"type": "topology_delta", "delta_id": "...", "change_kind": "degraded", ...}
  {"type": "ping", "ts": "..."}  -- sent every 30s to keep connection alive

Client may send:
  {"type": "subscribe", "kinds": ["interface_down", "bgp_session_dropped"]}
  {"type": "unsubscribe", "kinds": [...]}
  {"type": "pong"}

If the server does not receive a pong within 60s, it closes the connection.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel, Field

from netobserv.observability.logging import get_logger

router = APIRouter()
logger = get_logger("streaming.router")


def _get_engine(request: Request) -> Any:
    engine = getattr(request.app.state, "streaming_engine", None)
    if engine is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Streaming engine is not initialized or not enabled",
        )
    return engine


def _get_repository(request: Request) -> Any:
    repo = getattr(request.app.state, "streaming_repository", None)
    if repo is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Streaming repository is not initialized or not enabled",
        )
    return repo


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------


@router.get("/status")
async def get_streaming_status(request: Request) -> dict[str, Any]:
    """Get streaming engine health, session statistics, and observability data."""
    engine = _get_engine(request)
    topology_summary = engine.get_live_topology_summary()
    bus_stats = engine.get_bus_stats()
    pool_stats = engine.get_pool_stats()
    return {
        "engine_running": engine.is_running,
        "ws_clients_connected": engine.ws_client_count,
        "topology": topology_summary,
        "event_bus": bus_stats,
        "ssh_pool": pool_stats,
    }


@router.get("/topology")
async def get_live_topology(request: Request) -> dict[str, Any]:
    """Get the current live topology summary from the streaming patcher."""
    engine = _get_engine(request)
    return {
        "source": "streaming_patcher",
        "topology": engine.get_live_topology_summary(),
    }


@router.get("/topology/degraded")
async def get_degraded_edges(request: Request) -> dict[str, Any]:
    """Get all currently degraded topology edges."""
    engine = _get_engine(request)
    edges = engine.get_live_degraded_edges()
    return {
        "degraded_edges": edges,
        "total": len(edges),
    }


@router.get("/topology/neighbors/{device_id}")
async def get_live_neighbors(device_id: str, request: Request) -> dict[str, Any]:
    """Get live topology neighbors for a device from the streaming patcher."""
    engine = _get_engine(request)
    neighbors = engine.get_live_neighbors(device_id)
    return {
        "device_id": device_id,
        "neighbors": neighbors,
        "total": len(neighbors),
        "source": "streaming_patcher",
    }


class DeviceRegistrationRequest(BaseModel):
    device_id: str
    hostname: str
    management_ip: str | None = None
    platform: str = "generic"
    transport: str = "ssh"  # ssh | gnmi
    port: int | None = None
    # SSH options
    username: str = "netobserv"
    poll_interval_seconds: float = 30.0
    # gNMI options
    gnmi_port: int = 6030
    gnmi_insecure: bool = False


@router.post("/devices/register", status_code=status.HTTP_202_ACCEPTED)
async def register_device(
    payload: DeviceRegistrationRequest,
    request: Request,
) -> dict[str, Any]:
    """Register a device for real-time streaming collection."""
    engine = _get_engine(request)

    if payload.transport == "gnmi":
        from netobserv.streaming.gnmi_collector import GnmiTargetConfig
        engine.register_device_gnmi(GnmiTargetConfig(
            device_id=payload.device_id,
            hostname=payload.management_ip or payload.hostname,
            port=payload.gnmi_port,
            username=payload.username,
            insecure=payload.gnmi_insecure,
        ))
    else:
        from netobserv.streaming.session_pool import SessionConfig
        engine.register_device_ssh(SessionConfig(
            device_id=payload.device_id,
            hostname=payload.management_ip or payload.hostname,
            port=payload.port or 22,
            username=payload.username,
            platform=payload.platform,
            poll_interval_seconds=payload.poll_interval_seconds,
        ))

    if payload.management_ip:
        engine.register_ip_mapping(payload.management_ip, payload.device_id)

    return {
        "device_id": payload.device_id,
        "transport": payload.transport,
        "status": "registered",
        "message": f"Device {payload.device_id} registered for {payload.transport} streaming",
    }


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------



@router.get("/events")
async def get_recent_events(
    request: Request,
    kind: str | None = Query(default=None),
    device_id: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    repo = _get_repository(request)
    events = repo.list_recent_events(limit=limit, kind=kind, device_id=device_id, severity=severity)
    return {"events": events, "total": len(events)}


@router.get("/sessions")
async def list_streaming_sessions(
    request: Request,
    limit: int = Query(default=200, ge=1, le=500),
) -> dict[str, Any]:
    repo = _get_repository(request)
    sessions = repo.list_sessions(limit=limit)
    return {"sessions": sessions, "total": len(sessions)}

@router.websocket("/ws")
async def streaming_websocket(websocket: WebSocket, request: Request) -> None:
    """WebSocket endpoint — streams ChangeEvents and TopologyDeltas in real-time.

    Clients receive JSON messages as events occur. No polling required.
    """
    engine = getattr(websocket.app.state, "streaming_engine", None)
    if engine is None:
        await websocket.close(code=1011, reason="Streaming engine unavailable")
        return

    client_id = str(uuid.uuid4())
    await websocket.accept()

    # Get a dedicated queue for this client
    client_queue: asyncio.Queue[dict[str, Any]] = engine.subscribe_websocket(client_id)

    # Optional kind filter (client can update via subscribe message)
    kind_filter: set[str] | None = None

    try:
        # Send connection acknowledgement
        await websocket.send_json({
            "type": "connected",
            "client_id": client_id,
            "message": "Streaming connected. You will receive ChangeEvents and TopologyDeltas.",
        })

        async def receive_loop() -> None:
            """Handle incoming messages from client (subscribe/unsubscribe/pong)."""
            nonlocal kind_filter
            while True:
                try:
                    msg = await asyncio.wait_for(websocket.receive_text(), timeout=0.1)
                    data = json.loads(msg)
                    if data.get("type") == "subscribe":
                        kind_filter = set(data.get("kinds", []))
                    elif data.get("type") == "unsubscribe":
                        kind_filter = None
                except asyncio.TimeoutError:
                    pass
                except json.JSONDecodeError as exc:
                    logger.warning("Malformed websocket payload received", error=str(exc), client_id=client_id)
                    continue
                except Exception as exc:
                    logger.warning("Websocket receive loop terminated", error=str(exc), client_id=client_id)
                    break

        async def send_loop() -> None:
            """Forward events from the client queue to the WebSocket."""
            ping_counter = 0
            while True:
                try:
                    payload = await asyncio.wait_for(client_queue.get(), timeout=30.0)
                    # Apply kind filter
                    if kind_filter is not None:
                        event_kind = payload.get("kind") or payload.get("change_kind", "")
                        if event_kind not in kind_filter:
                            continue
                    await websocket.send_json(payload)
                    client_queue.task_done()
                    ping_counter = 0
                except asyncio.TimeoutError:
                    # Send keepalive ping
                    ping_counter += 1
                    await websocket.send_json({
                        "type": "ping",
                        "ts": datetime.now(timezone.utc).isoformat(),
                    })
                    if ping_counter > 2:
                        # No activity for 90s — close
                        break
                except Exception as exc:
                    logger.warning("Websocket send loop terminated", error=str(exc), client_id=client_id)
                    break

        # Run send and receive loops concurrently
        send_task = asyncio.create_task(send_loop())
        recv_task = asyncio.create_task(receive_loop())

        done, pending = await asyncio.wait(
            [send_task, recv_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.error("Websocket stream crashed", error=str(exc), client_id=client_id, exc_info=True)
    finally:
        engine.unsubscribe_websocket(client_id)
        try:
            await websocket.close()
        except Exception as exc:
            logger.warning("Websocket close failed", error=str(exc), client_id=client_id)


@router.get("/resolved-assertions")
async def list_resolved_assertions(request: Request, limit: int = 100) -> dict[str, Any]:
    engine = _get_engine(request)
    rows = await engine._repository.list_recent_resolved_assertions(limit=limit)
    return {"resolved_assertions": rows, "total": len(rows)}
