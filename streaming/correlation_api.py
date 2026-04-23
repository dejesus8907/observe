from __future__ import annotations
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from netobserv.streaming.correlation_repository import CorrelationRepository

router = APIRouter()

def _repo(request: Request) -> CorrelationRepository:
    repo = getattr(request.app.state, "correlation_repository", None)
    if repo is None:
        raise HTTPException(status_code=503, detail="Correlation repository is not initialized")
    return repo

@router.get("/clusters")
async def list_clusters(request: Request, state: str | None = None, limit: int = 100) -> dict[str, Any]:
    rows = await _repo(request).list_clusters(state=state, limit=limit)
    return {"clusters": rows, "total": len(rows)}

@router.get("/clusters/{cluster_id}")
async def get_cluster(cluster_id: str, request: Request) -> dict[str, Any]:
    history = await _repo(request).get_cluster_history(cluster_id)
    if history.get("cluster") is None:
        raise HTTPException(status_code=404, detail="Correlation cluster not found")
    return history

@router.get("/disputed")
async def list_disputed_clusters(request: Request, limit: int = 100) -> dict[str, Any]:
    rows = await _repo(request).list_disputed_clusters(limit=limit)
    return {"clusters": rows, "total": len(rows)}
