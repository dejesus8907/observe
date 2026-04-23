from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request

from netobserv.streaming.repository import StreamingRepository

router = APIRouter()


def _stream_repo(request: Request) -> StreamingRepository:
    repo = getattr(request.app.state, "streaming_repository", None)
    if repo is None:
        raise HTTPException(status_code=503, detail="Streaming repository is not initialized")
    return repo


@router.get("/evidence")
async def list_evidence(
    request: Request,
    subject_type: str | None = None,
    subject_id: str | None = None,
    field_name: str | None = None,
    source_type: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    rows = await _stream_repo(request).list_recent_evidence(
        subject_type=subject_type,
        subject_id=subject_id,
        field_name=field_name,
        source_type=source_type,
        limit=limit,
    )
    return {"evidence": rows, "total": len(rows)}


@router.get("/disputes")
async def list_disputes(
    request: Request,
    subject_type: str | None = None,
    field_name: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    rows = await _stream_repo(request).list_disputes(
        subject_type=subject_type,
        field_name=field_name,
        limit=limit,
    )
    return {"disputes": rows, "total": len(rows)}


@router.get("/resolved-assertions")
async def list_resolved_assertions(
    request: Request,
    limit: int = 100,
) -> dict[str, Any]:
    rows = await _stream_repo(request).list_recent_resolved_assertions(limit=limit)
    return {"resolved_assertions": rows, "total": len(rows)}


@router.get("/resolved-assertions/{subject_type}/{subject_id}")
async def get_subject_resolved_assertions(
    request: Request,
    subject_type: str,
    subject_id: str,
    limit: int = 100,
) -> dict[str, Any]:
    rows = await _stream_repo(request).list_recent_resolved_assertions(limit=limit * 3)
    rows = [r for r in rows if r.get("subject_type") == subject_type and r.get("subject_id") == subject_id][:limit]
    return {
        "subject_type": subject_type,
        "subject_id": subject_id,
        "resolved_assertions": rows,
        "total": len(rows),
    }


@router.get("/subjects/{subject_id}/history")
async def get_subject_history(
    request: Request,
    subject_id: str,
    subject_type: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    return await _stream_repo(request).list_subject_history(
        subject_id=subject_id,
        subject_type=subject_type,
        limit=limit,
    )


@router.get("/topology/current")
async def get_current_topology(request: Request, limit: int = 200) -> dict[str, Any]:
    payload = await _stream_repo(request).get_current_topology_with_truth(limit=limit)
    return payload
