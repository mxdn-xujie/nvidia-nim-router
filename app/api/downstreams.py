"""下游 Key 管理 API（§8.1）。"""
from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Request

from ..downstream import generate_downstream_key
from ..models import Downstream

if TYPE_CHECKING:
    from ..database import Database

router = APIRouter()


def serialize_downstream(d: Downstream) -> dict:
    """管理端返回完整 key（UI 打码显示、复制按钮复制完整值）。"""
    return {
        "id": d.id,
        "name": d.name,
        "apikey": d.apikey,
        "enabled": bool(d.enabled),
        "total_requests": d.total_requests,
        "total_tokens": d.total_tokens,
        "last_used_at": d.last_used_at,
        "created_at": d.created_at,
    }


@router.get("/api/downstreams")
async def list_downstreams(request: Request) -> dict:
    db: Database = request.app.state.db
    return {"items": [serialize_downstream(d) for d in db.list_downstreams()]}


@router.post("/api/downstreams")
async def add_downstream(request: Request) -> dict:
    """签发下游 Key：sk-router-{uuid}，完整 key 仅此一次返回给前端。"""
    db: Database = request.app.state.db
    body = await request.json()
    name = str(body.get("name") or "").strip() or None
    created = db.add_downstream(name, generate_downstream_key())
    return serialize_downstream(created)


@router.delete("/api/downstreams/{downstream_id}")
async def delete_downstream(request: Request, downstream_id: int) -> dict:
    db: Database = request.app.state.db
    if not db.delete_downstream(downstream_id):
        raise HTTPException(status_code=404, detail="Key 不存在")
    return {"ok": True}


@router.post("/api/downstreams/{downstream_id}/toggle")
async def toggle_downstream(request: Request, downstream_id: int) -> dict:
    db: Database = request.app.state.db
    updated = db.toggle_downstream(downstream_id)
    if updated is None:
        raise HTTPException(status_code=404, detail="Key 不存在")
    return serialize_downstream(updated)