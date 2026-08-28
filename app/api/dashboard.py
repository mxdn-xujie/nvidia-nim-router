"""仪表盘 API 与 SSE 实时推送（§8.1 / §8.3）。"""
from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

if TYPE_CHECKING:
    from ..database import Database
    from ..stats import StatsEngine

# 公开路由（无需管理鉴权，Docker HEALTHCHECK 使用）
public_router = APIRouter()
# 管理路由（鉴权由 main.py 注入）
router = APIRouter()


def build_dashboard(db: Database, stats: StatsEngine) -> dict:
    """组装 dashboard 同构数据（/api/dashboard 与 /api/events 共用）。"""
    counts = db.count_upstreams_by_status()
    data = stats.snapshot()
    data["active_upstreams"] = counts["active"]
    data["total_upstreams"] = counts["total"]
    return data


@public_router.get("/api/health")
async def health() -> dict:
    """健康检查（Docker HEALTHCHECK 使用）。"""
    return {"status": "ok"}


@router.get("/api/dashboard")
async def dashboard(request: Request) -> dict:
    db: Database = request.app.state.db
    stats: StatsEngine = request.app.state.stats
    db.restore_expired_cooldowns()
    return build_dashboard(db, stats)


@router.get("/api/events")
async def events(request: Request) -> StreamingResponse:
    """SSE：每秒推送一次 dashboard 同构数据。"""
    db: Database = request.app.state.db
    stats: StatsEngine = request.app.state.stats

    async def generator():
        while True:
            if await request.is_disconnected():
                break
            db.restore_expired_cooldowns()
            data = build_dashboard(db, stats)
            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )