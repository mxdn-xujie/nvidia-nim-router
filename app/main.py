"""FastAPI 入口：生命周期、鉴权、路由注册、静态托管（§8）。"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import config
from .api import dashboard, downstreams, upstreams
from .api import settings as settings_api
from .database import Database
from .downstream import bearer_token, require_downstream_key
from .models import Downstream
from .scheduler import Scheduler
from .stats import StatsEngine
from .upstream import REQUEST_STRIP_HEADERS, UpstreamProxy

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("router")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 初始化组件
    db = Database()
    app.state.db = db
    app.state.scheduler = Scheduler(db)
    stats = StatsEngine()
    metrics = db.get_metrics()
    started_at = metrics.get("started_at")
    stats.restore(
        int(metrics.get("total_requests", 0)),
        int(metrics.get("total_tokens", 0)),
        float(started_at) if started_at else None,
    )
    if not started_at:
        db.set_metric("started_at", str(stats.started_at))
    app.state.stats = stats
    app.state.proxy = UpstreamProxy(db, app.state.scheduler, stats)

    # 定期落库 + 冷却恢复
    async def flush_loop():
        while True:
            await asyncio.sleep(StatsEngine.FLUSH_INTERVAL)
            try:
                stats.flush(db)
                db.restore_expired_cooldowns()
            except Exception:  # noqa: BLE001 - 后台任务不中断
                logger.exception("统计落库/冷却恢复失败")

    flush_task = asyncio.create_task(flush_loop())
    logger.info("nvidia-nim-router 已启动，数据库：%s", config.DB_PATH)
    yield
    # 优雅停机：立即落库，保证重启不丢累计数据
    flush_task.cancel()
    stats.flush(db)
    await app.state.proxy.aclose()
    db.close()


app = FastAPI(title="NVIDIA NIM Router", version=config.APP_VERSION, lifespan=lifespan)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """detail 为 OpenAI 风格错误体（含 error 键）时直接平铺返回。"""
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        return JSONResponse(status_code=exc.status_code, content=exc.detail)
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


async def require_admin(request: Request) -> None:
    """管理端鉴权：设置 admin_password 后，/api 需 Bearer（SSE 允许 ?token=）。"""
    password = request.app.state.db.get_setting("admin_password")
    if not password:
        return
    token = bearer_token(request) or request.query_params.get("token", "")
    if token != password:
        raise HTTPException(
            status_code=401,
            detail={
                "error": {
                    "message": "管理后台鉴权失败：请提供正确的管理密码。",
                    "type": "authentication_error",
                    "code": 401,
                }
            },
        )


# ---------- 管理端 API ----------
app.include_router(dashboard.public_router)
app.include_router(dashboard.router, dependencies=[Depends(require_admin)])
app.include_router(upstreams.router, dependencies=[Depends(require_admin)])
app.include_router(downstreams.router, dependencies=[Depends(require_admin)])
app.include_router(settings_api.router, dependencies=[Depends(require_admin)])


# ---------- 代理端（下游 Bearer Key 鉴权，§6.1 / §8.2） ----------
def _forward_headers(request: Request) -> dict:
    """透传请求头：剥离 hop-by-hop 与 Authorization（由代理替换）。"""
    return {
        k: v for k, v in request.headers.items() if k.lower() not in REQUEST_STRIP_HEADERS
    }


@app.post("/v1/chat/completions")
async def proxy_chat_completions(
    request: Request, downstream: Downstream = Depends(require_downstream_key)
):
    """核心端点：完整支持流式。"""
    return await request.app.state.proxy.forward(
        method="POST",
        path="/chat/completions",
        query=request.url.query,
        headers=_forward_headers(request),
        body=await request.body(),
        downstream_id=downstream.id,
    )


@app.post("/v1/completions")
async def proxy_completions(
    request: Request, downstream: Downstream = Depends(require_downstream_key)
):
    return await request.app.state.proxy.forward(
        method="POST",
        path="/completions",
        query=request.url.query,
        headers=_forward_headers(request),
        body=await request.body(),
        downstream_id=downstream.id,
    )


@app.get("/v1/models")
async def proxy_models(request: Request, downstream: Downstream = Depends(require_downstream_key)):
    return await request.app.state.proxy.forward(
        method="GET",
        path="/models",
        query=request.url.query,
        headers=_forward_headers(request),
        body=None,
        downstream_id=downstream.id,
    )


@app.get("/v1/models/{model}")
async def proxy_model_detail(
    request: Request, model: str, downstream: Downstream = Depends(require_downstream_key)
):
    return await request.app.state.proxy.forward(
        method="GET",
        path=f"/models/{model}",
        query=request.url.query,
        headers=_forward_headers(request),
        body=None,
        downstream_id=downstream.id,
    )


# ---------- 静态前端（最后挂载，API 路由优先） ----------
app.mount("/", StaticFiles(directory=config.WEB_DIR, html=True), name="web")