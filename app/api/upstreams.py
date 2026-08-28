"""上游管理 API：CRUD、CSV 导入、连通性检测（§8.1）。"""
from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import httpx
from fastapi import APIRouter, HTTPException, Request

from ..csv_import import decode_csv_bytes, import_csv
from ..models import Upstream

if TYPE_CHECKING:
    from ..database import Database

router = APIRouter()

# 连通性检测并发信号量（检测任务数 = 池内 key 总数，动态无上限假设）
CHECK_CONCURRENCY = 10
CHECK_TIMEOUT_SECONDS = 5.0


# ---------- 脱敏 ----------
def mask_key(apikey: str) -> str:
    if apikey.startswith("nvapi-"):
        return f"nvapi-****{apikey[-4:]}"
    return f"****{apikey[-4:]}"


def mask_email(email: str | None) -> str | None:
    if not email:
        return None
    if "@" in email:
        local, _, domain = email.partition("@")
        head = local[:2] if len(local) >= 2 else local
        return f"{head}***@{domain}"
    return f"{email[:2]}***"


def mask_password(password: str | None) -> str | None:
    if not password:
        return None
    if len(password) <= 4:
        return "****"
    return f"{password[:2]}****{password[-2:]}"


def serialize_upstream(u: Upstream) -> dict:
    now = int(time.time())
    return {
        "id": u.id,
        "email": mask_email(u.email),
        "password": mask_password(u.password),
        "apikey": mask_key(u.apikey),
        "status": u.status,
        "cooldown_remaining": max(0, u.cooldown_until - now) if u.status == "cooldown" else 0,
        "last_check_at": u.last_check_at,
        "last_latency_ms": u.last_latency_ms,
        "last_http_code": u.last_http_code,
        "last_error": u.last_error,
        "total_requests": u.total_requests,
        "total_tokens": u.total_tokens,
        "created_at": u.created_at,
    }


# ---------- 连通性检测（§6.5） ----------
async def check_upstream(db: Database, upstream: Upstream, client: httpx.AsyncClient) -> dict:
    """单个 key 检测：GET $BASE/models，超时 5 秒。"""
    base_url = (db.get_setting("nvidia_base_url") or "").rstrip("/")
    start = time.perf_counter()
    code: int | None = None
    error: str | None = None
    try:
        resp = await client.get(
            f"{base_url}/models",
            headers={"Authorization": f"Bearer {upstream.apikey}"},
        )
        code = resp.status_code
    except Exception as exc:  # noqa: BLE001 - 任何网络异常都记录
        error = type(exc).__name__
    latency_ms = int((time.perf_counter() - start) * 1000)

    # 判定规则：200→active；401→invalid；429→cooldown；其他→记录 last_error 状态不变；
    # disabled 的 key 只记录检测结果，不改状态
    if upstream.status == "disabled":
        status: str | None = None
        cooldown_seconds = 0
    elif code == 200:
        status = "active"
        cooldown_seconds = 0
    elif code == 401:
        status = "invalid"
        cooldown_seconds = 0
    elif code == 429:
        status = "cooldown"
        try:
            cooldown_seconds = int(db.get_setting("cooldown_seconds") or 60)
        except (TypeError, ValueError):
            cooldown_seconds = 60
    else:
        status = None
        cooldown_seconds = 0
    if error is None and code is not None and code != 200:
        error = f"HTTP {code}"

    db.record_check(
        upstream.id, latency_ms, code, error, status=status, cooldown_seconds=cooldown_seconds
    )
    updated = db.get_upstream(upstream.id)
    return serialize_upstream(updated if updated is not None else upstream)


async def _run_check_all(db: Database) -> None:
    """全部并发检测（后台任务）：Semaphore(10)，任务数 = 池内 key 总数。"""
    upstream_list = db.get_all_upstreams()
    semaphore = asyncio.Semaphore(CHECK_CONCURRENCY)
    timeout = httpx.Timeout(CHECK_TIMEOUT_SECONDS, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        async def one(u: Upstream) -> None:
            async with semaphore:
                try:
                    await check_upstream(db, u, client)
                except Exception:  # noqa: BLE001 - 单个失败不影响整体
                    pass

        await asyncio.gather(*(one(u) for u in upstream_list))


# ---------- CRUD ----------
@router.get("/api/upstreams")
async def list_upstreams(request: Request, page: int = 1, size: int = 50) -> dict:
    db: Database = request.app.state.db
    page = max(1, page)
    size = min(max(1, size), 200)
    items, total = db.list_upstreams(page=page, size=size)
    return {
        "items": [serialize_upstream(u) for u in items],
        "total": total,
        "page": page,
        "size": size,
    }


@router.post("/api/upstreams")
async def add_upstream(request: Request) -> dict:
    db: Database = request.app.state.db
    body = await request.json()
    apikey = str(body.get("apikey") or "").strip()
    if not apikey.startswith("nvapi-"):
        raise HTTPException(status_code=400, detail="apikey 必须以 nvapi- 开头")
    email = str(body.get("email") or "").strip() or None
    password = str(body.get("password") or "").strip() or None
    if not db.add_upstream(email, password, apikey):
        raise HTTPException(status_code=409, detail="该 Key 已存在")
    return {"ok": True}


@router.post("/api/upstreams/import")
async def import_csv_endpoint(request: Request) -> dict:
    """CSV 导入：multipart 文件上传 或 JSON {content: 文本}。"""
    db: Database = request.app.state.db
    content_type = (request.headers.get("content-type") or "").lower()
    if content_type.startswith("multipart/form-data"):
        form = await request.form()
        upload = form.get("file")
        if upload is None or isinstance(upload, str):
            raise HTTPException(status_code=400, detail="请选择要导入的 CSV 文件")
        data = await upload.read()
    else:
        body = await request.json()
        content = body.get("content")
        if not content:
            raise HTTPException(status_code=400, detail="CSV 内容不能为空")
        data = str(content).encode("utf-8")
    try:
        text = decode_csv_bytes(data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return import_csv(db, text)


@router.delete("/api/upstreams/{upstream_id}")
async def delete_upstream(request: Request, upstream_id: int) -> dict:
    db: Database = request.app.state.db
    if not db.delete_upstream(upstream_id):
        raise HTTPException(status_code=404, detail="Key 不存在")
    return {"ok": True}


@router.post("/api/upstreams/check_all")
async def check_all(request: Request) -> dict:
    """全部并发检测（后台执行，前端轮询列表刷新结果）。"""
    db: Database = request.app.state.db
    total = db.count_upstreams_by_status()["total"]
    asyncio.create_task(_run_check_all(db))
    return {"started": total}


@router.post("/api/upstreams/{upstream_id}/check")
async def check_one(request: Request, upstream_id: int) -> dict:
    db: Database = request.app.state.db
    upstream = db.get_upstream(upstream_id)
    if upstream is None:
        raise HTTPException(status_code=404, detail="Key 不存在")
    timeout = httpx.Timeout(CHECK_TIMEOUT_SECONDS, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await check_upstream(db, upstream, client)


@router.post("/api/upstreams/{upstream_id}/toggle")
async def toggle_upstream(request: Request, upstream_id: int) -> dict:
    db: Database = request.app.state.db
    updated = db.toggle_upstream(upstream_id)
    if updated is None:
        raise HTTPException(status_code=404, detail="Key 不存在")
    return serialize_upstream(updated)