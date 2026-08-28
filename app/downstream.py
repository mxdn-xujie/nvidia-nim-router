"""下游 Key 签发与鉴权（§8.2）。"""
from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from fastapi import HTTPException, Request

from .models import Downstream

if TYPE_CHECKING:
    from .database import Database


def generate_downstream_key() -> str:
    """生成下游访问 Key：sk-router-{uuid hex}。"""
    return f"sk-router-{uuid.uuid4().hex}"


def error_body(status: int, message: str, err_type: str = "invalid_request_error") -> dict:
    """OpenAI 官方风格错误体。"""
    return {"error": {"message": message, "type": err_type, "code": status}}


def bearer_token(request: Request) -> str:
    """从 Authorization: Bearer xxx 提取 token。"""
    auth = request.headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


async def require_downstream_key(request: Request) -> Downstream:
    """代理端鉴权依赖：校验下游 Bearer Key（enabled=1），失败返回 OpenAI 风格 401。"""
    db: Database = request.app.state.db
    token = bearer_token(request)
    row: Downstream | None = db.get_downstream_by_key(token) if token else None
    if row is None or not row.enabled:
        raise HTTPException(
            status_code=401,
            detail=error_body(
                401,
                "无效的 API Key：请使用下游管理页签发的 sk-router- 开头 Key。",
                "authentication_error",
            ),
        )
    return row