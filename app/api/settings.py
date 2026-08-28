"""设置 API（§8.1 / §4.2）。"""
from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException, Request

from ..config import SETTING_RANGES, VALID_STRATEGIES, VALID_THEMES

if TYPE_CHECKING:
    from ..database import Database

router = APIRouter()


def _settings_view(db: Database) -> dict:
    s = db.get_settings()

    def as_int(key: str, default: int) -> int:
        try:
            return int(s.get(key, default))
        except (TypeError, ValueError):
            return default

    return {
        "strategy": s.get("strategy", "round_robin"),
        "switch_every": as_int("switch_every", 3),
        "timeout_ms": as_int("timeout_ms", 30000),
        "max_retries": as_int("max_retries", 3),
        "cooldown_seconds": as_int("cooldown_seconds", 60),
        "theme": s.get("theme", "dark"),
        "admin_password_set": bool(s.get("admin_password")),
        "nvidia_base_url": s.get("nvidia_base_url", "https://integrate.api.nvidia.com/v1"),
    }


@router.get("/api/settings")
async def get_settings(request: Request) -> dict:
    db: Database = request.app.state.db
    return _settings_view(db)


@router.put("/api/settings")
async def update_settings(request: Request) -> dict:
    db: Database = request.app.state.db
    body = await request.json()
    updates: dict[str, str] = {}

    if "strategy" in body:
        strategy = str(body["strategy"])
        if strategy not in VALID_STRATEGIES:
            raise HTTPException(status_code=400, detail="strategy 必须是 round_robin/random/sequential")
        updates["strategy"] = strategy

    if "theme" in body:
        theme = str(body["theme"])
        if theme not in VALID_THEMES:
            raise HTTPException(status_code=400, detail="theme 必须是 dark/light")
        updates["theme"] = theme

    # 数值项：超出范围时收敛到边界
    for key in ("switch_every", "timeout_ms", "max_retries", "cooldown_seconds"):
        if key in body:
            low, high = SETTING_RANGES[key]
            try:
                value = int(body[key])
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail=f"{key} 必须是整数") from None
            updates[key] = str(max(low, min(high, value)))

    if "admin_password" in body:
        # 留空 = 不启用后台鉴权
        updates["admin_password"] = str(body["admin_password"] or "")

    if "nvidia_base_url" in body:
        base_url = str(body["nvidia_base_url"] or "").strip().rstrip("/")
        if not base_url.startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail="nvidia_base_url 必须以 http(s):// 开头")
        updates["nvidia_base_url"] = base_url

    for key, value in updates.items():
        db.set_setting(key, value)
    return _settings_view(db)