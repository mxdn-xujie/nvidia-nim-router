"""数据表 dataclass 定义（§4.1）。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Upstream:
    """上游 NVIDIA Key 池条目。"""

    id: int
    email: str | None
    password: str | None
    apikey: str
    status: str                 # active | cooldown | disabled | invalid
    cooldown_until: int         # epoch 秒
    last_check_at: int | None
    last_latency_ms: int | None
    last_http_code: int | None
    last_error: str | None
    total_requests: int
    total_tokens: int
    consecutive_failures: int
    created_at: int


@dataclass
class Downstream:
    """下游访问 Key 条目。"""

    id: int
    name: str | None
    apikey: str
    enabled: int
    total_requests: int
    total_tokens: int
    last_used_at: int | None
    created_at: int