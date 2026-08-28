"""故障转移控制器（§6.3 处置表）。

对上游响应/异常进行分类，给出处置动作：
- 超时 / 连接错误 / 429 → 冷却 cooldown_seconds 后换 key 重试；
- 401 → 标记 invalid 后换 key 重试；
- 403（模型级权限不足）→ 只冷却 10 分钟，不标 invalid，换 key 重试；
- 5xx → 换 key 重试，key 本身不冷却；
- 400 / 404 / 422（请求本身错误）→ 不转移，原样返回下游，不消耗重试次数。
"""
from __future__ import annotations

# 处置动作常量
RETRY = "retry"                          # 换 key 重试，key 不冷却（5xx）
COOLDOWN_RETRY = "cooldown_retry"        # 冷却 cooldown_seconds 后重试（429 / 网络异常）
INVALID_RETRY = "invalid_retry"          # 标记 invalid 后重试（401）
COOLDOWN_10M_RETRY = "cooldown_10m_retry"  # 403：冷却 10 分钟，不标 invalid
NO_RETRY = "no_retry"                    # 请求本身错误，原样返回（400/404/422 及其余 4xx）

# 403 专用冷却时长（秒）
COOLDOWN_403_SECONDS = 600


def classify_status(status_code: int) -> str:
    """按上游 HTTP 状态码返回处置动作。"""
    if status_code == 429:
        return COOLDOWN_RETRY
    if status_code == 401:
        return INVALID_RETRY
    if status_code == 403:
        return COOLDOWN_10M_RETRY
    if 500 <= status_code < 600:
        return RETRY
    # 400 / 404 / 422 及其他 4xx 均视为请求本身错误，不转移
    return NO_RETRY


def classify_transport_error() -> str:
    """连接/读超时、网络不可达等传输层异常 → 冷却后重试。"""
    return COOLDOWN_RETRY