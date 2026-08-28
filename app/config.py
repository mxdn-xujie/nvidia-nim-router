"""配置模块：数据目录、路径与 settings 表默认值（§4.2）。"""
from __future__ import annotations

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _default_data_dir() -> str:
    """数据目录：优先环境变量 DATA_DIR；Linux 容器用 /data；Windows 本地开发回退到项目内 data/。"""
    env = os.environ.get("DATA_DIR")
    if env:
        return env
    if os.name != "nt":
        return "/data"
    return os.path.join(PROJECT_ROOT, "data")


DATA_DIR = _default_data_dir()
DB_PATH = os.path.join(DATA_DIR, "router.db")
WEB_DIR = os.path.join(PROJECT_ROOT, "web")

APP_NAME = "nvidia-nim-router"
APP_VERSION = "1.0.0"

# settings 表预置默认值
DEFAULT_SETTINGS: dict[str, str] = {
    "strategy": "round_robin",       # round_robin / random / sequential
    "switch_every": "3",             # 每 N 次请求切换下一个 key（1-1000）
    "timeout_ms": "30000",           # 上游请求超时时间
    "max_retries": "3",              # 故障转移最大重试次数
    "cooldown_seconds": "60",        # 429/超时后 key 冷却时长
    "theme": "dark",                 # dark / light
    "admin_password": "",            # 管理后台密码，空 = 不鉴权
    "nvidia_base_url": "https://integrate.api.nvidia.com/v1",
    "auto_stream": "1",              # 自动流式：非流式请求转流式上游+聚合回包（1 开 / 0 关）
}

# 设置项取值范围（用于校验/收敛）
SETTING_RANGES = {
    "switch_every": (1, 1000),
    "timeout_ms": (1000, 120000),
    "max_retries": (1, 5),
    "cooldown_seconds": (10, 600),
}

VALID_STRATEGIES = ("round_robin", "random", "sequential")
VALID_THEMES = ("dark", "light")
