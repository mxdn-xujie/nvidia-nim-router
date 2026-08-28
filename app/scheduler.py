"""调度器：三策略 + 每 N 次请求切换（§7）。

- round_robin / sequential：同一 key 连续服务 switch_every 次「成功转发」后切换到下一个可用 key；
- random：每次随机，switch_every 无效；
- 故障转移换 key 不消耗 N 次窗口（计数口径按成功转发次数）；
- 每次选取实时从数据库读取可用池，不缓存 key 列表快照（动态规模适配）。
"""
from __future__ import annotations

import asyncio
import random
from collections.abc import Iterable
from typing import TYPE_CHECKING

from .models import Upstream

if TYPE_CHECKING:
    from .database import Database


class Scheduler:
    """单例调度器，asyncio.Lock 保护计数器与索引。"""

    def __init__(self, db: Database):
        self._db = db
        self._lock = asyncio.Lock()
        self._current_id: int | None = None  # 当前窗口 key id
        self._served = 0                        # 当前 key 已成功服务次数
        self._advance_next = False              # 窗口到期，下次选取切换到下一个 key

    # ---------- 内部 ----------
    def _strategy(self) -> str:
        return self._db.get_setting("strategy") or "round_robin"

    def _switch_every(self) -> int:
        try:
            return max(1, int(self._db.get_setting("switch_every") or 3))
        except (TypeError, ValueError):
            return 3

    def _pool_excluding(self, exclude_ids: Iterable[int]) -> list[Upstream]:
        exclude = set(exclude_ids)
        return [u for u in self._db.get_available_upstreams() if u.id not in exclude]

    # ---------- 对外 ----------
    async def pick(self) -> Upstream | None:
        """为一次下游请求选取 key；无可用 key 返回 None（由调用方返回 503）。"""
        async with self._lock:
            pool = self._db.get_available_upstreams()
            if not pool:
                return None
            if self._strategy() == "random":
                return random.choice(pool)

            ids = [u.id for u in pool]
            # 窗口到期、或当前 key 已不在可用池（被冷却/删除）→ 切换
            if self._advance_next or self._current_id not in ids:
                if self._current_id in ids:
                    idx = ids.index(self._current_id)
                    self._current_id = ids[(idx + 1) % len(ids)]
                else:
                    self._current_id = ids[0]  # 从第一个可用 key 开始
                self._served = 0
                self._advance_next = False
            return pool[ids.index(self._current_id)]

    async def pick_failover(self, exclude_ids: Iterable[int]) -> Upstream | None:
        """故障转移选 key：排除已失败的 key，不消耗 N 次窗口、不改变当前窗口状态。"""
        async with self._lock:
            pool = self._pool_excluding(exclude_ids)
            if not pool:
                return None
            if self._strategy() == "random":
                return random.choice(pool)
            ids = [u.id for u in pool]
            if self._current_id in ids:
                idx = ids.index(self._current_id)
                return pool[(idx + 1) % len(pool)]
            return pool[0]

    async def report_success(self, upstream_id: int) -> None:
        """一次成功转发后上报：按成功次数推进 N 次切换窗口。"""
        async with self._lock:
            if self._strategy() == "random":
                return
            if upstream_id != self._current_id:
                # 故障转移换 key 不消耗窗口
                return
            self._served += 1
            if self._served >= self._switch_every():
                self._advance_next = True

    async def reset(self) -> None:
        """重置窗口状态（测试用）。"""
        async with self._lock:
            self._current_id = None
            self._served = 0
            self._advance_next = False