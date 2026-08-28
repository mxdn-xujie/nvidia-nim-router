"""统计引擎：内存滑动窗口 + 定期落库（§8.4）。

- 内存维护最近 60 秒请求与 token（rpm / tokens_per_sec）；
- 维护最近 60 分钟每分钟请求桶（requests_1h，60 个点）；
- 每 10 秒落库累计值到 metrics 表；重启后恢复 total_tokens / total_requests / started_at；
- 优雅停机时由 main 调用 flush() 立即落库一次。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .database import Database


class StatsEngine:
    WINDOW_SECONDS = 60   # rpm / tokens_per_sec 滑动窗口
    MINUTE_POINTS = 60    # requests_1h 点数（每分钟 1 点）
    FLUSH_INTERVAL = 10   # 落库间隔（秒）

    def __init__(self):
        self.total_requests = 0
        self.total_tokens = 0
        self.started_at = time.time()
        self._events: deque[tuple[float, int]] = deque()   # (时间戳, token 数) 最近 60 秒
        self._minute_counts: dict[int, int] = {}           # minute_epoch -> 请求数
        self._lock = threading.Lock()

    # ---------- 恢复 ----------
    def restore(
        self,
        total_requests: int,
        total_tokens: int,
        started_at: float | None = None,
    ) -> None:
        with self._lock:
            self.total_requests = int(total_requests)
            self.total_tokens = int(total_tokens)
            if started_at:
                self.started_at = float(started_at)

    # ---------- 记录 ----------
    def record_request(self, tokens: int = 0) -> None:
        now = time.time()
        minute = int(now // 60)
        with self._lock:
            self.total_requests += 1
            self.total_tokens += int(tokens)
            self._events.append((now, int(tokens)))
            self._prune(now)
            self._minute_counts[minute] = self._minute_counts.get(minute, 0) + 1
            # 清理 60 分钟之前的桶
            expired = minute - self.MINUTE_POINTS
            for m in [m for m in self._minute_counts if m < expired]:
                del self._minute_counts[m]

    def _prune(self, now: float) -> None:
        cutoff = now - self.WINDOW_SECONDS
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    # ---------- 快照 ----------
    def snapshot(self) -> dict:
        now = time.time()
        with self._lock:
            self._prune(now)
            rpm = len(self._events)
            window_tokens = sum(t for _, t in self._events)
            buckets = dict(self._minute_counts)
            total_requests = self.total_requests
            total_tokens = self.total_tokens
            started_at = self.started_at

        tokens_per_sec = round(window_tokens / self.WINDOW_SECONDS, 1)
        # 最近 60 分钟，每分钟 1 点，最旧在前
        current_minute = int(now // 60)
        requests_1h = [
            buckets.get(current_minute - i, 0) for i in range(self.MINUTE_POINTS - 1, -1, -1)
        ]
        return {
            "total_requests": total_requests,
            "rpm": rpm,
            "tokens_per_sec": tokens_per_sec,
            "total_tokens": total_tokens,
            "uptime_seconds": max(0, int(now - started_at)),
            "requests_1h": requests_1h,
        }

    # ---------- 落库 ----------
    def flush(self, db: Database) -> None:
        with self._lock:
            total_requests = self.total_requests
            total_tokens = self.total_tokens
            started_at = self.started_at
        db.set_metric("total_requests", str(total_requests))
        db.set_metric("total_tokens", str(total_tokens))
        db.set_metric("started_at", str(started_at))