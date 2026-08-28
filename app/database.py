"""SQLite 初始化与 DAO 层（§4）。

单文件数据库（WAL 模式），所有操作经 threading.RLock 串行化，
SQLite 本地操作耗时微秒级，可直接在异步端点中同步调用。
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time

from .config import DB_PATH, DEFAULT_SETTINGS
from .models import Downstream, Upstream

SCHEMA = """
CREATE TABLE IF NOT EXISTS upstreams (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT,
  password TEXT,
  apikey TEXT NOT NULL UNIQUE,
  status TEXT DEFAULT 'active',
  cooldown_until INTEGER DEFAULT 0,
  last_check_at INTEGER,
  last_latency_ms INTEGER,
  last_http_code INTEGER,
  last_error TEXT,
  total_requests INTEGER DEFAULT 0,
  total_tokens INTEGER DEFAULT 0,
  consecutive_failures INTEGER DEFAULT 0,
  created_at INTEGER
);
CREATE TABLE IF NOT EXISTS downstreams (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  apikey TEXT NOT NULL UNIQUE,
  enabled INTEGER DEFAULT 1,
  total_requests INTEGER DEFAULT 0,
  total_tokens INTEGER DEFAULT 0,
  last_used_at INTEGER,
  created_at INTEGER
);
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT
);
CREATE TABLE IF NOT EXISTS metrics (
  key TEXT PRIMARY KEY,
  value TEXT
);
"""

_UPSTREAM_COLS = (
    "id, email, password, apikey, status, cooldown_until, last_check_at, "
    "last_latency_ms, last_http_code, last_error, total_requests, "
    "total_tokens, consecutive_failures, created_at"
)
_DOWNSTREAM_COLS = (
    "id, name, apikey, enabled, total_requests, total_tokens, last_used_at, created_at"
)


def _row_to_upstream(row: sqlite3.Row) -> Upstream:
    return Upstream(
        id=row["id"],
        email=row["email"],
        password=row["password"],
        apikey=row["apikey"],
        status=row["status"],
        cooldown_until=row["cooldown_until"],
        last_check_at=row["last_check_at"],
        last_latency_ms=row["last_latency_ms"],
        last_http_code=row["last_http_code"],
        last_error=row["last_error"],
        total_requests=row["total_requests"],
        total_tokens=row["total_tokens"],
        consecutive_failures=row["consecutive_failures"],
        created_at=row["created_at"],
    )


def _row_to_downstream(row: sqlite3.Row) -> Downstream:
    return Downstream(
        id=row["id"],
        name=row["name"],
        apikey=row["apikey"],
        enabled=row["enabled"],
        total_requests=row["total_requests"],
        total_tokens=row["total_tokens"],
        last_used_at=row["last_used_at"],
        created_at=row["created_at"],
    )


class Database:
    """DAO 层：上游 / 下游 / 设置 / 运行指标。"""

    def __init__(self, path: str = DB_PATH):
        self._path = path
        self._lock = threading.RLock()
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        with self._lock:
            self._conn.executescript(SCHEMA)
            for key, value in DEFAULT_SETTINGS.items():
                self._conn.execute(
                    "INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (key, value)
                )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- 内部工具 ----------
    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur

    def _query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def _query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params).fetchone()

    # ---------- settings ----------
    def get_setting(self, key: str) -> str | None:
        row = self._query_one("SELECT value FROM settings WHERE key=?", (key,))
        return row["value"] if row is not None else None

    def get_settings(self) -> dict[str, str]:
        return {r["key"]: r["value"] for r in self._query("SELECT key, value FROM settings")}

    def set_setting(self, key: str, value: str) -> None:
        self._execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    # ---------- upstream ----------
    def list_upstreams(self, page: int = 1, size: int = 50) -> tuple[list[Upstream], int]:
        total = self._query_one("SELECT COUNT(*) AS c FROM upstreams")["c"]
        offset = max(0, (page - 1) * size)
        rows = self._query(
            f"SELECT {_UPSTREAM_COLS} FROM upstreams ORDER BY id LIMIT ? OFFSET ?",
            (size, offset),
        )
        return [_row_to_upstream(r) for r in rows], total

    def get_all_upstreams(self) -> list[Upstream]:
        rows = self._query(f"SELECT {_UPSTREAM_COLS} FROM upstreams ORDER BY id")
        return [_row_to_upstream(r) for r in rows]

    def get_upstream(self, upstream_id: int) -> Upstream | None:
        row = self._query_one(
            f"SELECT {_UPSTREAM_COLS} FROM upstreams WHERE id=?", (upstream_id,)
        )
        return _row_to_upstream(row) if row is not None else None

    def get_available_upstreams(self) -> list[Upstream]:
        """可用池：status=active，或冷却已到期的 cooldown key；不缓存快照，实时读取。"""
        now = int(time.time())
        rows = self._query(
            f"SELECT {_UPSTREAM_COLS} FROM upstreams "
            "WHERE status='active' OR (status='cooldown' AND cooldown_until <= ?) "
            "ORDER BY id",
            (now,),
        )
        return [_row_to_upstream(r) for r in rows]

    def add_upstream(self, email: str | None, password: str | None, apikey: str) -> bool:
        """插入上游 key，重复（apikey 唯一约束）返回 False。"""
        cur = self._execute(
            "INSERT OR IGNORE INTO upstreams(email, password, apikey, status, cooldown_until, created_at) "
            "VALUES(?, ?, ?, 'active', 0, ?)",
            (email, password, apikey, int(time.time())),
        )
        return cur.rowcount > 0

    def delete_upstream(self, upstream_id: int) -> bool:
        return self._execute("DELETE FROM upstreams WHERE id=?", (upstream_id,)).rowcount > 0

    def set_upstream_status(self, upstream_id: int, status: str) -> None:
        self._execute(
            "UPDATE upstreams SET status=?, cooldown_until=0 WHERE id=?", (status, upstream_id)
        )

    def toggle_upstream(self, upstream_id: int) -> Upstream | None:
        row = self.get_upstream(upstream_id)
        if row is None:
            return None
        new_status = "active" if row.status == "disabled" else "disabled"
        self.set_upstream_status(upstream_id, new_status)
        return self.get_upstream(upstream_id)

    def cooldown_upstream(self, upstream_id: int, seconds: int, error: str | None = None) -> None:
        """进入冷却并累计连续失败次数。"""
        until = int(time.time()) + max(0, int(seconds))
        self._execute(
            "UPDATE upstreams SET status='cooldown', cooldown_until=?, "
            "consecutive_failures=consecutive_failures+1, last_error=COALESCE(?, last_error) "
            "WHERE id=?",
            (until, error, upstream_id),
        )

    def mark_invalid(self, upstream_id: int, error: str | None = None) -> None:
        self._execute(
            "UPDATE upstreams SET status='invalid', cooldown_until=0, "
            "last_error=COALESCE(?, last_error) WHERE id=?",
            (error, upstream_id),
        )

    def record_check(
        self,
        upstream_id: int,
        latency_ms: int | None,
        http_code: int | None,
        error: str | None,
        status: str | None = None,
        cooldown_seconds: int = 0,
    ) -> None:
        """记录连通性检测结果；status 非 None 时同步更新状态（disabled key 不改状态由调用方保证）。"""
        now = int(time.time())
        if status == "cooldown":
            self._execute(
                "UPDATE upstreams SET last_check_at=?, last_latency_ms=?, last_http_code=?, "
                "last_error=?, status='cooldown', cooldown_until=? WHERE id=?",
                (now, latency_ms, http_code, error, now + cooldown_seconds, upstream_id),
            )
        elif status is not None:
            self._execute(
                "UPDATE upstreams SET last_check_at=?, last_latency_ms=?, last_http_code=?, "
                "last_error=?, status=?, cooldown_until=0 WHERE id=?",
                (now, latency_ms, http_code, error, status, upstream_id),
            )
        else:
            self._execute(
                "UPDATE upstreams SET last_check_at=?, last_latency_ms=?, last_http_code=?, "
                "last_error=? WHERE id=?",
                (now, latency_ms, http_code, error, upstream_id),
            )

    def record_upstream_success(self, upstream_id: int, tokens: int) -> None:
        self._execute(
            "UPDATE upstreams SET total_requests=total_requests+1, total_tokens=total_tokens+?, "
            "consecutive_failures=0, last_error=NULL WHERE id=?",
            (tokens, upstream_id),
        )

    def record_upstream_failure(self, upstream_id: int, error: str | None = None) -> None:
        self._execute(
            "UPDATE upstreams SET consecutive_failures=consecutive_failures+1, "
            "last_error=COALESCE(?, last_error) WHERE id=?",
            (error, upstream_id),
        )

    def restore_expired_cooldowns(self) -> None:
        """冷却到期自动恢复 active。"""
        self._execute(
            "UPDATE upstreams SET status='active', cooldown_until=0 "
            "WHERE status='cooldown' AND cooldown_until <= ?",
            (int(time.time()),),
        )

    def count_upstreams_by_status(self) -> dict[str, int]:
        now = int(time.time())
        active = self._query_one(
            "SELECT COUNT(*) AS c FROM upstreams "
            "WHERE status='active' OR (status='cooldown' AND cooldown_until <= ?)",
            (now,),
        )["c"]
        total = self._query_one("SELECT COUNT(*) AS c FROM upstreams")["c"]
        return {"active": active, "total": total}

    # ---------- downstream ----------
    def list_downstreams(self) -> list[Downstream]:
        rows = self._query(f"SELECT {_DOWNSTREAM_COLS} FROM downstreams ORDER BY id DESC")
        return [_row_to_downstream(r) for r in rows]

    def get_downstream_by_key(self, apikey: str) -> Downstream | None:
        row = self._query_one(
            f"SELECT {_DOWNSTREAM_COLS} FROM downstreams WHERE apikey=?", (apikey,)
        )
        return _row_to_downstream(row) if row is not None else None

    def add_downstream(self, name: str | None, apikey: str) -> Downstream:
        cur = self._execute(
            "INSERT INTO downstreams(name, apikey, enabled, created_at) VALUES(?, ?, 1, ?)",
            (name, apikey, int(time.time())),
        )
        row = self._query_one(
            f"SELECT {_DOWNSTREAM_COLS} FROM downstreams WHERE id=?", (cur.lastrowid,)
        )
        return _row_to_downstream(row)

    def delete_downstream(self, downstream_id: int) -> bool:
        return self._execute("DELETE FROM downstreams WHERE id=?", (downstream_id,)).rowcount > 0

    def toggle_downstream(self, downstream_id: int) -> Downstream | None:
        row = self._query_one(
            f"SELECT {_DOWNSTREAM_COLS} FROM downstreams WHERE id=?", (downstream_id,)
        )
        if row is None:
            return None
        new_enabled = 0 if row["enabled"] else 1
        self._execute("UPDATE downstreams SET enabled=? WHERE id=?", (new_enabled, downstream_id))
        row = self._query_one(
            f"SELECT {_DOWNSTREAM_COLS} FROM downstreams WHERE id=?", (downstream_id,)
        )
        return _row_to_downstream(row)

    def record_downstream_usage(self, downstream_id: int, tokens: int) -> None:
        self._execute(
            "UPDATE downstreams SET total_requests=total_requests+1, "
            "total_tokens=total_tokens+?, last_used_at=? WHERE id=?",
            (tokens, int(time.time()), downstream_id),
        )

    # ---------- metrics ----------
    def get_metrics(self) -> dict[str, str]:
        return {r["key"]: r["value"] for r in self._query("SELECT key, value FROM metrics")}

    def set_metric(self, key: str, value: str) -> None:
        self._execute(
            "INSERT INTO metrics(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )