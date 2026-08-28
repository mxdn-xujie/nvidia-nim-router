"""failover 单元测试（§6.3 处置表 + 转发核心集成）。"""
import json
import time

import httpx
import pytest

from app.database import Database
from app.failover import (
    COOLDOWN_10M_RETRY,
    COOLDOWN_RETRY,
    INVALID_RETRY,
    NO_RETRY,
    RETRY,
    classify_status,
)
from app.scheduler import Scheduler
from app.stats import StatsEngine
from app.upstream import StreamUsageParser, UpstreamProxy, extract_usage_tokens


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


def make_proxy(db, handler):
    scheduler = Scheduler(db)
    stats = StatsEngine()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=10.0)
    proxy = UpstreamProxy(db, scheduler, stats, client=client)
    return proxy, scheduler, stats


def add_keys(db, n):
    for i in range(n):
        db.add_upstream(f"u{i}@x.com", f"pw{i}", f"nvapi-k{i}")


# ---------- 处置表分类 ----------
def test_classify_status_table():
    assert classify_status(429) == COOLDOWN_RETRY
    assert classify_status(401) == INVALID_RETRY
    assert classify_status(403) == COOLDOWN_10M_RETRY
    assert classify_status(500) == RETRY
    assert classify_status(503) == RETRY
    assert classify_status(400) == NO_RETRY
    assert classify_status(404) == NO_RETRY
    assert classify_status(422) == NO_RETRY


# ---------- 转发集成（MockTransport） ----------
async def test_429_fails_over_to_next_key(db):
    """mock 3 上游，第 1 个 429 → 自动换 key 成功。"""
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        auth = request.headers["Authorization"]
        seen.append(auth)
        if auth == "Bearer nvapi-k0":
            return httpx.Response(429, json={"error": "rate limited"})
        return httpx.Response(200, json={"id": "cmpl", "usage": {"total_tokens": 42}})

    proxy, _, stats = make_proxy(db, handler)
    add_keys(db, 3)
    ds = db.add_downstream("t", "sk-router-test")

    resp = await proxy.forward(
        method="POST", path="/chat/completions", headers={}, body=b"{}", downstream_id=ds.id
    )
    assert resp.status_code == 200
    body = json.loads(resp.body)
    assert body["usage"]["total_tokens"] == 42

    # k1 被冷却，k2 成功
    k0, k1, k2 = (db.get_upstream(i) for i in (1, 2, 3))
    assert k0.status == "cooldown"
    assert k1.total_requests == 1 and k1.total_tokens == 42
    assert k2.total_requests == 0
    assert stats.total_requests == 1 and stats.total_tokens == 42
    assert ds.id == 1
    ds_row = db.get_downstream_by_key("sk-router-test")
    assert ds_row.total_requests == 1 and ds_row.total_tokens == 42


async def test_all_429_exhaust_returns_last_response(db):
    """全部 429：重试耗尽后原样透传最后一次上游响应（429）。"""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, json={"error": {"message": "rate limited"}})

    proxy, _, stats = make_proxy(db, handler)
    add_keys(db, 3)
    ds = db.add_downstream("t", "sk-router-x")

    resp = await proxy.forward(
        method="POST", path="/chat/completions", headers={}, body=b"{}", downstream_id=ds.id
    )
    assert resp.status_code == 429
    assert json.loads(resp.body)["error"]["message"] == "rate limited"
    # max_retries=3 → 初始 1 次 + 重试 3 次 = 4 次尝试
    assert stats.total_requests == 1


async def test_401_marks_invalid(db):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers["Authorization"] == "Bearer nvapi-k0":
            return httpx.Response(401, json={"error": "invalid key"})
        return httpx.Response(200, json={"ok": True})

    proxy, _, _ = make_proxy(db, handler)
    add_keys(db, 2)
    ds = db.add_downstream("t", "sk-router-y")

    resp = await proxy.forward(
        method="POST", path="/chat/completions", headers={}, body=b"{}", downstream_id=ds.id
    )
    assert resp.status_code == 200
    assert db.get_upstream(1).status == "invalid"


async def test_403_cooldown_10m_not_invalid(db):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers["Authorization"] == "Bearer nvapi-k0":
            return httpx.Response(403, json={"error": "model forbidden"})
        return httpx.Response(200, json={"ok": True})

    proxy, _, _ = make_proxy(db, handler)
    add_keys(db, 2)
    ds = db.add_downstream("t", "sk-router-z")
    before = int(time.time())

    resp = await proxy.forward(
        method="POST", path="/chat/completions", headers={}, body=b"{}", downstream_id=ds.id
    )
    assert resp.status_code == 200
    k0 = db.get_upstream(1)
    assert k0.status == "cooldown"          # 不标 invalid
    assert 570 <= k0.cooldown_until - before <= 610  # 冷却约 10 分钟


async def test_400_no_retry_passthrough(db):
    """400：请求本身错误 → 不转移、不消耗重试，原样返回。"""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.headers["Authorization"])
        return httpx.Response(400, json={"error": {"message": "model not found"}})

    proxy, _, _ = make_proxy(db, handler)
    add_keys(db, 3)
    ds = db.add_downstream("t", "sk-router-w")

    resp = await proxy.forward(
        method="POST", path="/chat/completions", headers={}, body=b"{}", downstream_id=ds.id
    )
    assert resp.status_code == 400
    assert json.loads(resp.body)["error"]["message"] == "model not found"
    assert len(calls) == 1  # 只尝试了 1 次


async def test_5xx_retries_without_cooldown(db):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.headers["Authorization"] == "Bearer nvapi-k0":
            return httpx.Response(503, json={"error": "upstream down"})
        return httpx.Response(200, json={"ok": True})

    proxy, _, _ = make_proxy(db, handler)
    add_keys(db, 2)
    ds = db.add_downstream("t", "sk-router-v")

    resp = await proxy.forward(
        method="POST", path="/chat/completions", headers={}, body=b"{}", downstream_id=ds.id
    )
    assert resp.status_code == 200
    k0 = db.get_upstream(1)
    assert k0.status == "active"  # 5xx：key 不冷却
    assert k0.consecutive_failures == 1


async def test_no_available_key_returns_503(db):
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        return httpx.Response(200)

    proxy, _, _ = make_proxy(db, handler)
    ds = db.add_downstream("t", "sk-router-u")

    resp = await proxy.forward(
        method="POST", path="/chat/completions", headers={}, body=b"{}", downstream_id=ds.id
    )
    assert resp.status_code == 503
    assert json.loads(resp.body)["error"]["type"] == "router_error"
    assert json.loads(resp.body)["error"]["message"] == "no available upstream key"


async def test_stream_forward_tokens_and_usage(db):
    """流式转发：字节块透传 + 最后一个 chunk 的 usage.total_tokens 统计。"""
    sse = (
        b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n'
        b'data: {"choices":[{"delta":{"content":" world"}}],"usage":{"total_tokens":11}}\n\n'
        b"data: [DONE]\n\n"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=sse
        )

    proxy, _, _ = make_proxy(db, handler)
    add_keys(db, 1)
    ds = db.add_downstream("t", "sk-router-s")

    resp = await proxy.forward(
        method="POST", path="/chat/completions", headers={}, body=b"{}", downstream_id=ds.id
    )
    assert resp.status_code == 200
    chunks = [c async for c in resp.body_iterator]
    if resp.background is not None:
        await resp.background()
    assert b"".join(chunks) == sse  # 原样字节透传
    k0 = db.get_upstream(1)
    assert k0.total_tokens == 11 and k0.total_requests == 1


async def test_stream_estimate_tokens_without_usage(db):
    """整个流无 usage：按 len(文本)//4 估算。"""
    sse = b'data: {"choices":[{"delta":{"content":"abcdefgh"}}]}\n\ndata: [DONE]\n\n'

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=sse
        )

    proxy, _, _ = make_proxy(db, handler)
    add_keys(db, 1)
    ds = db.add_downstream("t", "sk-router-e")

    resp = await proxy.forward(
        method="POST", path="/chat/completions", headers={}, body=b"{}", downstream_id=ds.id
    )
    _ = [c async for c in resp.body_iterator]
    if resp.background is not None:
        await resp.background()
    assert db.get_upstream(1).total_tokens == 2  # 8 字符 // 4


# ---------- 纯函数 ----------
def test_extract_usage_tokens():
    assert extract_usage_tokens(json.dumps({"usage": {"total_tokens": 99}}).encode()) == 99
    assert extract_usage_tokens(b'{"no_usage": true}') == 0
    assert extract_usage_tokens(b"not-json") == 0


def test_stream_usage_parser_chunk_split():
    """chunk 边界切断 data 行时仍能正确解析。"""
    p = StreamUsageParser()
    line = b'data: {"choices":[{"delta":{"content":"hi"}}],"usage":{"total_tokens":7}}\n\n'
    p.feed(line[:10])
    p.feed(line[10:25])
    p.feed(line[25:])
    assert p.tokens == 7