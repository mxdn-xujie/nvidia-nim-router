"""批量检测限速单元测试：串行执行 + 固定间隔（每分钟 1-10 次）。"""
import asyncio

import httpx
import pytest

from app.api.upstreams import _run_check_all, check_rate_per_minute, check_upstream
from app.database import Database


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "test.db"))
    for i in range(3):
        d.add_upstream(f"u{i}@x.com", f"pw{i}", f"nvapi-k{i}")
    return d


@pytest.fixture
def no_sleep(monkeypatch):
    """记录 sleep 调用的间隔值，避免测试真实等待。"""
    sleeps: list[float] = []
    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return sleeps


def test_rate_setting_clamped(db):
    """设置读取：异常值收敛到 [1, 10]，默认 6。"""
    assert check_rate_per_minute(db) == 6  # 未设置时默认
    db.set_setting("check_rate_per_minute", "0")
    assert check_rate_per_minute(db) == 1  # 下界
    db.set_setting("check_rate_per_minute", "99")
    assert check_rate_per_minute(db) == 10  # 上界
    db.set_setting("check_rate_per_minute", "abc")
    assert check_rate_per_minute(db) == 6  # 非法回退默认
    db.set_setting("check_rate_per_minute", "3")
    assert check_rate_per_minute(db) == 3


async def test_check_all_serial_with_interval(db, no_sleep):
    """串行执行：按列表顺序逐个检测，两次之间 sleep(60/rate)。"""
    order: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        # 记录被检测的 key（Authorization: Bearer nvapi-kN）
        order.append(request.headers.get("authorization", ""))
        return httpx.Response(200, json={"object": "list", "data": []})

    db.set_setting("check_rate_per_minute", "6")  # 间隔 10 秒
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5.0)
    try:
        await _run_check_all(db, client)
    finally:
        await client.aclose()

    # 串行按顺序完成全部 3 个
    assert order == [
        "Bearer nvapi-k0", "Bearer nvapi-k1", "Bearer nvapi-k2",
    ]
    # 3 个 key = 2 次间隔，每次 60/6 = 10 秒
    assert no_sleep == [10.0, 10.0]
    # 检测成功后状态为 active
    for u in db.get_all_upstreams():
        assert u.status == "active"


async def test_check_all_rate_one_60s_interval(db, no_sleep):
    """速率 1 次/分钟：间隔 60 秒。"""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"object": "list", "data": []})

    db.set_setting("check_rate_per_minute", "1")
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5.0)
    try:
        await _run_check_all(db, client)
    finally:
        await client.aclose()
    assert no_sleep == [60.0, 60.0]


async def test_check_all_empty_pool_no_calls(db, no_sleep):
    """空池：直接返回，无请求无间隔。"""
    for u in db.get_all_upstreams():
        db.delete_upstream(u.id)

    called = []

    def handler(request: httpx.Request) -> httpx.Response:
        called.append(request.url.path)
        return httpx.Response(200, json={})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5.0)
    try:
        await _run_check_all(db, client)
    finally:
        await client.aclose()
    assert called == []
    assert no_sleep == []


async def test_check_all_single_failure_continues(db, no_sleep):
    """单个 key 检测抛异常：跳过继续下一个，不影响整体。"""
    order: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("authorization", "")
        order.append(auth)
        if auth == "Bearer nvapi-k1":
            raise httpx.ConnectError("boom")
        return httpx.Response(200, json={"object": "list", "data": []})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5.0)
    try:
        await _run_check_all(db, client)
    finally:
        await client.aclose()

    assert len(order) == 3  # 三个都尝试了
    # k1 网络失败记录 error，k0/k2 正常 active
    statuses = {u.apikey: u.status for u in db.get_all_upstreams()}
    assert statuses["nvapi-k0"] == "active"
    assert statuses["nvapi-k2"] == "active"


async def test_check_upstream_classifies(db):
    """单 key 检测判定：200→active，401→invalid，429→cooldown。"""
    codes = {"nvapi-k0": 200, "nvapi-k1": 401, "nvapi-k2": 429}
    hits: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        key = request.headers.get("authorization", "").replace("Bearer ", "")
        hits.append(key)
        return httpx.Response(codes[key])

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=5.0)
    try:
        for u in db.get_all_upstreams():
            await check_upstream(db, u, client)
    finally:
        await client.aclose()

    assert hits == ["nvapi-k0", "nvapi-k1", "nvapi-k2"]
    statuses = {u.apikey: (u.status, u.last_http_code) for u in db.get_all_upstreams()}
    assert statuses["nvapi-k0"] == ("active", 200)
    assert statuses["nvapi-k1"] == ("invalid", 401)
    assert statuses["nvapi-k2"] == ("cooldown", 429)
