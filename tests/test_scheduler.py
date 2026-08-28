"""scheduler 单元测试（§7）。"""
import pytest

from app.models import Upstream
from app.scheduler import Scheduler


class FakeDB:
    """调度器测试用假数据库：返回固定可用池与设置。"""

    def __init__(self, upstreams, settings=None):
        self._upstreams = upstreams
        self._settings = settings or {}

    def get_available_upstreams(self):
        return list(self._upstreams)

    def get_setting(self, key):
        return self._settings.get(key)


def make_upstreams(n):
    return [
        Upstream(
            id=i + 1, email=None, password=None, apikey=f"nvapi-k{i+1}",
            status="active", cooldown_until=0, last_check_at=None,
            last_latency_ms=None, last_http_code=None, last_error=None,
            total_requests=0, total_tokens=0, consecutive_failures=0,
            created_at=0,
        )
        for i in range(n)
    ]


@pytest.fixture
def pool():
    return make_upstreams(3)


async def test_switch_every_two(pool):
    """switch_every=2：连续 2 次请求同一 key，第 3 次切换。"""
    s = Scheduler(FakeDB(pool, {"strategy": "round_robin", "switch_every": "2"}))
    p1 = await s.pick()
    await s.report_success(p1.id)
    p2 = await s.pick()
    await s.report_success(p2.id)
    p3 = await s.pick()
    assert p1.id == p2.id
    assert p3.id != p1.id


async def test_switch_every_one_alternates(pool):
    """switch_every=1：每次请求都切换。"""
    s = Scheduler(FakeDB(pool, {"strategy": "round_robin", "switch_every": "1"}))
    picks = []
    for _ in range(6):
        u = await s.pick()
        picks.append(u.id)
        await s.report_success(u.id)
    assert picks == [1, 2, 3, 1, 2, 3]


async def test_round_robin_starts_from_first(pool):
    s = Scheduler(FakeDB(pool, {"strategy": "round_robin", "switch_every": "5"}))
    assert (await s.pick()).id == 1


async def test_sequential_starts_from_first(pool):
    s = Scheduler(FakeDB(pool, {"strategy": "sequential", "switch_every": "5"}))
    assert (await s.pick()).id == 1


async def test_random_picks_from_pool(pool):
    s = Scheduler(FakeDB(pool, {"strategy": "random"}))
    ids = {u.id for _ in range(30) for u in [await s.pick()]}
    assert ids.issubset({1, 2, 3})


async def test_no_available_returns_none():
    s = Scheduler(FakeDB([], {"strategy": "round_robin"}))
    assert await s.pick() is None
    assert await s.pick_failover({1}) is None


async def test_pick_failover_excludes(pool):
    s = Scheduler(FakeDB(pool, {"strategy": "round_robin", "switch_every": "9"}))
    current = await s.pick()  # k1
    nxt = await s.pick_failover({current.id})
    assert nxt.id != current.id
    assert nxt.id in {2, 3}


async def test_failover_does_not_consume_window(pool):
    """故障转移换 key 不消耗 N 次窗口：窗口仍停留在原 key。"""
    s = Scheduler(FakeDB(pool, {"strategy": "round_robin", "switch_every": "2"}))
    p1 = await s.pick()          # k1
    await s.report_success(p1.id)
    failover_key = await s.pick_failover({p1.id})  # k1 失败换 key
    await s.report_success(failover_key.id)        # 上报的不是窗口 key → 不计数
    p2 = await s.pick()           # 窗口未到期 → 仍是 k1
    assert p2.id == p1.id


async def test_pool_shrink_adapts(pool):
    """key 删除后索引对池长度取模，自动适应。"""
    db = FakeDB(pool, {"strategy": "round_robin", "switch_every": "1"})
    s = Scheduler(db)
    ids = [(await s.pick()).id for _ in range(2)]
    for i in ids:
        await s.report_success(i)
    # 模拟 k3 被删除
    db._upstreams = pool[:2]
    u = await s.pick()
    assert u.id in {1, 2}