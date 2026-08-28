"""自动流式单元测试：非流式请求 → 上游强制流式 → 服务端聚合回包。"""
import json

import httpx
import pytest

from app.database import Database
from app.scheduler import Scheduler
from app.stats import StatsEngine
from app.upstream import StreamAggregator, UpstreamProxy


@pytest.fixture
def db(tmp_path):
    return Database(str(tmp_path / "test.db"))


def make_proxy(db, handler):
    scheduler = Scheduler(db)
    stats = StatsEngine()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=10.0)
    return UpstreamProxy(db, scheduler, stats, client=client), stats


def add_keys(db, n):
    for i in range(n):
        db.add_upstream(f"u{i}@x.com", f"pw{i}", f"nvapi-k{i}")


def sse(*events: str, done: bool = True) -> bytes:
    """把 JSON 事件列表拼成 SSE 字节流。"""
    out = "".join(f"data: {e}\n\n" for e in events)
    if done:
        out += "data: [DONE]\n\n"
    return out.encode("utf-8")


CHAT_SSE = sse(
    '{"id":"cmpl-1","created":123,"model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"你"}}]}',
    '{"id":"cmpl-1","created":123,"model":"m","choices":[{"index":0,"delta":{"content":"好"}}]}',
    '{"id":"cmpl-1","created":123,"model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"total_tokens":9}}',
)


# ---------- StreamAggregator 纯逻辑 ----------
def test_aggregator_chat_completion():
    agg = StreamAggregator()
    agg.feed(CHAT_SSE)
    resp = agg.to_response()
    assert resp["object"] == "chat.completion"
    assert resp["id"] == "cmpl-1"
    assert resp["choices"][0]["message"]["content"] == "你好"
    assert resp["choices"][0]["finish_reason"] == "stop"
    assert resp["usage"]["total_tokens"] == 9
    assert agg.tokens == 9


def test_aggregator_reasoning_and_estimated_usage():
    sse_bytes = sse(
        '{"id":"c","created":1,"model":"m","choices":[{"index":0,"delta":{"reasoning_content":"思考"}}]}',
        '{"choices":[{"index":0,"delta":{"content":"abcdefgh"}}]}',
    )
    agg = StreamAggregator()
    agg.feed(sse_bytes)
    resp = agg.to_response()
    msg = resp["choices"][0]["message"]
    assert msg["content"] == "abcdefgh"
    assert msg["reasoning_content"] == "思考"
    assert resp["usage"]["total_tokens"] == 2  # 8 字符 // 4，无 usage 时估算


def test_aggregator_tool_calls_merge():
    sse_bytes = sse(
        '{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1",'
        '"type":"function","function":{"name":"get_w","arguments":""}}]}}]}',
        '{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"ci"}}]}}]}',
        '{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":'
        '{"arguments":"ty\\":\\"北京\\"}"}}]}}]}',
    )
    agg = StreamAggregator()
    agg.feed(sse_bytes)
    tc = agg.to_response()["choices"][0]["message"]["tool_calls"][0]
    assert tc["id"] == "call_1"
    assert tc["function"]["name"] == "get_w"
    assert tc["function"]["arguments"] == '{"city":"北京"}'


def test_aggregator_records_stream_error():
    sse_bytes = sse(
        '{"error":{"message":"Service temporarily overloaded",'
        '"type":"service_unavailable","code":503}}'
    )
    agg = StreamAggregator()
    agg.feed(sse_bytes)
    assert agg.stream_error is not None
    assert agg.stream_error["code"] == 503
    assert not agg.has_content()


def test_aggregator_legacy_completions():
    sse_bytes = sse(
        '{"id":"c","created":1,"model":"m","choices":[{"text":"he"}]}',
        '{"choices":[{"text":"llo"}]}',
    )
    agg = StreamAggregator()
    agg.feed(sse_bytes)
    resp = agg.to_response()
    assert resp["object"] == "text_completion"
    assert resp["choices"][0]["text"] == "hello"


# ---------- forward 集成（MockTransport） ----------
async def test_nonstream_request_converted_and_aggregated(db):
    """非流式请求：上游收到 stream=true，下游拿到聚合后的完整 JSON。"""
    seen_bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_bodies.append(json.loads(request.content))
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=CHAT_SSE
        )

    proxy, _ = make_proxy(db, handler)
    add_keys(db, 1)
    ds = db.add_downstream("t", "sk-router-a")

    resp = await proxy.forward(
        method="POST",
        path="/chat/completions",
        headers={},
        body=json.dumps({"model": "m", "messages": []}).encode(),
        downstream_id=ds.id,
        force_stream=True,
    )
    # 上游收到被改写的流式请求
    assert seen_bodies[0]["stream"] is True
    assert seen_bodies[0]["model"] == "m"
    # 下游拿到完整 JSON（非 SSE）
    assert resp.status_code == 200
    assert resp.media_type == "application/json"
    body = json.loads(resp.body)
    assert body["choices"][0]["message"]["content"] == "你好"
    assert body["usage"]["total_tokens"] == 9
    # 统计与上游记录
    assert db.get_upstream(1).total_tokens == 9


async def test_stream_request_passthrough_unchanged(db):
    """下游本来就请求流式：不改写，原样 SSE 透传。"""
    seen_bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_bodies.append(json.loads(request.content))
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=CHAT_SSE
        )

    proxy, _ = make_proxy(db, handler)
    add_keys(db, 1)
    ds = db.add_downstream("t", "sk-router-b")

    resp = await proxy.forward(
        method="POST",
        path="/chat/completions",
        headers={},
        body=json.dumps({"model": "m", "messages": [], "stream": True}).encode(),
        downstream_id=ds.id,
        force_stream=True,
    )
    assert seen_bodies[0]["stream"] is True
    chunks = [c async for c in resp.body_iterator]
    if resp.background is not None:
        await resp.background()
    assert b"".join(chunks) == CHAT_SSE  # 字节级透传


async def test_auto_stream_setting_off_disables_conversion(db):
    """设置关闭（auto_stream=0）：请求原样转发，不做流式转换。"""
    db.set_setting("auto_stream", "0")
    seen_bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "c", "usage": {"total_tokens": 5}})

    proxy, _ = make_proxy(db, handler)
    add_keys(db, 1)
    ds = db.add_downstream("t", "sk-router-c")

    resp = await proxy.forward(
        method="POST",
        path="/chat/completions",
        headers={},
        body=json.dumps({"model": "m", "messages": []}).encode(),
        downstream_id=ds.id,
        force_stream=True,
    )
    assert "stream" not in seen_bodies[0]  # 未被改写
    assert resp.status_code == 200
    assert json.loads(resp.body)["usage"]["total_tokens"] == 5


async def test_aggregated_stream_error_returns_error_json(db):
    """流内 error 事件且无内容：重试耗尽后按错误状态码返回 JSON。"""
    sse_bytes = sse(
        '{"error":{"message":"Service temporarily overloaded",'
        '"type":"service_unavailable","code":503}}'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=sse_bytes
        )

    proxy, _ = make_proxy(db, handler)
    add_keys(db, 1)
    ds = db.add_downstream("t", "sk-router-d")

    resp = await proxy.forward(
        method="POST",
        path="/chat/completions",
        headers={},
        body=json.dumps({"model": "m", "messages": []}).encode(),
        downstream_id=ds.id,
        force_stream=True,
    )
    assert resp.status_code == 503
    body = json.loads(resp.body)
    assert body["error"]["message"] == "Service temporarily overloaded"


async def test_instream_error_fails_over_to_next_key(db):
    """流内错误（HTTP 200 + 503 error 事件）：自动换 Key 重试，下游拿到正常聚合响应。"""
    auths = []

    def handler(request: httpx.Request) -> httpx.Response:
        auths.append(request.headers.get("authorization"))
        if len(auths) == 1:
            # 第一个 Key：上游过载，HTTP 200 + 流内 error 事件
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=sse(
                    '{"error":{"message":"Service temporarily overloaded",'
                    '"type":"service_unavailable","code":503}}'
                ),
            )
        # 第二个 Key：正常流式响应
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=CHAT_SSE
        )

    proxy, _ = make_proxy(db, handler)
    add_keys(db, 2)
    ds = db.add_downstream("t", "sk-router-f")

    resp = await proxy.forward(
        method="POST",
        path="/chat/completions",
        headers={},
        body=json.dumps({"model": "m", "messages": []}).encode(),
        downstream_id=ds.id,
        force_stream=True,
    )
    assert len(auths) == 2  # 第一个 Key 流内报错 → 自动换第二个 Key
    assert auths[0] != auths[1]
    assert resp.status_code == 200
    body = json.loads(resp.body)
    assert body["choices"][0]["message"]["content"] == "你好"


async def test_non_json_body_not_converted(db):
    """body 非 JSON（或非对象）：不转换，原样转发。"""
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.content)
        return httpx.Response(200, json={"ok": True})

    proxy, _ = make_proxy(db, handler)
    add_keys(db, 1)
    ds = db.add_downstream("t", "sk-router-e")

    resp = await proxy.forward(
        method="POST",
        path="/chat/completions",
        headers={},
        body=b"not-json",
        downstream_id=ds.id,
        force_stream=True,
    )
    assert seen[0] == b"not-json"
    assert resp.status_code == 200
