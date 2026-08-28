"""NVIDIA 上游客户端：透传转发核心（§6）。

透传原则（强制）：
- 请求体不解析业务字段、不改写，model/messages/stream 等原样字节转发；
- 仅做两件事：替换 Authorization 头为调度器选中的 nvapi-key；移除 hop-by-hop 头；
- 响应体原样返回（含 content-type）。

故障转移（§6.3）见 failover.py 处置表；重试耗尽后把最后一次上游响应原样透传。
自动流式（auto_stream）：非流式请求改写为 stream=true 发往上游，服务端聚合回完整 JSON，
下游无感知，规避大模型长响应读超时；流内 error 事件（HTTP 200 + error）按其状态码
走与 HTTP 错误相同的故障转移流程（如过载 503 → 自动换 Key 重试）。
"""
from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import httpx
from fastapi.responses import Response, StreamingResponse
from starlette.background import BackgroundTask

from .failover import (
    COOLDOWN_10M_RETRY,
    COOLDOWN_403_SECONDS,
    COOLDOWN_RETRY,
    INVALID_RETRY,
    NO_RETRY,
    classify_status,
)
from .models import Upstream

if TYPE_CHECKING:
    from .database import Database
    from .scheduler import Scheduler
    from .stats import StatsEngine

logger = logging.getLogger("router.upstream")

# 请求侧需剥离的头（Authorization 由本模块替换；Content-Length/编码由 httpx 重算）
REQUEST_STRIP_HEADERS = {
    "host", "authorization", "content-length", "connection", "keep-alive",
    "transfer-encoding", "upgrade", "te", "trailer", "expect", "accept-encoding",
    "content-encoding",
}
# 响应侧需剥离的头（httpx 已解码 content-encoding，StreamingResponse 自管分块）
RESPONSE_STRIP_HEADERS = {
    "connection", "keep-alive", "transfer-encoding", "content-length",
    "content-encoding", "te", "trailer",
}

# 流中途断流时注入的 SSE 错误事件（§6.4 规则 3）
STREAM_ERROR_EVENT = (
    b'data: {"error": {"message": "upstream stream interrupted", '
    b'"type": "router_error", "code": "stream_interrupted"}}\n\n'
)


class StreamUsageParser:
    """流式 token 统计：解析 data: 块的 usage.total_tokens；无 usage 时按文本长度估算。"""

    def __init__(self) -> None:
        self.usage_tokens: int | None = None
        self.text_chars = 0
        self.broken = False
        self._buf = b""

    def feed(self, chunk: bytes) -> None:
        self._buf += chunk
        while b"\n" in self._buf:
            line, self._buf = self._buf.split(b"\n", 1)
            self._handle_line(line)

    def _handle_line(self, line: bytes) -> None:
        text = line.decode("utf-8", "replace").strip()
        if not text.startswith("data:"):
            return
        payload = text[5:].strip()
        if not payload or payload == "[DONE]":
            return
        try:
            obj = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(obj, dict):
            return
        usage = obj.get("usage")
        if isinstance(usage, dict) and usage.get("total_tokens") is not None:
            try:
                self.usage_tokens = int(usage["total_tokens"])
            except (TypeError, ValueError):
                pass
        # 累计增量文本长度，用于无 usage 时 len(文本)//4 估算
        choices = obj.get("choices")
        if isinstance(choices, list):
            for choice in choices:
                if not isinstance(choice, dict):
                    continue
                delta = choice.get("delta")
                if isinstance(delta, dict) and isinstance(delta.get("content"), str):
                    self.text_chars += len(delta["content"])
                elif isinstance(choice.get("text"), str):
                    self.text_chars += len(choice["text"])

    @property
    def tokens(self) -> int:
        if self.usage_tokens is not None:
            return self.usage_tokens
        return self.text_chars // 4


class StreamAggregator:
    """把上游 SSE 流聚合为完整响应 JSON（自动流式：上游流式、下游非流式）。

    兼容 chat.completions（delta.content/reasoning_content/tool_calls）
    与 legacy completions（choices[].text）两种块格式。
    """

    _META_KEYS = ("id", "created", "model", "system_fingerprint", "service_tier")

    def __init__(self) -> None:
        self.meta: dict = {}
        self.role = "assistant"
        self.content_parts: list[str] = []
        self.reasoning_parts: list[str] = []
        self.text_parts: list[str] = []  # legacy completions
        self.tool_calls: dict[int, dict] = {}
        self.finish_reason: str | None = None
        self.usage: dict | None = None
        self.stream_error: dict | None = None  # 流内 error 事件（如上游过载 503）
        self.broken = False
        self._buf = b""

    def feed(self, chunk: bytes) -> None:
        self._buf += chunk
        while b"\n" in self._buf:
            line, self._buf = self._buf.split(b"\n", 1)
            self._handle_line(line)

    def _handle_line(self, line: bytes) -> None:
        text = line.decode("utf-8", "replace").strip()
        if not text.startswith("data:"):
            return
        payload = text[5:].strip()
        if not payload or payload == "[DONE]":
            return
        try:
            obj = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            return
        if not isinstance(obj, dict):
            return
        if isinstance(obj.get("error"), dict) and self.stream_error is None:
            # 上游以 HTTP 200 + 流内 error 事件报错（如过载），记录首个错误
            self.stream_error = obj["error"]
        for key in self._META_KEYS:
            if obj.get(key) is not None and key not in self.meta:
                self.meta[key] = obj[key]
        usage = obj.get("usage")
        if isinstance(usage, dict) and usage.get("total_tokens") is not None:
            self.usage = usage
        choices = obj.get("choices")
        if not isinstance(choices, list):
            return
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            if choice.get("finish_reason"):
                self.finish_reason = choice["finish_reason"]
            delta = choice.get("delta")
            if isinstance(delta, dict):
                if delta.get("role"):
                    self.role = str(delta["role"])
                if isinstance(delta.get("content"), str):
                    self.content_parts.append(delta["content"])
                if isinstance(delta.get("reasoning_content"), str):
                    self.reasoning_parts.append(delta["reasoning_content"])
                self._merge_tool_calls(delta.get("tool_calls"))
            elif isinstance(choice.get("text"), str):
                self.text_parts.append(choice["text"])

    def _merge_tool_calls(self, tcs: object) -> None:
        if not isinstance(tcs, list):
            return
        for tc in tcs:
            if not isinstance(tc, dict):
                continue
            try:
                idx = int(tc.get("index", 0))
            except (TypeError, ValueError):
                idx = 0
            slot = self.tool_calls.setdefault(
                idx, {"id": None, "type": "function", "function": {"name": None, "arguments": ""}}
            )
            if tc.get("id"):
                slot["id"] = tc["id"]
            fn = tc.get("function")
            if isinstance(fn, dict):
                if fn.get("name"):
                    slot["function"]["name"] = fn["name"]
                if isinstance(fn.get("arguments"), str):
                    slot["function"]["arguments"] += fn["arguments"]

    @property
    def tokens(self) -> int:
        if self.usage is not None:
            try:
                return int(self.usage["total_tokens"])
            except (KeyError, TypeError, ValueError):
                pass
        text_len = sum(len(s) for s in self.content_parts) + sum(len(s) for s in self.text_parts)
        return text_len // 4

    def has_content(self) -> bool:
        return bool(
            self.content_parts or self.text_parts or self.reasoning_parts or self.tool_calls
        )

    def to_response(self) -> dict:
        """重建完整响应体；usage 缺失时按文本长度估算。"""
        if self.text_parts and not self.content_parts and not self.tool_calls:
            # legacy /v1/completions
            return {
                **self.meta,
                "object": "text_completion",
                "choices": [
                    {
                        "index": 0,
                        "text": "".join(self.text_parts),
                        "logprobs": None,
                        "finish_reason": self.finish_reason or "stop",
                    }
                ],
                "usage": self.usage or self._estimated_usage(),
            }
        message: dict = {"role": self.role, "content": "".join(self.content_parts)}
        if self.reasoning_parts:
            message["reasoning_content"] = "".join(self.reasoning_parts)
        if self.tool_calls:
            message["tool_calls"] = [self.tool_calls[i] for i in sorted(self.tool_calls)]
        return {
            "id": self.meta.get("id"),
            "object": "chat.completion",
            "created": self.meta.get("created"),
            "model": self.meta.get("model"),
            "system_fingerprint": self.meta.get("system_fingerprint"),
            "choices": [
                {
                    "index": 0,
                    "message": message,
                    "logprobs": None,
                    "finish_reason": self.finish_reason or "stop",
                }
            ],
            "usage": self.usage or self._estimated_usage(),
        }

    def _estimated_usage(self) -> dict:
        est = self.tokens
        return {"prompt_tokens": 0, "completion_tokens": est, "total_tokens": est}


def extract_usage_tokens(content: bytes) -> int:
    """非流式响应：直接读取 usage.total_tokens。"""
    try:
        obj = json.loads(content)
    except (json.JSONDecodeError, ValueError):
        return 0
    if not isinstance(obj, dict):
        return 0
    usage = obj.get("usage")
    if isinstance(usage, dict) and usage.get("total_tokens") is not None:
        try:
            return int(usage["total_tokens"])
        except (TypeError, ValueError):
            return 0
    return 0


def router_error_body(status: int, message: str) -> dict:
    return {"error": {"message": message, "type": "router_error", "code": status}}


class UpstreamProxy:
    """转发核心：调度选 key → httpx 透传 → 故障转移 → 统计。"""

    def __init__(
        self,
        db: Database,
        scheduler: Scheduler,
        stats: StatsEngine,
        client: httpx.AsyncClient | None = None,
    ):
        self._db = db
        self._scheduler = scheduler
        self._stats = stats
        self._client = client
        self._external_client = client is not None  # 外部注入（测试）时不重建
        self._client_timeout_ms = -1  # 用于跟踪 timeout 设置变化并重建客户端

    # ---------- 客户端管理 ----------
    async def _get_client(self, timeout_ms: int) -> httpx.AsyncClient:
        if self._external_client:
            return self._client  # type: ignore[return-value]
        timeout_s = max(0.5, timeout_ms / 1000)
        if self._client is not None and self._client_timeout_ms == timeout_ms:
            return self._client
        if self._client is not None:
            await self._client.aclose()
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_s, connect=min(timeout_s, 10.0)),
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
            follow_redirects=False,
        )
        self._client_timeout_ms = timeout_ms
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ---------- 转发入口 ----------
    async def forward(
        self,
        *,
        method: str,
        path: str,
        query: str = "",
        headers: dict,
        body: bytes | None,
        downstream_id: int,
        force_stream: bool = False,
    ) -> Response:
        """透传转发一次下游请求（含故障转移与统计）。

        force_stream：自动流式开关开启时，非流式请求会被改写为流式发往上游，
        并在服务端聚合回完整 JSON 返回（下游无感知），避免长响应触发读超时。
        流内 error 事件（HTTP 200 + error）按其状态码走与 HTTP 错误相同的
        故障转移流程（如上游过载 503 → 自动换 Key 重试）。
        """
        db = self._db
        base_url = (db.get_setting("nvidia_base_url") or "").rstrip("/")
        timeout_ms = _to_int(db.get_setting("timeout_ms"), 30000)
        max_retries = _to_int(db.get_setting("max_retries"), 3)
        cooldown_seconds = _to_int(db.get_setting("cooldown_seconds"), 60)
        client = await self._get_client(timeout_ms)

        # 自动流式：非流式请求改写为 stream=true（设置关闭或 body 非 JSON 时保持原样）
        aggregate = False
        send_body = body
        if force_stream and (db.get_setting("auto_stream") or "1") != "0" and body:
            try:
                obj = json.loads(body)
            except (json.JSONDecodeError, ValueError):
                obj = None
            if isinstance(obj, dict) and not obj.get("stream"):
                obj["stream"] = True
                send_body = _json_bytes(obj)
                aggregate = True

        url = f"{base_url}{path}" + (f"?{query}" if query else "")
        exclude: set[int] = set()
        # 最后一次上游响应 (status, headers, content)，重试耗尽时原样透传
        last: tuple[int, dict, bytes] | None = None

        upstream = await self._scheduler.pick()
        if upstream is None:
            self._stats.record_request(0)
            content = _json_bytes(router_error_body(503, "no available upstream key"))
            return Response(content=content, status_code=503, media_type="application/json")

        attempts = 0
        while True:
            attempts += 1
            req_headers = dict(headers)
            req_headers["Authorization"] = f"Bearer {upstream.apikey}"
            try:
                req = client.build_request(method, url, headers=req_headers, content=send_body)
                resp = await client.send(req, stream=True)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                # 连接/读超时、网络不可达：冷却 + 换 key 重试
                db.cooldown_upstream(upstream.id, cooldown_seconds, error=f"{type(exc).__name__}")
                last = (
                    504,
                    {"content-type": "application/json"},
                    _json_bytes(router_error_body(504, f"上游连接失败：{type(exc).__name__}")),
                )
            else:
                status = resp.status_code
                if 200 <= status < 300:
                    result = await self._handle_success(
                        resp, upstream, downstream_id, aggregate=aggregate
                    )
                    if isinstance(result, Response):
                        return result
                    # 流内错误且无内容（HTTP 200 + error 事件）：按该状态码
                    # 走与 HTTP 错误完全相同的分类与换 Key 重试流程
                    status, content = result
                    resp_headers = {"content-type": "application/json"}
                else:
                    # 非成功：读取 body 用于判定与透传
                    content = await resp.aread()
                    resp_headers = {
                        k: v for k, v in resp.headers.items()
                        if k.lower() not in RESPONSE_STRIP_HEADERS
                    }
                    await resp.aclose()
                last = (status, resp_headers, content)

                action = classify_status(status)
                if action == NO_RETRY:
                    # 请求本身错误（400/404/422 等）：不转移、不消耗重试次数，原样返回
                    db.record_upstream_failure(upstream.id, error=f"HTTP {status}")
                    self._stats.record_request(0)
                    return Response(content=content, status_code=status, headers=resp_headers)
                if action == COOLDOWN_RETRY:
                    db.cooldown_upstream(upstream.id, cooldown_seconds, error=f"HTTP {status}")
                elif action == COOLDOWN_10M_RETRY:
                    # 403：模型级权限不足，只冷却 10 分钟，不标 invalid
                    db.cooldown_upstream(upstream.id, COOLDOWN_403_SECONDS, error=f"HTTP {status}")
                elif action == INVALID_RETRY:
                    db.mark_invalid(upstream.id, error=f"HTTP {status}")
                else:  # RETRY：5xx，key 不冷却
                    db.record_upstream_failure(upstream.id, error=f"HTTP {status}")

            # 统一重试判定（含网络异常分支）
            exclude.add(upstream.id)
            if attempts - 1 >= max_retries:
                break
            nxt = await self._scheduler.pick_failover(exclude)
            if nxt is None:
                break
            upstream = nxt

        # 重试耗尽：把最后一次上游响应原样透传给下游（保留状态码与 body）
        self._stats.record_request(0)
        assert last is not None
        status, resp_headers, content = last
        return Response(content=content, status_code=status, headers=resp_headers)

    # ---------- 成功响应处理 ----------
    async def _handle_success(
        self,
        resp: httpx.Response,
        upstream: Upstream,
        downstream_id: int,
        aggregate: bool = False,
    ) -> Response | tuple[int, bytes]:
        """处理 2xx 响应。

        返回 Response 为最终回包；返回 (status, body) 表示自动流式聚合时
        遇到流内 error 事件且无内容，由调用方按该状态码进入故障转移。
        """
        content_type = (resp.headers.get("content-type") or "").lower()
        headers = {
            k: v for k, v in resp.headers.items() if k.lower() not in RESPONSE_STRIP_HEADERS
        }
        if "text/event-stream" in content_type:
            if aggregate:
                # 自动流式：上游流式 → 服务端聚合 → 下游拿到完整 JSON
                return await self._aggregate_stream_response(resp, upstream, downstream_id)
            return self._make_stream_response(resp, upstream, downstream_id, headers)
        # 非流式：读全量 body，解析 usage token
        content = await resp.aread()
        await resp.aclose()
        tokens = extract_usage_tokens(content)
        self._finalize_success(upstream.id, downstream_id, tokens)
        return Response(content=content, status_code=resp.status_code, headers=headers)

    async def _aggregate_stream_response(
        self,
        resp: httpx.Response,
        upstream: Upstream,
        downstream_id: int,
    ) -> Response | tuple[int, bytes]:
        """消费上游 SSE 流并聚合为完整 JSON 响应（自动流式的非流式回包）。

        流内 error 事件且无任何内容时返回 (status, error_body)，
        调用方据此做换 Key 重试（如上游过载 503）。
        """
        aggregator = StreamAggregator()
        try:
            async for chunk in resp.aiter_bytes():
                aggregator.feed(chunk)
        except Exception:  # noqa: BLE001 - 网络异常：尽力返回已聚合内容
            aggregator.broken = True
        try:
            await resp.aclose()
        except Exception:  # noqa: BLE001 - 收尾阶段尽力关闭
            pass

        # 流内 error 且无任何内容：交给调用方按状态码分类重试
        if aggregator.stream_error is not None and not aggregator.has_content():
            code = aggregator.stream_error.get("code") or 502
            try:
                status = int(code)
            except (TypeError, ValueError):
                status = 502
            return (status, _json_bytes({"error": aggregator.stream_error}))
        if aggregator.broken and not aggregator.has_content():
            # 断流且无内容：同样交给调用方重试（502 → 换 Key）
            return (
                502,
                _json_bytes(router_error_body(502, "upstream stream interrupted")),
            )

        tokens = aggregator.tokens
        self._db.record_upstream_success(upstream.id, tokens)
        self._db.record_downstream_usage(downstream_id, tokens)
        self._stats.record_request(tokens)
        try:
            import asyncio

            loop = asyncio.get_running_loop()
            loop.create_task(self._scheduler.report_success(upstream.id))
        except RuntimeError:  # pragma: no cover - 无运行循环时跳过窗口计数
            pass
        return Response(
            content=_json_bytes(aggregator.to_response()),
            status_code=200,
            media_type="application/json",
        )

    def _make_stream_response(
        self,
        resp: httpx.Response,
        upstream: Upstream,
        downstream_id: int,
        headers: dict,
    ) -> StreamingResponse:
        """流式转发：字节块逐块透传；token 统计与收尾在 BackgroundTask 中完成。"""
        parser = StreamUsageParser()

        async def generator() -> AsyncIterator[bytes]:
            try:
                async for chunk in resp.aiter_bytes():
                    parser.feed(chunk)
                    yield chunk
            except Exception:
                # 流中途断流：无法撤回重试，注入错误事件后关闭（§6.4 规则 3）
                parser.broken = True
                yield STREAM_ERROR_EVENT

        async def finalize() -> None:
            try:
                await resp.aclose()
            except Exception:  # noqa: BLE001 - 收尾阶段尽力关闭
                pass
            tokens = parser.tokens
            if parser.broken:
                self._db.record_upstream_failure(upstream.id, error="stream interrupted")
            else:
                self._db.record_upstream_success(upstream.id, tokens)
                await self._scheduler.report_success(upstream.id)
            self._db.record_downstream_usage(downstream_id, tokens)
            self._stats.record_request(tokens)

        return StreamingResponse(
            generator(),
            status_code=resp.status_code,
            headers=headers,
            background=BackgroundTask(finalize),
        )

    def _finalize_success(self, upstream_id: int, downstream_id: int, tokens: int) -> None:
        """非流式成功：同步完成统计（均为此后不再 await 的快速操作）。"""
        self._db.record_upstream_success(upstream_id, tokens)
        self._db.record_downstream_usage(downstream_id, tokens)
        self._stats.record_request(tokens)
        # 调度窗口计数（异步锁内为纯内存操作，快速完成）
        try:
            import asyncio

            loop = asyncio.get_running_loop()
            loop.create_task(self._scheduler.report_success(upstream_id))
        except RuntimeError:  # pragma: no cover - 无运行循环时跳过窗口计数
            pass


def _to_int(value: str | None, default: int) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default


def _json_bytes(obj: dict) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")
