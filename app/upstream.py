"""NVIDIA 上游客户端：透传转发核心（§6）。

透传原则（强制）：
- 请求体不解析业务字段、不改写，model/messages/stream 等原样字节转发；
- 仅做两件事：替换 Authorization 头为调度器选中的 nvapi-key；移除 hop-by-hop 头；
- 响应体原样返回（含 content-type）。

故障转移（§6.3）见 failover.py 处置表；重试耗尽后把最后一次上游响应原样透传。
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
    ) -> Response:
        """透传转发一次下游请求（含故障转移与统计）。"""
        db = self._db
        base_url = (db.get_setting("nvidia_base_url") or "").rstrip("/")
        timeout_ms = _to_int(db.get_setting("timeout_ms"), 30000)
        max_retries = _to_int(db.get_setting("max_retries"), 3)
        cooldown_seconds = _to_int(db.get_setting("cooldown_seconds"), 60)
        client = await self._get_client(timeout_ms)

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
                req = client.build_request(method, url, headers=req_headers, content=body)
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
                    return await self._handle_success(resp, upstream, downstream_id)
                # 非成功：读取 body 用于判定与透传
                content = await resp.aread()
                resp_headers = {
                    k: v for k, v in resp.headers.items() if k.lower() not in RESPONSE_STRIP_HEADERS
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
    async def _handle_success(self, resp: httpx.Response, upstream: Upstream, downstream_id: int) -> Response:
        content_type = (resp.headers.get("content-type") or "").lower()
        headers = {
            k: v for k, v in resp.headers.items() if k.lower() not in RESPONSE_STRIP_HEADERS
        }
        if "text/event-stream" in content_type:
            return self._make_stream_response(resp, upstream, downstream_id, headers)
        # 非流式：读全量 body，解析 usage token
        content = await resp.aread()
        await resp.aclose()
        tokens = extract_usage_tokens(content)
        self._finalize_success(upstream.id, downstream_id, tokens)
        return Response(content=content, status_code=resp.status_code, headers=headers)

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