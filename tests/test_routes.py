"""路由集成测试：验证 /v1 端点路由匹配与转发（含带斜杠的模型 ID）。"""
import importlib

import httpx
import pytest
from fastapi.testclient import TestClient

from app.upstream import UpstreamProxy


@pytest.fixture
def client(tmp_path, monkeypatch):
    """DATA_DIR 指向临时目录后重载 config/main，避免污染真实数据库。"""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    import app.config as config
    import app.database as database
    import app.main as main

    importlib.reload(config)
    importlib.reload(database)  # database 模块级引用 DB_PATH，需随之重载
    importlib.reload(main)

    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"object": "list", "data": [{"id": "x"}]})

    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=10.0)
    with TestClient(main.app) as c:
        # lifespan 已初始化 db/scheduler/stats，注入 mock 上游客户端（外部客户端不重建）
        main.app.state.proxy = UpstreamProxy(
            main.app.state.db,
            main.app.state.scheduler,
            main.app.state.stats,
            client=mock_client,
        )
        main.app.state.db.add_upstream("routes@x.com", "pw", "nvapi-routes-test")
        yield c, main, seen


def test_model_detail_with_slash_in_id(client):
    """模型 ID 含斜杠（nvidia/nemotron-...）：路由需整段匹配并转发。"""
    c, main, seen = client
    ds = main.app.state.db.add_downstream("t", "sk-router-routes-a")

    resp = c.get(
        "/v1/models/nvidia/nemotron-3-ultra-550b-a55b",
        headers={"Authorization": f"Bearer {ds.apikey}"},
    )
    assert resp.status_code == 200
    # url.path 含 base_url（https://integrate.api.nvidia.com/v1）的 /v1 前缀
    assert seen[-1] == "/v1/models/nvidia/nemotron-3-ultra-550b-a55b"


def test_model_list_still_works(client):
    """模型列表端点不受 :path 路由影响。"""
    c, main, seen = client
    ds = main.app.state.db.add_downstream("t", "sk-router-routes-b")

    resp = c.get("/v1/models", headers={"Authorization": f"Bearer {ds.apikey}"})
    assert resp.status_code == 200
    assert seen[-1] == "/v1/models"


def test_model_detail_requires_downstream_key(client):
    """/v1/models/{id} 同样需要下游 Key 鉴权。"""
    c, _, _ = client
    resp = c.get("/v1/models/nvidia/some-model")
    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == 401
