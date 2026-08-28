# NVIDIA NIM Router

面向 **NVIDIA NIM（build.nvidia.com）** 的多 API Key 负载均衡反向代理：OpenAI 兼容接口、浏览器管理 UI、故障转移与实时监控，**一行 Docker 命令即可部署**。

## 架构

```
┌──────────────┐      ┌─────────────────────────────────┐      ┌──────────────────────┐
│   下游应用    │      │        nvidia-nim-router         │      │    NVIDIA NIM 上游    │
│  (OpenAI SDK) │─────▶│  ┌──────────┐  ┌─────────────┐  │─────▶│ integrate.api.nvidia │
│  model 名透传 │      │  │ 鉴权中间件 │  │  调度器      │  │ nvapi│      .com/v1         │
└──────────────┘      │  └──────────┘  └─────────────┘  │ -key │   (Key 池，数量由     │
                      │  ┌──────────┐  ┌─────────────┐  │ 池   │     CSV 导入动态决定) │
                      │  │ 故障转移  │  │  统计引擎    │  │      └──────────────────────┘
                      │  └──────────┘  └─────────────┘  │
                      │        浏览器 UI（4 页面）         │
                      └─────────────────────────────────┘
```

- **透传原则**：下游传什么模型名就转发什么（`model` 字段逐字节透传，零映射、零校验）
- **动态规模**：上游 Key 池规模完全由导入的 CSV 决定，1 个到上万个均可
- **调度策略**：轮巡 / 随机 / 顺序，且支持「每 N 次请求才切换到下一个 Key」
- **故障转移**：超时 / 429 / 5xx / 401 自动换 Key 重试；403 只冷却 10 分钟不标失效；流内 error 事件（HTTP 200 + error）同样触发转移
- **自动流式**：非流式请求自动转为流式发往上游并在服务端聚合回完整 JSON（下游无感知），规避大模型长响应读超时；设置页可关
- **限速检测**：「全部检测」串行限速执行（每分钟 1-10 个 Key，可设），避免批量连通性检测触发上游限流
- **技术栈**：Python 3.11 + FastAPI + httpx + SQLite，前端原生 HTML/CSS/JS，零外部依赖单镜像

## 一行命令部署

无需克隆仓库，机器上有 Docker 即可（BuildKit 会自动拉取 GitHub 代码构建）：

```bash
docker build -t nim-router https://github.com/mxdn-xujie/nvidia-nim-router.git && \
docker run -d --name nim-router -p 8016:8000 -v nim-router-data:/data --restart unless-stopped nim-router
```

或用 docker-compose（推荐，便于后续更新管理）：

```bash
git clone https://github.com/mxdn-xujie/nvidia-nim-router.git && \
cd nvidia-nim-router && docker compose up -d --build
```

启动后浏览器打开 `http://localhost:8016`（远程服务器换成对应 IP）。数据持久化在 Docker volume，容器重启不丢 Key 与统计。仍需换端口时用 `PORT=xxxx docker compose up -d --build` 覆盖。

更新版本：

```bash
cd nvidia-nim-router && git pull && docker compose up -d --build
```

本地开发（Python 3.11+）：

```bash
pip install -r requirements-dev.txt
uvicorn app.main:app --host 0.0.0.0 --port 8016
```

## 下游接入示例

先在「下游管理」页签发一个 `sk-router-` 开头的访问 Key，然后像使用 OpenAI 一样调用：

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://你的服务器:8016/v1",
    api_key="sk-router-你的下游key",
)

# model 名与 NVIDIA 官网（build.nvidia.com）完全一致，路由直接透传
resp = client.chat.completions.create(
    model="deepseek-ai/deepseek-r1",
    messages=[{"role": "user", "content": "你好"}],
    stream=True,
)
for chunk in resp:
    print(chunk.choices[0].delta.content or "", end="")
```

curl 示例：

```bash
curl http://localhost:8016/v1/chat/completions \
  -H "Authorization: Bearer sk-router-你的下游key" \
  -H "Content-Type: application/json" \
  -d '{"model": "deepseek-ai/deepseek-r1", "messages": [{"role": "user", "content": "你好"}]}'
```

> 开启自动流式（默认开启）时，即使不带 `stream=true`，路由也会对上游强制流式并在服务端聚合，下游拿到的仍是完整 JSON。

## CSV 导入说明

- 标准格式：`email,password,apikey`（**带表头**）；表头有无均可自动识别
- 支持拖拽上传或直接粘贴文本；字段分隔容忍「逗号+空格」，列序自动定位（`nvapi-` 开头为 Key，含 `@` 为 email）
- Key 数量不限：由导入的 CSV 实际内容动态决定；**重复导入自动去重**（库内与文件内重复均计入 `duplicates` 并返回脱敏 Key 明细）
- 模板可在导入弹窗中一键下载

| 上游返回 | 处置 |
|---|---|
| 超时 / 连接错误 | 冷却 `cooldown_seconds` → 换 Key 重试 |
| 429 | 冷却 → 换 Key 重试 |
| 401 | 标记 invalid → 换 Key 重试 |
| 403 | 只冷却 10 分钟（模型级权限不足，Key 可能仍有效） |
| 5xx | 换 Key 重试，Key 不冷却 |
| 流内 error 事件（HTTP 200 + error） | 按事件内状态码同上分类转移 |
| 400 / 404 / 422 | 请求本身错误，原样返回下游，不转移 |

## 管理功能

- **首页**：实时请求数（RPM）、Token 速度、总 Token、运行时长，最近 1 小时请求折线图（SSE 每秒推送）
- **上游页**：CSV 导入、逐 Key 连通性检测（延时/状态码）、调度策略设置、分页表格；「全部检测」为限速串行（每分钟 1-10 个 Key，可设），避免批量检测触发上游限流
- **下游页**：服务端接口地址展示、签发/禁用/删除下游 Key
- **设置页**：超时转移时间、重试次数、冷却时间、自动流式开关、批量检测速率、主题（深/浅）、管理密码、NVIDIA Base URL（可改指向私有 NIM 部署）

## 开发

```bash
pip install -r requirements-dev.txt
pytest          # 运行单元测试（csv_import / scheduler / failover / auto_stream / check_rate / routes）
ruff check app tests   # 代码检查
```

构建镜像：

```bash
docker build -t nvidia-nim-router .
```

## 安全提示

数据库文件（`/data/router.db`）包含**明文密码与 API Key**，请保护数据卷权限，不要将其提交到代码仓库。生产环境建议在设置页配置管理密码。

## License

MIT
