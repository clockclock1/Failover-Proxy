# Failover Proxy 中文 | [English](#english)

Failover Proxy 是一个 OpenAI 兼容的大模型故障转移代理，内置可视化管理界面。它可以暴露自定义 API Key 和 OpenAI 风格接口，并把一个或多个公开模型名按策略路由到多个上游模型，适合低延迟代理、故障转移和容器化部署场景。

## 功能

- OpenAI 兼容接口：
  - `GET /v1/models` 和 `GET /models`
  - `POST /v1/chat/completions` 和 `POST /chat/completions`
  - `POST /v1/responses`、`POST /responses`、`POST /v1/response` 和 `POST /response`
  - `POST /v1/completions` 和 `POST /completions`
- 自定义代理 API Key，客户端通过 `Authorization: Bearer ...` 调用。
- Admin Token 登录与 Session 管理，支持 `/api/login`、`/api/logout`、`/api/session`。
- 多模型故障转移链，每个公开模型可配置有序 targets。
- Chat Completions 与 Responses 双向适配：自动按目标端点探测并转换请求、普通响应和 SSE 流；覆盖工具调用历史、文本格式、推理片段、结束原因、用量和常见图像/音频/文件/视频内容。`previous_response_id` 等有状态字段仅在本机已有对应响应历史时展开后转换，否则保留到原生 Responses 上游。
- Chat Completions 与 Responses 自动双向路由：普通请求和 Chat 函数工具请求优先走对应原生端点，端点不受支持时自动尝试另一协议；Responses 函数工具请求自动优先尝试 Chat Completions，以兼容只在 Chat 接口实现工具调用的上游。Responses 有状态字段及 Chat 多候选等无法安全转换的请求保留原生路由，无需单独配置。
- 支持上游字段：`name`、`baseUrl`、`apiKey`、`modelName`、`enabled`、`priority`、`weight`、`maxRetries`、`timeoutMs` 等。
- 故障转移策略：`priority`、`round-robin`、`weighted`、`latency-based`。
- 模型源模式：从自定义 `/v1/models` URL 拉取模型，支持 include/exclude 过滤、publicPrefix/publicSuffix 和 `{model}` 模板。
- 流式 SSE/chunked 原样转发或逐事件跨协议转换；早期分块会探测上游错误。一旦开始向客户端输出字节，中途错误只记录，不再切换上游。
- 熔断器：连续失败阈值、冷却时间、立即冷却状态码，例如 `429`。
- 被动故障检测：真实代理请求触发转移和熔断，管理界面健康检查只更新在线状态与延迟。
- 代理请求不再限制每个模型的并发线程数；请求或流结束后实时线程记录立即释放。
- 统计与日志：总请求、成功、失败、转移次数，按模型和 target 的细粒度统计，请求日志，实时线程，进程内存占用。
- 配置持久化：`data/config.json` 原子写入；请求日志、模型统计和运行统计分别保存到 CSV。旧 `data/stats.json` 仅用于兼容迁移，不会被修改。
- 管理界面以内嵌多文件前端形式随服务二进制发布，无需额外前端运行时。
- CORS、请求超时、优雅关闭和结构化日志。

## 本地启动

Windows 一键启动：

```powershell
.\start.ps1
```

也可以双击：

```text
start.bat
```

Linux/macOS：

```bash
chmod +x ./start.sh
./start.sh
```

直接使用 Cargo：

```bash
cargo run
```

默认配置：

- 管理后台：`http://127.0.0.1:8787`
- 监听地址：`0.0.0.0:8787`
- Admin Token：`admin`
- 代理 Key：`sk-local-test`
- 配置文件：`data/config.json`
- 请求日志：`data/request-logs.csv`
- 模型统计：`data/model-stats.csv`
- 运行统计：`data/runtime-stats.csv`
- `data/stats.json` 仅在首次迁移时读取，之后不再使用或写入

首次运行时，如果 `data/config.json` 不存在，程序会自动创建默认配置。

常用环境变量：

```bash
HOST=0.0.0.0
PORT=8787
DATA_DIR=./data
CONFIG_PATH=./data/config.json
STATS_PATH=./data/stats.json  # 仅用于兼容迁移，不会写入
REQUEST_LOGS_PATH=./data/request-logs.csv
MODEL_STATS_PATH=./data/model-stats.csv
RUNTIME_STATS_PATH=./data/runtime-stats.csv
STREAM_FAILURE_PROBE_KB=64
RUST_LOG=failover_proxy=info,tower_http=info
```

## 构建前端

仓库中的 `ui/` 是前端源码，`assets/` 是 Rust 服务启动时嵌入的静态资源产物。修改前端后执行：

```bash
npm ci
npm --prefix ui ci
npm run build:ui
```

`npm run build:ui` 会构建 React UI，并把产物复制到 `assets/index.html`、`assets/app.css`、`assets/app.js` 和 `assets/app-core.js`。

## 发布构建

```bash
cargo build --release
```

启用可选分配器：

```bash
cargo build --release --features mimalloc
```

Release profile 已启用 `opt-level=3`、LTO、单 codegen unit、符号裁剪和 `panic=abort`。构建完成后可直接分发：

```text
target/release/failover-proxy
target/release/failover-proxy.exe
```

GitHub Actions 的 `Build Executables` 工作流会生成以下二进制产物：

- `failover-proxy-windows-amd64.exe`
- `failover-proxy-windows-arm64.exe`
- `failover-proxy-linux-amd64`
- `failover-proxy-linux-arm64`
- `failover-proxy-macos-amd64`
- `failover-proxy-macos-arm64`

发布 Release 时，`Docker Image` 工作流还会构建并推送多架构 Docker 镜像到 GHCR。

## Docker 发布

本地构建镜像：

```bash
docker build -t failover-proxy .
```

运行：

```bash
docker run --rm -p 8787:8787 -v failover-proxy-data:/app/data failover-proxy
```

使用 Compose：

```bash
cd deploy/compose
cp .env.example .env
# 发布 Release 后把 FAILOVER_PROXY_VERSION 改成 v0.1.0 等版本号
# 如需自定义宿主机端口，把 FAILOVER_PROXY_PORT 改成 18080 等端口
docker compose up -d
```

可通过环境变量覆盖：

```bash
FAILOVER_PROXY_IMAGE=ghcr.io/clockclock1/failover-proxy
FAILOVER_PROXY_PORT=8787
FAILOVER_PROXY_DATA_DIR=./data
FAILOVER_PROXY_VERSION=latest
FAILOVER_PROXY_NETWORK=failover-proxy-network
RUST_LOG=failover_proxy=info,tower_http=info
```

## 编排部署

Kubernetes 配置位于 `deploy/kubernetes/`：

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/pvc.yaml
kubectl apply -f deploy/kubernetes/deployment.yaml
kubectl apply -f deploy/kubernetes/service.yaml
```

默认镜像：

```text
ghcr.io/clockclock1/failover-proxy:latest
```

默认端口：

```text
8787
```

## 配置模型

配置文件示例位于 `data/config.example.json`。最小模型配置如下：

```json
{
  "adminToken": "admin",
  "proxyKeys": [
    {
      "name": "test-key",
      "key": "sk-local-test",
      "enabled": true
    }
  ],
  "models": [
    {
      "publicName": "gpt-failover",
      "enabled": true,
      "targets": [
        {
          "name": "primary-openai",
          "baseUrl": "https://api.openai.com/v1",
          "apiKey": "sk-replace-me",
          "modelName": "gpt-4.1-mini",
          "enabled": true
        }
      ]
    }
  ]
}
```

请求示例：

```bash
curl http://127.0.0.1:8787/v1/chat/completions \
  -H "Authorization: Bearer sk-local-test" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-failover",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": false
  }'
```

流式请求：

```bash
curl -N http://127.0.0.1:8787/v1/chat/completions \
  -H "Authorization: Bearer sk-local-test" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-failover",
    "messages": [{"role": "user", "content": "Say hello slowly"}],
    "stream": true
  }'
```

## 性能测试建议

建议先使用本地 OpenAI 兼容 mock 上游，再用相同配置进行压测：

```bash
oha -z 60s -c 128 -m POST http://127.0.0.1:8787/v1/chat/completions \
  -H "Authorization: Bearer sk-local-test" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-failover","messages":[{"role":"user","content":"ping"}],"stream":false}'
```

建议观察：

- QPS 和 p50/p95/p99 延迟：`oha`、`wrk`、`bombardier`。
- 内存：Windows 任务管理器、`Get-Process failover-proxy`、Linux `pidstat -r`。
- 连接复用：上游访问日志，短时间调试可打开 `RUST_LOG=reqwest=debug`。
- 故障转移：让主上游返回 429、500 或超时，检查 `/api/stats` 和 `x-proxy-target`。
- 流式转发：使用 `curl -N` 或流式压测工具，同时观察 CPU 和实时状态。

---

## English

Failover Proxy is an OpenAI-compatible LLM failover proxy with a visual management UI. It exposes custom API keys and OpenAI-style endpoints while routing one or many public model names through configurable upstream failover targets, making it suitable for low-latency proxying, failover, and containerized deployments.

## Features

- OpenAI-compatible endpoints:
  - `GET /v1/models` and `GET /models`
  - `POST /v1/chat/completions` and `POST /chat/completions`
  - `POST /v1/responses`, `POST /responses`, `POST /v1/response`, and `POST /response`
  - `POST /v1/completions` and `POST /completions`
- Bidirectional Chat Completions / Responses adaptation: endpoint detection converts requests, non-streaming responses, and SSE streams, including tool history, text formats, reasoning, finish reasons, usage, and common image/audio/file/video content. Stateful fields such as `previous_response_id` are expanded only when this process has the matching response history; otherwise the request stays on a native Responses endpoint.
- Automatic bidirectional Chat Completions / Responses routing: ordinary requests and Chat function-tool requests use their native endpoint first and try the other protocol when unsupported. Responses function-tool requests automatically prefer Chat Completions to support upstreams that implement tool calling only on the Chat endpoint. Stateful Responses fields and Chat multi-choice requests that cannot be safely converted stay on their native route; no separate setting is required.
- Custom proxy API keys via `Authorization: Bearer ...`.
- Admin token login and session management with `/api/login`, `/api/logout`, and `/api/session`.
- Multiple model failover chains. Each public model can define ordered upstream targets.
- Each proxy model exposes its configurable `contextWindowTokens` (default 1,000,000) through `/v1/models`, and inbound requests are checked against that model's window. Target fields include `name`, `baseUrl`, `apiKey`, `modelName`, `enabled`, `priority`, `weight`, `maxRetries`, and `timeoutMs`. Failover Proxy sends the original request first. Only when an upstream returns a context-length error with HTTP 422 does it lossily compact the request context and retry that same upstream until it succeeds or the payload cannot be reduced further (with a 32-round loop-safety guard); unrelated 422 responses keep their normal error behavior.
- Failover strategies: `priority`, `round-robin`, `weighted`, `latency-based`.
- Model-source mode: fetch models from a custom `/v1/models` URL, apply include/exclude filters, add public prefixes/suffixes, expand `{model}` templates, and set a shared `contextWindowTokens` for the generated proxy models.
- Streaming SSE/chunked pass-through or per-event protocol translation with early upstream error probing. After bytes are written to the client, mid-stream errors are recorded but not failed over.
- Circuit breaker with failure threshold, cooldown duration, and immediate cooldown status codes such as `429`.
- Passive failure detection: real proxy requests trigger failover and circuit breaking; UI health checks only update observed provider status and latency.
- Proxy requests are no longer capped by per-model thread slots. Active thread records are released immediately when the request or stream finishes.
- Stats and logs: total requests, successes, failures, failovers, per-model and per-target stats, request logs, live threads, and process memory.
- Config persistence: `data/config.json` is saved atomically; request logs, model stats, and runtime stats are stored as CSV files. Legacy `data/stats.json` is read only for compatibility migration and is never modified.
- Embedded multi-file admin UI served from the service binary. No extra frontend runtime is required.
- CORS, request timeout, graceful shutdown, and structured logging.

## Local Start

Windows one-click start:

```powershell
.\start.ps1
```

Or double-click:

```text
start.bat
```

Linux/macOS:

```bash
chmod +x ./start.sh
./start.sh
```

Direct Cargo start:

```bash
cargo run
```

Defaults:

- Admin UI: `http://127.0.0.1:8787`
- Bind address: `0.0.0.0:8787`
- Admin Token: `admin`
- Proxy Key: `sk-local-test`
- Config: `data/config.json`
- Request logs: `data/request-logs.csv`
- Model stats: `data/model-stats.csv`
- Runtime stats: `data/runtime-stats.csv`
- `data/stats.json` is read only for first-run migration and is never written or removed

On first run, the server creates `data/config.json` automatically if it does not exist.

Common environment variables:

```bash
HOST=0.0.0.0
PORT=8787
DATA_DIR=./data
CONFIG_PATH=./data/config.json
STATS_PATH=./data/stats.json  # legacy migration input only; never written
REQUEST_LOGS_PATH=./data/request-logs.csv
MODEL_STATS_PATH=./data/model-stats.csv
RUNTIME_STATS_PATH=./data/runtime-stats.csv
STREAM_FAILURE_PROBE_KB=64
RUST_LOG=failover_proxy=info,tower_http=info
```

## Build UI

The `ui/` directory contains the frontend source. The `assets/` directory contains static assets embedded by the Rust service. After changing the frontend, run:

```bash
npm ci
npm --prefix ui ci
npm run build:ui
```

`npm run build:ui` builds the React UI and copies the output to `assets/index.html`, `assets/app.css`, `assets/app.js`, and `assets/app-core.js`.

## Release Build

```bash
cargo build --release
```

Optional allocator:

```bash
cargo build --release --features mimalloc
```

The release profile enables `opt-level=3`, LTO, single codegen unit, symbol stripping, and `panic=abort`. After building, distribute the binary directly:

```text
target/release/failover-proxy
target/release/failover-proxy.exe
```

The GitHub Actions `Build Executables` workflow builds these artifacts:

- `failover-proxy-windows-amd64.exe`
- `failover-proxy-windows-arm64.exe`
- `failover-proxy-linux-amd64`
- `failover-proxy-linux-arm64`
- `failover-proxy-macos-amd64`
- `failover-proxy-macos-arm64`

Publishing a Release also runs the `Docker Image` workflow, which builds and pushes a multi-arch Docker image to GHCR.

## Docker Release

Build locally:

```bash
docker build -t failover-proxy .
```

Run:

```bash
docker run --rm -p 8787:8787 -v failover-proxy-data:/app/data failover-proxy
```

Use Compose:

```bash
cd deploy/compose
cp .env.example .env
# after publishing a Release, set FAILOVER_PROXY_VERSION to v0.1.0 or another tag
# to customize the host port, set FAILOVER_PROXY_PORT to 18080 or another port
docker compose up -d
```

Override with environment variables:

```bash
FAILOVER_PROXY_IMAGE=ghcr.io/clockclock1/failover-proxy
FAILOVER_PROXY_PORT=8787
FAILOVER_PROXY_DATA_DIR=./data
FAILOVER_PROXY_VERSION=latest
FAILOVER_PROXY_NETWORK=failover-proxy-network
RUST_LOG=failover_proxy=info,tower_http=info
```

## Orchestration Deployment

Kubernetes manifests live under `deploy/kubernetes/`:

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/pvc.yaml
kubectl apply -f deploy/kubernetes/deployment.yaml
kubectl apply -f deploy/kubernetes/service.yaml
```

Default image:

```text
ghcr.io/clockclock1/failover-proxy:latest
```

Default port:

```text
8787
```

## Model Configuration

The example config lives at `data/config.example.json`. Minimal model configuration:

```json
{
  "adminToken": "admin",
  "proxyKeys": [
    {
      "name": "test-key",
      "key": "sk-local-test",
      "enabled": true
    }
  ],
  "models": [
    {
      "publicName": "gpt-failover",
      "enabled": true,
      "targets": [
        {
          "name": "primary-openai",
          "baseUrl": "https://api.openai.com/v1",
          "apiKey": "sk-replace-me",
          "modelName": "gpt-4.1-mini",
          "enabled": true
        }
      ]
    }
  ]
}
```

Example request:

```bash
curl http://127.0.0.1:8787/v1/chat/completions \
  -H "Authorization: Bearer sk-local-test" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-failover",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": false
  }'
```

Streaming request:

```bash
curl -N http://127.0.0.1:8787/v1/chat/completions \
  -H "Authorization: Bearer sk-local-test" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-failover",
    "messages": [{"role": "user", "content": "Say hello slowly"}],
    "stream": true
  }'
```

## Benchmark Suggestions

Use a local OpenAI-compatible mock upstream first, then run benchmarks with the same config:

```bash
oha -z 60s -c 128 -m POST http://127.0.0.1:8787/v1/chat/completions \
  -H "Authorization: Bearer sk-local-test" \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-failover","messages":[{"role":"user","content":"ping"}],"stream":false}'
```

Track:

- QPS and p50/p95/p99 latency: `oha`, `wrk`, `bombardier`.
- Memory: Windows Task Manager, `Get-Process failover-proxy`, Linux `pidstat -r`.
- Connection reuse: upstream access logs and `RUST_LOG=reqwest=debug` for short runs.
- Failover behavior: inject 429/500/timeouts from the primary and verify `/api/stats` plus `x-proxy-target`.
- Streaming forwarding: use `curl -N` or a streaming benchmark while watching CPU and live status.
