# Agent Registry Relay

一个面向 Agent 服务的轻量注册中心，并提供可选的 Relay 数据面。仓库只包含两类能力：

- 注册中心控制面：namespace、服务注册/注销/更新/发现、鉴权、心跳、预约锁和集群同步。
- Relay 数据面：A2A 请求转发、WebSocket Tunnel、Artifact Relay 和独立 Stream Proxy。

本仓库不包含 A2X 搜索、分类树构建、向量检索、Traditional 评估、LLM Provider 或演示 UI。

## 组件

| 组件 | 进程/端口 | 作用 |
| --- | --- | --- |
| Registry API | `agent-registry`，HTTP `8000` | 服务注册、发现、鉴权、心跳、预约和集群控制面 |
| A2A Relay | Registry 同进程，HTTP `8000` | 将 JSON/SSE 请求转发到已注册 A2A Agent |
| WebSocket Tunnel | Registry 同进程，WS `8001` | 为无法被外部直连的端侧 Agent 提供反向通道 |
| Artifact Relay | Registry 同进程，HTTP `8000` | 有界、可续传、带哈希校验的文件中转 |
| Stream Proxy | `agent-stream-proxy`，WS `8002` | 独立的成对二进制流转发服务 |

## 安装与启动

要求 Python 3.10 或更高版本。

```bash
git clone https://github.com/kwistzzqq-byte/agent-registry-relay.git
cd agent-registry-relay
python -m venv .venv
source .venv/bin/activate
pip install -e .
agent-registry
```

默认 API 地址是 `http://127.0.0.1:8000`，OpenAPI 文档位于 `/docs`。

```bash
agent-registry --host 0.0.0.0 --port 8000
```

内部 Python 包名 `a2x_registry`、`A2X_*` 环境变量和 `a2x-registry` 命令保留为兼容层；新部署优先使用 `agent-registry`、`agent-register` 和 `agent-stream-proxy`。

## 注册中心快速验证

创建 namespace：

```bash
curl -X POST http://127.0.0.1:8000/api/datasets \
  -H 'Content-Type: application/json' \
  -d '{"name":"default"}'
```

注册 generic 服务：

```bash
curl -X POST http://127.0.0.1:8000/api/datasets/default/services/generic \
  -H 'Content-Type: application/json' \
  -d '{"name":"weather","description":"Weather service","url":"http://agent.example.com"}'
```

列出服务：

```bash
curl http://127.0.0.1:8000/api/datasets/default/services
```

注册中心支持三类记录：

- `generic`：通用 HTTP 或本地服务描述；
- `a2a`：A2A Agent Card，可被 Relay 解析；
- `skill`：包含 `SKILL.md` 的 ZIP 包。

Python SDK 位于 [`client/`](client/README.md)，可单独安装：

```bash
pip install "agent-registry-client @ git+https://github.com/kwistzzqq-byte/agent-registry-relay.git@main#subdirectory=client"
```

## 可选鉴权与心跳

鉴权默认关闭。初始化后会一次性输出管理员 token；请立即存入密码管理器，不要写进仓库或示例配置。

```bash
agent-registry auth init
```

创建受保护 namespace 时传入 `auth_required=true`。心跳也按 namespace 显式启用：

```json
{
  "name": "team",
  "auth_required": true,
  "lease_config": {
    "enabled": true,
    "min_ttl": 10,
    "max_ttl": 600,
    "grace_period": 60
  }
}
```

详细权限模型见 [`docs/auth_design.md`](docs/auth_design.md)，心跳状态机见 [`docs/heartbeat_design.md`](docs/heartbeat_design.md)。

## Relay

Relay 只转发到注册中心中 `type=a2a` 且健康的服务。默认关闭；最小开发配置：

```bash
export A2X_RELAY_ENABLED=true
export A2X_RELAY_PUBLIC_BASE_URL=http://registry.example.com:8000
export A2X_RELAY_ALLOW_ALL_TARGETS=true
agent-registry --host 0.0.0.0
```

生产环境应使用目标 allowlist，避免 `ALLOW_ALL_TARGETS=true`。主要接口：

- `POST /a2a/{dataset}/{service_id}`
- `GET /api/datasets/{dataset}/services/{service_id}/agent-card`（用 `route=direct|relay` 选择直连或 Relay Agent Card）

A2A 方法由 POST 请求的 JSON body 表达，不使用 `/v1/message:send` 或 `/v1/message:stream` 路径后缀。

### WebSocket Tunnel

```bash
export A2X_TUNNEL_ENABLED=true
export A2X_TUNNEL_SHARED_TOKEN='<generate-a-32+char-random-token>'
export A2X_TUNNEL_AUTO_BIND_REGISTERED_SERVICES=true
agent-registry
```

Tunnel 默认监听 `8001`。Relay 优先使用已绑定的 Tunnel 连接，否则回退到注册卡中的 HTTP 目标。

### Artifact Relay

```bash
export A2X_ARTIFACT_RELAY_ENABLED=true
export A2X_ARTIFACT_RELAY_PUBLIC_BASE_URL=https://registry.example.com
export A2X_ARTIFACT_RELAY_STORAGE_DIR=/var/lib/agent-registry/artifact-relay
export A2X_ARTIFACT_RELAY_CREATE_TOKEN='<generate-a-32+char-random-token>'
agent-registry
```

Artifact Relay 与 Registry 共用 HTTP 进程，但拥有独立 token、配额和存储目录。

### Stream Proxy

```bash
export A2X_STREAM_PROXY_CREATE_TOKEN='<generate-a-32+char-random-token>'
export A2X_STREAM_PROXY_PUBLIC_WS_BASE_URL=wss://stream.registry.example.com
agent-stream-proxy
```

Stream Proxy 必须单独启动；Registry 健康不代表 Stream Proxy 已运行。

## 部署与安全

可共享的 systemd/environment 模板位于 [`deploy/`](deploy/README.md)。示例只使用 `example.com`、通用服务账号和占位 token。

生产部署至少应做到：

- TLS 终止于可信反向代理或服务本身；
- token 通过环境注入或 Secret Manager 提供；
- Relay 配置目标 allowlist；
- Artifact Relay 和 Stream Proxy 设置独立高熵 token；
- 持久化目录权限限制为服务账号可读写；
- 不提交 `.env`、SSH key、API key、真实主机/IP 或个人绝对路径。

## 开发验证

```bash
uv run --isolated --extra dev python -m pytest -q tests client/tests
uv build
```

仓库范围约束测试会阻止搜索、分类、向量、LLM 和 UI 文件重新进入发布包：

```bash
uv run python -m pytest -q tests/deploy/test_repository_scope.py
```

## 文档

- [`docs/backend_api.md`](docs/backend_api.md)：API 边界和启动方式
- [`docs/register_design.md`](docs/register_design.md)：注册中心存储与请求流
- [`docs/auth_design.md`](docs/auth_design.md)：鉴权和 namespace 权限
- [`docs/heartbeat_design.md`](docs/heartbeat_design.md)：心跳与健康状态
- [`docs/cluster_design.md`](docs/cluster_design.md)：多节点同步
- [`README_forDistributed.md`](README_forDistributed.md)：分布式部署步骤

许可证：Apache-2.0。
