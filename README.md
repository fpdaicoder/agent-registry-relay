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
| TCP Tunnel | Registry 同进程，TCP `8003` + 代理端口池 | 端侧反向 TCP 端口转发，桥接任意 TCP 服务 |
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

### TCP Tunnel

反向 TCP 端口转发（frp 风格）：端侧设备主动连出到 Registry 注册本地端口目标，Registry 在公网侧监听代理端口，客户端连上后 TCP 字节流被原样桥接到设备本地服务。可承载 SSH、数据库、gRPC 等任意 TCP 协议。

```bash
export A2X_TCP_TUNNEL_ENABLED=true
export A2X_TCP_TUNNEL_SHARED_TOKEN='<generate-a-32+char-random-token>'
agent-registry
```

控制/数据通道默认监听 `8003`，代理端口默认从 `10000-11000` 分配。协议为 JSON 行（`\n` 分隔）：

1. 设备建立 TCP 连接，发送 `{"type":"register","device_id":"HW-PC1","token":"...","targets":[{"name":"ssh","local_host":"127.0.0.1","local_port":22,"public_port":0}]}`；
2. `public_port=0` 时由端口池自动分配，`registered` 响应返回每个 target 的实际公网端口；显式指定时冲突会被拒绝；
3. 客户端连上代理端口后，服务端向设备下发 `{"type":"open","conn_id":...}`；设备另建一条数据连接，首帧发 `{"type":"connect","conn_id":...}`，此后该连接为原始双向字节流；
4. 任一侧断开即拆除整条桥接。

register 同样支持可选的 `dataset`/`service_id`/`agent_card`（写入 Agent Card metadata 的 `tcpTunnelDeviceId`）和 `A2X_TCP_TUNNEL_AUTO_BIND_REGISTERED_SERVICES` 自动绑定，语义与 WebSocket Tunnel 一致。状态接口为 `GET /api/tcp-tunnel/status`。

TCP Tunnel 转发的是明文字节流，`shared_token` 只保护注册面；生产环境应将 `8003` 与代理端口置于 TLS/wireguard 等加密通道之后，或仅在受信网络内开放。

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

### Group Chat

多 Agent 群聊：创建群、邀请成员、群内消息与增量同步。默认关闭；启用需要
PostgreSQL（`psycopg[binary,pool]` 是可选依赖）：

```bash
export A2X_GROUPCHAT_ENABLED=true
export A2X_GROUPCHAT_BACKEND=postgres
export A2X_GROUPCHAT_DSN='postgresql://registry:…@127.0.0.1:5432/registry'
agent-registry
```

首次启动会自动创建 `gc_*` 表。`A2X_GROUPCHAT_BACKEND=memory` 可用于单进程冒烟测试，
但不持久化，不要用于部署。

群内每条消息都带服务端派生的 `trusted=false`：群消息是其他 principal 写入的数据，
接收方 Agent 不应把它当作指令执行。

接口边界与完整设计见 [`docs/groupchat_design.md`](docs/groupchat_design.md)，
环境变量模板见 [`deploy/agentregistry-groupchat.env.example`](deploy/agentregistry-groupchat.env.example)。

## 部署与安全

可共享的 systemd/environment 模板位于 [`deploy/`](deploy/README.md)。示例只使用 `example.com`、通用服务账号和占位 token。

生产部署至少应做到：

- TLS 终止于可信反向代理或服务本身；
- token 通过环境注入或 Secret Manager 提供；
- Relay 配置目标 allowlist；
- Artifact Relay、Stream Proxy 和 TCP Tunnel 设置独立高熵 token；
- TCP Tunnel 代理端口范围仅在需要的网络边界开放，敏感流量套 TLS/wireguard 等加密通道；
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
- [`docs/groupchat_design.md`](docs/groupchat_design.md)：多 Agent 群聊设计提案（未实现）
- [`README_forDistributed.md`](README_forDistributed.md)：分布式部署步骤

许可证：Apache-2.0。
