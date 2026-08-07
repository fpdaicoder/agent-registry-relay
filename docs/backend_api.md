# Backend API

默认地址：`http://127.0.0.1:8000`。完整 OpenAPI 以运行时 `/docs` 为准。

## Registry

所有 namespace 路由以 `/api/datasets` 开头。

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| GET / POST | `/api/datasets` | 列出或创建 namespace |
| DELETE | `/api/datasets/{dataset}` | 删除 namespace |
| GET / POST | `/{dataset}/register-config` | 读取或更新允许的服务格式 |
| GET / POST | `/{dataset}/auth-config` | 读取或更新鉴权开关 |
| GET / POST | `/{dataset}/lease-config` | 读取或更新心跳配置 |
| GET | `/{dataset}/services` | 列出并过滤服务 |
| GET / PUT / DELETE | `/{dataset}/services/{service_id}` | 获取、更新或注销服务 |
| POST | `/{dataset}/services/generic` | 注册 generic 服务 |
| POST | `/{dataset}/services/a2a` | 注册 A2A Agent Card |
| POST / DELETE | `/{dataset}/skills...` | 上传、删除或下载 Skill |
| POST / DELETE | `/{dataset}/reservations...` | 创建、释放或延长预约 |

创建 namespace 的最小 body：

```json
{"name":"default"}
```

可选字段是 `formats`、`auth_required` 和 `lease_config`。接口不接受 embedding、搜索或分类配置。

## Auth 与 Heartbeat

- Auth 前缀：`/api/auth`，包含 `whoami`、`principals` 和 `keys`。
- Heartbeat：`POST|DELETE /api/datasets/{dataset}/services/{service_id}/heartbeat`。
- 鉴权未初始化时，旧 namespace 保持匿名模式；受保护 namespace 使用 Bearer token。

## Cluster

Cluster 前缀为 `/api/cluster`，模块未初始化时返回 404。运维常用 `/state` 和 `/set`；peer/session/merkle/pull/update 路由用于节点间协议，不应直接暴露到不可信网络。

## A2A Relay

Relay 默认关闭，启用后提供：

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| POST | `/a2a/{dataset}/{service_id}` | 转发 A2A JSON/SSE 请求 |
| GET | `/api/datasets/{dataset}/services/{service_id}/agent-card?route=direct|relay` | 获取直连或 Relay Agent Card |

Relay 拒绝非 JSON body、超限 body、非 A2A 目标、不健康目标、不支持的 binding、未允许的目标和回环 URL。

## Tunnel

Tunnel 的 Registry 状态接口是 `/api/tunnel/status`；WebSocket listener 默认位于独立端口 `8001`。未启用时状态接口返回结构化 404。

## Artifact Relay

前缀 `/api/artifact-relay`：

- `GET /status`
- `POST /transfers`
- `PUT|GET|HEAD|DELETE /transfers/{transfer_id}`
- `GET /transfers/{transfer_id}/status`
- `GET|HEAD /uri/{transfer_id}/{uri_token}`

创建操作使用 `X-Artifact-Relay-Key`；传输操作使用每个 transfer 独立的 Bearer token。服务校验长度、Range、TTL、配额和 SHA-256。

## Stream Proxy

Stream Proxy 是独立进程：

- `GET /healthz`
- `POST /api/stream-proxy/sessions`
- `GET /api/stream-proxy/status`
- `GET /api/stream-proxy/sessions/{transfer_id}`
- `POST /api/stream-proxy/sessions/{transfer_id}/cancel`
- `WS /v1/stream/{transfer_id}/{sender|receiver}`

控制面使用 `X-Stream-Proxy-Key`；发送端和接收端使用创建 session 时返回的独立凭据。

## 错误边界

- `400`：请求或注册格式无效；
- `401/403`：缺少凭据或权限不足；
- `404`：资源不存在或可选模块未启用；
- `409`：租约、健康状态或 Relay 状态冲突；
- `413`：请求/文件/流超过配置上限；
- `429`：数据面容量已满；
- `501`：Relay binding 不受支持。
