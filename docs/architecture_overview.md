# 架构总览

本仓库由一个注册中心控制面和四个可选数据面组成。

```text
a2x_registry/
├── backend/          FastAPI 入口、namespace/service 路由、生命周期
├── register/         注册业务逻辑与持久化
├── auth/             API key、principal、namespace 权限
├── heartbeat/        服务租约与健康状态
├── cluster/          多节点成员关系、复制与反熵
├── relay/            A2A HTTP/SSE 请求转发
├── tunnel/           端侧 WebSocket 反向通道
├── artifact_relay/   可续传文件中转
├── stream_proxy/     独立二进制流代理
└── common/           原子写入、路径、鉴权上下文等共享设施
```

## 进程边界

- `agent-registry` 启动 Registry API，并在同一进程内按配置启用 Relay、Tunnel 和 Artifact Relay。
- `agent-stream-proxy` 是独立 FastAPI/WebSocket 进程，Registry 不代理它的流量。
- Tunnel 默认使用独立 WS 监听端口；Registry、Relay 和 Artifact Relay 共用 HTTP 端口。

## 启停顺序

启动顺序：

1. Registry、Auth、Heartbeat、Cluster；
2. Relay；
3. Tunnel；
4. Artifact Relay。

关闭按相反的数据面顺序执行，最后停止 Registry 的后台 sweeper 和 Cluster transport。

## 数据流

注册写入首先更新内存视图，再以原子替换方式持久化 `api_config.json` 和 `service.json`。读取、预约、Relay 解析都使用同一个 `RegistryService`，避免不同数据面各自维护服务目录。

Relay 的目标选择顺序：

1. 从本地或集群副本中解析已注册 A2A Agent Card；
2. 拒绝非 A2A、未知或不健康目标；
3. 若有 Tunnel binding，走反向通道；否则直连 Agent Card URL；
4. 应用 body 上限、header 过滤、目标 allowlist 和 loop 检测。

Artifact Relay 与 Stream Proxy 不复用 A2A JSON 通道，分别执行配额、token、TTL、字节数和 SHA-256 校验。

## 兼容边界

Python import namespace `a2x_registry`、环境变量前缀 `A2X_*` 和旧 CLI 名称仅为兼容层。它们不表示仓库仍包含搜索、分类、向量、LLM 或 UI 功能。
