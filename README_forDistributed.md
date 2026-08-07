# Agent 注册中心分布式部署

Cluster 模块用于多个 Registry 节点之间复制服务记录、成员关系和墓碑。它默认关闭，单节点部署不需要任何 Cluster 配置。

## 准备

每个节点都安装相同版本：

```bash
git clone https://github.com/kwistzzqq-byte/agent-registry-relay.git
cd agent-registry-relay
pip install -e .
```

为每个节点准备独立的 `A2X_REGISTRY_HOME` 持久化目录，并确保节点间 Registry HTTP 地址互相可达。

## 初始化节点

节点 A：

```bash
export A2X_REGISTRY_HOME=/var/lib/agent-registry/a
export A2X_REGISTRY_CLUSTER_ADVERTISE=http://registry-a.example.com:8000
agent-registry cluster init --node-id A
agent-registry --host 0.0.0.0 --port 8000
```

节点 B、C 使用各自的数据目录、advertise 地址和 node id 重复上述步骤。

`A2X_REGISTRY_CLUSTER_ADVERTISE` 必须是其他节点实际可访问的 base URL；跨主机部署不能使用 `127.0.0.1`。

## 建立成员集

在 A 上添加 B、C：

```bash
agent-registry cluster set add \
  http://registry-b.example.com:8000 \
  http://registry-c.example.com:8000
```

检查：

```bash
agent-registry cluster set show --server http://registry-a.example.com:8000
agent-registry cluster status --server http://registry-a.example.com:8000
```

Cluster 会持久化 cluster id 和成员集。节点重启后按持久成员表自动重连，不需要重复 `set add`。

## 鉴权

如果 Registry 已启用鉴权，Cluster 管理命令必须使用 admin token：

```bash
agent-registry cluster set add http://registry-b.example.com:8000 \
  --token '<admin-token>'
```

不要把 token 写进文档、shell script 或仓库。生产环境通过 Secret Manager 或受限环境变量注入。

## 失活与反熵

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `A2X_REGISTRY_CLUSTER_HOLD_TIMEOUT` | `30` | 直链静默多久后驱逐 |
| `A2X_REGISTRY_CLUSTER_KEEPALIVE_INTERVAL` | `10` | 保活周期 |
| `A2X_REGISTRY_CLUSTER_ANTI_ENTROPY_INTERVAL` | `20` | 反熵和 GC 周期 |
| `A2X_REGISTRY_CLUSTER_HTTP_TIMEOUT` | `5` | 节点调用超时 |
| `A2X_REGISTRY_CLUSTER_BROADCAST_WORKERS` | `32` | 广播并发上限 |
| `A2X_REGISTRY_CLUSTER_MERKLE_BUCKETS` | `256` | Merkle 桶数，所有节点必须一致 |

主动移除成员：

```bash
agent-registry cluster set remove <node_id>
```

主动移除会立即更新成员集；网络失活则由 keepalive/hold timeout 驱动。

## 验证复制

1. 在 A 创建 namespace 并注册一个服务；
2. 在 B、C 的 `/api/datasets/{dataset}/services` 中确认该服务出现；
3. 停止 C，等待超过 hold timeout，确认 C 的记录按协议处理；
4. 重启 C，确认它自动重连并通过反熵恢复状态；
5. 在任一节点注销服务，确认墓碑传播，旧副本不会复活。

更详细的协议、Merkle 对账和成员集时序见 [`docs/cluster_design.md`](docs/cluster_design.md)。
