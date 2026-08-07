# 心跳与健康状态

心跳按 namespace 显式启用。默认关闭，因此未声明 `lease_ttl` 的旧服务仍是永久记录。

## 配置

```json
{
  "enabled": true,
  "min_ttl": 10,
  "max_ttl": 600,
  "grace_period": 60
}
```

- `min_ttl` / `max_ttl` 限制注册时的 `lease_ttl`；
- `grace_period` 是服务失联后从 unhealthy 到硬删除的宽限期；
- 关闭心跳的 namespace 不接受带 `lease_ttl` 的注册请求。

## 状态流

```text
registered/healthy
        |
        | TTL 到期
        v
    unhealthy
        |
        | grace 到期
        v
   deregistered
```

在 unhealthy 阶段，默认列表查询和 Relay 都会排除该服务；显式 `include_unhealthy=true` 可用于诊断。新的合法 heartbeat 会恢复 healthy。

## 接口

- `POST /api/datasets/{dataset}/services/{service_id}/heartbeat`
- `DELETE /api/datasets/{dataset}/services/{service_id}/heartbeat`
- `GET|POST /api/datasets/{dataset}/lease-config`

受保护 namespace 中，heartbeat 必须由 owner 或 admin 发送。

## 重启恢复

`lease_ttl` 会随服务记录持久化，但倒计时只存在内存。服务启动时为这些记录重建一个 grace 窗口，客户端必须在窗口内重新发送 heartbeat，否则 sweeper 会走正常注销路径删除记录。

后台 sweeper 使用 monotonic time，避免系统时钟跳变破坏 TTL。硬删除复用 `RegistryService.deregister()`，因此持久化和集群复制仍保持一致。
