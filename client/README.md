# Agent Registry Client SDK

注册中心的独立 Python SDK。它只依赖 `httpx`，提供同步与异步接口，并支持鉴权、ownership、心跳和预约锁。

## 安装

```bash
pip install "agent-registry-client @ git+https://github.com/kwistzzqq-byte/agent-registry-relay.git@main#subdirectory=client"
```

Python import namespace 和类名暂时保留兼容名称：

```python
from a2x_registry_client import A2XRegistryClient, AsyncA2XRegistryClient
```

## 登录

```bash
agent-registry-client login
```

CLI 将 `base_url` 和 token 写入 `~/.a2x_registry_client/cli_token.json`。也可以在构造客户端时显式传入：

```python
client = A2XRegistryClient(
    base_url="https://registry.example.com",
    api_key="<registry-token>",
)
```

不要把真实 token 写进代码、README、`.env.example` 或 Git 历史。

## 创建 namespace

```python
admin = A2XRegistryClient()
created = admin.create_dataset(
    "team",
    formats={"a2a": "v0.0", "generic": "v0.0", "skill": "v0.0"},
    auth_required=True,
    lease_config={
        "enabled": True,
        "min_ttl": 10,
        "max_ttl": 600,
        "grace_period": 60,
    },
)
```

`create_dataset` 只接受注册中心配置，不包含 embedding、搜索或分类参数。

## 注册与发现

```python
provider = A2XRegistryClient()

registered = provider.register_agent(
    "team",
    {
        "name": "weather-agent",
        "description": "Weather service",
        "url": "https://agent.example.com",
        "protocolVersion": "0.0.1",
        "version": "1.0",
        "capabilities": {},
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": [],
    },
    lease_ttl=60,
    auto_renew=True,
)

services = provider.list_agents("team", status="online")
detail = provider.get_agent("team", registered.service_id)
```

客户端会把自己持久注册的 sid 保存到 ownership store，在更新或注销前本地 fail-fast。受保护 namespace 中，服务端仍会再次检查 owner。

## 更新与注销

```python
provider.update_agent("team", registered.service_id, {"status": "busy"})
provider.deregister_agent("team", registered.service_id)
provider.close()
```

## 预约锁

```python
user = A2XRegistryClient(ownership_file=False)

with user.reserve_agents(
    "team",
    filters={"status": "online"},
    n=1,
    ttl_seconds=30,
) as reservation:
    for service in reservation.services:
        print(service["id"])
```

预约使用短期内存 lease；上下文退出会 best-effort 释放。服务重启会清空预约。

## 管理 principal

```python
principal = admin.create_principal(
    "weather-provider",
    "provider",
    namespaces=["team"],
)
print(principal.token)  # plaintext 只在这次响应中出现
```

请立即把 token 存入密码管理器并从终端历史中清理。

## 异步客户端

异步客户端与同步客户端方法对称：

```python
import asyncio
from a2x_registry_client import AsyncA2XRegistryClient


async def main():
    async with AsyncA2XRegistryClient() as client:
        services = await client.list_agents("team")
        print(services)


asyncio.run(main())
```

## 主要异常

- `A2XConnectionError`：连接或超时；
- `A2XAuthenticationError`：token 缺失、无效或已撤销；
- `A2XAuthorizationError`：角色、scope 或 owner 不匹配；
- `NotFoundError`：namespace 或服务不存在；
- `ValidationError`：注册格式或心跳参数无效；
- `NotOwnedError`：本地 ownership 检查失败；
- `ServerError`：其他服务端错误。

这些 `A2X*` 类型名同样是兼容 API，不代表 SDK 包含 A2X 搜索功能。

## 测试

```bash
uv run --project .. --extra dev python -m pytest -q tests
```
