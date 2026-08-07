# 鉴权设计

鉴权是可选模块。未初始化时，默认 namespace 保持匿名兼容行为；初始化后可按 namespace 开启强制鉴权。

## 角色

| 角色 | 权限 |
| --- | --- |
| `admin` | 管理 namespace、principal、key 和任意服务 |
| `provider` | 在授权 namespace 中注册并维护自己的服务 |
| `user` | 在授权 namespace 中读取服务和创建预约，不可注册服务 |

非 admin principal 必须绑定一个或多个已存在 namespace。服务记录在受保护 namespace 中会保存 `owner_id`；非 admin 只能修改自己的记录。

## 初始化

```bash
agent-registry auth init
```

命令创建首个 admin principal/key，并只显示一次 plaintext token。服务端仅保存 token hash。token 应通过密码管理器或 Secret Manager 分发。

## 请求鉴权

客户端使用：

```http
Authorization: Bearer <token>
```

验证流程：token hash 查找 key → 检查 key/principal 是否禁用 → 检查角色和 namespace scope → 生成 `AuthContext`。

## API

前缀 `/api/auth`：

- `GET /whoami`
- `POST|GET /principals`
- `GET|PATCH /principals/{principal_id}`
- `POST|GET /keys`
- `DELETE /keys/{key_id}`

创建或轮换 key 的 plaintext 只在创建响应中出现一次。

## namespace 行为

- `auth_required=false`：注册、读取和管理保持匿名兼容；
- `auth_required=true`：所有服务读写先校验 namespace scope；
- dataset-level 操作（删除 namespace、修改 auth/register/lease 配置）只允许 admin；
- provider 只能维护自己的记录；
- user 只能读取和预约。

审计日志记录 principal/key 管理、鉴权失败和权限拒绝，不记录 plaintext token。
