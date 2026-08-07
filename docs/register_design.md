# 注册中心设计

## 数据模型

一个 namespace 对应 `database/{name}/`，可包含：

```text
api_config.json        API 持久注册记录
user_config.json       运维人员维护的只读输入
service.json           合并后的查询输出
register_config.json   允许的服务类型和最低版本
auth_config.json       namespace 鉴权开关（可选）
lease_config.json      心跳参数（可选）
skills/                已上传 Skill（可选）
```

注册记录支持 `generic`、`a2a` 和 `skill`。`api_config.json` 是可写来源，`user_config.json` 是运维来源，`service.json` 是派生输出，不应手工修改。

## 启动流程

1. 枚举具有注册配置或服务文件的 namespace；
2. 读取 `user_config.json`、`api_config.json` 和 `skills/`；
3. 按 `register_config.json` 校验类型与版本；
4. 对 URL 型 A2A 记录抓取并校验 Agent Card；
5. 合并为内存 `_entries`，生成 `service.json` 和 `_output_cache`。

## 写入流程

注册、更新、注销都遵循相同顺序：

1. 完成鉴权、ownership、格式和心跳参数校验；
2. 在锁内提交内存记录；
3. 对持久记录原子更新 `api_config.json`；
4. 重新生成 `service.json`；
5. 触发 Cluster 复制回调。

文件写入使用临时文件加原子替换。预约锁保存在内存中，使用 monotonic time 计算 TTL，服务重启后不会恢复。

## 自动初始化

向不存在的 namespace 注册时，服务会创建目录和默认 `register_config.json`。默认允许三类记录的 `v0.0`。自动初始化不启用鉴权或心跳；这两项必须显式配置。

## 更新与删除

- `user_config` 记录不能通过 API 更新或注销；
- `skill_folder` 必须走 Skill 专用接口；
- 受保护 namespace 中，provider 只能修改自己的记录，admin 可管理全部记录；
- 删除 namespace 会清理内存缓存并删除该 namespace 目录。

## 查询与 Relay

列表接口支持字段过滤、分页、是否包含已预约记录和是否包含不健康记录。Relay 直接通过 `get_entry()` 解析目标，因此注册结果、健康状态和转发目标使用同一份事实来源。
