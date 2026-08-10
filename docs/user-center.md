# 用户中心与管理员操作

当前项目的用户主数据保存在 MySQL，Redis 只保存登录凭证、用户 Token 反向索引和在线路由，
Kafka 不承载用户主表。用户状态采用软删除，避免直接删除消息历史后留下孤儿引用。

## 数据模型

`user` 表新增字段：

| 字段 | 含义 |
|---|---|
| `status=1` | active，可登录、可建立 WebSocket、可发送消息 |
| `status=2` | disabled，账号被管理员临时禁用 |
| `status=3` | deleted，软删除，保留审计和历史引用 |
| `deleted_at` | 软删除时间，恢复为 active 时清空 |
| `updated_at` | 用户记录最后修改时间 |

`audit_log` 只追加用户生命周期事件，包含操作者、目标用户、动作、原因、元数据和时间。
启动 Logic 时会幂等补齐旧开发库字段；生产环境应执行
[`sql/migrations/001_user_center.sql`](../sql/migrations/001_user_center.sql) 并由迁移工具记录版本。

## 管理员配置

在 `.env.local` 中设置，两个变量都非空时才启用管理员登录：

```dotenv
SPARK_PUSH_ADMIN_ACCOUNT=admin
SPARK_PUSH_ADMIN_PASSWORD=请替换为本机强口令
```

登录获取管理员 Token：

```bash
curl -sS -X POST http://127.0.0.1:9101/api/login \
  -H 'Content-Type: application/json' \
  -d '{"account":"admin","password":"你的管理员口令"}'
```

后续请求携带：

```text
Authorization: Bearer <admin-token>
```

## 浏览器管理员控制台

本地 Demo 启动后直接访问：

```text
http://127.0.0.1:9010/admin_chatroom.html
```

普通聊天首页 `http://127.0.0.1:9010/index.html` 左侧也提供“管理员控制台”入口。管理员登录后
默认展示用户中心，可在左侧导航切换运行概览、用户中心、审计日志、房间与广播。控制台调用的
就是下方管理 API，用户列表中的“禁用 / 恢复 / 软删除 / 撤销 Token / 审计”按钮会真实修改或
查询后端数据；它不是静态演示页面。

## 管理 API

所有接口都要求 `POST` 和管理员 Bearer Token。

### 分页查询用户

```bash
curl -sS -X POST http://127.0.0.1:9101/api/admin/user/list \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"offset":0,"limit":20,"status":"active"}'
```

`status` 可选值为 `active`、`disabled`、`deleted`；省略表示全部状态。
响应只返回账号资料和状态，不返回 `password_hash`。

### 禁用、恢复和软删除

```bash
# 禁用：撤销全部 Token，并将 status 设为 disabled
curl -sS -X POST http://127.0.0.1:9101/api/admin/user/disable \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"user_id":1,"reason":"manual security review"}'

# 恢复
curl -sS -X POST http://127.0.0.1:9101/api/admin/user/restore \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"user_id":1,"reason":"review passed"}'

# 软删除：保留用户行、消息引用和审计记录，不物理删除历史消息
curl -sS -X POST http://127.0.0.1:9101/api/admin/user/delete \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"user_id":1,"reason":"account cancellation"}'
```

也可以调用 `/api/admin/user/status`，请求体传
`{"user_id":1,"status":"active|disabled|deleted","reason":"..."}`。

### 修改昵称和撤销 Token

```bash
curl -sS -X POST http://127.0.0.1:9101/api/admin/user/update \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"user_id":1,"name":"新的昵称"}'

curl -sS -X POST http://127.0.0.1:9101/api/admin/user/revoke_tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"user_id":1}'
```

每次登录都会维护 `user:tokens:<user_id>` 反向索引。批量撤销会删除该索引中的所有
`token:<token>`，并清理 `route:user:<user_id>`；对升级前没有反向索引的旧 Token 会用
Redis `SCAN` 兜底查找，避免旧会话逃逸。

### 查询审计日志

```bash
curl -sS -X POST http://127.0.0.1:9101/api/admin/audit/list \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"offset":0,"limit":50,"target_user_id":1}'
```

当前记录 `user.register`、`user.login`、`admin.login`、`user.disable`、
`user.restore`、`user.delete`、`user.update_profile` 和 `user.revoke_tokens`。

## 一致性边界

- 登录、WebSocket 握手和 Logic 上行消息都会检查 MySQL 用户状态；被禁用的用户不能重新鉴权或发送新消息。
- 禁用/删除先撤销 Token，再修改 MySQL 状态；Token 撤销失败时不会继续修改状态。
- 已建立的 WebSocket 连接不会依赖浏览器主动退出，后续上行请求会被状态检查拒绝；生产集群通常还会通过踢线 RPC 或 Redis Pub/Sub 立即关闭连接。
- 审计写入失败会记录 Logic 错误日志并在响应中返回 `audit_recorded=false`，生产环境应将审计库改为强一致事务或独立可靠日志管道。
- 账号默认不复用：软删除后仍保留 `account` 唯一索引，避免旧消息和风控记录指向新用户。
