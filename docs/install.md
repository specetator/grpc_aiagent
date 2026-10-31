# Docker 安装与本地开发

适用：单机 Linux 或启用 Docker 的 WSL2，3–10 人团队。基础 IM 不需要模型、Agent、
GPU，也不需要在主机安装 CMake、gRPC 或 Protobuf。

## 首次安装

```bash
git clone --depth 1 https://github.com/specetator/grpc_aiagent.git
cd grpc_aiagent
cp .env.example .env
docker compose up -d
docker compose ps
```

打开 `http://localhost:9010`。其他成员使用同一主机的局域网 IP，例如
`http://192.168.1.20:9010`。注册后创建私有协作空间并邀请已注册成员接受邀请。
首次安装使用发布镜像；镜像尚未发布或需要从本地源码构建时：

```bash
docker compose up -d --build
```

构建发生在多阶段容器内。首次下载耗时取决于网络；不能把本机已有镜像的启动时间
当作所有新用户的安装时间。建议至少 4 GiB 内存，源码构建建议 6 GiB 以上。

默认只发布 9010，Web、HTTP API 和 WebSocket 共用该入口。MySQL、Redis、Kafka、
内部 gRPC 和 metrics 不映射主机端口。公网使用应将 9010 放在 HTTPS 反向代理后；
浏览器通过 HTTPS 打开时使用同源 WSS。

`.env` 可修改 `SPARK_PUSH_PORT`、`SPARK_PUSH_BIND`。首次启动自动生成数据库
口令、RPC 角色密钥和内部 TLS 证书，保存在命名卷中。模板数据库密码占位符在
Docker 模式表示自动生成；后续启动保留已生成值。不要删除安全卷或随意更换
已有数据库的密码。管理员账号默认关闭，可在 `.env` 显式设置账号和密码。

## 状态、日志与停止

```bash
docker compose ps
docker compose logs --tail=100 -f logic comet job gateway
docker compose down           # 保留数据与密钥卷
docker compose up -d           # 恢复服务
```

`bootstrap`、`migrate` 和 `kafka-init` 成功退出是正常状态，常驻服务应为 healthy。
数据库、Redis、Kafka、附件和安全材料均使用命名卷；`down -v` 会删除这些数据，
仅在明确卸载或独立测试环境中使用。

## 数据库升级边界

更新版本前停止业务写入并备份，之后再运行新版本迁移与服务：

```bash
docker compose stop gateway logic comet job agent-bridge
# 在停机状态下备份所有命名卷，再更新源码或镜像版本。
docker compose pull
docker compose up -d
```

迁移会在应用启动前串行执行并校验已执行 SQL 的 checksum，不清理用户、消息、
任务或旧 Agent Session。当前使用 durable message reservations 的数据库可以
直接采用增量迁移。缺失分配器的早期库须先停止旧写入、排空旧 Kafka 并备份，
再临时设置 `SPARK_LEGACY_KAFKA_DRAINED=true`。损坏的主键、幂等键、关键类型或
字符集会阻止升级，需要先恢复正确数据结构，不会由启动脚本静默重建。

MySQL DDL 不提供整个升级事务回滚。失败的迁移不会标记成功，已完成的幂等 DDL
可在修复原因后重跑。安全卷与数据卷需要作为同一备份集合保存。

## 可选 Agent

基础服务默认关闭 Agent。管理员配置独立 Gateway 后，可通过 `agents` profile
运行 Bridge，并在 `.env` 中设置 `SPARK_PUSH_HERMES_ENABLED=true`、
`SPARK_PUSH_AGENT_GATEWAY_URL` 和匹配 Gateway 的 API key。不要填原始模型
Chat Completions 地址，因为那会绕过已有 AgentRouter 和 Runtime。

当前公共房间 Agent 配置参见 [通用 Room Agent](generic-room-agents.md)。
公共 Connector 的 Docker 连接方式见 [可选 Connector 部署](optional-agent-connector.md)。
v0.1.0 的正式公共 Runtime 启动器和完整多人浏览器验收仍在实施，以
[交付跟踪](v0.1.0-delivery.md) 为准，不以 Compose profile 的存在声称全部已验收。

## 保留 WSL 本地开发

原生开发继续使用 `scripts/sparkctl.sh`，其依赖配置已单独保存在
`docker-compose.dev.yml`。它保留原 `spark-*` 容器、目录项目名、卷名和
6379/3307/9092 端口，不接管新的 `sparkpush` Compose 数据。

```bash
cp .env.example .env.local
# 编辑 .env.local，填写开发数据库密码。
./scripts/sparkctl.sh doctor
SPARK_PUSH_BUILD_JOBS=2 ./scripts/sparkctl.sh up
./scripts/sparkctl.sh health
```

开发模式仍需 C++ 构建依赖，详见 [原生运行说明](../scripts/00_prepare_and_run.md)。
两种模式的 Web 默认都使用 9010，并行运行时为 Docker 设置其他 `SPARK_PUSH_PORT`。
