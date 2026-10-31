# 可选公共 Connector 部署

基础 `docker-compose.yml` 独立提供 IM；公共 Agent 使用额外的
`docker-compose.agents.yml` 和宿主机上的独立 Connector。这里描述部署连接边界。
正式公共 Pi/Hermes 启动器和完整浏览器闭环尚未交付；不能直接将私人 Gateway、
Hermes technical Profile 或个人工作目录当作公共 Connector。

## 配置与端口

先准备 operator 管理的文件：

```bash
cp deploy/agents/agents.json.example deploy/agents/agents.json
cp deploy/agents/projects.json.example deploy/agents/projects.json
```

示例只允许公共 Pi，没有默认项目目录。项目配置格式参见
[授权工作区](room-workspaces.md)；Logic 只返回项目 ID 和名称，源目录只供受信
Connector 创建快照。配置目录只读挂载，可用 `.env` 的
`SPARK_PUSH_AGENT_CONFIG_DIR` 指向其他目录。普通房间成员不能编辑此目录。
Catalog 的 bot ID 必须独立且未被真人账号占用；冲突会关闭本次公共 Agent 配置，
不会覆盖真人账号。正式部署应预先配置计划使用的 Agent 身份，后续不复用已有 ID。

```bash
docker compose -f docker-compose.yml -f docker-compose.agents.yml \
  --profile agents up -d
```

- 用户仍通过 9010 使用 Web、API 和 WebSocket。
- `SPARK_PUSH_AGENT_GATEWAY_ORIGIN` 默认是 `http://host.docker.internal:8643`。
  Logic 控制接口和 Bridge 推理接口使用同一 origin；后者追加 `/v1`。
  Connector 须监听 Docker 能到达的宿主机接口，宿主 `127.0.0.1` 并非容器回环。
- 授权回连默认只发布 `127.0.0.1:9011`。Connector 的
  `SPARK_WORKSPACE_AUTH_ORIGIN` 应为 `http://127.0.0.1:9011`。
  此入口只代理 `POST /internal/room-agent/authorize`；其他 API、metrics 和方法返回 404。
- Windows Native 或其他受信宿主需要回连时，可显式设置
  `SPARK_PUSH_AGENT_AUTH_BIND`、`SPARK_PUSH_AGENT_AUTH_PORT` 为其可达接口。
  Logic 仍校验专用 Bearer；网络可达本身不授予房间访问权。

两个 origin 由 operator 配置，不能由聊天正文或用户 API 改写。

## 专用凭据

首次 bootstrap 生成持久的 Gateway 和工作区授权密钥。旧安装升级只补充缺少的
工作区密钥，不轮换已有值、不删除未知字段。已有但无效的值不会静默替换。
Compose 中空密钥使用安全卷中的值；非空显式覆盖必须同步到 Connector。
Docker 的两个 Connector key 要求 32–256 个可打印 ASCII 字符（不含空格）。
损坏的 NUL、Unicode 或类型错误值会关闭可选 Agent 配置，仍保留原值供管理员
修复；基础 IM 不因此退出。原 WSL 启动脚本及个人配置未改动。

从运行中的 Logic 导出仅包含这两个密钥的 JSON 文件：

```bash
python3 scripts/ops/export-agent-credentials.py /absolute/private/connector-keys.json -- \
  -f docker-compose.yml -f docker-compose.agents.yml --profile agents
```

父目录须已存在且仅供管理员使用；输出权限为 `0600`，已有文件不会被覆盖。
导出不包含数据库口令、RPC 密钥或 TLS 私钥，也不打印凭据。Gateway key 不能
代替 workspace key 调用授权回连。该 JSON 是数据文件，不能作为 shell 脚本 source。

## 故障与恢复

Catalog、项目、授权 key、Gateway origin 或公共 schema 校验失败时，Logic
关闭本次公共 Agent 配置并记录出错阶段，普通 IM 继续启动。成员的 Agent 管理
接口返回 503；非成员仍被拒绝。运行中的 Connector 离线不会阻止普通消息的
Kafka ACK、历史或补推。

配置错误不删除旧任务、绑定、请求或发布决定；排队请求保留，未发布结果不会
仅因配置错误被持久标记为取消。只修正 Catalog 或项目列表时重新创建 Logic：

```bash
docker compose -f docker-compose.yml -f docker-compose.agents.yml \
  --profile agents up -d --no-deps --force-recreate logic
```

更改 Gateway origin/key 时，同时重新创建 `logic agent-bridge`，并同步更新、
重启独立 Connector；更改 workspace authorization key 时也须更新使用它的
Connector/Native 服务。导出新凭据到新的私有文件后再交给运行时，不能认为重建
Logic 会自动更新宿主进程。当前密钥覆盖回归验证 Logic 与导出值一致，未将其
宣称为 Bridge/Gateway 同步轮换的 E2E。

数据库或 Kafka 核心故障仍按原可靠性门禁处理。可选 Agent 故障隔离不代表
基础存储不可用时仍可收发消息。

验证脚本：`tests/connector_deployment_e2e.py`。它运行真实 Compose、MySQL、Redis、
Kafka、Logic 和 WebSocket，验证部署、密钥和对象授权边界；不调用模型，不能
替代真实 Pi/Hermes 文件读取验收。

2026-10-31 当前镜像的 16 项部署检查与 11 项基础 IM 回归结果记录于
[阶段回执](benchmarks/2026-10-31-optional-connector.json)，含源文件指纹、镜像 ID
与未验证项。这里的阶段通过不代表 v0.1.0 已完成发行验收。
