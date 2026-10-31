# 可选公共 Connector 部署

基础 `docker-compose.yml` 独立提供 IM；公共 Agent 使用额外的
`docker-compose.agents.yml` 和宿主机上的独立 Connector。这里描述部署连接边界。
公共启动器复用现有 Router、Store、Scheduler 和原生 Runtime。不能直接将私人
Gateway、Hermes technical Profile 或个人工作目录当作公共 Connector。
完整浏览器闭环和发行验收仍单独进行。

## 配置与端口

先准备 operator 管理的文件：

```bash
cp deploy/agents/agents.json.example deploy/agents/agents.json
cp deploy/agents/projects.json.example deploy/agents/projects.json
```

示例启用公共 Pi，同时预留禁用的 Hermes 身份，没有默认项目目录。项目配置格式参见
[授权工作区](room-workspaces.md)；Logic 只返回项目 ID 和名称，源目录只供受信
Connector 创建快照。配置目录只读挂载，可用 `.env` 的
`SPARK_PUSH_AGENT_CONFIG_DIR` 指向其他目录。普通房间成员不能编辑此目录。
Catalog 的 bot ID 必须独立且未被真人账号占用；冲突会关闭本次公共 Agent 配置，
不会覆盖真人账号。正式部署应预先配置计划使用的 Agent 身份，后续不复用已有 ID。
首次启用或修改会替换 Logic 的配置，需要维护窗口。已有 IM 先用正在运行的配置
执行 `docker compose down --remove-orphans`，不使用 `-v`；再用完整配置冷启动。
热替换 Logic 曾出现 DNS 空窗和 Comet 重连未就绪，当前不承诺无中断配置切换。

```bash
docker compose -f docker-compose.yml -f docker-compose.agents.yml \
  --profile agents up -d --wait
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

## 独立公共启动器

以专用服务账号运行 Connector，使用新的绝对目录。不要把 `home` 指向个人 Pi、
Hermes、项目根目录或已有非空目录。首次启动只初始化空目录；后续要求匹配服务
身份的 marker，并持有独占进程锁。已有配置不会自动覆盖，重复进程会被拒绝。
服务目录、显式模型凭据和导出的 Connector key 应仅供该服务账号读取。

复制 `deploy/agents/public-gateway.json.example` 到私有配置目录，填写已安装 Pi 的
绝对可执行路径、批准的 provider/model、显式模型 JSON 和模型环境文件。模型
JSON 使用现有 Pi 格式；启动器只复制选中的 provider/model，拒绝 `!command`
动态凭据。模型环境文件仅读取支持的 API key 名称，不继承私人进程环境。
配置中 `registry` 必须与 Logic 挂载的 Catalog 为同一份 operator 配置。

```bash
python3 cannbot/scripts/public_agent_gateway.py --config /srv/spark-public/public-gateway.json
```

公共 Pi Gateway 当前仅支持 Linux；停机依赖原生进程组及 `/proc` 检查。
此入口清理环境、固定单 worker，创建无工具的独立 Pi prototype，再调用已有
`load_router` / `make_handler`。每个房间/Agent/授权代次使用独立原生 session。
公共请求目前只接受 Logic 构造的文本上下文；图片没有进入公共 Agent 协议。
未绑定项目时也检查当前成员权。绑定项目后，只能通过 guarded read 读取该次
请求的不可变快照；不开放 shell、写入、全局扩展或私人 Skills。

Hermes 另用 `public-hermes.json.example`，在已安装的 Native Python 环境中运行：

```bash
/opt/hermes-agent/venv/bin/python cannbot/scripts/hermes_public_gateway.py \
  --config /srv/spark-public/public-hermes.json
```

Native API key 由 operator 单独生成：`api_key_file` 存放原始 key，Catalog 的
`credential_env_file` 存放同一值的 `API_SERVER_KEY=...`。不要使用 Connector
授权 key 或登录 token 代替。Native 仅监听受信回环接口；Gateway 的监听接口
需可被 Docker 访问，但应限制在受信主机网络。没有模型配置时基础 IM 仍可运行。

启动器只创建独立的 `spark-public-*` Profile，模型配置显式给出。可选
`model_auth_file` 仅用于 operator 明确准备的专用原生登录状态，首次复制到本服务
home；不发现或复制默认个人 Profile。它关闭源码目录 `.env` 自动加载及外部
secret source，拒绝继承其他 global profile root，也拒绝 home 内隐式 `.env` /
`.op.env`，统一使用显式模型环境文件。配置校验、原生工具预热和路由检查通过
后才输出 ready。

Native 只开放需要的 Runs、session 创建/查询、状态、事件、停止及审批接口。
`/p/...` Profile 切换、chat/responses、浏览器、插件路由、任意 session 配置与
工具参数入口均拒绝。工具实际列表必须限制为 `workspace_read` / `todo_list`，
Memory、用户 Profile、MCP、工具搜索与 checkpoints 关闭。原生能力不匹配时
启动或调用失败，不回退到私人服务。

Linux Connector 负责创建快照。Native 仅接收 snapshot root；部署时应给它该
目录的只读权限或只读挂载，独立 state home 可写。当前 Windows `/mnt/f` 的
真实测试证明跨系统的读取与消息链路，**不证明 OS 只读沙箱或 Windows junction
防护**。不要据此开放不受控工具执行。公共工具边界和 OS 沙箱是两种不同证据。

确认隔离 Native 可用后，再将 Catalog 中 `public-hermes.room_enabled` 改为
`true` 并在维护窗口内重启公共 Gateway、冷启动 IM 栈。正式公共合同要求 PUBLIC 开关、
`room_public_profile=true`、`spark-public-*`、workspace contract 1 和禁止交互式
工具扩权；原有 `spark-room-test-*` 仍使用独立 TEST 门禁。

## 停机与维护锁

Linux 公共 Gateway 收到 SIGTERM/SIGINT 后停止接收请求，等待正在处理的 HTTP
请求完成，再回收 Router 和 Pi 进程组。公共 worker 回收失败或无法检查进程状态时，
服务目录会保留未确认标记，不能把释放锁当作数据已静止。私有 Pi/CANN 不启用此公共维护检查。

两个启动器也支持 `--control-stdin`：由进程管理器保持 stdin 管道，写入一行
`shutdown`，或关闭该管道，即开始受控停机。Windows Native 应使用这个入口，
不要用强制终止冒充正常停机；Native 退出前会完成其原生 adapter 清理。
停机可能等待当前请求结束，进程管理器须留出相应时间。

由同一服务账号、同一原生操作系统和进程命名空间检查已停止的公共服务：

```bash
python3 cannbot/scripts/public_runtime_maintenance.py check \
  --runtime public_gateway --config /srv/spark-public/public-gateway.json
# Native 必须使用实际运行它的 Python；Windows 路径也由 Windows Python 解释。
/opt/hermes-agent/venv/bin/python cannbot/scripts/public_runtime_maintenance.py check \
  --runtime hermes_native --config /srv/spark-public/public-hermes.json
```

`check` 仅证明检查时刻已停止。自动维护协调器使用 `freeze --batch-id <32位小写十六进制>`，
读到 `state=frozen` 后必须保持进程和 stdin；原生排他锁阻止 Runtime 重启。
只有向 stdin 写入匹配的 `{"operation":"release","batch_id":"..."}` 后才释放。
EOF、错误批次或 helper 退出都是失败，不能继续声称持有维护锁。
此命令还不归档文件，也不替代 IM 与 Runtime 的整批备份恢复。

强制退出、清理失败或旧 home 缺少生命周期记录时，下一次普通启动不会抹掉未确认状态。
现有业务会话仍可继续使用，但冷备检查会拒绝。当前恢复维护资格的方法是：先安排真正的
原生 OS 重启，待新进程停止后显式执行同一命令的 `audit-after-reboot` 操作。
它核对原机器、boot 和进程身份，仅更新维护记录，不修改任务、账本、Session 或 unknown 状态。
本工具不会重启主机；Windows 必须有 Windows 重启证据，重启 WSL 不等价。
跨机器搬迁和不同 PID namespace 的同 boot 操作不支持，不能删除状态文件绕过检查。

`tests/public_runtime_lifecycle_e2e.py --out /tmp/public-runtime-lifecycle.json` 使用真实
Linux 进程验证锁、停机、强杀残留和冻结，不调用模型。可选 `--windows-python`
验证 Windows 原生锁；再加 `--native-live` 验证本地隔离 Native API 启动和停机。
OS 重启成功路径目前只有契约测试，没有实际重启主机的验收。
2026-11-01 原生生命周期 9 项及真实模型链路回归 10 项通过，详见
[停机与冻结验收回执](benchmarks/2026-11-01-runtime-shutdown.json)。

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
仅因配置错误被持久标记为取消。修正 Catalog、项目列表或升级时，安排维护窗口，
保留卷并冷启动整栈，避免只替换 Logic 后旧 gRPC 客户端仍未就绪：

```bash
docker compose -f docker-compose.yml -f docker-compose.agents.yml \
  --profile agents down --remove-orphans
docker compose -f docker-compose.yml -f docker-compose.agents.yml \
  --profile agents up -d --wait
```

更改 Gateway origin/key 时，按上述维护步骤重新启动整栈，并同步更新、
重启独立 Connector；更改 workspace authorization key 时也须更新使用它的
Connector/Native 服务。导出新凭据到新的私有文件后再交给运行时，不能认为重建
Logic 会自动更新宿主进程。当前密钥覆盖回归验证 Logic 与导出值一致，未将其
宣称为 Bridge/Gateway 同步轮换的 E2E。

停用 Agent 时保留原 Catalog 身份并设置 `room_enabled=false`。已完成请求在
验证原始输入后返回原结果，避免撤权后重投改变发布内容；Logic 的持久化发布
决定仍控制最终结果。新请求和未知结果恢复继续检查当前授权，不重新执行未知
工具。删除整个 Catalog 条目、改动稳定 Agent ID/Profile/endpoint 不属于这种
终态重放兼容操作。不要清除 Router ledger、Native run journal 或 session 文件。

Native 最终回复得到 Kafka 确认后，Bridge 携带原房间 scope 提交专用
`confirm_reply` 回执。Gateway 只允许这一种内部房间确认；它核对 completed turn
与 Native journal 的 session、Profile、endpoint 和 run ID，再持久写入
`broker_confirmed`。撤权、停用或 Native 停机不妨碍确认已有结果；确认不会执行
模型、读取凭据或生成聊天消息。用户的房间控制 API 不接受此内部操作。

已完成 turn 重放保留原业务正文和元数据；Bridge 重新传输时，外层事件的
`sequence`、派生 `event_id`、`replayed` 和生成时间可以变化。发布摘要 v2 仅在
核验投影 ID 后忽略这四个传输字段，仍比较持久正文、身份、执行状态及代码快照。
Logic 返回第一次保存的完整 publication，保留消息原时间戳和内容，数据库不重写
旧决定。旧成功记录只有在重新生成的旧摘要与数据库原摘要一致时，才允许这种
兼容比较；已剥离原内容的旧取消记录只支持旧摘要精确重放。无法证明一致的旧
取消变体继续进入失败处理，需用保留的原始 `ai_reply` 核实，不能盲目重执行工具。

数据库或 Kafka 核心故障仍按原可靠性门禁处理。可选 Agent 故障隔离不代表
基础存储不可用时仍可收发消息。

验证脚本：`tests/connector_deployment_e2e.py`。它运行真实 Compose、MySQL、Redis、
Kafka、Logic 和 WebSocket，验证部署、密钥和对象授权边界；不调用模型，不能
替代真实 Pi/Hermes 文件读取验收。

`tests/public_room_runtime_e2e.py` 使用正式启动器、真实 Compose、Pi 与隔离的
Windows Hermes 模型；三个独立 HTTP/WS 客户端验证实际文件读取、消息归属、
任务、重投和持久恢复。它不等于三个独立浏览器验收，也不替代 CANN/私人 Agent
及未知执行故障恢复的专项测试。
2026-10-31 这条真实链路的 10 项检查已通过；镜像、源码指纹、重投 offset 与
限制见 [公共 Runtime 回执](benchmarks/2026-10-31-public-runtime.json)。

追加跨房间专项使用同一条真实链路：

```bash
python3 tests/public_room_runtime_e2e.py --cross-room \
  --image spark-push:team-backup --out /tmp/spark-cross-room.json
```

`--image` 应指向本机已构建的待验镜像。该选项增加第二个私有空间和独立批准项目，
检查两种 Agent 的原生 Session、真实读取回执、服务端请求上下文及对象权限；同时
要求普通聊天在 Pi 实际执行期间完成投递，并验证两个同房间 Pi 请求最终按序返回。
第二个请求先被受理并不等同于已观测到 Runtime Scheduler 排队；回执分别记录这些边界。
它需要已安装的 Pi、Windows Hermes 和专用测试模型凭据，不能在没有真实 Runtime
的机器上以 Mock 替代。详细夹具前提与原有测试相同，三浏览器及私人 Agent/CANN
仍需各自的验收结果。
跨房间与 `--team-backup` 场景分别运行；当前不支持组合这两个选项。

2026-11-01 最终真实运行的 16 项检查通过，详见
[两空间回执](benchmarks/2026-11-01-public-cross-room.json)。它包括消费进度、原生
记录指纹、六次请求到四个会话的映射以及实际 HTTP/WS 拒绝；首次调试运行和随后
主动中止的运行分别保留，未用于替代最终结果。此处不宣称 Scheduler 排队公平性、
Native 跨房并行或任意工具副作用已完成验收。

2026-10-31 当前镜像的 16 项部署检查与 11 项基础 IM 回归结果记录于
[阶段回执](benchmarks/2026-10-31-optional-connector.json)，含源文件指纹、镜像 ID
与未验证项。这里的阶段通过不代表 v0.1.0 已完成发行验收。
