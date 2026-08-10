# 本地准备、启动与停止

这份说明以根目录当前源码为准。目标是执行一次启动脚本后，直接打开：

```text
http://127.0.0.1:9010/index.html
```

## 1. 前置条件

本机需要安装并运行：

- Docker / Docker Compose；
- CMake、C++17 编译器；
- `curl`（启动脚本用它确认 metrics 和 Job→Comet 长连接已经就绪）；
- gRPC、Protobuf、librdkafka、Redis、MySQL、Muduo 等构建依赖。

如果 Docker daemon 没有运行，先启动 Docker Desktop 或 `docker.service`。

## 2. 配置 MySQL 密码

首次使用时执行：

```bash
cd /home/peco/cppcode/fenbushi/11.2-spark_push
cp .env.example .env.local
${EDITOR:-vi} .env.local
```

至少填写：

```env
SPARK_PUSH_MYSQL_PASSWORD=本机MySQL_ROOT密码
```

如果 Docker 的 MySQL 数据卷已经初始化过，必须继续使用原来的 root 密码；不要只修改
`.env.local`。`.env.local` 已被 `.gitignore` 忽略，不会进入仓库。

也可以不创建 `.env.local`，直接从当前 shell 注入密码：

```bash
SPARK_PUSH_MYSQL_PASSWORD='本机MySQL_ROOT密码' \
  ./scripts/start_demo.sh
```

## 3. 一键启动

默认启动脚本会自动完成：

1. 启动 Redis、MySQL、Kafka 容器并等待健康检查；
2. 初始化 `push_single`、`push_group`、`broadcast_task`、`persist_message`、`ai_request`、`ai_delta`、`ai_reply` 等 topic；
3. 配置 CMake 并编译业务程序（包含可选 `hermes_bridge`）；
4. 清理当前项目的旧业务进程；
5. 启动 Logic、Comet、Job，等待 Job→Comet `PushStream` 真正建立；如果 Hermes 已启用，再启动 `hermes_bridge`；
6. 启动 WebDemo 并检查端口。

执行：

```bash
./scripts/start_demo.sh
```

启动成功后访问：

```text
http://127.0.0.1:9010/index.html
```

如果希望脚本一直保持前台运行，并在 Ctrl-C 时自动停止业务进程：

```bash
./scripts/start_demo.sh --foreground
```

在 IDE、自动化执行器或会回收后台子进程的环境中，推荐使用 `--foreground`。

常用选项：

```bash
./scripts/start_demo.sh --skip-build   # 已确认二进制最新时跳过编译
./scripts/start_demo.sh --skip-deps    # Redis/MySQL/Kafka 已由其他方式启动时
./scripts/start_demo.sh --help
```

## 4. 端口与健康检查

| 服务 | 端口 | 用途 |
|---|---:|---|
| Logic | 9100 | 内部 gRPC |
| Logic | 9101 | HTTP API / metrics |
| Comet | 9000 | WebSocket |
| Comet | 9105 | Job→Comet gRPC |
| Comet | 9203 | metrics |
| Job | 9202 | metrics |
| WebDemo | 9010 | 浏览器页面 |

```bash
curl http://127.0.0.1:9101/metrics
curl http://127.0.0.1:9202/metrics
curl http://127.0.0.1:9203/metrics
```

业务日志位于 `logs/logic.out`、`logs/comet.out`、`logs/job.out`、`logs/web.out`；启用 Hermes 后增加 `logs/hermes_bridge.out`；
PID 文件位于 `.run/`。

## 5. 浏览器体验

打开两个浏览器窗口或一个普通窗口加一个无痕窗口：

1. 分别注册两个用户并登录；
2. 记录页面显示的数字用户 ID；
3. 在一个窗口选择单聊，输入另一个用户的数字 ID；
4. 发送消息，观察 accepted / delivered 状态和对方窗口的真实收消息；
5. 需要测试群聊时访问 `http://127.0.0.1:9010/chatroom.html`。

管理员前端控制台访问：

```text
http://127.0.0.1:9010/admin_chatroom.html
```

也可以从 `index.html` 登录/注册面板下方的“管理员控制台”入口进入。管理员登录后默认进入
“用户中心”，可以在左侧切换运行概览、用户中心、审计日志、房间与广播；普通账号的登录态
和管理员登录态分别保存在浏览器本地存储中。

单聊目标必须填写数字用户 ID，不是账号名。

### 5.1 与 Windows Hermes 多轮对话

Windows 上 `F:\hermes` 的目录不会被虚拟机直接读取，必须先让 Hermes API Server
对虚拟机可达。在 Windows Hermes 配置中开启（配置项名称以当前 Hermes 版本为准）：

```dotenv
API_SERVER_ENABLED=true
API_SERVER_HOST=0.0.0.0
API_SERVER_PORT=8642
API_SERVER_KEY=随机长密钥
```

在 Windows 本机和当前虚拟机分别测试：

```powershell
curl.exe http://127.0.0.1:8642/v1/models `
  -H "Authorization: Bearer 随机长密钥"
```

```bash
curl --noproxy '*' http://host.docker.internal:8642/v1/models \
  -H 'Authorization: Bearer 随机长密钥'
```

虚拟机访问失败时，把 `host.docker.internal` 换成 Windows 主机在虚拟机网络中的 IP，
并检查 Windows 防火墙；只绑定 Windows `127.0.0.1` 时虚拟机通常访问不到。

在 `.env.local` 配置：

```env
SPARK_PUSH_HERMES_ENABLED=true
SPARK_PUSH_HERMES_BASE_URL=http://host.docker.internal:8642/v1
SPARK_PUSH_HERMES_API_KEY=与 API_SERVER_KEY 相同
SPARK_PUSH_HERMES_MODEL=hermes-agent
SPARK_PUSH_HERMES_STREAMING=true
```

重启脚本后，登录普通用户，点击单聊页的“与 Hermes Bot 对话”，固定机器人 ID 是
`900000000001`。默认调用流式 `/v1/chat/completions`：每轮把该单聊最近 50 条消息按
`msg_seq` 组装为 `messages`，SSE 文本增量发布到 `ai_delta` 并实时追加到当前气泡；完整
回答从 `ai_reply` 回来后仍走 `persist_message → push_single → Job → Comet`。最终回答
才分配正式 `msg_seq` 和写入历史，断线时由最终消息支持游标补偿和离线补推。

如需回退到完整 JSON 回复模式，设置 `SPARK_PUSH_HERMES_STREAMING=false`；其余
`ai_reply`、历史和离线语义不变。当前 Bridge 只转发文本 SSE，工具进度等非聊天事件会
被忽略。

验证两轮：发送“我叫 Alice”，再发送“我叫什么？”，然后刷新页面重新打开会话，确认
回答能使用第一轮上下文、历史消息仍在。观察：

```bash
tail -f logs/hermes_bridge.out logs/logic.out
curl -sS http://127.0.0.1:9101/metrics | grep hermes
```

完整网络、消息语义和故障排查见 [`../docs/hermes-integration.md`](../docs/hermes-integration.md)。

## 6. 用户中心与管理员操作

如果要验证管理员用户管理，先在 `.env.local` 中填写：

```env
SPARK_PUSH_ADMIN_ACCOUNT=admin
SPARK_PUSH_ADMIN_PASSWORD=本机管理员强口令
```

重启 Demo 后，可以直接打开
`http://127.0.0.1:9010/admin_chatroom.html` 使用管理员前端；也可以调用 `/api/login` 获取管理员 Token，再参考
[`docs/user-center.md`](../docs/user-center.md) 查询用户、禁用/恢复/软删除账号、撤销全部
Token 和查询审计日志。用户删除是软删除，不会物理删除 `message` 历史。

Logic 启动时会自动补齐旧数据库的 `user.status`、`user.deleted_at`、`user.updated_at` 和
`audit_log`；生产部署应显式执行并记录 `sql/migrations/001_user_center.sql`。

## 7. 独立 E2E 验证

先在浏览器注册两个账号，再执行：

```bash
./build/load_test/e2e_bench \
  --sender-account <sender> --sender-password '<password>' \
  --receiver-account <receiver> --receiver-password '<password>' \
  --connections 2 --messages-per-conn 10 --timeout-ms 15000
```

成功条件是：

```text
sent == accepted_ack == delivered_ack == delivered
send_failed == 0
ack_errors == 0
```

确认小规模通过后，再执行 `--connections 8 --messages-per-conn 100` 观察队列、尾延迟和
metrics。E2E 结束后不要立刻停止 Job，等待 `persist_message` consumer 追平，再检查 MySQL。

## 8. 停止服务

只停止当前启动的业务进程（包含已启用的 Hermes Bridge）：

```bash
./scripts/stop_demo.sh
```

同时停止 Redis、MySQL、Kafka，但保留数据卷：

```bash
./scripts/stop_demo.sh --with-deps
```

停止依赖时 `stop_demo.sh` 不会连接 MySQL；即使当前终端没有重新加载密码，也可以安全执行该命令。

不要使用 `docker compose down -v`，除非明确要删除本地数据库和 Redis 数据。

## 9. 常见问题

### `spark-kafka` 容器名称冲突

启动脚本会自动清理已停止且不属于当前 Compose 项目的旧同名容器，只删除容器本身，
不会删除数据卷。如果同名旧容器仍在运行，先手工停止它，再执行启动脚本。

### MySQL 登录失败

检查 `.env.local` 中的 `SPARK_PUSH_MYSQL_PASSWORD` 是否与已有 MySQL 数据卷初始化时的
root 密码一致。不要随意更换密码，也不要先执行清库脚本。

### 端口被占用

```bash
ss -ltnp | grep -E '9000|9010|9100|9101|9105|9202|9203'
./scripts/stop_demo.sh
```

### 页面能打开但消息发不出去

依次检查：

```bash
tail -f logs/logic.out logs/comet.out logs/job.out
curl http://127.0.0.1:9202/metrics | grep spark_push
curl http://127.0.0.1:9203/metrics | grep spark_push
```

重点确认 Job 已启动、Kafka topic 存在、发送目标填写的是数字用户 ID。
如果脚本提示 `Job→Comet PushStream` 未就绪，先查看 `logs/job.out`、`logs/comet.out`，
并确认 `curl http://127.0.0.1:9203/metrics` 中的
`spark_push_comet_push_stream_ready 1` 已出现。

如果只有 Hermes 没有回答，检查：

```bash
tail -n 80 logs/hermes_bridge.out
docker exec spark-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:29092 --list | grep -E 'ai_request|ai_delta|ai_reply'
```

先从虚拟机直接请求 Hermes `/v1/models`；本机 Windows 请求成功不代表虚拟机路由、
防火墙和代理配置已经正确。第一阶段 Bridge 只支持 `http://`；若最终回答正常但没有
逐字增量，检查 `ai_delta` topic、Logic 的 delta consumer，以及：

```bash
curl -sS http://127.0.0.1:9101/metrics | grep 'spark_push_hermes_stream'
```

体感延迟应拆成 accepted ACK、首个 `hermes_delta`、最终 `single_chat` 三段观察。若
accepted 很快而首个 delta 很慢，优先优化 Hermes 模型、推理/工具调用和上下文长度，
不要先调 Kafka 或 WebSocket 的限流参数。
