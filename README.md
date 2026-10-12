# Spark Push

C++17 分布式实时消息 / IM 项目，提供 WebSocket 接入、gRPC 双向流、Kafka 投递、MySQL 历史与 SQL 幂等/outbox、Redis 路由缓存，以及 Web 和原生 Android 客户端。Pi Agent 和 GPU 推理为可选扩展。

## 快速开始

基础 IM 可独立运行，不需要模型 API、Pi Agent、CANN 知识库或 GPU。
目前构建与 CI 验证环境为 Ubuntu 22.04 / Linux；Windows 用户可在 WSL2 中运行服务。

### 1. 准备依赖

先安装并启动 Docker Engine 和 Docker Compose v2。Ubuntu 22.04 构建依赖：

```bash
sudo apt-get update
sudo apt-get install -y git build-essential cmake pkg-config curl python3 \
  libssl-dev libcurl4-openssl-dev default-libmysqlclient-dev libhiredis-dev \
  librdkafka-dev libprotobuf-dev protobuf-compiler protobuf-compiler-grpc \
  libgrpc++-dev nlohmann-json3-dev bubblewrap util-linux

docker info
docker compose version
```

Muduo 和 JSON 头文件已包含在源码中，不使用仓库预编译库。Android、模型 runtime 和 Agent 依赖不属于基础 IM 启动条件。

### 2. 克隆、配置与启动

```bash
git clone https://github.com/specetator/grpc_aiagent.git
cd grpc_aiagent
cp .env.example .env.local
${EDITOR:-vi} .env.local
# 将 SPARK_PUSH_MYSQL_PASSWORD 改为本机密码；首次测试保持 Agent 开关为 false。

./scripts/sparkctl.sh doctor
SPARK_PUSH_BUILD_JOBS=2 ./scripts/sparkctl.sh up
./scripts/sparkctl.sh health
```

首次启动会创建 Docker Redis/MySQL/Kafka、初始化 SQL 和 topics、从源码编译服务、生成本机 RPC 密钥并检查双向流就绪。
打开 `http://127.0.0.1:9010/index.html`，注册两个账号，用两个浏览器窗口登录后互发消息。
远程设备使用服务主机的可达 IP；外网部署需自行配置 HTTPS/WSS、访问控制与数据备份。

`.env.local` 和自动生成的 `.env.rpc` 均不提交。管理员功能由 `SPARK_PUSH_ADMIN_ACCOUNT` / `SPARK_PUSH_ADMIN_PASSWORD` 显式启用。
已初始化的 MySQL 卷必须继续使用原密码。已有旧版部署须先排空消息并执行 [升级说明](docs/review-remediation.md)，首次空库使用 `sql/schema.sql`，不执行开发清库脚本。

```bash
./scripts/sparkctl.sh status
./scripts/sparkctl.sh logs all -f
./scripts/sparkctl.sh down               # 停业务，保留 Docker 依赖
./scripts/sparkctl.sh down --with-deps   # 停业务和依赖，保留数据卷
```

详细参数和排障见 [运行说明](scripts/00_prepare_and_run.md)。

## 架构与目录

```text
Client ──WebSocket──► Comet ──MessageStream──► Logic
  ▲                     ▲                       ├─ Redis 路由 / token / 缓存
  │                     │                       ├─ MySQL 序号 / reservation / outbox
  │                     │                       └─ Kafka persist / push / broadcast
  │                     └──── PushStream ◄──── Job ◄────┘
  └──────────────────── 下推 / ACK / 缺口与离线补偿
```

| 目录 | 用途 |
|---|---|
| `comet/` | WebSocket、连接管理、上行流、下推与恢复 |
| `logic/` | HTTP/gRPC adapter、Service、Repository、权限与消息编排 |
| `job/` | Kafka 消费、PushStream、幂等落库和 offset |
| `common/`、`proto/` | 配置、池、队列、鉴权、协议 |
| `web_demo/`、`android-app/` | Web / Kotlin Compose 客户端 |
| `hermes_bridge/`、`cannbot/`、`inference/` | 可选 Agent、知识和推理扩展 |
| `conf/`、`sql/`、`scripts/` | 配置、schema/迁移、启动运维 |
| `tests/`、`load_test/`、`docs/` | 回归、压测、运行与算法文档 |
| `muduo/`、`third_party/` | 构建所需的第三方源码 |

| 服务 | 默认端口 |
|---|---|
| Logic | HTTP 9101、内部 gRPC 9100 |
| Comet | WebSocket 9000、内部 gRPC 9105、metrics 9203 |
| Job | metrics/readiness 9202 |
| Web | HTTP 9010 |

旧教学版本、课件、迁移备份及未使用的预编译文件已移出工作树；历史演进可通过 Git 历史查阅。
弹幕页选择本地视频播放，视频不上传服务器，仓库不再内置大视频；多人测试使用同一视频。

## 可靠性与实际边界

- `accepted_ack`：持久化事件已获 Kafka broker 确认；不等于历史已入 SQL或接收者已收到。
- `delivered_ack`：目标连接接受下推；不等于客户端已经显示或阅读。
- SQL 保存会话序号、canonical 原文和 outbox；同 ID 改正文被拒绝，确认不确定时使用同 ID 重试。
- 客户端去重、连续游标、receipt/safe_seq、分页 Sync/Offline 处理乱序和重连。
- 消费者按业务结果确认 offset；prepared outbox 可恢复，持久化失败有 DLQ 与重放工具。
- 有界队列、流 deadline、慢连接输出预算、角色 RPC 鉴权和 readiness 用于限制故障扩散。

默认依赖为单机单 broker，未验证生产 SLA、多机高可用或长期容量，不承诺 exactly-once。
最新 [负载结果](docs/benchmarks/2026-10-11-current/README.md)：三轮各 160 条消息 ACK/接收/SQL 完整，E2E 吞吐中位数 33.52 msg/s；800 条同会话突发失败，存在 ACK 超时边界。其他历史报告不能替代当前链路容量。

## 构建与测试

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -j2
ctest --test-dir build --output-on-failure -j2
python3 cannbot/scripts/validate_knowledge.py --strict
python3 cannbot/scripts/test_pi_citations.py

# 三项数据库 opt-in 测试使用独立临时容器补跑。
python3 tests/run_mysql_review_tests.py build
python3 tests/run_redis_review_tests.py build
python3 tests/run_kafka_volume_test.py build
```

默认 CTest 注册 43 项，其中 3 项 SQL/Redis 集成测试按环境跳过；GitHub CI 会额外执行集成验证。
可用标准库 harness 启动隔离三服务并做实际 ACK/接收/SQL 校验，不使用个人 `.env.local`：

```bash
python3 tests/run_current_benchmarks.py --build build \
  --out /tmp/spark-bench --repeats 3 --messages-per-conn 20
```

验证 Git 当前提交能从全新克隆构建并启动，可运行以下检查（需提交当前改动，且标准端口和 Spark 容器名空闲）：

```bash
python3 tests/run_release_smoke.py --out /tmp/spark-release-smoke.json --jobs 2
```

它不会读取个人环境文件，会验证 doctor、源码构建、CTest、完整启动/健康、80 条消息 ACK/独立接收/SQL，并清理自身容器和卷。

## 可选功能与文档

| 主题 | 文档 |
|---|---|
| 当前架构与调用链 | [架构](docs/spark-push-architecture.md)、[Logic 业务层](docs/logic-business-layer.md) |
| 算法、复杂度、锁与失败路径 | [项目手册](docs/project-guide-and-resume.md)、[模块算法](docs/spark-push-internals.md) |
| ACK / 游标 / 迁移 / 排障 | [可靠性](docs/reliability-optimization.md)、[工程修复](docs/review-remediation.md) |
| Pi Agent 与流式回复 | [Pi 集成](docs/pi-agent-integration.md)、[多 Agent](docs/multi-agent-integration.md) |
| 共享房间 Agent | [房间协作](docs/room-agent-collaboration.md)，默认关闭 |
| GPU / 推理 Gateway | [推理服务](docs/inference-serving.md)，独立部署与验证 |
| Android | [构建与连接](android-app/README.md)，需 Android SDK / JDK |
| 管理员 | [用户中心](docs/user-center.md) |

保留的带日期测试报告用于复现证据，适用环境和版本以各报告为准。

第三方源码的版权与许可见 [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES.md)。
