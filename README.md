# Spark Push 火花消息推送

基于 reactor、gRPC、Kafka、Redis、MySQL 的推送系统示例，包含 Logic（业务入口）、Comet（长连接网关）、Job（消息投递）以及一个简单的 Web Demo。


## 课件说明
- 1-项目整体架构和技术选型及部署 对应当前目录的源码
- 2-用户认证服务与gRPC接口实现   对应目录 02_auth_subset的源码
- 3-实时消息推送和kafka集成 对应目录03_msg_subset的源码
- 4-话题管理与分布式架构实现 对应目录04_room_subset的源码
- 5.1-http消息推送性能测试 对应目录5.1_room_subset的源码
- 6.1-websocket消息性能测试-待实现 具体找Darren老师
<br>
二次开发相关:
- 消息存储与检索服务实现 参考课件：5-消息存储与检索服务实现，参考代码：05_persist_subset
**特别说明**: 不要一行行抠代码，这个项目是偏技术架构讲解，不是一行行写代码的项目，如果基础不好，可以先学2410期的 GitHub聊天室项目，然后再学这个项目。
<br>

## 目录速览
- `logic/`：登录、消息、gRPC 服务等核心业务。
- `comet/`：WebSocket 长连接服务与 gRPC 下行接口。
- `job/`：Kafka 消费者，将消息投递到各 comet。
- `common/`：配置、日志、Redis/MySQL 连接池、Kafka 封装等公共组件。
- `proto/`：Protocol Buffers 定义。
- `web_demo/`：静态页面与示例 HTTP Server。
- `conf/`：示例配置文件（`logic.conf`、`comet.conf`、`job.conf`）。
- `sql/schema.sql`：初始化数据库脚本。

## 端口一览

| 组件            | 协议/用途                 | 默认端口   | 配置项                                                   |
| --------------- | ------------------------- | ---------- | -------------------------------------------------------- |
| Logic gRPC      | gRPC 业务入口             | 9100       | `logic.conf: listen_port`                                |
| Logic HTTP      | 登录/发送消息 HTTP API    | 9101       | `logic.conf: http_port`                                  |
| Comet WebSocket | 用户长连接                | 9000       | `comet.conf: listen_port`                                |
| Comet gRPC      | Job 下行到 Comet          | 9105       | `comet.conf: comet_grpc_port`；`job.conf: comet_targets` |
| Job             | Kafka 消费，向 Comet 推送 | 无监听端口 | -                                                        |
| Web Demo        | 静态页面访问              | 9010       | `--port` 启动参数                                        |

- `comet.conf: logic_grpc_target` 必须指向 Logic 的 gRPC 地址（默认 `127.0.0.1:9100`）。
- 如果调整端口，请同时修改相互引用的配置项（如 `logic_grpc_target`、`comet_targets`），以免连不上。

## 环境依赖

- g++/clang++ (C++17) 与 CMake ≥ 3.14
- muduo、gRPC / Protobuf
- MySQL client (libmysqlclient)、Hiredis
- Apache Kafka client (librdkafka++)
- pthread、OpenSSL 等上述依赖所需库

确保上述库已安装且 `pkg-config`/CMake 能够找到相应头文件与链接库。

## 编译（推荐）

```bash
cd spark_push
mkdir -p build
cd build
# 如需关闭 Web Demo：cmake .. -DBUILD_WEB_DEMO=OFF
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
```

编译输出在 `build` 目录下：

- `logic/logic_server`
- `comet/comet_server`
- `job/job_server`
- `web_demo/web_demo_server`（若启用 `BUILD_WEB_DEMO`）

## 运行指引

### 1) 配置

- 默认配置位于 `conf/logic.conf`、`conf/comet.conf`、`conf/job.conf`。
- 重点字段：
  - `logic.conf`: `listen_port`(gRPC)、`http_port`(HTTP)、Kafka/Redis/MySQL 连接信息。
  - `comet.conf`: `listen_port`(WS)、`comet_grpc_port`、`logic_grpc_target`。
  - `job.conf`: `kafka_*` 以及 `comet_targets`（`名称=地址:端口`）。
- 每个可执行文件均支持 `--config <path>` 或位置参数指定配置文件。

### 2) 启动顺序（本地示例）

```bash
# 进入构建目录
cd  spark_push/build

# 依次启动
./logic/logic_server --config ../conf/logic.conf
./comet/comet_server --config ../conf/comet.conf
./job/job_server   --config ../conf/job.conf

# 可选：启动示例页面
./web_demo/web_demo_server --port 9010 --doc-root ../web_demo/static
```

- 多个 Comet 实例可复用同一配置文件拷贝，只需调整 `comet_id`、`listen_port`、`comet_grpc_port` 并在 `job.conf` 的 `comet_targets` 中一一列出。
- 若端口占用，可在对应配置中修改，再按上表保持互相引用的一致性。

## 数据库与其他服务

- 使用 `sql/schema.sql` 初始化 MySQL（示例，按需调整账号/数据库名）：

```bash
mysql -u root -p < sql/schema.sql
```

- Redis、Kafka、Logic/Comet 的监听地址与端口均可在对应的 `conf/*.conf` 中调整。
- Kafka **不会由程序自动创建主题**，请先准备以下 Topic（或在集群侧开启自动创建）：

```bash
# 推送主通道
bin/kafka-topics.sh --create \
  --bootstrap-server 127.0.0.1:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic push_to_comet

# 广播任务通道
bin/kafka-topics.sh --create \
  --bootstrap-server 127.0.0.1:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic broadcast_task
```

可用 `kafka-topics.sh --list --bootstrap-server ...` 验证是否已存在。

## 常见问题

- **依赖缺失**：若 CMake 报告库未找到，请确认相关 `-dev` 包已安装，并检查 `CMAKE_PREFIX_PATH`/`PKG_CONFIG_PATH`。
- **端口冲突 / 互相引用错误**：
  - `logic.conf: listen_port` 用于 gRPC，`http_port` 用于 HTTP 接口；
  - `comet.conf: listen_port` 用于 WebSocket，`comet_grpc_port` 用于向 Job 暴露的 gRPC；
  - `comet.conf: logic_grpc_target` 必须指向 Logic 的 gRPC 地址；
  - `job.conf: comet_targets` 中的端口必须与对应 Comet 的 `comet_grpc_port` 一致。
- **Kafka 主题不存在**：启动 `job_server` 前请先创建 `push_to_comet`、`broadcast_task`，或启用集群自动创建。
