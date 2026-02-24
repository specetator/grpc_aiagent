# Spark-Push

高性能 IM 推送系统，基于 C++17 实现，参考 B 站 goim 三层架构设计。

支持**单聊**、**聊天室**、**视频弹幕**、**全局广播**四种消息场景，单聊 QPS 达到 **50,000**。

## 架构

```
                           ┌─────────────┐
                           │  Web Client  │
                           └──────┬───────┘
                                  │ WebSocket (9200)
                                  ▼
┌──────────────────────────────────────────────────────────────┐
│                     Comet 接入层                              │
│  WebSocket 连接管理 · 帧解析 · 心跳检测 · 本地房间路由         │
└────────────┬──────────────────────────────────┬──────────────┘
             │ gRPC 双向流 (9100)                │ gRPC (9205)
             ▼                                   ▲
┌────────────────────────┐          ┌────────────┴─────────────┐
│     Logic 逻辑层        │          │        Job 消费层         │
│  鉴权 · 路由 · 限流     │──Kafka──→│  推送(PushHandler)        │
│  序号分配 · HTTP API    │          │  持久化(PersistHandler)   │
└────────────────────────┘          └──────────────────────────┘
     │          │                            │
     ▼          ▼                            ▼
  Redis      Kafka                        MySQL
```

## 核心特性

- **三层解耦架构**：Comet/Logic/Job 独立部署、独立扩缩容
- **gRPC 双向流**：Comet↔Logic 持久连接，消除逐条 RPC 开销，QPS 翻倍
- **Kafka 双消费组**：推送和持久化解耦，互不影响
- **本地路由缓存**：`shared_mutex` 读写锁，1 秒 TTL，减少 Redis 查询
- **手写 WebSocket**：基于 Muduo，完整实现 RFC 6455 握手 + 帧解析
- **Redis 滑动窗口限流**：Sorted Set + MULTI 事务

## 性能数据

| 优化阶段 | 单聊 QPS | 关键改动 |
|---------|---------|---------|
| 基线（同步 MySQL） | 8,330 | — |
| + 本地缓存 + shared_mutex | 16,656 | 路由查询走内存 |
| + gRPC 双向流 | 33,322 | 持久流替代 Unary RPC |
| + 200 并发连接 | **~50,000** | 充分利用多线程 |

| 场景 | QPS |
|-----|-----|
| 房间聊天（100 用户） | 20,030 |
| HTTP API（4 线程） | 5,710 |

## 技术栈

| 组件 | 技术 |
|------|------|
| 语言 | C++17 |
| 网络框架 | Muduo (Reactor, one loop per thread) |
| RPC | gRPC (Unary + 双向流 + 客户端流) |
| 消息队列 | Apache Kafka (librdkafka) |
| 缓存 | Redis (hiredis) |
| 数据库 | MySQL (libmysqlclient) |
| 序列化 | Protocol Buffers 3 |
| JSON | nlohmann/json |
| 构建 | CMake 3.14+ |

## 快速开始

### 环境依赖（Ubuntu 22.04）

```bash
sudo apt-get install -y \
    build-essential cmake \
    libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc \
    librdkafka-dev libhiredis-dev libmysqlclient-dev \
    nlohmann-json3-dev libssl-dev zlib1g-dev libc-ares-dev
```

### 编译

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### 启动

```bash
# 确保 Redis、MySQL、Kafka 已启动
mysql -u root -p < sql/schema.sql    # 初始化数据库

# 启动三个服务（分别在不同终端）
./build/logic/logic_server
./build/comet/comet_server
./build/job/job_server

# 启动 Web Demo（可选）
./build/web_demo/web_demo_server
# 访问 http://localhost:9001/index.html
```

## 目录结构

```
spark-push/
├── comet/          # 接入层：WebSocket 连接管理、gRPC 客户端
├── logic/          # 逻辑层：gRPC 服务、HTTP API、Redis 路由
├── job/            # 消费层：Kafka 消费、推送、持久化
├── common/         # 公共组件：连接池、线程池、Kafka 封装
├── proto/          # Protocol Buffers 定义
├── muduo/          # Muduo 网络库（内置）
├── web_demo/       # Web 演示前端
├── load_test/      # 压测工具
├── sql/            # 数据库 Schema
├── conf/           # 配置文件
├── docs/           # 文档（ 
├── scripts/        # 启停脚本
└── logs/           # 运行日志
```
 

## 端口规划

| 服务 | 端口 | 协议 |
|------|------|------|
| Comet WebSocket | 9200 | WebSocket |
| Comet gRPC | 9205 | gRPC |
| Logic gRPC | 9100 | gRPC |
| Logic HTTP | 9101 | HTTP |
| Web Demo | 9001 | HTTP |


# 异常问题
## grpc protoc多个版本
cmake ..时报错：
```
CMake Warning at /usr/cmake-3.22/Modules/FindProtobuf.cmake:524 (message):
  Protobuf compiler version 3.12.4 doesn't match library version 3.19.4
Call Stack (most recent call first):
  proto/CMakeLists.txt:3 (find_package)
```
导致后续编译报错或者启动comet server时卡住，可以使用`whereis protoc`命令查看对应的`protoc`在哪里，使用绝对路径查看其版本，比如
```
root@VM-8-4-ubuntu:~/darren/11.2-spark_push/06# whereis protoc
protoc: /usr/bin/protoc /usr/local/bin/protoc /usr/share/man/man1/protoc.1.gz
root@VM-8-4-ubuntu:~/darren/11.2-spark_push/06# /usr/bin/protoc --version
libprotoc 3.12.4
root@VM-8-4-ubuntu:~/darren/11.2-spark_push/06# /usr/local/bin/protoc --version
libprotoc 3.19.4
```
如果我们需要手动设置默认版本，则使用`export PATH=/usr/local/bin:$PATH`进行设置，即是：
```
# 在终端执行
export PATH=/usr/local/bin:$PATH
# 然后
protoc --version
此时就显示的是/usr/local/bin/protoc路径的版本。

```
## SSL报错
将11.2-spark_push/cmake目录里的 FindgRPC-bk.cmake替换默认的FindgRPC.cmake


