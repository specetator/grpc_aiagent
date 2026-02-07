## load_test 测试工具说明

本目录提供多种 C++ 测试/压测客户端：

| 工具 | 用途 | 性能 |
|-----|------|-----|
| `async_ws_bench` | 高性能WebSocket消息推送压测 (epoll) | ~50,000 QPS |
| `http_bench` | 高性能HTTP API压测 (epoll) | ~86 QPS |
| `ws_client_cli` | 功能验证用的WebSocket命令行客户端 | - |
| `load_tester` | 多线程压测客户端（WebSocket/HTTP/业务流程） | - |

### 一、编译方式

在工程根目录下执行（与主工程一起编译）：

```bash
cd /home/ubuntu/course/spark_push
mkdir -p build
cd build
cmake ..
cmake --build . -j$(nproc)
```

生成的可执行文件路径：

- `load_test/ws_client_cli`
- `load_test/load_tester`

### 二、ws_client_cli：功能验证 WebSocket 客户端

#### 2.1 参数说明

```bash
./load_test/ws_client_cli \
  --account <账号> \
  --password <密码> \
  [--logic-host 127.0.0.1] \
  [--logic-port 9101] \
  [--comet-host 127.0.0.1] \
  [--comet-port 9000] \
  [--to-user <uid>] | [--room-id <room_id>]
```

- `--account/--password`：登录账号、密码（与 HTTP `/api/login` 保持一致）。
- `--logic-host/--logic-port`：logic HTTP 地址（默认 `127.0.0.1:9101`）。
- `--comet-host/--comet-port`：comet WebSocket 地址（默认 `127.0.0.1:9000`）。
- `--to-user`：单聊目标用户 ID，进入单聊模式。
- `--room-id`：聊天室 `room_id`，进入聊天室模式。

#### 2.2 使用流程

1. 客户端通过 HTTP 调用 `/api/login` 获取 `user_id/token`。
2. 使用 `token` 建立 `ws://comet_host:comet_port/ws?token=...` 连接。
3. 终端中逐行输入文本，每行会被包装为：
   - 单聊：`{"type":"single_chat","to_user_id":...,"content":{"text":...}}`
   - 聊天室：`{"type":"chatroom","group_id":room_id,"content":{"text":...}}`
4. 收到服务器下行消息会在终端打印 `"[RECV] ..."`。

示例：

```bash
./load_test/ws_client_cli \
  --account user1 --password 123456 \
  --to-user 2
```

聊天室示例：

```bash
./load_test/ws_client_cli \
  --account user1 --password 123456 \
  --room-id 1001
```

### 三、load_tester：压测客户端

`load_tester` 通过 `--mode` 切换不同压测场景，核心公共参数如下：

```bash
./load_test/load_tester \
  --mode <ws_send|http_history|biz_flow> \
  --connections N \
  --io-threads M \
  --messages-per-conn K \
  --interval-ms T \
  [功能相关参数...]
```

- `--connections N`：总连接数 / 虚拟用户数量（默认 10）。
- `--io-threads M`：工作线程数（默认 4）。
- `--messages-per-conn K`：
  - `ws_send`：每连接发送的消息条数。
  - `http_history`：每线程执行的请求次数。
  - `biz_flow`：每个虚拟用户执行完整业务脚本的次数。
- `--interval-ms T`：两次消息/请求之间的睡眠时间（默认 10ms）。

程序结束时会输出：

- `sent`：成功请求/消息总数。
- `failed`：失败次数。
- `duration`：总耗时（秒）。
- `approx QPS`：近似 QPS（`sent / duration`）。

#### 3.1 WebSocket 发消息压测（mode=ws_send）

用途：压测 comet 的 WebSocket 收发性能（单聊/聊天室）。

额外参数（与 `ws_client_cli` 相同风格）：

- `--logic-host/--logic-port`：logic HTTP，用于登录。
- `--comet-host/--comet-port`：comet WebSocket。
- `--account/--password`：账号、密码（所有连接可以共用或配合前缀策略）。
- `--to-user`：单聊目标用户 ID（单聊模式）。
- `--room-id`：聊天室 ID（聊天室模式）。

示例：100 条连接压单聊

```bash
./load_test/load_tester \
  --mode ws_send \
  --logic-host 127.0.0.1 --logic-port 9101 \
  --comet-host 127.0.0.1 --comet-port 9000 \
  --account user --password 123456 \
  --to-user 2 \
  --connections 100 --io-threads 4 \
  --messages-per-conn 100 --interval-ms 10
```

示例：聊天室消息压测

```bash
./load_test/load_tester \
  --mode ws_send \
  --logic-host 127.0.0.1 --logic-port 9101 \
  --comet-host 127.0.0.1 --comet-port 9000 \
  --account user --password 123456 \
  --room-id 1001 \
  --connections 100 --io-threads 4 \
  --messages-per-conn 100 --interval-ms 10
```

#### 3.2 HTTP 历史接口压测（mode=http_history）

用途：压测 logic 的历史消息拉取能力 `/api/session/history`。

额外参数：

- `--history-session-id SID`：会话 ID（必填），如 `s_1_2` 或 `r_1001`。
- `--history-anchor N`：`anchor_seq`（默认 0）。
- `--history-limit N`：每次返回条数（默认 20）。

示例：

```bash
./load_test/load_tester \
  --mode http_history \
  --logic-host 127.0.0.1 --logic-port 9101 \
  --history-session-id r_1001 \
  --history-anchor 0 \
  --history-limit 50 \
  --io-threads 4 \
  --messages-per-conn 1000 \
  --interval-ms 5
```

#### 3.3 业务流程压测（mode=biz_flow）

用途：压测完整业务链路（注册 → 登录 → 创建聊天室 → 加入聊天室），主要看 MySQL/Redis/logic 的综合能力。

额外参数：

- `--account <前缀>`：账号前缀，例如 `user_`。
- `--password <密码>`：注册/登录密码。

说明：

- 对第 `i` 个虚拟用户，实际账号为：`<前缀><i>`（例如 `user_0`, `user_1`, ...）。
- 每个虚拟用户每轮脚本包含：
  1. `POST /api/register`（账号已存在会收到 409，视为“注册已完成”）。
  2. `POST /api/login`。
  3. `POST /api/admin/chatroom/create` 创建聊天室。
  4. `POST /api/chatroom/join` 加入刚创建的聊天室。
- 每个步骤成功一次都会被统计为一次 `sent`。

示例：

```bash
./load_test/load_tester \
  --mode biz_flow \
  --logic-host 127.0.0.1 --logic-port 9101 \
  --account user_ --password 123456 \
  --connections 50 \
  --io-threads 4 \
  --messages-per-conn 5 \
  --interval-ms 100
```

### 四、建议的使用顺序

1. 使用 `ws_client_cli` 手动验证登录、单聊、聊天室功能是否正常。
2. 用 `load_tester --mode ws_send` 做小规模 WebSocket 压测，验证 comet + logic + job + Kafka 主链路。
3. 用 `load_tester --mode http_history` 压 `/api/session/history`，观察 MySQL/Redis 的读性能与缓存效果。
4. 用 `load_tester --mode biz_flow` 压注册/登录/建聊天室流程，验证数据库写入能力与业务正确性。

在压测过程中建议同时结合 `logic.log` / `comet.log` 及监控（CPU、QPS、Kafka lag、MySQL 慢查询等）一起分析瓶颈。 


