# Spark IM Android

这是 Spark Push 的原生 Kotlin/Jetpack Compose Android 客户端。它不再把 WebDemo 嵌入手机，而是通过 Spark Push 的 HTTP API、Comet WebSocket 和通用 AgentEvent 协议连接 Android 专用 Agent 通道。界面采用轻量的 Claude Desktop 风格，适合手机竖屏使用。

当前原生客户端覆盖：账号登录/注册、单聊历史与分页、实时消息和离线补推、发送回执、未读/已读、Android 专用 Pi Agent 与 Hermes technical 会话、Agent 命令和操作卡片、真实 provider 模型选择/思考深度/创作命令、CANN 引用与受保护知识原文、管理员用户/Token/审计/全站广播控制台。Android 使用专用 Agent 联系人 `900000000201/900000000211`、独立会话和模型状态，并从会话列表过滤 PC/WebDemo/Telegram 的旧 Agent 联系人 `900000000001/900000000101`；普通 Spark Push 用户仍可按需建立单聊。聊天室普通用户页、聊天室运营界面和视频弹幕页按当前产品范围不放入 Android 客户端，后端协议仍保留。

界面采用 Claude Desktop 的视觉语言做手机化适配：暖白画布、低对比分隔线、窄信息层级、无边框助手消息、淡紫用户消息和轻量 Agent 操作卡。Android 目标版本为 `0.4.0`；聊天页显式处理 Android 15 状态栏和键盘 `adjustResize` 安全区。

在 WSL/Android Studio 中打开本目录并执行 `./gradlew assembleDebug`。手机和 PC 登录同一 Tailnet 后，建议使用 `https://<电脑名>.<tailnet>.ts.net:9010/index.html`；如果服务暂时只有 HTTP，可使用对应 `http://...ts.net:9010/index.html` 开发地址。确保 PC 防火墙允许 Tailscale 网卡访问 9010、9000、9101 等端口。

APK 使用 `com.peco.sparkim` 包名。构建后安装 `app/build/outputs/apk/debug/app-debug.apk`。首次启动输入 PC 的 Tailscale 地址（例如 `http://100.89.19.125`），客户端会自动派生 9101 HTTP API 和 9000 WebSocket 地址，并把服务器地址保存在 Android 本地。手机和 PC 需要能通过同一 Tailnet 访问这些端口。


## 代码与验证索引

原生客户端的模块职责、WebSocket 连接代数、历史游标、40 ms 流式合并、Agent envelope、输出长度审计和后端数据边界见 ../docs/spark-push-implementation.md。

`SparkMessageStore` 使用 SQLite 保存本地消息、严格连续的设备接收游标、待确认的稀疏接收回执以及持久化发送队列。所有数据按规范化 Logic endpoint + 用户 ID 隔离；退出登录暂停该账号队列，重新登录同一账号后继续恢复，不把旧账号的消息发送到新账号。SharedPreferences 保存安装级稳定 UUID、服务器和登录凭证。

消息发送时，先在同一事务保存稳定 `client_msg_id`、完整帧和乐观消息，然后再执行网络发送。离线文本、Agent 命令和图片都可排队；图片 bytes 一并保存，上传完成后逐张保存附件 ID，随后发送消息帧。发送队列上限 200 条/32 MiB；2 秒起步指数退避，最大 60 秒，网络故障、408、429、5xx 和 ACK 超时会保留重试；明确业务拒绝记录 FAILED 并停止自动重试。`accepted_ack` 先事务保存服务端消息身份/序号，再删除待发送项；`delivered_ack` 仍表示服务端投递阶段。

连接携带 `device_id` 和 `receive_ack=1`。接收事务提交后才发送 `received_ack`，包括连续前缀 `msg_seq` 和实际已保存的 `received_seqs`（每帧最多 256 个）。收到 `received_ack_ok` 后删除本地待确认回执，断线/进程重启时每秒重试。最近 50 条 history 不能跳过缺失前缀；SQLite 中的实际序号决定游标，永久数值洞使用稀疏回执。未选中的会话同样先持久化再 ACK；历史、实时消息和乐观消息按 `msg_id`/`client_msg_id`/会话序号合并。`ai_delta`/`hermes_delta` 仅在内存中绘制，最终消息才持久化。

可靠性自动测试与本地 SQLite 性能测试：

```bash
./gradlew assembleDebug testDebugUnitTest --no-daemon --console=plain
```

`SparkMessageStoreTest` 使用 Robolectric Android 28 native SQLite 验证历史缺口、重启、回执重试、accepted ACK 事务、幂等合并、账号/endpoint 隔离、非当前会话和 delta 拒绝。性能测试写入 5000 条消息（100 批，每批 50 条）并重复读取 200 条 timeline，原始耗时和分位数输出到 `test-results/android-sqlite-benchmark.json`。该数据代表 WSL 上 Robolectric 的 native SQLite，不代表真机、Android UI 或生产容量。

WSL 构建会在本机生成 android-app/local.properties，其中的 SDK 路径不应提交到 Git。真机验证需要在线 ADB 设备；没有设备时至少完成 Gradle 构建、后端 CTest、Router 测试和 sparkctl health。
