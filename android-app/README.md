# Spark IM Android

这是 Spark Push 的原生 Kotlin/Jetpack Compose Android 客户端。它不再把 WebDemo 嵌入手机，而是通过 Spark Push 的 HTTP API、Comet WebSocket 和通用 AgentEvent 协议连接 Android 专用 Agent 通道。界面采用轻量的 Claude Desktop 风格，适合手机竖屏使用。

当前原生客户端覆盖：账号登录/注册、单聊历史与分页、实时消息和离线补推、发送回执、未读/已读、Android 专用 Pi Agent 与 Hermes technical 会话、Agent 命令和操作卡片、真实 provider 模型选择/思考深度/创作命令、CANN 引用与受保护知识原文、管理员用户/Token/审计/全站广播控制台。Android 使用专用 Agent 联系人 `900000000201/900000000211`、独立会话和模型状态，并从会话列表过滤 PC/WebDemo/Telegram 的旧 Agent 联系人 `900000000001/900000000101`；普通 Spark Push 用户仍可按需建立单聊。聊天室普通用户页、聊天室运营界面和视频弹幕页按当前产品范围不放入 Android 客户端，后端协议仍保留。

界面采用 Claude Desktop 的视觉语言做手机化适配：暖白画布、低对比分隔线、窄信息层级、无边框助手消息、淡紫用户消息和轻量 Agent 操作卡。Android 目标版本为 `0.4.0`；聊天页显式处理 Android 15 状态栏和键盘 `adjustResize` 安全区。

在 WSL/Android Studio 中打开本目录并执行 `./gradlew assembleDebug`。手机和 PC 登录同一 Tailnet 后，建议使用 `https://<电脑名>.<tailnet>.ts.net:9010/index.html`；如果服务暂时只有 HTTP，可使用对应 `http://...ts.net:9010/index.html` 开发地址。确保 PC 防火墙允许 Tailscale 网卡访问 9010、9000、9101 等端口。

APK 使用 `com.peco.sparkim` 包名。构建后安装 `app/build/outputs/apk/debug/app-debug.apk`。首次启动输入 PC 的 Tailscale 地址（例如 `http://100.89.19.125`），客户端会自动派生 9101 HTTP API 和 9000 WebSocket 地址，并把服务器地址保存在 Android 本地。手机和 PC 需要能通过同一 Tailnet 访问这些端口。


## 代码与验证索引

原生客户端的模块职责、WebSocket 连接代数、历史游标、40 ms 流式合并、Agent envelope、输出长度审计和后端数据边界见 ../docs/spark-push-implementation.md。Android 客户端只在 SharedPreferences 保存服务器地址和登录恢复信息，聊天历史仍由 Spark Push 服务端返回。

WSL 构建会在本机生成 android-app/local.properties，其中的 SDK 路径不应提交到 Git。真机验证需要在线 ADB 设备；没有设备时至少完成 Gradle 构建、后端 CTest、Router 测试和 sparkctl health。
