# Spark IM Android

这是第一阶段 Android 迁移：原生 WebView 外壳复用现有 WebDemo，因此登录、WebSocket、AgentEvent 卡片、Hermes/Pi 会话和附件选择保持同一套实现。首次启动输入 PC 的 Tailscale MagicDNS 地址，地址会保存在 Android 本地，后续直接连接。

在 WSL/Android Studio 中打开本目录并执行 `./gradlew assembleDebug`。手机和 PC 登录同一 Tailnet 后，建议使用 `https://<电脑名>.<tailnet>.ts.net:9010/index.html`；如果服务暂时只有 HTTP，可使用对应 `http://...ts.net:9010/index.html` 开发地址。确保 PC 防火墙允许 Tailscale 网卡访问 9010、9000、9101 等端口。

后续阶段可将 WebView channel 替换为 Kotlin/Compose 原生界面，继续复用同一 AgentEvent 和 WebSocket 协议。
