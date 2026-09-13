# Spark IM Android

这是第一阶段 Android 迁移：原生 WebView 外壳复用现有 WebDemo，因此登录、WebSocket、AgentEvent 卡片、Hermes/Pi 会话和附件选择保持同一套实现。

在 WSL/Android Studio 中打开本目录并执行 `./gradlew assembleDebug`。模拟器默认访问宿主机 `http://10.0.2.2:9010/index.html`；真机请通过 `adb reverse tcp:9010 tcp:9010` 后把 URL 改为 `http://127.0.0.1:9010/index.html`，或传入 Activity extra `url` 指向局域网地址。

后续阶段可将 WebView channel 替换为 Kotlin/Compose 原生界面，继续复用同一 AgentEvent 和 WebSocket 协议。
