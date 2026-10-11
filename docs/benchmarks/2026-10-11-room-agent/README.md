# 2026-10-11 多人共享 CANN Agent 验收

这是功能、安全及跨用户交接测试，不是 QPS / NPU 性能报告。

| 文件 | 测试范围 |
|---|---|
| [validation.json](validation.json) | 构建、29/29 CTest、工具 guard、DOM 合同及边界 |
| [source-manifest.json](source-manifest.json) | 本次改动的实现、配置及测试文件 SHA-256，用于复核被测源码 |
| [controlled-e2e.json](controlled-e2e.json) | 受控模型决策，真实 IM 依赖、bubblewrap 与 C++ Add 执行，44 条检查 |
| [live-pi-e2e.json](live-pi-e2e.json) | 真实 Pi，两个真实注册用户接手同一源码并执行测试，16 条检查 |
| [business-regression.json](business-regression.json) | 既有 HTTP/gRPC/WS 业务，88 条检查 |

使用独立 MySQL 3308 / Redis 6381 DB11 / Kafka 29092；没有清空开发业务数据库。
模型默认入口为本机已配置的 deepseek/deepseek-flash，未改私人配置。
secret、token、详细运行日志及生成源码留在本机，仅上传无凭据结构化报告。
复现与部署见 [协作规范](../../room-agent-collaboration.md#7-验证与复现)。
