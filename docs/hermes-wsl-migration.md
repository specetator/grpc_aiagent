# Hermes technical 迁移到 WSL 的评估

评估日期：2026-09-07；检查的 Windows Hermes 源码提交为 `6bcac6a473`。当前保持 Windows technical 服务，未迁移凭据、记忆或历史，也未切换 IM 路由。

## 结论

可以迁移为独立的 Linux Hermes 实例，建议与 IM 一同运行在 WSL 的 Linux 文件系统内。
迁移单位是「Hermes 运行环境 + technical 数据 + 工具依赖 + 服务管理」，不能仅复制 Windows venv。
不需要改变 Pi Bot 或 Hermes Bot 的用户 ID；保留 Hermes Agent ID 和 Router 数据库即可维持
IM 会话命名空间，但继续旧 Hermes 对话还需要迁移相应的 Hermes session 数据。

Hermes 官方提供 [WSL2 安装指南](https://hermes-agent.nousresearch.com/docs/user-guide/windows-wsl-quickstart)。
Microsoft 建议 Linux 工作负载将文件放在 [WSL 文件系统](https://learn.microsoft.com/en-us/windows/wsl/filesystems)
而非挂载的 Windows 盘。因此如果迁移后仍从 `/mnt/f/hermes` 导入大量 Python 模块、访问状态库
和运行工具，不能据此期待更快；运行环境、工作区和状态应一起放到 Linux 文件系统。

## 本机审计结果

审计工具：`python3 cannbot/scripts/audit_hermes_wsl.py`。仅输出字段位置和计数，不输出凭据、
URL 原文或用户消息。扫描文本命中可能是示例，不能把命中数直接当作不可迁移依赖数。

| 项目 | 当前发现 | 迁移处理 |
| --- | --- | --- |
| Python | 系统默认 3.10；另有 Python 3.11 和 uv。安装源码要求 >=3.11,<3.14 | 显式建立 Python 3.11 的 Linux venv |
| Hermes 运行环境 | 尚未发现本机 Linux Hermes 安装 | 使用与 Windows 相同的源码版本，重装 Linux 依赖；不要复制 Windows venv |
| 终端 | technical 使用 local backend | 设置独立 Linux cwd，不能沿用 Windows 路径 |
| 配置中的 Windows 路径 | 语音提供商 gpt-sovits-shinku command | 重建 Linux 语音服务，或明确保留 Windows 服务调用 |
| 配置中的 localhost 服务 | 两处 fallback provider、一处 custom provider | 确认这些服务属于哪个 OS；Linux localhost 不能直接假定指向 Windows 服务 |
| Skills | 576 个文件，其中 15 个含 Windows 路径、7 个含 Windows shell 引用、29 个含 loopback URL | 区分文档示例和可执行依赖，逐项验证常用 Skills |
| 记忆 | memories 4 个文件、memory 3 个文件，本次未发现 Windows 路径命中 | 私有复制，保留内容；核对外部长期记忆服务配置 |
| 常用工具 | node/npm/git/docker 可用；ffmpeg 不在 PATH | 文本对话可先验收，媒体能力另装依赖并测试 |
| `/restart` | 当前 IM 生命周期适配只支持固定 Windows technical 路径 | 切换前实现 Linux 生命周期操作，不能仅替换 base_url |

除了 technical 文件夹，还要核对 profile 继承、父目录 `.env`/`auth.json` 与进程环境中提供的
凭据。不能根据目录存在就认定凭据已完整迁移；OAuth、外部记忆和本地服务需要独立验证。

## 性能收益的边界

本轮已经对 Windows 路径做了优化，以下为本机少量只读样本，非压力测试：

| 操作 | 优化前 | 优化后 |
| --- | --- | --- |
| 简单 API 请求，含中继回收 | 约 1.2 秒 | 首次约 140 毫秒；复用后约 2～3 毫秒 |
| 中继进程回收等待 | 每请求约 1 秒 | 成功请求复用进程，不在请求末尾等待退出 |
| `/model` | 每次取目录约 3.6 秒 | 缓存命中约 1 毫秒；首次/强制刷新仍约 2.5～3.2 秒 |

真实两轮无工具测试中，连接约 6 ms，首段正文约 24～29 s，且多轮上下文与中继复用均通过。
这是当前配置的两次样本，尚未分解 Hermes 内部初始化与模型请求各占多少时间。

所以迁到 WSL **有机会进一步改善启动、工具调用和运维一致性**，但最明显的每请求固定开销
已经由中继复用消除。不能承诺迁移后模型回答快几倍。模型 API、思考等级、输入 token 数、
工具调用次数、Hermes 每轮 Agent 初始化仍然影响首字和总时间。

新增 `hermes_timing` 记录 trace_id、连接、首段正文和完整回复耗时；失败仅记录异常类型及耗时。
它不记录消息内容、凭据或原始 upstream 错误。当前计时从 Adapter 入口开始，不包括之前的
Kafka/Router 排队时间；仍需同模型、同思考配置、同上下文和同任务的对照才能评估迁移净收益。

## 推荐迁移步骤及回退

1. 在独立 Linux 目录准备相同版本运行环境，例如 `~/.local/share/spark-agents/hermes-technical/`，
   单独放 runtime、home、workspace。不要共用 `.pi-spark-agent`，也不要共写 Windows 状态库。
2. 建立私有 profile 副本，调整路径和本地服务访问方式。只启用 IM API，使用独立 API key；
   试运行阶段关闭副本的 Telegram、其他渠道、cron 和自动任务，避免重复消费或重复执行。
3. 先使用独立测试状态和独立端口（例如 Linux loopback 8645，启动前检查占用）验证模型目录、
   模型切换、文件读写、常用 Skills、重试、上下文和 Linux 重启功能。不要同时使用会相互
   刷新或失效的 OAuth 状态；根据认证方式安排独立登录。
4. 切换窗口内先停止向旧实例提交新请求，等待已开始的任务结束；使用 SQLite 一致性备份迁移
   状态，不能在线裸拷贝 `.db` 并遗漏 WAL。同时迁移必要 session、记忆和工作区文件。
   历史中的 Windows 附件路径也要验证，不能只确认聊天文本能够恢复。
5. 保持 `hermes-technical` ID、Bot ID、owner ACL 和 Router 状态不变，改为本机 HTTP Adapter。
   启用新实例前完成单一写入者切换；Linux 目录隔离不等于 OS 权限沙箱，仍需独立用户或容器
   才能阻止工具跨目录访问。
6. 验收真实 IM 流式回复、卡片点击、断线恢复、历史和跨 Agent 隔离，并做相同条件的性能对照。
   保留切换前快照与旧注册表以便回退。新实例已产生写入后，不能简单切回旧库而丢失新会话；
   必须先停止新写入，迁回增量或明确保留新实例历史。
