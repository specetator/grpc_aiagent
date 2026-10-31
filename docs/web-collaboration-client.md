# Web 协作客户端

适用于当前主线的原生 Web 客户端。源码依据：`web_demo/static/index.html`、`collaboration_ui.js`、
`spaces.js`、`room_agents.js`，以及 `logic/services/{user,conversation,room}_service.*`。
文档核对日期：2026-11-01；执行结果见 [Web 阶段回执](benchmarks/2026-11-01-web-collaboration.json)。

Compose 的统一入口 `/` 打开 `index.html`。本地开发直接访问 Web 服务的首页。
页面沿用原生 JavaScript；旧 `chatroom.html`、私人 Pi/Hermes 命令卡和 CANN 协作入口保留。

## 使用流程

1. 注册或登录，在左侧「创建 / 加入」创建空间。默认私有；公开空间允许登录用户申请加入。
2. 在「协作详情 → 成员」输入队友的完整登录账号发送邀请。邀请不是成员关系，队友需要在自己的「创建 / 加入」中接受。
3. 从左侧空间列表进入讨论。私聊使用对方完整账号，昵称只用于展示。
4. 房主在「Agent」中安装管理员允许的 Agent。在消息框输入 `@` 并点击候选项，选择保存在结构化 Agent ID 中。普通文本不会自动调用通用 Agent。
5. 房主在「工作区」选择管理员已发布的项目。此处不能输入本地目录、Runtime URL、模型凭据或工具权限。公共 Agent 继续使用只读权限。
6. 在已完成的 Agent 请求中「保存为协作任务」，核对模型给出的标题、描述和步骤。成员可接手、更新进度和交接摘要；这些字段由服务端持久保存，人工标记完成不代表测试执行通过。
7. 「CANN 算子协作」保留原来的任务配置、显式开发授权、交接和执行回执；不改变既有工作区和 Session。

右侧详情可折叠；手机上通过左上方按钮打开会话列表。Agent 最终回复显示身份、发起者，并能跳转到已加载的原提问。
失败请求可查看真实执行状态；未知结果仍走原请求恢复，不因刷新页面自动创建新工具任务。

## 服务边界

现有成员、角色、安装和工作区权限不变。新增接口只提供普通用户选择对象所需的目录信息。
所有接口使用 bearer 身份；浏览器隐藏操作按钮不能代替服务端校验。

| 接口 | 请求 | 响应与限制 |
| --- | --- | --- |
| `POST /api/users/lookup` | 仅 `account` 字符串 | 精确账号查找；返回 `user_id/account/name`。不列举前缀、不返回密码摘要、状态或凭据；禁用、删除、系统管理员、Bot 不返回 |
| `POST /api/spaces/discover` | `offset/limit` | 仅当前公开聊天室的名称、描述、ID、可见性；按 ID 倒序稳定分页，最多 100 条。发现不授予访问权 |
| `POST /api/session/list_single` | 既有 `user_id` | 保留 `session_id/peer_user_id/last_msg_seq`；增加 `peer_name/unread_count`，未读基于读游标 |

加入公开空间继续使用 `/api/chatroom/join` 的 `room_id/user_id` 协议；WS 在线订阅使用 `group_id`。
邀请、接受、角色、退出、成员移除仍使用已有空间 API；工作区采用已有 generation 校验；任务更新采用已有 revision 校验。
没有新数据库表或迁移。MySQL 公共目录使用参数绑定；账号查找复用现有 UserRepository。

## 消息与缓存恢复

- 只有一个 WebSocket、历史查询和连续消息游标实现。`collaboration_ui.js` 管导航及表单，消息可靠性留在原消息中心。
- `accepted_ack` 只表示持久化事件经 Kafka 确认；`delivered_ack` 不表示 SQL 已落库或用户已读。正式消息按 `msg_seq` 排序，`msg_id/client_msg_id` 去重。
- 非当前会话的正式消息继续处理并缓存。每个会话最多缓冲 2000 条；超过上限保留缓冲前的位置，返回该会话时从该位置补拉。不会以已接收但未保留的最高序号跳过消息。
- 首屏建立最近历史窗口后再补拉缺口；SQL 页的最大序号不能直接作为连续游标。服务端 `safe_seq` 与分页 `next_seq` 仍承担不同职责。
- 当前可见会话每 5 秒复用 WS cursor sync 核对一次，补偿重连查询早于 SQL 落库的尾消息；一次空页不会永久停止恢复。活动同步期间不重复发起，15 秒没有结束帧时允许幂等补拉。隐藏、断线、退出和撤权时不发周期请求；恢复可见时立即核对。
- 尚无历史的私聊先通过认证会话目录发现真实 Session，再走同一 WS 同步路径；目录序号不推进消息游标。发现请求有 10 秒超时，退出登录会取消，旧账号返回值不能写入新账号缓存。
- `hermes_delta` / `ai_delta` 是临时展示，不推进游标。最终结果可通过 WS 或历史替换临时行，私人 Agent 的终态仅从历史恢复也会清理旧 stream。
- 发送捕获会话、账号和附件；上传期间切换会话不会把图片发往新会话。继续编辑的正文、图片和 Mention 选择保留为草稿。失败消息的手动重发保留原始 `client_msg_id`。
- 打开空间先查当前成员关系再恢复缓存；撤权清理该空间的消息、草稿、附件、请求和游标，拒绝晚到帧与异步响应。退出登录清理所有账号所属缓存，旧连接和延迟同步不能写入新登录。

## 可重复验证

构建及业务回归：

```bash
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure
python3 tests/run_mysql_review_tests.py build-wsl
python3 tests/run_redis_review_tests.py build-wsl
```

浏览器契约测试使用受控 HTTP/WS，验证排序、缓存、竞态与界面行为，不替代真实 E2E：

```bash
export SPARK_PLAYWRIGHT_MODULE=/path/to/node_modules/playwright
node tests/collaboration_browser_test.cjs
node tests/agent_channel_browser_test.cjs
node tests/room_agents_browser_test.cjs
node tests/spaces_browser_test.cjs
```

真实验证使用临时 Compose 项目、独立卷和三个独立 Chromium 进程。没有身份注入、请求 Mock 或数据库写入捷径。
基础模式完全关闭 Agent；可选模式复用公共 Runtime fixture，需要真实 Pi 与已授权的隔离 Windows Hermes 模型环境。

```bash
docker build -t spark-push:web-collaboration .
python3.11 tests/collaboration_web_e2e.py --image spark-push:web-collaboration \
  --out /tmp/spark-web-im.json
python3.11 tests/collaboration_web_e2e.py --image spark-push:web-collaboration \
  --agents --out /tmp/spark-web-agents.json
```

Python 3.11 环境需安装 `websockets` 和 `PyYAML`；Node 使用外部安装的 Playwright/Chromium，不在项目中添加大型前端依赖。
中文截图需要测试机有中文字形。测试产生桌面/手机截图，并记录镜像 ID、源码及实际服务出的前端文件 SHA。
截图本身不是验收通过证据。

本轮真实截图：[桌面协作与任务](images/collaboration-desktop.png)、[手机聊天](images/collaboration-mobile.png)。

网络故障通过仅代理 Bob 连接的透明 TCP 转发器实现：不修改 HTTP/WS 内容，断开实际 socket，其他用户继续聊天。
暂停实际 Job 的故障步骤先确认 Kafka 已接受、HTTP 历史尚无新消息，并观察 Bob 重连取得旧游标空页；
恢复 Job 后验证 SQL 和浏览器自动补齐。房间、私聊都比较完整顺序及去重结果，恢复期间不刷新或切换页面。
全栈 `down/up` 保留数据卷；之后启动全新的三个浏览器进程，排除旧内存、本地存储或已打开页面掩盖持久化问题。
测试完成只清理自己创建的容器、卷和临时 Runtime，不读取部署者个人环境文件作为基础 IM 配置。

真实 CANN、私人 Agent、Kafka 故障注入、未知 Native 执行恢复及全新 Linux 主机安装仍需各自的独立回归证据。
只有这些阶段及发行检查均满足时，才能把整个版本标记为 v0.1.0 / MVP 验收完成。
