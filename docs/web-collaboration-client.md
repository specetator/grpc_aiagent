# Web 协作客户端

适用于当前主线的原生 Web 客户端。源码依据：`web_demo/static/index.html`、`collaboration_ui.js`、
`spaces.js`、`room_agents.js`、`room_media.js`，以及 `logic/services/{user,conversation,room,media}_service.*`。
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

## 房间资料

开发工作树新增「房间资料」按钮，也可从「协作详情 → 资料」进入；旧 `chatroom.html` 有相同入口。
部署者需要先完成[媒体迁移与 worker 配置](multimodal-service-e2e.md)。资料上传成功后，只有后台处理完成才会显示「可供 Agent 阅读」。

1. 选择文本/Markdown、PDF/DOCX 或图片。网页单文件上限为 256 MiB，通过 HTTPS 或本机地址访问。
2. 上传显示进度，可暂停。断线或丢失回执后，重新选择同一文件即可续传；已确认分片不会重新上传。自己的未完成上传可取消。
3. 资料列表显示解析状态。就绪后可查看带页码的文本片段及图片，也可校验并下载原文件。
4. 点击「请 Agent 分析」，选择「总结资料」或「整理决策与任务」，再选择已安装且启用的 Agent。
   草稿携带所选资料 ID 与固定 revision，要求不可用时停止而非替换同名文件；
   用户可继续编辑，点击发送后才调用模型。Agent Mention 使用结构化 ID，资料目标目前仍在正文中，工具逐次执行服务端授权。
5. 若已完成回复包含 `tasks` 数组，可逐项「保存任务」，修改标题、描述、步骤后保存到现有协作任务；负责人由真实成员接手，不按转写中的姓名自动分配。
   任务建议可以带 `needs_confirmation`、`verbatim_quote`、`interpretation` 和 `reason`。保存表单会把待确认提示、
   模型摘录、解释和确认说明保留在可编辑的任务描述中，继续通过现有任务 API 保存；模型摘录仍须与来源核对。
   这表示内容需要人工确认，不把任务或 Runtime 执行状态改成 `unknown`，也不自动执行或分配任务。
6. 在 Agent 请求旁「查看读取来源」，或在任务详情中「查看任务来源」，点击文件名回查原页码/时间片段。
   来源由服务端按原请求复验；失效来源显示不可用。模型任务建议中的来源 ID 会随任务及修改历史保存，不能绑定别的请求或房间的来源。
7. Agent 最终消息下方也有「查看读取来源」，从历史恢复的消息使用同一入口，不需要先找到执行详情。
8. 在资料页「检索资料正文」输入关键词，可以查找已处理文档、转写和 OCR 文本。点击「查看匹配片段」从命中的位置打开，
   也可选择 Agent 分析该资料。当前采用字面匹配，最多显示 20 个片段；超过时需细化关键词。清空检索或切换房间会丢弃晚到结果。

2026-11-02 按用户决定暂缓音频、视频：当前网页拒绝发起新的音视频上传，隐藏播放和分析入口，
历史原文件仍可下载，已有来源记录仍可回查。后台协议、解析代码和旧数据保留；这是本阶段的客户端交付范围，
没有用隐藏按钮代替服务端权限校验。以下音视频验证记录是历史证据，不再作为当前阶段验收门槛。
收敛后的 27 项受控浏览器检查见[范围调整回执](benchmarks/2026-11-02-document-scope.json)。
现有 `spark-push:media-review` 镜像早于这次界面调整；使用新入口前需从当前源重新构建，尚未替换任何部署。

选择其他房间、退出登录或确认撤权时清空资料视图并取消尚未完成的请求。服务端逐次校验成员与对象权限，
文件名及提取文本按纯文本展示；上传、预览和轮询不改变消息游标或 ACK 语义。

当前限制：网页完整文件校验需要内存；服务端 1 GiB 上限不等于网页支持同等大小。取消后重传同一文件需重命名；
资料列表预览只显示前 20 个画面入口，来源链接可以打开原请求读取的固定画面。分析草稿中的资料 ID/revision 是目标提示，
尚无服务端绑定目标 asset 的独立协议或最终消息的引用协议。任务来源已持久保存，回执只证明读取发生，不证明模型结论正确。
最初的网页入口仅通过受控浏览器检查，见[阶段回执](benchmarks/2026-11-01-multimodal-service.json)。
来源跳转与任务保存的后续结果见[来源关联回执](benchmarks/2026-11-01-source-links.json)。
该回执的 `web_follow_up` 记录时间线入口和正文检索：25 项受控浏览器检查通过；未改后端协议，也未重跑全量 CTest 或真实模型。
后续三种分析草稿的 25 项短浏览器检查与独立真实视频场景见[视频与分析入口回执](benchmarks/2026-11-01-video-focused.json)；
真实视频使用专门验收提示，不宣称这三种中文草稿已分别通过真实模型验收。
原资料播放加入后，26 项受控浏览器检查通过，见 [ASR 与播放回执](benchmarks/2026-11-01-asr-and-playback.json)。
播放复用已授权分片读取，仍受网页 256 MiB 上限约束，完整文件就绪后才显示播放器；不是流式下载。
格式是否能播放取决于浏览器解码支持，不支持时提示下载原文件。当前 ASR 仍可能误识别姓名和术语，任务保存前需要核对。
来源定位播放加入后，28 项受控浏览器检查通过，包含真实 WAV 解码、定位不自动播放和下载期间来源失效；
这里的 HTTP/WS 仍是受控测试数据，不能作为真实服务或模型验收。代码与本地镜像对应关系见
[来源播放回执](benchmarks/2026-11-01-source-playback.json)。
随后三名用户经真实 Chromium 完成 PDF、20 分钟录音、视频的上传、共享库查看、原件播放/下载、
服务重启后的全新浏览器重登和成员撤权，6 组检查通过，见[音频与 Web 回执](benchmarks/2026-11-01-audio-and-web.json)。
这次没有启动 parser 或模型，不覆盖解析结果、来源时间定位与 Agent 分析草稿的真实服务交互；
测试机缺少中文字体，保存的截图也不作为中文视觉验收。
后续任务审核说明与同一草稿格式化函数已打包成本地 `spark-push:media-review`，六个关键静态文件 hash 与工作树一致，尚未部署。
相关 28 项受控浏览器检查与真实短片段诊断见
[转写与任务审核回执](benchmarks/2026-11-02-transcript-review.json)；它不代表完整录音的任务准确性已通过。
随后真实音频链路复用同一草稿格式化函数，经任务 API 保存并在重启后恢复了标题、审核说明、步骤、来源和交接摘要，
见[音频任务恢复回执](benchmarks/2026-11-02-audio-task-recovery.json)。这次是 HTTP/WS 与真实模型验收，
没有操作浏览器；整份回答仍因一个转写术语错误而失败。
后续 [PDF 网页分析回执](benchmarks/2026-11-02-web-media-analysis.json)通过 7 组真实浏览器检查：
上传、隔离 Hermes 的 PDF+代码分析、精确图片来源查看、任务保存/接手/交接、重启后新浏览器恢复与成员撤权。
实际引用图片在 Native 工具、模型请求和浏览器预览中的哈希一致。该定向模式没有调用 Pi 或处理音视频。
[后续视频网页回执](benchmarks/2026-11-02-web-video-analysis.json)也通过 7 组真实检查：
Hermes 的画面与音轨事实正确，引用画面可打开并定位到原视频第 5 秒，实际播放/暂停、重启恢复和撤权均通过。
素材是 10 秒合成视频，长录音网页分析及真实会议准确性仍未验收。

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
空间目录改进没有新增数据库表。资料功能依赖独立增量媒体迁移；网页只是调用既有 API。
MySQL 公共目录使用参数绑定；账号查找复用现有 UserRepository。

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

按用户要求，以功能交付为先，只运行与本次修改直接相关的检查。纯网页修改优先完成以下快速路径：

```bash
node --check web_demo/static/room_media.js
node --check web_demo/static/room_agents.js
node --check web_demo/static/collaboration_ui.js
SPARK_PLAYWRIGHT_MODULE=/path/to/node_modules/playwright node tests/collaboration_browser_test.cjs
git diff --check
```

改动服务端权限、数据库或消息可靠性时补对应构建和集成测试；全量回归及真实模型验收放在阶段集成时，
已有通过结果不因无关的文案/布局调整重复执行。保留未通过、跳过和未运行的记录，不降低真实验收标准。
完整回归命令按需使用：

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
