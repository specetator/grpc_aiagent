# 多模态 Agent 上下文接入进度与设计

状态：2026-11-01，`295fa3a` 之后的开发工作树。上下文代次基础和服务端资料搜索/读取、来源回执已实现；
Pi/Hermes 原生工具接线已实现，视觉语义尚未全部通过；共享删除、任务来源及 Web 仍待完成。独立 Architect 已审查完整接入方向。
[媒体 worker 阶段](media-worker.md) 已完成解析、发布和恢复，完整多模态产品目标仍未完成。
本阶段要将授权资料交给真实 Pi/Hermes，产生可核对引用和持久协作任务，随后接入 Web 与三用户验收。

## 已有可复用入口

| 责任 | 当前入口 | 此阶段扩展位置 |
|---|---|---|
| Agent 请求与权限 | `RoomAgentService::BuildRequests/CheckDispatch/AuthorizeWorkspace` | 持久 request 的 context generation、资料引用与授权 |
| 最终发布 | `RoomAgentService::PublishReply`、`MySqlRoomAgentRepository::Publication` | 首次决定时的 generation 检查；保留已提交决定 |
| 媒体读取 | `MediaService`、`MediaProcessingService` 与现有 Repository | 组合成 MediaContextService，不复制 SQL/权限到 handler |
| Session 与执行恢复 | `execution_namespace`、AgentRouter/Store/SessionScheduler | room/agent/generation 的隔离、原请求恢复准入 |
| Pi 原生工具 | PublicPiFactory、`public-workspace.ts` | 独立媒体 extension，与是否授权代码项目无关 |
| Hermes 原生工具 | `hermes_public_workspace.py`、HermesNativeAdapter | 媒体 capability、精确 allowlist 与原生多模态结果 |
| 持久协作任务 | `room_task_service.cpp`、现有任务 Repository | 权威来源校验、任务来源与失效状态 |

工具参数只能选择查询词、来源 ID 和有界范围。request_id 由受信 Runtime 适配层注入，
Service 从已存 request 取得 room、actor、Agent 和权限代次；聊天正文不能提供身份、URL、工作目录或权限。
调用前后复查正式成员、绑定、工作区及当前代次，结果不确定时拒绝返回受保护数据。

## 1. 上下文代次与发布边界

已新增增量迁移 [`20261101_media_context.sql`](../sql/migrations/20261101_media_context.sql)，
建立独立 `room_context(room_id, generation, updated_at)`，不重写现有 request、task、publication 或原生 Session。
来源回执表由后续的 `20261101_media_context_sources.sql` 增量建立，详情见 [资料 API](room-context-tools.md)。
旧库启用新 generic 请求前先应用代次迁移；新库已包含在 schema.sql。
初始代次为 0；新请求显式保存 `context_generation`。旧请求缺字段只能解释为原代次 0，
不能在重放时升级到当前代次。缺迁移或查询错误对新媒体上下文能力返回不可用，不能当作代次 0 放行。

代次已加入执行 namespace：新显式代次使用 `_c` 加绑定、工作区、项目和代次的摘要；
旧请求缺字段时保持原 `_w` 或旧无后缀 namespace。显式 0 不导入旧 Session，旧目录本身保留。
同一房间、同一 Agent、同一授权代次的不同真实用户共享执行会话；其他房间、Agent 或代次使用不同 namespace。
Router 的缓存、交接摘要、原生 history/todo 恢复必须遵守同一边界，不能只换外层 session_id。
普通人类消息按当前房间权限继续可用；旧代次 Agent 摘要、失效来源的派生内容不自动导入新上下文。
BuildRequests 的背景筛选已实现：Agent 消息根据持久 request 校验房间、bot 身份及原代次；
消息正文自报的代次不授予资格。跨房间/未来消息和旧代次派生消息不能进入新 prompt，普通人类讨论保留。
聊天搜索和直接读取现已复用同一房间/来源/代次筛选；最终答案与任务摘要的来源校验仍待接入。
不能仅凭 namespace 宣称真实 Native history/todo 已完成隔离验收。

当前 Publication 事务锁定 room/user/request。若已有 publication，先核对 digest 并返回原 canonical reply；
只有首次决定才检查当前成员、绑定、工作区和上下文代次。当前实现保持这个区分：

- **尚未形成 publication**：代次失效时，沿原有剥离内容并持久决定 cancelled 的路径处理。
- **已经提交 publication**：保留原正文、digest、消息身份与重放，不能把它改造成另一条“新答案”。

更早的 generic 请求即使没有 workspace 字段，首次发布也进入上述事务；不存在绕过代次的直接成功分支。
如果它已在旧版本持久保存最终消息和 result，则复用 CANN 的原文核对流程收养原决定，验证正文、时间、类型、
结果及消息身份，保留原 msg_seq；原回执/历史不一致时拒绝，不推测成功。CANN 的原任务和发布合同独立保留。

新 generic 请求固定记录原代次；重投原样恢复，不升级到今日代次。同一多 Agent 消息只保存部分请求后若代次变化，
拒绝在新代次补建其余请求。C++/Python 均限制为 0～2^53-1 的整数；只有字段缺失代表历史 0，显式 null/bool/浮点等拒绝。
`ContextGeneration` 缺行返回 0，缺表/查询失败返回不可用；dispatch 的暂时故障不被持久化为取消。
已有 publication 重放不依赖新的 context 查询，未决定的首次发布遇到查询故障则回滚。

现有 `HandleHermesDelta` 在入推送链路前调用 CheckDispatch，新到 Logic 的 generic delta/progress 同样检查代次。
已经授权并进入 Kafka/Comet 队列的帧未增加代次撤回屏障；本阶段不宣称强同步撤销或追回已送达内容。
没有公开代次修改/删除接口；下一阶段的撤销事务必须先锁 `im_group` 再改 `room_context`，与首次发布串行化。

实现入口：`logic/services/room_agent_catalog.cpp`、`room_workspace_service.cpp`、
`logic/repositories/room_agent_repository.cpp`、`cannbot/scripts/room_workspace.py`、`agent_router.py`。
验证命令与证据层级见 [代次基础回执](benchmarks/2026-11-01-media-context.json)。

代次基础阶段的历史结果为：构建通过；CTest 75 注册、65 通过、10 跳过，另以隔离数据库补跑文本 HTTP 和 4 项 SQL，均通过。
SQL 测试实际观察两种 room 行锁等待顺序，覆盖重复迁移不重置代次、缺表失败关闭、旧 generic 原结果收养及拒绝不一致原文。
合同使用受控 Runtime；没有把这些结果算作真实 Pi/Hermes 媒体或三浏览器验收。

```bash
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure
python3 tests/run_mysql_review_tests.py build-wsl
```

runner 创建并清理自己的 MySQL/Redis 容器，不访问个人运行实例。代次基础阶段没有补跑独立 Redis 路由/序号、
媒体 parser HTTP、持久 worker HTTP 和浏览器 fixture 共 5 个环境跳过项；具体名称列在回执中。

旧记录可作为历史显示并注明来源已移除；不将它自动用作新的 Agent 证据。
撤销不能收回已发出的字节或用户已经看过、复制的内容，也不能将“禁止发布”表述为“原生执行已停止”。
停止沿真实 Runtime 控制接口执行，不支持或结果未知时保留明确状态。

## 2. 权威搜索、读取与来源

已新增 MediaContextService 组合已有 RoomAgent/Media/Conversation 授权。内部 adapter 验证传输形状，
沿 Connector 的受保护授权链把 request_id 交给 Service；所有 actor/room/generation 从持久 request 派生。
服务端内部接口为 `/internal/room-agent/context/{search,read,media}`，参数及当前测试边界见 [资料 API](room-context-tools.md)。
下表的工具已向 Pi/Hermes 注册；实际配置、降级保护和未通过的 Pi 视觉检查见 [原生工具](room-context-native-tools.md)。
服务端 HTTP、Native 合同和真实模型验收分别计数。

资料 API 阶段构建通过；CTest 77 注册、66 通过、11 跳过。独立 MySQL/Redis 环境通过新增上下文 HTTP 11 组场景、
原文本 HTTP 与 4 项 SQL，补齐 6 项跳过；剩余 5 项与 Native/Web 边界列在 [资料 API 阶段报告](benchmarks/2026-11-01-room-context-tools.json)。

| 工具 | 模型可选择的字段 | 返回 |
|---|---|---|
| room_search | query、来源类型、有限 limit | 当前房间普通聊天与资料命中、权威来源引用 |
| room_read | 来源 ID、revision、有界片段或时间范围 | 原文/转写、真实行号/页码/段落/时间锚点 |
| room_read_media | 来源 ID、revision、有限 artifact index | 实际 JPEG、原生图像 content、锚点与引用 |

搜索必须在 SQL 中先限定房间、当前 publication 和上下文资格，再 LIMIT；用户输入用参数绑定。
不得先查全局再靠模型过滤。直接读取 message ID 也要确认房间，不能仅凭 ID 存在。
工具没有 project snapshot 时仍可用，代码 read 则继续保持批准项目范围。

消息引用由 `msg_id/msg_seq` 定位；媒体引用由 `asset_id/revision/index` 定位。
页码、时间和 hash 由服务端解析，模型自报的定位信息不作为权限或真实性依据。
request 来源回执记录服务器读取的 canonical 来源；读取后失权仍会拒绝返回内容，回执不证明客户端收到。
最终回复和任务引用的校验是后续工作，必须在当时的授权范围内解析。
回执不保证模型判断正确；视觉结论仍需实际图像与对应问题验证。

## 3. Pi 与 Hermes 原生图像通路

Pi 保留禁用内置危险工具和显式 extension 加载，返回原生 `{type:"image", mimeType, data}` content。
文本和图像不能先统一 stringify 成文本。媒体工具独立于已有代码 read，不开放 shell 或任意路径。

本机 Hermes 源码已只读核对：`/mnt/f/hermes/hermes-agent`，HEAD `67764dc0863349a384c16425e73ee8571f3a94b7`。
该源码树有既有未提交改动，以下以实际读取的工作树为准，不能当作该 HEAD 的干净发行验收；本阶段未修改它。
以下是接口存在的源码证据，**不是模型 E2E 通过证据**：

- `tools/vision_tools.py::_build_native_vision_tool_result` 使用 `_multimodal` 字典，content 中带 image_url/data URL。
- `agent/tool_executor.py::_commit_tool_result` 调用原生内容转换、追加 canonical tool message 并 flush SessionDB，随后投影 tool.completed。
- `agent/vision_message_prep.py::_tool_result_content_for_active_model` 可能按模型能力、provider 或已知拒绝记录降级成 text_summary。
- `agent/agent_init.py` 为每个 Agent 创建 TodoStore；`run_agent.py::_hydrate_todo_store` 可从已有历史恢复 todo。

因此新增独立媒体 capability 与精确工具 allowlist，保留任意顶层 images、session/root 参数的现有限制。
必须用隔离公共 Profile 进行真实探针：原始工具含图、canonical/provider 输入实际含图、随机无文字图像回答正确，
三项分别核实。静默降级或只返回 OCR 不能计作视觉理解；不修改私人 Profile 或 Hermes 核心 Agent Loop。
关闭 memory 并不证明 todo/history 隔离，需两房间、两 Agent、代次切换与重启的真实测试。

原生接线已完成：显式 capability、无代码项目的媒体读取、实际图片投影、实例级 todo hydration 兼容与去图错误保护。
当前同图测试中 Hermes 图像/重启/隔离通过，Pi 图像与来源绑定正确但视觉答案错误；保留失败回执，不宣称整个阶段通过。
详见 [原生工具报告](benchmarks/2026-11-01-room-context-native.json)。

## 4. 删除、任务与 Web

共享删除入口在代次、工具复查、上下文筛选和首次发布守卫全部接通后开放。
删除事务沿 room → asset 锁顺序校验权限，撤销 publication、封住运行中的 processing、递增代次并保存幂等回执。
重复删除不重复递增。物理文件在后续 GC 中按固定文件锁清理，成功后才释放配额；不把文件 IO 塞进授权事务。

任务新增 `source_refs`，从 request 的真实来源回执解析并持久保存。创建任务的用户来自 Bearer，
原 request.actor 只表示原发起者；B 接手 A 的成果不能被记成 A 操作。
在途 Agent 的新成果或来源变更需要当前代次；旧代次不得写入新的派生内容。
已有任务继续保留历史，当前成员可以按原角色/CAS 更新人工进度并看到“来源不可用”；
不能因源文件删除永久锁死所有人工协作，也不能把失效摘要自动喂给新 Session。

Web 使用既有上传/续传接口和消息时间线，增加资料库、处理状态、页码/时间轴预览、来源引用和错误恢复入口。
房间文字、资料与任务查询均走当前成员权限；移除成员或上下文失效时清理当前显示/自动上下文缓存，
最终消息仍按原 msg_seq、ACK、outbox、历史和离线协议处理。

## 实施和验收顺序

| 模块 | 首次交付 | 验证 |
|---|---|---|
| 代次合同（已实现基础） | 增量表、canonical scope、授权、背景筛选与 namespace；暂不开放删除 | C++/Python 合同、真实 SQL 迁移/锁竞争/旧结果收养；实际 Native 隔离留待 E2E |
| 服务端工具 | MediaContextService、聊天搜索、权威来源回执 | 真实 HTTP/SQL、跨房间 ID、伪造身份、读取期间撤权 |
| 原生适配 | Pi/Hermes 三工具、能力检查与真实图像结果 | 隔离模型探针；无 project；两房间/两 Agent/session/todo |
| 删除和来源 | 撤销事务、旧上下文筛选、任务来源与失效显示 | 删除/读取/首次发布竞态；旧 publication 重放不变；真实重启 |
| Web 与完整产品 | 资料交互、引用、恢复、任务协作 | 三独立浏览器、真实 Pi/Hermes、20 分钟音频、PDF+代码、视频视听 |

最终验收必须检查：录音尾段决策进入持久任务；视频回答引用实际帧与音轨时间；PDF 图表与批准代码被原生读取；
所有成员看到一致最终结果；断线、重登、重启、Kafka 重投不串房间、不重做未知副作用；旧 CANN/私聊/消息同步回归。
合同、实际依赖测试、原生模型探针和完整 E2E 分开记录。当前仅代次基础落地，不能将其余设计模块或完整目标计为完成。
