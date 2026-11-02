# 房间多模态协作

适用：从 `295fa3a` 开始的多模态升级工作树。更新至 2026-11-02。
2026-11-02 范围调整：按用户决定，本阶段只交付文本、PDF/DOCX、图片及其 Agent 分析、检索与任务来源。
音频、视频暂缓，不再要求提供录音，也不再以此前 ASR 失败阻塞当前阶段。已有实现、数据及原始失败回执保留。
这次范围调整不表示音视频通过，也不是新的部署或 v0.1.0 发布验收。
当前图文交付的需求与证据对应关系见[范围调整回执](benchmarks/2026-11-02-document-scope.json)：
网页已收敛入口，27 项受控浏览器检查通过；现有真实 PDF/Hermes/任务/恢复证据继续按原范围引用。
Pi 的历史视觉误判仍保留，图像理解的成功证据来自 Hermes，不能宣称所有模型均通过视觉验收。

2026-11-02 源码整理：三个媒体服务共用字段解析，网页资料与固定来源共用图片预览，知识索引压缩格式；
授权、租约、幂等及 Worker 指纹行为保留。定向构建通过，`media_context_service_test` 通过，
`tests/run_mysql_review_tests.py build-wsl` 的 6 项真实 MySQL/Redis/HTTP 集成通过；
浏览器契约检查 29 项通过（模拟 HTTP/WS，包含两种图片预览及 URL 回收），知识来源校验 208 项通过。
本轮未重跑完整 CTest、Kafka 或真实模型验收；历史验收仍按各自源码指纹和范围解释。

## 长期方向与实施记录

一个房间中的文字、PDF/文档、图片、20 分钟会议录音和视频都应成为可持久保存、
按当前成员权限检索、供原生 Pi/Hermes 使用并可引用出处的协作资料。
Hermes 要能把 PDF 与批准项目代码一起分析，Pi 要能从真实录音生成决策与持久任务，
视频分析必须结合真实画面和对应音轨。仅上传、OCR、mock 模型成功均不满足目标。

源码审查发现旧附件只接收最多 4 MiB 的图片，HTTP 总体积上限 8 MiB；已有图片消息、
对象级房间权限、Agent 请求账本、SessionScheduler 和受限原生代码读取工具，
但没有大文件续传、解析任务、房间资料索引、转写及视频时间轴。

本次设计经独立 Architect 审查，沿用 Logic Adapter → Service → Repository、
现有 MySQL 与附件卷，以及 Router/Store/Scheduler/Bridge。解析 worker 不实现 Agent Loop。
worker 之后的 [上下文接入](media-agent-context.md) 已实现代次存储、请求/授权、背景筛选与 namespace 基础；
[服务端资料 API](room-context-tools.md) 已实现房间聊天/媒体搜索、片段/图片读取和持久来源回执。
[原生工具接线与降级保护](room-context-native-tools.md)已实现，Pi 同图识别仍失败；
[来源回查及任务来源](room-context-tools.md#成员回查与任务来源)已接入，最终消息引用协议和共享删除仍待实现。
上传、生产 worker 和原生工具已串入同一套三用户真实服务链路。首次完整运行中，三份资料发布成功，Hermes 完成 PDF 与代码关联读取；
Pi 读取了 20 分钟合成录音的转写，但严格负责人/行动验收未通过，当次视频分析及后续恢复步骤未跑到。
Web 资料入口已实现。内部代理、独立凭据与可选测试入口见[服务链路说明](multimodal-service-e2e.md)，
实际通过范围和失败记录见[阶段回执](benchmarks/2026-11-01-multimodal-service.json)。
已提交 publication 的幂等决定保留；真实图像进入模型与回答正确分别验收，
不能把 OCR 或文本降级当成视觉完成。

| 阶段 | 范围 | 当前状态 |
|---|---|---|
| 资料上传与授权 | 分片、续传、原文件完整性、房间库、处理任务入队 | 文件、Service/SQL、真实 HTTP 上传重启已验证 |
| 解析与索引 | PDF/DOCX/text、图片安全解码/OCR、ASR、视频帧/音轨、租约恢复 | 文本/媒体 worker 与 HTTP 发布已贯通；当前成员可读图/文本，20 分钟合成音频强杀恢复、运行中失效和有限派生物 GC 已验证；不等于 Agent 理解 |
| 原生理解与引用 | Pi/Hermes 媒体工具、真实视觉、房间文字检索、任务来源 | 资料 API、原生工具、来源回查与任务持久关联已实现；Hermes 图像/重启/隔离有独立回执，Pi 同图识别失败；最终消息结构化引用待接入 |
| Web | 上传进度与恢复、资料列表/正文检索、页码预览、下载和分析入口 | 当前入口只开放图文；PDF+代码及任务流程 7 组真实检查通过；已有音视频原件可下载，播放和新分析入口暂缓 |
| 真实验收 | 当前三用户图文协作、PDF+代码、来源与任务恢复、撤权 | 对应真实回执保留；音视频相关结果属于延期功能的历史记录，不计为当前阶段的成功或阻塞项 |

视频定向结果与新的 Web 分析目标入口见[阶段回执](benchmarks/2026-11-01-video-focused.json)。
其三账号客户端是 HTTP/WS，不是浏览器；未选音频/PDF，不能覆盖此前的音频准确性失败。
媒体 worker 现有[独立启停入口](media-worker-operations.md)，私有配置和 systemd 用户服务保持原恢复协议。
对应[运维阶段回执](benchmarks/2026-11-01-media-operations.json)含 15 组真实 worker 检查；不覆盖新的长录音 Agent 理解。
[音频与 Web 回执](benchmarks/2026-11-01-audio-and-web.json)保留完整 turbo 音频失败和三浏览器原件流程成功；
后者没有启动 parser 或模型，不能覆盖前者的准确性问题。
随后[音频任务恢复回执](benchmarks/2026-11-02-audio-task-recovery.json)证明同次实际 Pi 输出中独立合格的任务可以保存、接手，
内容、来源和交接摘要在完整服务重启后保持一致。整份回答仍因首个检查点的术语错误而失败；
任务持久化成功不改变该结论。测试按受影响路径执行，后续无需为网页修改重复转写整段录音。
[PDF 网页分析回执](benchmarks/2026-11-02-web-media-analysis.json)进一步覆盖三名用户的完整页面操作，
以及引用图片进入真实 Native 模型请求的哈希证据；任务内容、接手人、状态和版本均在新浏览器登录后核对一致。
[视频网页回执](benchmarks/2026-11-02-web-video-analysis.json)覆盖相同页面中的真实视听分析、引用画面和原视频定位播放，
并核对重启与撤权；没有使用视频通过结果覆盖长录音失败。
最后的[通用会议背景诊断](benchmarks/2026-11-02-asr-domain-context.json)仍未纠正原始短片段中的技术术语，
没有修改正式 ASR 配置。该失败属于范围调整前的记录；用户已暂缓音视频，当前不再等待录音材料或继续调优。

## 数据与状态

原文件沿用 `att_*` 随机身份、`attachment` 元信息和现有附件数据卷。
该行是上传者、房间会话、展示文件名、声明大小及最终校验 SHA-256 的唯一来源。
`room_media_asset` 增加资料类型、分页序号及上传幂等身份；`room_media_chunk` 保存分片清单；
`room_media_job` 保存待处理任务及后续租约/attempt 字段。

增量迁移：`sql/migrations/20261101_room_media.sql`，扩展附件大小/MIME 字段并新增三表。
不清空旧附件、消息、任务或原生 Session。未迁移的旧库仍可启动基础 IM，多模态 API 返回不可用。

当前上传状态：`uploading → queued`；上传者可以把未完成上传经 `cancelling` 取消为 `expired`。
`queued` 表示原文件完整性检查通过、一个处理任务已事务入队，**不表示已转写或已理解**。
文本处理已实现 `running / ready / failed`，外部推理的 `unknown` 留给后续原生执行恢复。
文档容器/音视频 magic 只是初筛；格式解析、解码和能力检查通过后才能进入 `ready`。

上传开始时固定房间。未完成草稿只有上传者在仍为成员时可查询/管理，不出现在共享列表。
`finish` 是明确的“共享到该房间资料库”动作，不依赖是否另外发送聊天引用；之后现有成员可读原文件。
资料消息将只引用 ID，不把大字节或转写全文放入 WS/Kafka 消息包。

当前默认限制：原文件 1 B–1 GiB、分片 1 MiB、每房间 10 GiB 预留容量、同用户同房间最多 4 个未完成上传。
初始化会先预留声明容量；分片长度由总大小与下标决定。完整文件 MIME 支持清单在
`logic/services/media_service.cpp`，声明为可上传格式不代表解析器已经实现。
上传者可重新列出自己的未完成上传，并取消回收其分片、组装文件与崩溃遗留临时文件。
自动过期回收及无人继续操作时的垃圾清理仍需随 worker 生命周期完成。

## API 与权限

以下接口均为 `POST /api/room/media/<operation>`，使用现有 Bearer 身份及 `{code,message,data}` 包络。
HTTP 200 不一定等于业务成功，应检查 `code`。每个请求体最大 2 MiB，保留全局 HTTP 上限。

| operation | 字段 | 语义 |
|---|---|---|
| init | room_id, client_upload_id, name, mime, bytes, sha256 | 当前成员预留上传；同 ID 同输入返回原对象，改内容冲突 |
| chunk | id, index, data | 上传者提交标准 base64；乱序可接受，已存位置的不同字节冲突 |
| status | id | 状态、固定元数据、已存分片下标/大小/hash，用于重连续传 |
| finish | id | 验证全部分片、整体 hash 和文件头，原子发布原文件并入队一次 |
| abort | id | 仅上传者取消未共享上传；不把删除共享资料混同为取消 |
| list | room_id, before?, limit? | 当前成员分页读取已共享资料，limit 默认 50、最大 100 |
| uploads | room_id | 仅列出当前成员自己的未完成上传，用于丢失客户端状态后的恢复/取消 |
| read | id, offset?, length? | 当前成员读取原文件有界区间，最大 1 MiB，以 base64 返回 |

普通成员可以上传和阅读已共享资料，只有上传者管理自己的上传；owner/admin 不因角色获得任意文件路径或 Runtime 配置权。
每次请求都重新走 `ConversationService::CheckAccess`；读文件后、返回结果前再次检查。
移除后，上传者也失去续传、原文件和元信息访问。旧图片 API 拒绝非就绪、非图片或超过旧 4 MiB 上限的对象，
已解析的 Markdown 也不能冒充图片引用或通过旧图片下载接口读取。

客户端重试保持 `client_upload_id`，服务器用 actor/session/client ID 的规范 JSON hash 定位原上传。
所有 SQL 值参数绑定。用户展示名不会用于存储路径。
文件通过生成名、目录 fd、`openat(O_NOFOLLOW)`、普通文件与单硬链接检查访问；
分片和原文件先 fsync，再 `renameat2(RENAME_NOREPLACE)` 发布并 fsync 目录。
finalize 在 SQL commit 前发布的文件，重试时必须与同一清单/hash 一致，不能覆盖不同字节。
清理分片失败不撤销已经提交的原文件和任务；取消草稿先持久提交 cancelling，清理全部受控文件后才提交 expired 并释放配额。
全部文件写入/完成/取消操作先取得同一资产的固定 inode `flock`，再取得 SQL 行锁；
SQL 断连不会让第二个进程越过仍在进行的文件操作。锁文件不删除。支持现有单机共享附件卷，
不宣称支持分离存储卷或没有这些文件锁/原子改名语义的文件系统。

这些动作不会生成 accepted/delivered ACK，也不推进 `msg_seq` 或离线游标。
当前 finalization 仍是同步的流式校验，有界内存但占用 HTTP worker；大文件负载和异步完成路径尚待验证。

## 解析与检索设计

### 已实现的文本处理协议

第二个增量迁移 `sql/migrations/20261101_media_processing.sql` 新增领取账本、不可变 revision、
暂存片段与当前发布指针四张表。顺序为上传迁移 → 解析迁移 → `20261101_media_artifacts.sql` → `20261101_media_publication.sql`；`schema.sql` 包含全新数据库结构。
文本 pipeline 为 `media-text-v1`，仅允许 `text/plain` 和 `text/markdown`，代码指纹由运维端批准。
新增媒体 profile、不同指纹及真实解析发布协议见 [媒体发布](media-publication.md)。
`processing.text_pipeline_supported=false` 只表示不适用于文本管线，不能据此判断媒体管线的完成状态。

worker 使用与普通登录 token、Agent Connector key 分离的凭据。Logic 配置
`SPARK_PUSH_MEDIA_WORKER_KEY`（32–256 字节）和 `SPARK_PUSH_MEDIA_PIPELINE_SHA256`，未配置时内部 worker API 关闭。
指纹来自 `python3 cannbot/scripts/media_text_worker.py --fingerprint`；不得由普通用户或聊天内容配置。
把同一凭据保存在 worker 专用、当前运行账号拥有的 0600 文件中，状态目录为 0700。例如：

```bash
python3 cannbot/scripts/media_text_worker.py \
  --origin http://127.0.0.1:9101 \
  --key-file /srv/spark-media-worker/worker.key \
  --state-dir /srv/spark-media-worker/state
```

这是运维配置接口，尚未加入 Compose 默认启动。远程部署需受保护的内部连接或 HTTPS。
worker 不读取私人 Agent 配置，不执行 shell/模型，不向原文件写入；HTTP 客户端不使用代理或重定向。

内部 `POST /internal/media/{claim,check,heartbeat,source,stage,complete,fail}` 全部由 Service 验证独立凭据。
`claim` 接收时间戳加随机数的 `claim_id` 与已批准指纹；其他操作使用服务端返回的
`id/attempt/revision/lease_token`。租约 token 与公开 revision 分别随机生成，公开查询不返回 token。

- 领取账本与新 attempt 在一个事务提交。同一 claim 重放始终指向原分配；两 worker 不会同时拥有有效租约。
  空队列没有分配，返回 `idle`，不保存空轮询记录；下一次轮询可使用新身份。
- 新分配身份允许一天重试窗口；更旧身份只查原账本，绝不新分配。存在则返回原结果；不存在返回明确 410，
  worker 才可清除此旧身份。通用 409、网络错误和指纹冲突不允许擅自丢掉原任务。
- worker 在请求前 fsync 保存 claim，进程锁防止两个实例共享状态目录。重启后先找回原 assignment。
  现有 claim 的代码指纹与运行文件不同会明确停止，保留状态供运维恢复，不冒用旧代码指纹执行新实现。
- 服务端以 MySQL 时间维护 120 秒租约，心跳不能复活已过期租约，每次 attempt 最长一小时。
  失联重取最多三次，随后持久失败；过期/失败暂存文本被回收。领取扫描负责发现失联，当前没有独立清理守护进程。
- 写入锁顺序统一为 claim（仅领取）→ asset → job/revision → fragment。旧 attempt 的源读、心跳、暂存、失败和发布均受 fencing 限制。
- 单段最多 16 KiB、2048 段；累计文本最多原文件字节数与 16 MiB 中的较小值。预算检查与插入同事务，
  不依赖 worker 自报；同下标同内容幂等，不同内容冲突。
- fragment 是严格的 `{kind:"text",text,anchor:{line_start,line_end}}`，不接受路径、room 或 actor 等附加字段。
  行号从 1 开始，以 LF 划界，保留 CR、BOM 和末尾换行。超长单行、非法 UTF-8、超量输出均明确失败。
- `complete` 以服务端存储的有序片段 hash 数组计算 SHA-256，验证数量、连续下标、连续行号，
  重组全文必须与原文件长度和 SHA-256 完全一致。revision、job、attachment 与发布指针在一个事务更新。
  完成响应丢失后，相同身份和 manifest 可以查询原回执；不会再次发布或重新领取。

新增公开接口仍位于 `/api/room/media/`，每次操作前后都复查正式成员关系：

| operation | 字段 | 结果 |
|---|---|---|
| processing | id | 任务状态、attempt、有限错误码、当前发布 revision；不暴露凭据 |
| fragments | id, revision?, offset?, limit? | 已发布的不可变片段与行号，limit 最大 20；暂存不可见 |
| search | room_id, query, limit? | 当前房间已发布文本的字面子串匹配，支持中文和 SQL 特殊字符；limit 最大 20 |

检索在 SQL 内先按房间、ready 与当前 revision 过滤；不先全库查询再在应用层过滤。
当前是资料文本的有界结果查询，尚未包含普通聊天消息、语义排名、跨片段匹配或多页搜索游标。
失败任务仍保留授权的原文件；用户重试、共享资料删除及其 context generation 必须与后续 Session 隔离一起接入。
当前没有开放半套删除接口，也没有将文本 ready 扩大为文档/音视频已理解。

<a id="derived-storage-foundation"></a>
### 派生物存储与发布

第三个增量迁移 `sql/migrations/20261101_media_artifacts.sql` 增加
`room_media_revision_storage` 和 `room_media_artifact`；旧数据不清理，重复执行保留原记录。
上传启用检查也要求预留表存在，缺少迁移时仅关闭新媒体入口，基础 IM 继续使用。

`logic/repositories/media_artifact_repository.cpp` 提供 Reserve / Stage / Complete / Reclaim 及受授权读取。
它复用 `MediaBlobStore` 的文件锁、有限 IO、hash、fsync 和 no-replace；
Complete 验证整个 manifest 后才能把 PDF/音视频置为 ready。文本 worker 不调用媒体管线；独立 [媒体 worker](media-worker.md) 已实现并验证，Compose 默认启动尚未接入。

- 一个 revision 在任何派生文件写入前，持久预留 176 MiB：图片最多 128 MiB、文本 16 MiB，
  其余用于有界元信息和暂存。预留未回收前一直计入同房间容量。
- **10 GiB 是新资料库的逻辑容量**：`room_media_asset` 原件加尚未回收 revision 的预留。
  旧独立图片上传没有加入此表，不在这个限额内；该数值也不是 MySQL 实际占盘或宿主磁盘水位保证。
- 原件 Create 与派生 Reserve 都先锁房间，再计算聚合用量。事务单独使用 READ COMMITTED，
  避免获取房间锁前的查询固定旧快照；不修改连接池连接的 session 隔离配置。
- Stage 使用固定 revision 文件锁，再锁 asset/job/revision/storage；先提交不可变的
  `(revision,index)` 元信息和服务端确定性文件 ID，再写 JPEG。SQL 断连不会换一个新文件名。
- 重放已安装索引时先持久改为未安装。写任何新索引前，回收该 revision 所有未完成索引的
  chunk 和组装 partial，保留已存在的正确原图；这样跨索引崩溃不会积累另一整份图片分片。
  当前写入最多额外保留一个 1 MiB chunk 和一个 1 MiB 组装临时文件。
- Logic 独立检查单图 hash/尺寸、单幅 RGB baseline JPEG 头与 scan、页码/时间/段落锚点。
  这是存储输入合同，不等于完整图像解码或整个文档/视频 manifest 验证。
- GC 先事务标记 `gc_pending` 并封住旧 revision，释放 SQL 锁后才等待固定文件锁；
  文件和暂存删除并 fsync 后，最后一个事务才释放 charge。任一步失败保留可重试状态。
  live、ready 或仍有 publication 的版本拒绝回收；固定锁 inode 永不 unlink。

2026-11-01 [真实测试与源码回执](benchmarks/2026-11-01-media-artifact-storage.json)：
真实 MySQL/HTTP 5 项通过；包含连接终止、并发额度竞争、锁等待后的当前用量、
文件清理失败与最终 SQL 回收回执失败。全 CTest 71 项中 64 通过、7 环境跳过；
其中 5 项已在独立数据库环境补跑通过，两个独立 Redis 测试此次没有补跑。

以上是存储基础阶段的历史回执。后续已接入 manifest/coverage 核对、预留收敛为实际费用和公开派生物读取，
协议与独立验证范围见 [媒体发布](media-publication.md)。后续的生产 worker 续租/接管与有限派生物 GC
见 [媒体 worker](media-worker.md)；[上下文代次基础](media-agent-context.md)、原生资料工具和 Web 入口已接入，共享删除仍需实现。

### 后续多模态处理

隔离解析器的当前代码、时间轴、管理员预置与可重复测试见 [媒体解析运行说明](media-parser-runtime.md)。
它的本地输出已由真实 HTTP 测试发布到房间；实际用户资料仍需持续运行的生产 worker，不能仅凭上传完成就标记 ready。

worker 经内部 Logic 服务领取带 attempt/fencing token 的租约，心跳、提交和失败都由 Service/Repository 持久化。
过期 worker 不得发布；输出先写独立不可变 revision，全部必需阶段成功后再原子切换当前索引版本。

| 内容 | 处理内容 | 保留出处 |
|---|---|---|
| 房间文本 | 授权消息检索与上下文裁剪 | msg_id、msg_seq |
| PDF | 文本提取、必要时页面渲染与 OCR | asset/hash/revision、页码、可选坐标 |
| DOCX/text/code | 段落或行提取 | 段落/节/行范围 |
| 图片 | 安全解码、受控缩放、OCR、原生视觉分析 | 原图/派生图 hash 与可选区域 |
| 音频 | 格式规范化、真实 ASR、分段转写 | 原始媒体的起止毫秒；不虚构说话人身份 |
| 视频 | 时间戳帧采样、音轨、ASR、按需补帧和原生视觉 | 帧 PTS、音轨片段与原视频时间映射 |

初期检索复用 MySQL，先按房间与有效 revision 过滤，再排名和 limit；支持中文的词法策略须实际验证。
源文件、模型描述、工具输出和聊天内容都是不可信数据，不能指定 actor、工作区、网络地址或执行权限。
外部推理出现回执不明时记 `unknown` 并核对原执行；本地可重复的提取阶段与有副作用工具执行区别处理。
解析子进程必须限制输入/输出、CPU/内存/时间和协议，无网络、无任意宿主目录读取。

## 原生 Agent 接入设计

新增受限 `room_search`、`room_read`、`room_read_media` 工具。模型只传 opaque asset ID、查询词和有界选择器；
身份/房间来自现有可信 request scope，通过 Connector → Logic 重新授权，不接受模型提供的 room/user/path。
授权前后都复核完整 scope；最终消息与持久任务仍走原 request_id、Router/Store/Scheduler 和发布账本。

已核对本机原生入口：Pi ToolResult 的 content 支持 TextContent/ImageContent；Hermes
`vision_tools._build_native_vision_tool_result` 构造 `_multimodal` envelope，原生 tool executor
交给模型专属 serializer。必须验证实际模型没有退化成文字摘要；仅源码存在接口不能证明视觉 E2E。
不给公共 Agent 开放原生任意 `vision_analyze` URL/路径能力，受限工具只提供授权媒体字节。

PDF+代码组合使用现有 `workspace_read`；音频决策/待办引用转写时间戳，再使用既有任务 revision/history 保存。
视频回答必须同时引用画面观察和音轨内容，不能以一段音频总结配几个缩略图代替视听理解。

资料删除或可见范围改变时，需要 room context generation 进入原生 Session namespace，
并在旧请求授权/发布处拒绝陈旧 generation，避免仅撤销工具权限却继续使用已缓存私有内容。
不迁移私人 Hermes Profile、记忆、工作区，不改变旧 CANN 或私人会话语义。

## 验收矩阵与证据要求

| 项目 | 必需证据 |
|---|---|
| 大文件续传 | 实际 20 分钟媒体分片中断后恢复；冲突块拒绝；整体 hash 匹配 |
| 文件安全 | 路径/软硬链接/错误 MIME/hash、坏文件/压缩炸弹/外部协议均拒绝或明确失败 |
| 房间授权 | 非成员/撤权成员不能上传、续传、检索、读取原文件/派生片段/引用或调用媒体工具 |
| PDF + 代码 | 真实 Hermes 回答同时依赖 PDF 内容和批准源码行，引用可回查 |
| 视觉 | 随机无文字图片的形状/空间关系实测，证明确实传入图像块 |
| 音频 | 真实 20 分钟音轨包含后段内容；Pi 的决策和持久任务引用正确时间戳 |
| 视频 | 真实视频含与语音相关的视觉变化；回答使用两种证据并引用正确时间 |
| 检索 | 无显式附件 ID 也能发现房间旧资料/文字；另一房间不可检索到 |
| 恢复 | 上传、解析、发布前后杀进程；过期租约无法发布；重启后只有一份有效结果 |
| 会话隔离 | 不同房间/Agent 不串内容；删除后新会话不泄露旧缓存 |
| Web | 三个独立浏览器完成上传/Agent/任务/断网恢复，看到一致最终时间线 |
| 回归 | 旧图片、CANN、私聊、ACK/outbox/seq、任务 CAS、原 Run unknown 恢复均保留 |

已新增 `media_blob_store_test`（真实本地文件）与 `media_mysql_integration_test`（opt-in 真实 MySQL + Service）。
后者通过 `python3 tests/run_mysql_review_tests.py build-wsl` 启动独立临时数据库，不操作个人环境。
这两项不包含 HTTP 身份认证、媒体解析、浏览器或真实模型，不能据此宣称多模态目标完成。

后续阶段在本文登记真实运行结果、源码指纹和未覆盖项。原项目测试及新增能力均需随实现推进回归。

Pi 的真实隔离图片能力证据见 `docs/benchmarks/2026-11-01-pi-media-capability.json`：
原生工具返回三张无文字随机图片，实际模型正确识别形状、颜色和位置，正常终态及图片块数量均被校验。
它使用已授权的默认 Hermes 模型访问凭据，但执行运行时是隔离 Pi；私人配置 hash 保持不变。
这不是 Hermes Native 或房间媒体工具 E2E。最初个人 Pi relay 模型返回 upstream 502 的失败也保留在阶段报告中。

### 2026-11-01 文本阶段验证

完整记录与源码 SHA-256 见 [阶段回执](benchmarks/2026-11-01-media-processing.json)。
全构建成功；启用既有 Playwright 合同测试的 CTest 注册 69 项，63 项通过、6 项基础设施测试默认跳过。
随后在独立临时容器中补跑真实 HTTP/MySQL/Redis 1 项、MySQL 回归 3 项、Redis 回归 2 项，全部通过。
浏览器合同使用受控 HTTP/WS fixture，不代表新的媒体 Web UI 已验收。

新的 HTTP 用例经过正式注册、Redis token、私有空间邀请接受及成员授权服务，验证超过 1 MiB 的上传续传、
完整文字校验、中文/特殊字符检索、同 claim 与不同 claim 并发、累计预算、错误行号/manifest、失败原件保留及撤权。
实际强杀并重启 HTTP fixture；另外实际运行生产 `worker.once()` 的子进程，在首段提交后强杀，再由正式 CLI 恢复，
确认 publication 从 0 到 1、attempt 不增加、全文不变。

租约到期和硬截止由 SQL 时间控制注入；旧 claim 账本由测试构造；完成回执丢失在真实提交后控制抛出客户端错误。
没有据此声称真实数据库/Redis 重启、断电或自然网络故障均已覆盖。读取返回前恰好撤权的专门竞态测试仍待补充。
独立 Reviewer 的两项阻塞（重复查询造成 lease 状态变化被忽略、超过一天的 claim 永久卡住）已修复并复审。

下一阶段依赖：安全的真实文档/图片/音视频解析、派生图像存储和统一时间锚点；随后接入原生媒体工具、
删除后的 Session generation、普通聊天检索及 Web 资料流。完整多模态目标继续进行，不标记为完成。
