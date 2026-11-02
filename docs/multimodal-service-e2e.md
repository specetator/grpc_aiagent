# 多模态真实服务链路

适用范围：当前开发工作树，更新至 2026-11-02。复用 `tests/public_room_runtime_e2e.py` 的三账号、
真实 Compose、Pi/Hermes、Kafka 重投和持久化恢复流程；它是 HTTP/WS 协议测试，不是浏览器验收。
测试结果需查看实际回执，脚本存在或镜像构建成功不代表该场景通过。

2026-11-02 用户明确暂缓音频、视频。本阶段验收聚焦文本、PDF/DOCX、图片、授权读取和任务来源；
不再运行长录音/视频用例，也不再请求录音材料。本文音视频结果保留为历史记录，未通过项不计为当前图文阶段阻塞。
当前网页已隐藏音视频新上传、播放和分析入口；历史原件下载及后端兼容路径保留。
对应当前源码的 27 项受控浏览器检查及既有真实证据映射见[范围调整回执](benchmarks/2026-11-02-document-scope.json)。
已有 `spark-push:media-review` 镜像未包含这次入口调整，真实服务复验前须重建；旧音视频命令只供未来恢复该范围时使用。

## 当前交付与结果

已接通正式 Web 与旧聊天室的上传/续传、资料列表、处理状态、文字及图片预览、原文件下载和 Agent 分析草稿。
操作方式见[Web 客户端](web-collaboration-client.md#房间资料)。本地 `spark-push:media-sources` 已包含来源关联与正文检索，
未推送或替换现有部署。后续本地镜像 `spark-push:media-playback` 已包含三种分析草稿、原文件播放、
转写与来源的时间定位入口；六个关键静态文件的镜像内 hash 与工作树一致。这是打包检查，不是浏览器真实服务验收。

[2026-11-01 阶段回执](benchmarks/2026-11-01-multimodal-service.json)保留三次真实运行的失败：
第一次是公共内部路由的状态码，第二次是 ASR 英美拼写差异，第三次在解析带 Agent 展示前缀的 JSON 回答时停止。
前缀解析已经修复，但原始转写仍把 Alice/Bob 的部分职责识别错误，不能将修复解析器当作会议任务验收通过。
第三次真实运行已完成三用户资料上传、真实解析、Hermes PDF+代码读取以及 Pi 读取转写；没有跑到视频 Agent、
媒体任务保存、媒体请求重投与重启恢复。整体 `ok` 仍为 `false`。

按用户要求，网页改动只做短浏览器检查；真实模型场景现在支持按 PDF、音频、视频单独执行，避免反复运行无关部分。
当时的浏览器 25 项检查使用受控 HTTP/WS，不能替代真实三浏览器媒体验收。此前 CTest 与真实 SQL 结果按原运行范围保留。

随后的视频定向运行已通过 13 组真实检查，见[视频与分析入口回执](benchmarks/2026-11-01-video-focused.json)。
三账号经真实上传/解析，Hermes 结合两幅实际 JPEG 与音轨给出正确结果；图片 hash 在原生工具和成功的模型请求中一致。
同一次运行完成 Kafka 重投、成员撤权、Native 重启、Compose 完整重启与资料/最终消息恢复；退出码为 0，临时环境已清理，私人配置未变。
第一次定向运行因错误地从 Native 文本历史核对 JPEG 而失败，原始失败回执保留；第二次复用已有原生观测后通过。
本次只选视频，没有重跑 PDF 或 20 分钟音频；原 ASR 负责人识别问题、Pi 独立视觉误判仍未解决，不能标记整体多模态验收完成。

后续增加了可选固定 turbo ASR 模型与网页录音/视频播放，见 [ASR 与播放回执](benchmarks/2026-11-01-asr-and-playback.json)。
同一失败录音的 30 秒诊断只修正了负责人，术语仍有误，且更慢；因此保留 small 默认配置，未重复长录音 E2E。
网页播放器使用授权原文件与哈希校验，当时 26 项受控浏览器检查通过。
来源定位播放加入后，28 项受控检查通过；本地新镜像已包含这些页面，但没有部署到现有服务。

最新[音频与真实 Web 回执](benchmarks/2026-11-01-audio-and-web.json)区分两次独立运行：

- `audio` 定向真实链路使用 turbo 模型，原 20 分钟录音处理约 575 秒，发布 381 个片段，三个检查点均存在，
  三名用户都收到 Pi 的完成回复。但首段术语仍误识别为 `QBAC Pressure`；回答还把附近普通讨论和未指定负责人事项
  混入检查点，严格负责人/行动验收失败。原运行退出码为 1，未执行音频任务保存及后续恢复，不能标为通过。
  验证器已能将空负责人报告为明确失败，10 个断言反例检查通过，不改变原失败结论。
- `web_media` 使用新 `spark-push:media-playback` 镜像，三个独立 Chromium 经真实页面注册、邀请、分别上传 PDF、
  20 分钟 WAV、视频，查看共享库、播放原件并下载哈希一致的 PDF；实际 Compose 重启及全新浏览器重登后保留资料，
  移除 Bob 后清空播放器且三种原文件访问均返回 403。6 组检查通过，退出码为 0。
  首次运行只检查媒体元信息；随后在同一窄场景中用原生控件实际启动、暂停播放，确认进度超过 0.2 秒且没有媒体错误，
  重启后再次播放 WAV，6 组检查仍通过。两次原始回执分别保留，不把元信息加载等同于播放成功。
  此场景没有配置 parser 或 Agent，不能证明浏览器中的转写、来源定位或模型分析。两个测试环境均已清理。

[后续短片段诊断](benchmarks/2026-11-02-transcript-review.json)发现，完整 large-v3 模型及其 beam=1 解码仍识别为相同的错误术语，
因此没有加入部署配置或升级默认模型。实际 Pi 对保留的转写明确返回术语未解决及 `needs_confirmation: true`，
这只是无工具的原生模型诊断，不是房间权限或 IM E2E。任务保存页现保留这类审核说明，避免结构化不确定信息丢失；
28 项受控浏览器检查通过。在这一短片段诊断阶段，没有再次运行完整音频流程。

后续的任务恢复验收将整份回答的严格质量检查与单项任务检查分开：若整份回答失败，先记录 `case_failures`，
只从同次真实模型回复中选择负责人、行动内容和本录音引用均通过原有检查、且未标为待确认的任务继续验证保存与恢复。
未通过检查的任务不会用于这条下游路径。下游成功不能清除质量失败，进程在恢复检查之后仍执行总门禁并以失败退出。
客户端描述直接复用 `RoomAgents.taskDraft`，通过真实登录身份调用现有任务 API；这不是浏览器交互，
但能核对同一个草稿转换函数产生的标题、描述、步骤和来源 ID 经真实 API 保存后在重启中保持一致。

[音频任务恢复回执](benchmarks/2026-11-02-audio-task-recovery.json)记录了这条路径的实际运行：
原 20 分钟合成录音处理约 567 秒；Pi 提出 6 项任务，其中 3 项通过独立负责人、行动与来源检查，
另 3 项因仍需确认而未被选用。测试选取第一项合格的数据库恢复验证任务，由 A 保存、B 接手；
完整 Compose 重启后，A、C 查到的标题、描述、步骤、来源 ID 和交接摘要与保存值一致。
该音频任务的重启断言没有逐字段比较接手人、状态和 revision，因此不将这些字段列为本次恢复证据。
同次运行还覆盖 Kafka 重投、成员撤权及资料/最终回复恢复；测试环境已清理，私人配置未变。
首个检查点的 `QBAC Pressure` 术语错误仍使整份回答失败，最终退出码为 1、`ok: false`；
日志中的 14 条记录包含这条失败记录，不能称为 14 项通过。另有 12 项验证器单测和 28 项受控浏览器检查通过。
本地 `spark-push:media-review` 镜像包含任务审核说明，尚未部署；未重复 PDF、视频、全量 CTest 或独立 SQL 套件。

随后[真实 Web 分析回执](benchmarks/2026-11-02-web-media-analysis.json)通过 7 组浏览器检查：
三名用户从页面共享 PDF、显式调用 Hermes、查看同一条回复及精确来源、保存任务并交接；
完整服务重启后，新浏览器登录恢复相同时间线、任务标题/描述/步骤/来源/交接摘要/接手人/状态/revision。
移除成员后，原文件、Agent 来源和任务读取均返回 403。Hermes 实际引用 PDF 第 2 页的图片；
其哈希在 Native 工具输出、成功的模型请求和浏览器打开的内容中一致。
两次前置运行分别因错误选择第一条来源、仅接受文字来源而失败，原始回执保留；第三次退出码为 0。
这是 PDF 与授权代码的网页链路，本次未运行 Pi、音频或视频，也不能消除此前的转写准确性失败。

[视频网页回执](benchmarks/2026-11-02-web-video-analysis.json)随后一次通过 7 组真实浏览器检查。
三名用户查看相同的 Hermes 视频结果，两个引用画面的颜色/形状与音轨含义通过原有严格检查；
引用 JPEG 的哈希在 Native 工具、成功模型请求和浏览器预览中一致。来源按钮定位原视频至第 5 秒，
保持不自动播放，用户通过原生控件实际播放并暂停。完整服务重启及全新浏览器登录后，最终消息、
固定来源和定位播放均恢复；成员移除后清空播放器，并拒绝原视频、派生画面和来源读取。
此定向运行仅使用已有 10 秒合成视频，不是生产事故录像验收，也没有重跑长录音。

剩余录音问题又做了一次[30 秒背景提示诊断](benchmarks/2026-11-02-asr-domain-context.json)：
使用同一 turbo 模型和原始片段，只提供通用软件技术会议背景，没有给出预期姓名或术语。
约 35 秒后仍输出 `QBAC pressure`，语义检查失败；未采纳到正式 parser，也未再次转写整段录音。
以上是范围调整前的诊断；当前不再请求新录音，也不以换素材或其他功能的成功覆盖该原始失败。
长录音的完整网页分析随音视频功能一起延期。

## 部署接线

`docker-compose.agents.yml` 的 Logic 现在接收以下可选管理员配置：

- `SPARK_PUSH_MEDIA_WORKER_KEY`：独立 worker 凭据，不能使用 Connector 授权密钥代替。
- `SPARK_PUSH_MEDIA_PARSER_PIPELINE_SHA256`、`SPARK_PUSH_MEDIA_PARSER_SHA256`：已批准解析管线与 parser 指纹。
- `SPARK_PUSH_MEDIA_PIPELINE_SHA256`：已有独立文本 worker 的指纹；不要复制媒体指纹到此字段。

空配置不授予媒体处理权限，Service 返回不可用；基础 IM 保持可用。生成管线描述的方法见
[媒体 worker](media-worker.md)。worker 仍由管理员在拥有 Docker 运行权限的独立进程中启动，
没有把 Docker socket 挂进 Logic，也没有给 Agent 增加任意 shell 工具。
现已提供 `sparkctl media` 与私有配置文件简化长期运行，见[媒体服务启停](media-worker-operations.md)；
独立 worker 的启动、停止与恢复状态由该入口管理，不会随基础 IM 的 `up/down` 隐式操作。

`deploy/Caddyfile.agents` 仅在内部 listener 放行 POST 到现有工作区授权、三个资料查询路径及
十二个已声明 worker 操作的完整路径。未知路径和其他方法返回 404；身份、成员和 worker 凭据继续由
Service 检查。内部端口默认绑定 loopback；跨宿主机部署应保持受保护的网络路径。
公共 `deploy/Caddyfile` 对 `/internal` 与 `/internal/*` 显式返回 404，不依赖 Web 服务的兜底行为。

公共 Hermes 的 Native 启动配置与 Connector registry 都要显式设置整数 `room_context_contract: 1`，
并通过原生 capability/精确工具集检查。Pi 使用独立公共模型配置；文字模型可以读取文档和转写，
实际图片能力仍由模型声明和真实视觉验收判断。详见[原生资料工具](room-context-native-tools.md)。

## 重复执行

需要已有 Docker、Python 3.11 环境（websockets、PyYAML）、公共 Pi 运行时、经操作者授权的
Windows Hermes **模型访问凭据**和预置 ASR 模型。测试创建自己的 Profile、工作区和 Compose 卷，
不复制私人会话、不刷新私人 token、不修改私人运行时配置。外部模型不可用时应记录失败。

先构建当前应用镜像及已有 parser/fixtures 镜像；后两者的构建和模型要求见[解析运行说明](media-parser-runtime.md)。
以下在仓库根目录执行，`PYTHON` 指向已经具备测试依赖的解释器：

```bash
docker build --build-arg BUILD_JOBS=2 -t spark-push:media-context .
fixture_dir=$(mktemp -d /tmp/spark-collaboration-fixtures-XXXXXX)
docker run --rm --network none --read-only --user "$(id -u):$(id -g)" \
  --cap-drop=ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,noexec,nosuid,size=128m \
  -v "$fixture_dir:/fixtures:rw" -v "$PWD/tests:/tests:ro" \
  --entrypoint python3 spark-media:fixtures \
  /tests/media_parser_fixtures.py /fixtures --long-audio --collaboration
PYTHON=/path/to/test-venv/bin/python
"$PYTHON" tests/public_room_runtime_e2e.py \
  --image spark-push:media-context --media --media-case pdf \
  --media-image spark-media:development \
  --media-model /path/to/approved-asr-model \
  --media-fixtures "$fixture_dir" --out /tmp/spark-public-media.json
```

应用容器实际 image ID 必须等于启动前记录的 ID。parser 使用精确 image ID，管线摘要涵盖模型与相关源文件。
HTTP/WS 请求、上传、Agent 请求和协作任务全部经真实用户入口生成；SQL/SQLite 只读取证，不注入成功状态。
原始测试日志可能包含临时测试消息和运行时诊断，私密日志保持 0600，不作为公开模型配置示例发布。

定位单项功能时，可在同一命令中增加 `--media-case pdf`、`--media-case audio` 或 `--media-case video`。
它只上传并分析所选素材，继续执行已有三用户、消息重投、成员撤权和恢复检查；例如视频验证无需再次处理 20 分钟录音。
默认 `--media-case all` 保持完整路径。回执中的 `selected_cases/cases_passed/not_selected` 明确区分执行、通过与未选项目，
单项成功不能替代完整多模态验收。音频任务创建现已提交模型给出的 `source_ids`，由服务端复验并随任务保存。
回执的 `case_failures`、`audio.task_evaluations` 与 `audio.selected_task_index` 区分整份回答失败和独立可验证的任务；
`cases_passed` 仍只记录通过完整质量检查的媒体场景。没有独立合格任务时，任务持久化标记为未执行。
CPU 处理长录音时，可显式增加 `--media-processing-timeout 3600`；默认每份资料等待 900 秒，允许 1–3600 秒，
实际配置写入回执。这只调整测试等待预算，不延长 worker 租约或 Runtime 执行限制，超时不触发自动重跑。

只定位真实网页的资料上传、原件播放、重启和撤权时，复用已有 Web 验收启动器，不启动模型或重复其他 IM 故障场景：

```bash
SPARK_PLAYWRIGHT_MODULE=/path/to/node_modules/playwright "$PYTHON" tests/collaboration_web_e2e.py \
  --image spark-push:media-playback --media-only --media-fixtures "$fixture_dir" \
  --out /tmp/spark-media-real-browser.json
```

`node` 须已在 PATH 中；`--media-only` 与 `--agents` 互斥。此时资料保持待处理状态，测试的是已共享原件，
不是模型理解。回执保存实际服务镜像与七个已提供给浏览器的静态文件 hash。

验证网页中的资料分析、来源导航和任务交接时，使用同一启动器的定向模式：

```bash
SPARK_PLAYWRIGHT_MODULE=/path/to/node_modules/playwright "$PYTHON" tests/collaboration_web_e2e.py \
  --image spark-push:media-review --agents --media-analysis \
  --media-image spark-media:development --media-model /path/to/approved-asr-model \
  --media-fixtures "$fixture_dir" --out /tmp/spark-web-media-analysis.json
```

它只处理 `design.pdf`，启用正式媒体 worker 与隔离 Hermes 的资料工具；模型目录仍用于固定解析管线指纹，
不会执行音频转写。三名用户从真实页面注册、邀请、上传、选择“整理决策与任务”、显式发送，再查看原请求来源、
保存任务和更新交接。额外提问要求模型读取授权代码并报告文档/代码中的随机标记，不把正确标记提供给模型。
重启后用全新浏览器会话复查任务及来源，最后检查成员移除后的访问。
该模式不重复单聊、Job 延迟注入、音视频或另一个 Agent 的模型调用；`--media-only` 与它互斥。
执行结果由对应回执给出，命令存在不表示已通过。

同一命令加 `--media-case video` 可只运行网页视频分析：上传 `incident.mp4`，通过“视频故障分析”草稿
显式调用 Hermes，查看引用画面并从来源跳到原视频时间位置，然后验证重启恢复与成员撤权。
模型的颜色、形状、转场区间和音轨含义复用协议场景的 `validate_video` 检查；正确答案不会写入提示。
实际引用 JPEG 仍须同时出现在 Native 工具与成功模型请求中。该模式不执行 PDF 任务保存或长录音转写。

## 各项断言意味着什么

| 场景 | 要检查的事实 | 不能据此声称 |
|---|---|---|
| 三人分别上传 PDF/音频/视频 | 分片、完成、真实 worker 发布、固定 revision | Web 上传/续传界面已完成 |
| Hermes 阅读 PDF 与代码 | PDF 随机标记不在提问中；答案含该标记和批准代码中的标记；存在资料来源回执 | 任意 PDF 阅读准确率或全部源码理解 |
| Pi 提取会议决策 | 合成 20 分钟连续语音；前、中、后三个检查点先通过 ASR，再逐项核对代号、负责人、行动和本音频片段引用 | 真人会议转写准确率、说话人识别；音频里的姓名不提供服务器身份 |
| 协作任务 | 模型输出建议，成员用任务 API 创建并接手；source_ids 经服务端复验并持久保存，重启后查询一致 | Agent 自动写任务，或模型建议已被证明正确 |
| Hermes 解释视频 | 转场前后的真实 JPEG；核对音轨未透露的图形、颜色、时间区间和音轨含义；来源 hash 与原生工具结果及实际 provider 请求中的图片对应 | 仅图片送达就证明视觉正确，或任意视频分析准确率 |
| 消息与恢复 | 三账号最终正文/身份一致；原请求及回复重投不重复生成结果；撤权与重启后边界保持 | 三浏览器 UX 或所有处理中故障恢复 |

已有 Pi 同图视觉误判记录继续保留，本场景不使用文字 Pi 的转写成功覆盖它。
资料共享删除、最终答案结构化引用、长录音模型分析的真实网页验收、真实用户资料准确性和完整产品验收仍需独立完成。
任务来源校验及成员回查已单独交付，见[来源关联回执](benchmarks/2026-11-01-source-links.json)。

Native 的 SQLite 历史将图片投影成 `[screenshot]`，不能从该文本表断言原始 JPEG 曾进入模型。
视频定向场景复用原生工具探针，仅记录实际工具与 provider 请求内的图片 SHA-256、request_id 和 HTTP 状态；
不保存请求正文、凭据或 Base64 图像。该观测不会替换模型响应或绕过授权。

断言本身的反例回归可运行 `"$PYTHON" tests/public_media_scenario_test.py`。这里的合成回答只用于检查
错误负责人、无关任务、错源引用、错误图形或时间能否被拒绝，不能代替真实模型测试。
原始 ASR 和答案保留，代号比较只明确兼容 `harbour`/`harbor` 的英美拼写；这是同文件真实诊断后确认的差异，
不使用模糊相似度接受其他代号。重启验证会实际读取原版本片段与 JPEG，不能只检查数据库 ready 状态。
