# 房间上下文读取 API

状态：2026-11-01，`295fa3a` 之后的开发工作树。服务端接口与 Pi/Hermes 原生工具接线已实现；
真实测试存在 Pi 视觉误判。成员来源回查与任务来源校验已接入；最终消息的结构化引用绑定仍未完成，
见 [原生工具与边界](room-context-native-tools.md)。完整多模态目标继续见 [总设计](multimodal-collaboration.md)。

本模块组合现有 RoomAgentService 与 MediaProcessingService，不新增 Agent Loop、模型连接或工具执行器。
原生工具把运行中的 request_id 作为受信上下文注入；模型参数只选择来源和读取范围。

## 配置和身份

先应用既有媒体迁移、`20261101_media_context.sql`，再增量应用
[`20261101_media_context_sources.sql`](../sql/migrations/20261101_media_context_sources.sql)。不清理消息、旧 request、CANN 或原生 Session。
新库 schema 已包含这些表；迁移缺失时，接口返回不可用，不返回未记录来源的数据。

使用管理员配置的 `SPARK_PUSH_WORKSPACE_AUTH_KEY` 作为内部 Connector Bearer。
没有 `SPARK_PUSH_ROOM_PROJECT_CATALOG` 时也可配置此 key：房间资料读取不要求批准代码项目，代码工具仍保留自己的项目授权。
普通登录 token 和媒体 worker key 都不能代替 Connector 凭据。不要将这些 key 送入提示词或浏览器。

身份来自已持久化的 generic Room Agent request：room、actor、Agent、binding generation、workspace generation、context generation。
新资料工具要求显式 context_generation；旧 request 不被升级到新代次。消息读取上界固定为该 request 的 message_seq，
不能通过分页参数读取触发请求之后的消息。绑定、成员、用户状态、项目和上下文代次都沿既有 CheckDispatch 检查。

## 接口

`POST /internal/room-agent/context/<operation>`，请求 JSON 不超过 4096 字节。
响应使用既有 `{code,message,data}`；应检查业务 code。未列出的字段被拒绝，特别是 room_id、actor_user_id、Runtime URL 和路径。

| operation | 接受字段（均含 request_id） | 内容 |
|---|---|---|
| search | query?、type?、limit?、before_seq?、before_ordinal? | type 为 all/messages/media/fragments；空 query 浏览消息/文件，fragments 要求非空查询；limit 默认 10、每类最大 20 |
| read，kind=message | msg_seq、offset?、limit? | 当前房间授权消息；offset 按 Unicode 码点计，limit 默认/最大 4096 码点 |
| read，kind=fragment | asset_id、revision、offset?、limit? | 固定版本的正文/OCR/转写片段；offset 是片段下标，limit 默认 5、最大 10 |
| read，kind=image_metadata | asset_id、revision、offset?、limit? | 固定版本的图片/页/帧清单；不读取图像字节 |
| media | asset_id、revision、index | 固定版本的单个真实 JPEG；含 MIME、base64、尺寸与权威锚点 |

search 的 `messages_page.before_seq` 与 `media_page.before_ordinal` 是独立游标。
all 每类最多 limit 条，非空查询最多共 60 条；消息摘录最多 300 个 Unicode 码点。搜索媒体匹配文件展示名或其已发布片段，
空查询也能发现完全没有 OCR 文字的图片。媒体分页按资产，而不是被一份文档的多个命中片段占满。
非空查询还返回实际命中的正文/转写片段，带 asset/revision/index 和真实行号、页码或时间锚点，
可直接从命中的 index 继续 read。fragments_has_more 表示还有命中，需细化查询；它不是媒体资产分页游标。
响应数据限制为略小于 2 MiB，过大返回 413 并要求缩小 limit 或范围，不静默截断来源。
消息 offset/length 不按 UTF-8 字节、UTF-16 单元或组合字形计数。读取到文本末尾允许空范围，回执 length=0；
它不表示已读取全文。整数不能使用 bool、浮点、负数或超出范围的值。

搜索和直接按 msg_seq 读取复用同一 SQL 资格筛选：房间、消息上界、可信 Agent 来源及 context_generation 先于 LIMIT。
普通人类讨论保留；Agent 派生消息必须匹配持久原 request 的房间、bot 身份及当前代次。
仅缺失代次的旧来源按 0 处理；显式 null/bool/浮点/字符串不被当作 0。
SQL 参数绑定处理查询词和 ID，正文不能授予权限。

## 来源回执

每个返回项带 `reference` 和 `source_id`。source_id 按 request 与规范来源计算，
`room_context_source` 以 request_id/source_id 唯一保存，不复制原始正文或图片。

| reference.kind | 它证明服务器读了什么 |
|---|---|
| message | 当前房间 msg_id/msg_seq 的具体字符范围，全文 hash 与返回范围 hash |
| media | 资产的已发布元数据与 manifest 身份；不代表正文或图像已读 |
| fragment | asset/revision/index 的规范片段，含服务端页码、行号或时间范围 |
| image_metadata | 某张图片/页面/视频帧的元数据与锚点 |
| image | 由现有存储验证后读取的真实 JPEG，含内容 hash 和锚点 |

Service 在读取前、写回执前、返回前重新授权，并核对同一 request、scope、消息上界和 bot 集合。
回执事务锁定 room → context → request，核对当前代次与 canonical request，再复验来源仍发布、hash 和范围一致。
每个 request 最多 8192 个不同回执；已记录的相同引用在达到限额后仍能重放，新范围返回 429。
事务中任何来源失败都不提交该批新增回执。读取别的房间资产返回拒绝，即使 actor 同时是两边成员。

回执证明服务端观察到指定来源，不证明客户端收到、模型理解或结论正确。
读取后授权失效，接口不返回内容；已生成的观察回执也不能绕过后续授权。已发出的网络字节无法追回，
本模块没有增加 Kafka/Comet 已排队帧的同步撤回屏障。

## 成员回查与任务来源

`POST /api/chatroom/agents/sources` 使用用户 Bearer 身份，必填 `room_id/request_id`。
查询的是该房间、该原请求的服务端读取回执，不使用 Connector 密钥，也不要求原发起者仍是成员或原 Agent 仍安装。
访问者必须是当前有效成员；HTTP handler 只转交给 `RoomAgentService`，来源解析由仓储完成。

| 查询方式 | 可选字段 | 结果 |
|---|---|---|
| 分页浏览 | `after`（上一页 `next_after`）、`limit`（1–32，默认 20） | `items/has_more/next_after`，按固定 source ID 排序 |
| 精确选择 | `source_ids`（最多 32 个不重复 ID） | 每项必须来自该原请求；不能与分页字段混用 |

每项包含 `source_id/available`。当前有效的项返回服务器保存的 `reference`、文件名，聊天来源另返回原字符范围的摘录。
资料未发布、版本/hash/上下文代次变化时，返回 `available:false` 与原因，不返回旧 reference、名称或摘录。
伪造、不属于原请求或另一个房间的来源返回拒绝。成员检查在读取前后执行；返回前再检查当前上下文代次。

`/api/chatroom/tasks/create` 可增加 `source_ids`。Service 校验字段及原请求归属，仓储在同一当前读事务中锁住
房间、用户和成员记录，再核对来源与当前代次，最后写任务及历史。当前可见性写路径须继续遵守先锁房间的约定。
仅 ID 保存到现有任务 JSON，不复制资料正文；无需新增迁移，也不重写旧任务。
更新任务不允许改来源或原请求；资料后来失效不会阻止进度/交接更新。写入事务仍复验当前成员、账号状态和接手权限。

网页执行详情和最终消息下方的「查看读取来源」分页列出服务端记录；「查看任务来源」只列任务保存的 ID。点击后重新解析来源，按固定
revision/index 读取片段或图片，再复核来源才展示，不自动跳到最新版本。目录回执和图片信息回执明确标注，
不能把它们当作 Agent 已读正文/图像。旧 `chatroom.html` 复用同一组件。

任务仍由成员审核后保存；回执存在不证明结论正确。任务创建沿用随机 task ID，网页禁止并发提交且不自动重试，
但用户重复确认仍可能创建两份任务，不能宣称创建 API 已实现幂等。最终消息尚未新增强制 `source_refs` 协议。

定向构建、2 项 Service 回归、23 项受控浏览器检查及 13 组真实 HTTP/MySQL/Redis 检查见
[来源关联阶段回执](benchmarks/2026-11-01-source-links.json)。真实检查包含 SQL 夹具生成的聊天/request，
不是 Kafka/Runtime E2E；本轮未启用真实图片和 Pi TypeScript 客户端分支。

## 验证与下一步

[`media_context_service_test.cpp`](../tests/media_context_service_test.cpp) 使用受控 Repository，检查读取中/回执中撤权、
严格参数、缺库、旧 scope 和返回数据清空；它不是模型或真实数据库测试。
[`media_context_http_test.py`](../tests/media_context_http_test.py) 使用真实 HTTP/MySQL/Redis，消息/request 由测试夹具写入，
上传和解析走真实 Service/worker。实际 SQL 行锁等待用来验证读取与代次提交的竞争；不是共享删除功能。

```bash
# 仅修改来源回查/任务来源时，优先跑相关目标和真实 HTTP 范围。
cmake --build build-wsl -j2 --target logic_server media_processing_http_probe room_agent_service_test media_context_service_test
ctest --test-dir build-wsl -R '^(room_agent_service_test|media_context_service_test)$' --output-on-failure
SPARK_TEST_MYSQL_CONTEXT_ONLY=1 python3 tests/run_mysql_review_tests.py build-wsl
# 以下是阶段集成命令，不要求每次网页/文档修改重复执行。
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure
# owned 临时 MySQL/Redis；默认补跑上下文 HTTP、文本 HTTP 和 4 项 SQL。
python3 tests/run_mysql_review_tests.py build-wsl
# 可选真实图片分支，要求已构建的解析镜像及管理员预置公共 ASR 目录。
SPARK_TEST_CONTEXT_IMAGE=spark-media:development \
SPARK_TEST_MEDIA_MODEL=/path/to/approved-asr-model \
python3 tests/run_mysql_review_tests.py build-wsl
```

实际命令、源文件指纹与回执见 [本阶段报告](benchmarks/2026-11-01-room-context-tools.json)。
现有 Pi Extension / Hermes Native 工具层已注入 request_id，JPEG 转为原生 image content；
当前模型识别失败与证据分类见 [原生工具](room-context-native-tools.md)。下一步完成同一次真实服务/模型联测，
以及最终答案结构化引用、资料删除与 Web 三用户验收；任务来源基础已实现，语义正确性仍需真实模型验证。
本模块没有改变 accepted/delivered、msg_seq、outbox、消息落库或旧 @CANN 发布合同。
