# 房间媒体派生物发布协议

适用：`295fa3a` 之后的多模态开发工作树，2026-11-01。
本阶段将真实解析器的 PDF/DOCX/图片/音视频输出接入现有 Logic、MySQL 和附件卷。
`ready` 表示解析与服务端发布检查完成；不表示 Pi/Hermes 已理解内容或生成任务。
完整产品目标与未完成模块见 [多模态协作](multimodal-collaboration.md)。

## 迁移与运维配置

旧数据库按顺序执行以下增量迁移，保留原附件、消息、任务和 Runtime Session：

1. `20261101_room_media.sql`：原文件、分片和处理任务。
2. `20261101_media_processing.sql`：claim、revision、片段与 publication。
3. `20261101_media_artifacts.sql`：派生图片、存储预留与回收状态。
4. `20261101_media_publication.sql`：不可变处理 profile，以及最终 coverage/派生物计数。

全新数据库使用 `sql/schema.sql`。四个迁移的重复执行由独立数据库测试覆盖。
旧 text revision 没有 profile 时，只能按其文本 MIME 解释为旧管线；非文本缺 profile 或 SQL 查询失败不会降级放行。

管理员配置 `SPARK_PUSH_MEDIA_WORKER_KEY`，文本管线继续使用 `SPARK_PUSH_MEDIA_PIPELINE_SHA256`。
媒体管线另配置 `SPARK_PUSH_MEDIA_PARSER_PIPELINE_SHA256` 与 `SPARK_PUSH_MEDIA_PARSER_SHA256`；
组合指纹与文本指纹必须不同，parser 指纹固定到被批准的解析代码。
普通登录 token 不能调用内部 worker 接口，用户消息不能指定这些配置。
本发布阶段测试 harness 的 pipeline 指纹包含测试驱动、parser、固定镜像 ID 与模型清单；它不是生产 worker 的发行配置。
后续已接入 [持久媒体 worker](media-worker.md)，其独立组合指纹由 `--describe` 生成。

媒体派生文件使用附件卷下独立 `media_derived` 目录，复用 `MediaBlobStore` 的生成名、固定文件锁、
有界读写、hash、fsync 与 no-replace 安装。不在 HTTP handler 中放 SQL、权限规则或 Kafka 操作。

## 领取、预留与暂存

所有内部操作位于 `POST /internal/media/`，使用独立 Bearer 凭据及现有业务包络。

| 操作 | 请求关键字段 | 合同 |
|---|---|---|
| claim_media | claim_id、pipeline_sha | 返回稳定 assignment、pipeline/parser 身份和剩余硬期限；同 claim 只恢复原分配 |
| reserve | id、revision、attempt、lease_token | 为原 revision 预留 176 MiB；配额不足不丢掉 claim，不开始写派生文件 |
| check / heartbeat | 同一 lease 字段 | 检查/续租；不能复活旧租约或越过硬期限 |
| source | lease、offset、length? | 原文件有界读取，返回来源 hash；不是可选的任意 URL 下载 |
| stage | lease、index、fragment | 精确文本、kind 和来源 anchor；同下标不同内容冲突 |
| stage_artifact | lease、index、metadata、data | 受限 baseline JPEG，确定性文件身份；重复提交须保持内容与元信息 |
| complete | lease、计数、coverage、manifest_sha256 | 服务端重新核对完整集合，原子发布；终态重放必须是原摘要 |
| fail | lease、有限 error_code | 持久失败；不伪装为完成 |
| reclaim | revision | 拒绝 live/ready/current publication，清理后才释放预留 |
| reclaim_candidates | after?、limit? | 有限候选页（最多 20），只有内部 worker 可用；每项仍须经 Reclaim 复查 |

只有 `stage_artifact` 的请求体上限是 2 MiB，其余内部操作仍为 128 KiB；base64 解码后的单图最多 1 MiB。
每 revision 最多 1024 图/128 MiB、4096 文本片段/16 MiB，单片段最多 16 KiB。
空白图片可以有零 OCR 文本，但必须保留真实派生图；不会用虚构文字凑出非空结果。

Claim 与 Reserve 显式分开。等待配额期间租约仍计时，worker 必须续租并使用服务端返回的剩余期限。
没有成功预留的等待不会耗尽三次已预留处理尝试；attempt 身份仍单调增加。
预留和新原件共同使用房间 10 GiB **逻辑容量**，不包含旧独立图片上传，也不代表宿主磁盘实际空间限制。

## 原子发布和读取

Complete 先取固定 revision 文件锁，再按 room → asset/job/revision 的顺序取 SQL 锁。
检查数据库实际条数与连续下标、片段 canonical hash、全部图片已安装、实际文件 hash/尺寸/格式，
以及 OCR 引用的 artifact 和页码/段落/时间轴属于同一 revision。
文件暂存清理并同步成功后，才在同一 SQL 事务中更新 ready、profile、publication 和实际费用。
失败不会提前释放预留；完成后重复请求返回原 receipt，不重新收费。

coverage 检查包括 PDF 所有页、DOCX 节和段落范围、图片视觉内容、音频实际采样数/时长/转写范围，
以及视频采样 PTS、末帧覆盖和音轨偏移。总时长必须等于选定实际轨道的最晚结束时刻，
最早选定音/视频样本接近统一时间轴原点；音频先开始而视频延迟出现是合法情况。
这些检查约束发布清单，完整解码与转写的证据仍来自受控解析器；结构合法不等于模型回答正确。

Python/C++ 共同使用 UTF-8、排序键、无空白分隔的 canonical JSON；时间和计数均为整数。
manifest 摘要是下面对象序列化后的 SHA-256：

```text
schema: "room-media-publication-v1"
source: {mime, bytes, sha256}
pipeline_sha256 / parser_sha256
coverage
fragments: 按 index 排列的 fragment canonical SHA-256
artifacts: 按 index 排列的 metadata canonical SHA-256
```

parser 的本地 index/file 字段不进入 fragment/metadata；文件路径始终由服务端生成。
空片段集合使用真实空数组，最终摘要不从聊天正文或客户端身份字段推导。

公开 `POST /api/room/media/` 保留 processing、fragments、search，新增：

| 操作 | 字段 | 返回 |
|---|---|---|
| artifacts | id、revision?、offset?、limit? | 当前已发布派生图列表与来源锚点，页长最多 20 |
| artifact | id、revision、index | 固定版本图片的元信息及 base64 字节 |

前后检查当前正式成员；第一次查询固定实际 revision，返回前再检查同一 publication。
暂存数据、过期版本、撤下 publication 的旧 ID 不能继续读取。Search 在 SQL 中先按房间和当前 ready publication 过滤。
撤权检查不能追回已经送出的字节；本阶段未实现共享资料删除引起的原生 Session context generation 更新。
旧 text 读取保持兼容；媒体 API 不生成消息 ACK、不推进 `msg_seq`，Kafka/离线同步语义不变。

## 可重复验证与范围

先按 [解析器运行说明](media-parser-runtime.md) 构建固定镜像、准备合成资料和公共 ASR 模型，然后运行：

```bash
cmake --build build-wsl -j2
ctest --test-dir build-wsl --output-on-failure -R '^media_manifest_test$'
SPARK_TEST_MEDIA_IMAGE=spark-media:development \
SPARK_TEST_MEDIA_FIXTURES=/path/to/owned-fixtures \
SPARK_TEST_MEDIA_MODEL=/path/to/public-asr-model \
SPARK_TEST_MEDIA_LONG_AUDIO=1 \
SPARK_TEST_MEDIA_REPORT=/tmp/media-publication.json \
python3 tests/run_mysql_review_tests.py build-wsl

# 同样设置 IMAGE / FIXTURES / MODEL，另跑超过旧 128 KiB 限制的真实 JPEG。
python3 tests/media_large_payload_test.py --build build-wsl --out /tmp/media-large.json
```

runner 创建自己的 MySQL/Redis 容器，三名真实测试用户通过 HTTP 注册、邀请和接受，随后上传真实合成资料、
运行实际解码/CPU ASR、通过 HTTP 暂存和完成、由另一成员读取并核对 hash。
测试包含 SQL 发布失败回滚、完成重放/改摘要冲突、非成员拒绝、成员移除、发布撤下以及 HTTP 服务实际重启。
20 分钟输入是连续合成技术语音，不代表真人会议转写准确率。

该 harness 的心跳与重启只验证所述 HTTP 协议；生产媒体 worker 接管和有限派生物 GC 后续已独立实现，
见 [worker 与恢复验收](media-worker.md)。原生 Pi/Hermes 工具、聊天资料引用、Web 与 Kafka/浏览器 E2E 仍需后续实现和单独验收。
当前精确测试结果与源码指纹见 [阶段回执](benchmarks/2026-11-01-media-publication.json)。
完整 7 种资料通过，20 分钟合成语音产生 395 片段、续租 21 次；最终大图套件 6 种资料通过，
成功图片 HTTP 正文实测 397,354 字节。全 CTest 是 65 通过、8 跳过，后续真实环境 2 项 HTTP、4 项 SQL 单列补跑通过；
两个独立 Redis 测试本阶段未重跑。合法替代摘要冲突由最终短套件验证，长音频使用的前一版测试驱动独立归档。

## 后续 worker 实现

独立 Architect 已核对当前 API 与 sandbox。worker 复用现有私有 State、HTTP Client 和解析 supervisor，
首次网络领取前持久保存 claim、execution ID 和组合代码指纹；固定私有 execution 目录，重启只恢复原 claim。
下载、配额等待、解析、暂存和 Complete 全程由独立线程续租；首次成功 Heartbeat 之后才做实际工作，
以发送前的单调时钟和服务端 remaining_seconds 计算保守期限，不能因重启重新获得一小时。

原子安装的输出需复用统一目录验证器核对原件身份、文件集合、hash、图像尺寸与 coverage 后才能恢复上传。
恢复中的旧容器须核对 execution 标签、精确镜像、名称和完整只读挂载，再按实际容器 ID 回收；无法核实则保留状态。
Complete 与 Heartbeat 的终态竞争以原 claim/原 summary 的权威回执解决，不能以续租 409 推断完成或覆盖已确认成功。
有限 GC 候选只是提示，每项仍由 Reclaim 重新判断。生产 worker、目录验证器重构和候选扫描已接入，
实际强杀、接管、丢回执及 20 分钟验证单列在 [worker 回执](benchmarks/2026-11-01-media-worker.json)；
本发布阶段的历史报告仍保留原版本与范围。
