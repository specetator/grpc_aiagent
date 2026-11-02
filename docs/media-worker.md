# 持久媒体 worker

适用：`295fa3a` 之后的多模态工作树，2026-11-01。
`cannbot/scripts/media_worker.py` 将已共享的 PDF、DOCX、图片、音频和视频任务接入
现有隔离 parser、Logic HTTP、MySQL 与附件卷。它不实现 Agent Loop。
解析后的 `ready` 仍不代表原生 Pi/Hermes 已阅读、理解或生成协作任务。

## 启动与批准配置

先按 [解析器说明](media-parser-runtime.md) 准备不可变镜像及固定公共 ASR 模型，
执行 [四个增量迁移](media-publication.md)。不删除旧数据或更改私人 Agent 配置。

```bash
python3 cannbot/scripts/media_worker.py \
  --image-id sha256:ADMIN_APPROVED_IMAGE_ID \
  --model-dir /srv/spark-media/models/approved-small \
  --describe
```

示例中的镜像占位符须替换为实际 64 位十六进制 digest；程序拒绝 tag。
`--describe` 输出 pipeline、parser、image 和模型清单指纹，不输出凭据。
组合 pipeline 绑定 worker、复用的 text worker/Client/State、sandbox、parser 四份源码，
以及精确镜像 ID 和模型文件清单。改变其中任何一项都需要新的批准指纹。

Logic 配置 `SPARK_PUSH_MEDIA_WORKER_KEY`、`SPARK_PUSH_MEDIA_PARSER_PIPELINE_SHA256`
和 `SPARK_PUSH_MEDIA_PARSER_SHA256`，后两项取自上述输出。
文本管线继续使用自己的 `SPARK_PUSH_MEDIA_PIPELINE_SHA256`，不混用指纹。
凭据保存在 worker 账号拥有的 0600 文件中；状态目录为该账号的 0700 私有目录。

```bash
python3 cannbot/scripts/media_worker.py \
  --image-id sha256:ADMIN_APPROVED_IMAGE_ID \
  --model-dir /srv/spark-media/models/approved-small \
  --origin http://127.0.0.1:9101 \
  --key-file /srv/spark-media/worker.key \
  --state-dir /srv/spark-media/state
```

默认持续运行；`--once` 处理或恢复一次领取，ready/idle 返回 0，失败、到期或不确定状态返回 1。
SIGINT/SIGTERM 停止新工作，取消自己的解析并保留需要恢复的状态，退出码为 1。
进程管理器应按此约定配置。worker 没有加入 Compose 默认启动，远程 origin 应使用受保护链路或 HTTPS。
可选 Agent overlay 已传递独立 worker 凭据/指纹，并为内部 worker/资料接口设置完整路径白名单；
部署方式与真实服务测试入口见[多模态服务链路](multimodal-service-e2e.md)。
每个 worker 实例使用独立状态目录；同目录的第二个实例被固定文件锁拒绝。

长期运行可使用私有 JSON 配置与 `scripts/sparkctl.sh media start/status/logs/stop`，
详见[媒体服务启停](media-worker-operations.md)。此入口复用本 worker 的恢复状态，由 systemd 用户服务管理进程，
不向 Compose 容器开放 Docker socket。配置中的批准指纹与当前代码/模型不一致时拒绝启动。

## 领取、期限和恢复

1. HTTP Claim 前先原子写入并 fsync `claim_id`、随机 execution ID 与 pipeline 指纹。
   网络失败、JSON 不完整、未知 409 或配置变化都保留原身份；不能换 ID 盲目领取。
2. 首次成功 Heartbeat 后才做实际工作。独立线程每 20 秒续租，租约 120 秒，attempt 硬期限一小时。
   本地预算用请求发送前的单调时间与服务端剩余硬期限计算；重启不重新获得完整一小时。
3. 先 Reserve，再下载和解析。真实配额不足的 413 保留同一领取，继续续租、有限 GC 和等待；
   未预留时不启动 parser。源文件按有界区间读取，并校验累计大小与完整 SHA-256。
4. parser 使用已登记 execution ID、精确镜像、只读源文件/模型挂载和剩余期限。
   失联、停止或到期会中断监督并回收该容器；服务端 fencing 独立拒绝旧 attempt 的暂存和发布。
5. 完整本地 bundle 使用统一 `validate_directory` 再验来源、parser、文件集合、hash、图像和 coverage，
   才恢复上传。文件替换、额外文件、链接或错误来源不能作为已完成解析使用。
6. 完成摘要先 fsync，再调用 Complete。响应丢失恢复原摘要；确认 ready 后不被迟到的心跳 409 覆盖。
   权威 receipt 校验原 asset、revision、状态与摘要，而非根据本地阶段猜成功。
7. 清理确认属于自己的容器、execution 目录及失败预留之后，才清除 pending 状态并同步目录。
   清理失败保留 pending；普通 daemon 错误不能被解释成“容器不存在”。

已验证 bundle 可直接恢复上传；没有完整 bundle 时，重启可能重新执行**只读媒体解析**。
这不是外部 Agent 工具 exactly-once 合同，不能据此重跑状态 unknown 的有副作用工具。
状态文件保存/读取均限制为 128 KiB；超限保存保留旧文件，不生成无法恢复的新状态。

恢复只 inspect 自己登记的名称，核对 execution 标签、精确镜像、实际容器 ID、用户、禁网、只读根目录、
源挂载及模型挂载，再按精确 ID 删除。外来同名容器被拒绝；没有全局按前缀删除容器的操作。
恢复的目录和文件仍须通过私有目录、O_NOFOLLOW、普通文件、单硬链接与完整性检查。

明确输入失败（例如加密 PDF）才提交 Fail。宽泛的 malformed_media、decoder_failed、parser_failed、
timeout、模型不可用和清理失败保留为待核实状态，避免将 OOM 或基础设施故障伪装成确定输入问题。
失败尝试与硬期限仍由服务端现有状态机约束，不无限延长租约。

## 有限垃圾回收

新增内部 `POST /internal/media/reclaim_candidates`，字段 `after`、`limit`，默认 5、上限 20。
Service 验证独立 worker 凭据和字段，Repository 用绑定参数返回 revision ID 及 `next_after`。
候选排除 live、ready 和仍有 publication 的版本，只是提示；Reclaim 会在原锁与事务中再次判断。

worker 在 idle 或等待配额时每次处理最多 5 个候选，并逐项检查停止/租约。
游标前进避免一个坏文件永远占住首页；扫描到空页后从头开始。
实际清理成功且 fsync 后才释放 charge。并发回收同一 revision 幂等，不删除已发布结果。
此 GC 处理派生 revision；未完成上传草稿的自动到期、共享资料删除和上下文失效仍属后续模块。

## 验证及限制

```bash
python3 tests/media_worker_test.py
python3 tests/media_parser_test.py
SPARK_TEST_MEDIA_IMAGE=spark-media:development \
SPARK_TEST_MEDIA_FIXTURES=/path/to/owned-fixtures \
SPARK_TEST_MEDIA_MODEL=/path/to/public-asr-model \
SPARK_TEST_MEDIA_WORKER_TEST=1 \
SPARK_TEST_MEDIA_LONG_AUDIO=1 \
SPARK_TEST_MEDIA_WORKER_REPORT=/tmp/media-worker.json \
python3 tests/run_mysql_review_tests.py build-wsl
```

真实套件创建独立 MySQL/Redis、真实用户和私有房间，执行生产 worker CLI、真实 Docker parser、CPU ASR、
HTTP 暂存/发布/授权读取。测试 relay 只注入转发后的回执丢失或指定阶段阻塞；SQL 故障注入限于测试资产。
16 项真实检查包含：五种媒体、原领取回执丢失、部分上传强杀与篡改、精确孤儿容器回收、Complete 回执丢失、
配额等待续租、加密 PDF、持续运行、20 分钟 ASR 强杀恢复、执行中租约失效与 GC 权限保护。
长音频最终恢复用时 201.386 秒、395 段、11 次心跳，完整解码 19,199,651 个样本。

7 项 worker 合同测试使用假时间/传输；13 项 parser 边界测试独立记录。
Complete 已确认与心跳失效竞争属于合同测试，不冒充实际网络竞争 E2E。
20 分钟输入是连续合成技术语音，不是真人会议准确率评估。
全量 CTest 注册 75 项：65 通过、10 项按依赖条件跳过。随后这 10 项分别补跑通过：
3 个真实媒体/文本 HTTP 套件、4 个真实 MySQL 测试、2 个真实 Redis 测试和 1 个浏览器合同测试。
浏览器合同使用受控 HTTP/WS fixtures，不是多模态三浏览器 E2E；当前 publication 短套件另验证 6 种合成资料。
具体源码、命令、全量回归与跳过项见 [worker 阶段回执](benchmarks/2026-11-01-media-worker.json)。
本阶段不覆盖原生 Pi/Hermes 媒体工具、聊天引用、任务来源、Web 多模态交互或最终三用户端到端目标。
