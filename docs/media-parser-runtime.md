# 隔离媒体解析器（多模态升级实施中）

本阶段从真实 PDF、DOCX、图片、音频和视频提取可引用资料。它复用上传后的原文件，
没有 Agent Loop、HTTP 服务、房间身份判断或模型工具权限。解析器的输出已接入
[Logic 派生物与 publication 协议](media-publication.md)，真实 HTTP 测试可发布图文和音视频转写。
持续运行的 [媒体 worker](media-worker.md) 已接入领取、续租、发布与强杀恢复；文本 worker 保持 `media-text-v1`，不会接管非文本任务。
不要把 parser 的代码 hash 填进现有文本 worker 的批准指纹，二者是不同管线。

总设计、当前成员授权与完整验收要求见 [房间多模态协作](multimodal-collaboration.md)。

## 实现与输出

| 文件 | 责任 |
|---|---|
| `cannbot/media/parser.py` | 容器内解码、渲染、OCR、ASR、来源锚点与有限输出 |
| `cannbot/media/sandbox.py` | 管理员指定的不可变镜像、输入快照、容器监督、结果独立核验 |
| `cannbot/media/provision_asr.py` | 预置固定公共 ASR 模型，验证 Git/LFS 身份；不读取私人模型配置 |
| `cannbot/media/Dockerfile` | 解析运行镜像与独立的合成测试资料生成层 |
| `tests/media_parser_test.py` | 归档、路径、来源、模型临时文件、时间戳、JPEG 头边界测试 |
| `tests/media_parser_e2e.py` | 真实隔离解码、真实 CPU ASR、时间轴和强杀父进程测试 |

Parser 固定读取 `/input/source`，模型只读挂载到 `/model`；暂存位于容器 tmpfs。
成功时仅输出 tar：`manifest.json` 和按序生成的 `image-NNNNNN.jpg`。
父进程不使用 `extractall`，拒绝未知文件名、链接、重复项、超量文件和字段，
校验 source/parser hash、实际文件 hash、字节数、片段与图片数量、模态覆盖字段。
父进程还检查 JPEG 实际 SOF 尺寸和单幅 baseline 扫描，不能用 manifest 自报尺寸绕过图像上限。

`room-media-v1` manifest 含：

- `source`：原文件 MIME、字节数、SHA-256；不含用户提供的路径或身份。
- `parser_sha256`：实际执行代码；批准配置还必须绑定镜像 ID 和模型指纹。
- `fragments`：有序的 `text / ocr / transcript`、正文及页码/段落/时间锚点。
- `artifacts`：派生 JPEG 的序号、hash、大小、尺寸及页码/图像/视频时间锚点。
- `coverage`：实际完成的页数、段落、解码样本数、轨道起止、采样帧时间等。

PDF 每页都渲染，原生文本缺失时再 OCR；文字提取不会使图表从资料中消失。
DOCX 提取正文、页眉页脚、脚注尾注中的段落和所引用嵌图；不取外部图片，
不执行宏或字段。加密 PDF、动画图片、多音/视频轨、无法可靠映射的时间戳、
未支持的嵌入对象与绘图会明确失败，不静默标记为完整。

图片由 Pillow 实际解码、按 EXIF 转正、透明背景铺白、缩放并清除输入元信息，OCR 使用 Tesseract。
PDF 使用 pypdf + Poppler；DOCX 使用有展开预算的 ZIP 与 defusedxml；音视频使用 FFmpeg/ffprobe。
音轨全量规范化为 16 kHz 单声道 PCM，再交给管理员预置并批准的本地 faster-whisper 模型；
不使用裁短输入、不启用默认 VAD 截段，使用原生词级对齐改善语句时间边界，也不虚构说话人。
ASR 输出与对齐仍可能有误差，不属于逐字准确率保证。

`faster-whisper==1.2.1` 的路径解码接口与较新的 PyAV 存在已观察到的不兼容；
本实现直接传入经验证的 PCM 数组，并固定 `av==16.0.1`。
运行行为与参数可核对 [faster-whisper 固定版本源码](https://github.com/SYSTRAN/faster-whisper/blob/v1.2.1/faster_whisper/transcribe.py)。

## 时间轴与完整性

共同零点是所选音/视频轨道的**首个解码样本中最早的 PTS**，原始值保存为 `source_origin_ms`。
它可以为负或大于一小时；限制的是媒体长度，不是源时钟。容器声明时长另记 `container_duration_ms`。
视频帧 `timestamp_ms`、转写的 `start_ms/end_ms` 都相对于这个共同零点。

每条轨道通过完整 packet 扫描得到自己的末端，不能要求短音轨填满整个视频，
也不能把音轨末端当作视频末端。音频 packet 不允许回退或重叠，仅容许 ffprobe 格式化的 2 微秒舍入误差；
视频 B 帧的 packet PTS 可以重排。音轨空隙在 PCM 中保留，不能压缩时间后只加一个起始偏移。

视频全量解码，每约 5 秒保留真实 PTS 帧；帧间隔与末帧覆盖受检查。
这属于采样，不保证发现两帧之间的瞬时事件。按需补帧、场景检索与原生视听联合回答仍待接入。
`coverage` 证明解析所做的工作，不能证明 Agent 已看图、已理解或已完成协作任务。

## 资源与恢复边界

| 项目 | 当前上限/规则 |
|---|---|
| 原文件 | 1 GiB；父进程 O_NOFOLLOW 读取普通单硬链接文件，并核对独立输入快照 hash |
| PDF | 200 页；每页实际渲染 |
| DOCX | 最多 2000 项、总展开 64 MiB、单项 32 MiB、压缩比预算；不提取到宿主文件树 |
| 图像 | 输入最多 4000 万像素，派生最长边 1600、单张 1 MiB |
| 输出 | 图片最多 1024 张/总计 128 MiB；文本 16 MiB、4096 片段、每段 16 KiB |
| 归档 | manifest 20 MiB，父进程总接收 152 MiB，stderr 64 KiB |
| 媒体长度 | 所选轨道整体时间范围最多 1 小时；不自动截断长文件 |
| 容器 | 禁网、只读根目录、非 root、drop ALL、no-new-privileges、64 pids、2 CPU、4 GiB 内存且禁止额外 swap |
| 临时空间 | `/tmp` 384 MiB、`/output` 160 MiB tmpfs，noexec/nosuid；内存计入 cgroup |
| 期限 | 父进程 wall timeout + 容器内 SIGALRM + CPU 硬上限；默认 3600 秒、管理员最多 7200 秒 |

先 `docker create` 得到确定容器 ID，再 attach/start；清理只操作自己创建的 ID，
同名 create 冲突不能删除其他任务。取消会强制停止容器，清理失败不返回成功。
父进程被强杀时，容器内期限仍会终止解析；PID namespace 退出会清理后代。
执行 ID 可由调用者持久保存，容器 label 保留该 ID，后续 worker 必须在取得自己的状态锁后恢复/回收登记句柄。
库不会扫描并删除其他 worker 的运行容器。

结果先写父进程私有暂存目录，校验、fsync 文件和目录后，以 no-replace rename 安装，随后 fsync 父目录。
这只安装本地解析包，不会改 SQL 状态。父进程硬杀留下的本地 input/bundle 临时文件，
由持久 worker 在独占状态锁下回收；测试 harness 同样只清理自己的目录与容器。

模型预置使用固定 repository/revision，不接受聊天提供的 URL 或凭据。
下载在同文件系统的独立 staging 目录进行，由稳定 inode flock 互斥；重启只回收受控命名的旧临时文件。
校验后 no-replace 发布，已存在文件必须重新核验；最终模型目录不混入临时文件、锁、私人缓存或配置。
批准后的模型目录应保持不可变；升级使用另一版本目录和新的管线批准指纹。

### 可选 ASR 模型

`provision_asr.py --model` 仅接受两个内置别名，默认仍为 `small`，不接受任意仓库、URL、revision 或聊天参数：

| 别名 | 固定公共来源 | 固定 revision | 用途 |
|---|---|---|---|
| `small` | `Systran/faster-whisper-small` | `536b0662742c02347bc0e980a01041f333bce120` | 保留既有低资源配置 |
| `large-v3-turbo` | `dropbox-dash/faster-whisper-large-v3-turbo` | `0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf` | 可选的较大多语言模型；需验证本机资源与实际资料准确性 |

turbo 是现有 [faster-whisper 1.2.1 支持的模型](https://github.com/SYSTRAN/faster-whisper/blob/v1.2.1/faster_whisper/utils.py)，
旧 `mobiuslabsgmbh` 地址现重定向到 [模型发布方的 dropbox-dash 仓库](https://huggingface.co/dropbox-dash/faster-whisper-large-v3-turbo)。
其权重文件约 1.62 GB；下载仍逐文件核对 Git/LFS 身份，再生成本地模型指纹。来源与许可见该模型卡及
[原始 Whisper 模型卡](https://huggingface.co/openai/whisper-large-v3-turbo)。选择更大模型本身不证明准确性提高。
大文件按最多 32 MiB 的 HTTP Range 请求下载；同一进程内遇到短响应或断线，从已写入位置续传，
连续三次传输失败则停止。服务端返回错误范围时直接拒绝，不能把重复字节拼成模型。
进程退出后的私有 partial 仍按原规则清理；这不是跨进程断点恢复，只有完整文件校验后才发布。

```bash
python3 cannbot/media/provision_asr.py --model large-v3-turbo \
  --directory /srv/spark-media/models/large-v3-turbo-0a363e9 > /tmp/asr-turbo-receipt.json
python3 cannbot/scripts/media_worker.py --image-id sha256:ADMIN_APPROVED_IMAGE_ID \
  --model-dir /srv/spark-media/models/large-v3-turbo-0a363e9 --describe
```

输出的模型与管线指纹须由管理员配置到 Logic；脚本不会覆盖现有 small 目录或自动更改 Logic 配置。
实际解析继续禁网、本地加载、2 CPU/4 GiB 限制，也不注入预期姓名或验收答案作为 ASR 提示。
旧 revision 与来源记录保留；切换模型不会自动改写既有转写。

实际对比见 [ASR 与原资料播放回执](benchmarks/2026-11-01-asr-and-playback.json)：相同 30 秒合成录音，
small 耗时 11.63 秒，turbo 耗时 46.044 秒。turbo 修正了负责人 Alice，仍将 `queue backpressure`
识别为 `QBAC Pressure`，故准确性门槛未通过。本轮未重跑完整 20 分钟录音，也不据此推荐切换默认模型。
第一次大文件下载因短响应而被校验拒绝；分段下载修复后完整预置通过，原失败日志保留。

## 可重复验证

从仓库根目录执行。基础边界测试只使用 Python 标准库；真实解析测试需要 Docker。

```bash
python3 tests/media_parser_test.py
docker build --target runtime -t spark-media:development cannbot/media
docker build --target fixtures -t spark-media:fixtures cannbot/media

parser_lab=$(mktemp -d)
mkdir "$parser_lab/fixtures"
python3 cannbot/media/provision_asr.py --directory "$parser_lab/model" > "$parser_lab/model-receipt.json"

docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges --user "$(id -u):$(id -g)" \
  --tmpfs /tmp:rw,size=256m \
  --mount "type=bind,source=$parser_lab/fixtures,target=/fixtures" \
  --mount "type=bind,source=$PWD/tests/media_parser_fixtures.py,target=/fixtures.py,readonly" \
  --entrypoint python3 spark-media:fixtures /fixtures.py /fixtures --long-audio

python3 tests/media_parser_e2e.py --image spark-media:development \
  --fixtures "$parser_lab/fixtures" --model "$parser_lab/model" --long-audio \
  --out "$parser_lab/parser-results.json"
```

测试入口将 tag 解析成精确 image ID；生产 supervisor 只接受 `sha256:...` ID。
生成的资料是非私人合成样例；20 分钟音频包含持续技术讨论与末尾独有决策，使用真实 ASR 全量转写。
这不能代替真实多人、有噪声、中文混合会议准确率评估，也不涉及 Pi/Hermes 推理或工具执行。
父进程强杀用例检查实际运行容器、挂载/网络/资源设置、内部期限退出码以及未安装结果。

## 本阶段验证（2026-11-01）

[固定源码与实际回执](benchmarks/2026-11-01-media-parser.json)：12 项边界单测、22 项真实隔离解析 E2E 通过。
20 分钟连续合成技术语音完整解码 19,199,651 个样本，产生 395 段，耗时 221.026 秒；末尾独有决策与待办均被识别。
视频派生帧经独立解码核对红→绿，35 秒音轨空隙及空隙后转写锚点被保留。
父进程强杀后，配置 8 秒内部期限的容器实际约 8.34 秒退出，无结果发布。
全 CTest 为 70 项、64 通过、6 项环境跳过，逐项原因见报告；跳过不计通过。

这些是真实解码和 CPU ASR，输入是合成测试资料，不是真人会议准确率或原生 Agent 理解验收。

## 下一步接入

1. 已接入 revision 派生物清单、生成名存储与完整 manifest 服务端原子发布，后续保持这些合同。
2. 已接入 worker 持久登记执行 ID 与 lease、独立续租及强杀恢复；失效停止解析，旧 attempt 不能提交，重启只回收自己的文件/容器。
3. 当前成员可授权读取派生图/转写；共享资料删除与 context generation 一起使旧会话上下文失效。
4. Pi/Hermes 受限原生媒体工具、房间消息检索、真实 PDF+代码/录音任务/视频视听回答，以及 Web 上传/预览/恢复。

派生物存储与发布已处理以下事务边界，后续 worker 必须复用：

- 房间逻辑配额统计新资料原件和所有未回收 revision 的派生物预留；在写文件之前提交预留，清理完成后才能释放。
- 当前 Create 先锁 `im_group`，新的媒体 Claim/配额操作不能先锁 attachment 再反向取 room；
  Claim 的幂等身份锁可在最前，但其他路径不得反向取 claim。
- Claim 选候选时可能已建立 RR 快照，取得 room 锁后不能用过期快照的 SUM 判断剩余容量；
  采用当前读的配额账本或适当事务隔离，必须用真实并发测试证明。
- 同一个 `(revision,index)` 重试必须复用服务端确定的文件身份。不能每次 SQL 回滚都生成新文件名，
  否则孤立文件可能绕过记录条数与配额；GC 与 writer 需共用固定 inode 的文件锁。

派生物预留、确定性文件身份和两阶段回收基础已实现，见 [持久存储边界](multimodal-collaboration.md#derived-storage-foundation)。
manifest publication 和公开派生物读取已接入；完整媒体 worker 及自动 GC 尚待完成，tar 校验不能替代服务端最终发布检查。

这些步骤未完成之前，完整多模态产品目标保持进行中。
