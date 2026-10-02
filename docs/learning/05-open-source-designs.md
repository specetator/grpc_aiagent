# 成熟开源 IM：可复用的设计与本项目落点

核验日期：2026-10-02。这里比较项目的公开协议、职责分层和客户端机制，所有外部事实链接到官方仓库或规范。本次没有复制这些项目的实现代码，也没有把 Spark Push 替换为其服务端。成熟度判断是针对可学习的设计与生态，不能由 star 数推导出生产容量。

## 1. 值得对照的四个方向

| 项目 | 官方材料与可学习重点 | 对本项目的适配判断 |
|---|---|---|
| OpenIM | [服务端](https://github.com/openimsdk/open-im-server)与[Go SDK 核心](https://github.com/openimsdk/openim-sdk-core)：服务端/SDK 分层，SDK 包含本地存储、连接与回调 | 优先学习客户端数据成为界面真源的分层；现有 Kotlin/SQLite 不必整体迁移到 Go SDK |
| Tinode | [官方仓库](https://github.com/tinode/chat)与[协议 API](https://github.com/tinode/chat/blob/master/docs/API.md)：topic 内 seq，明确 recv/read，临时通知与持久消息分开 | 适合逐项审查消息语义。其 README 自述 beta-quality；服务端 GPL-3.0、客户端 Apache-2.0，直接代码复用前须逐仓核对许可 |
| WuKongIM | [官方仓库](https://github.com/WuKongIM/WuKongIM)与[消息概念](https://origin-docs.githubim.com/en/guide/core-concepts/messages/)：channel 内顺序，client number / server ID / sequence 三种身份 | 与本项目 client_msg_id / msg_id / msg_seq 很接近。当前 v3 主线明确 beta，不能把新主线称作稳定生产版；可学习 channel 边界与确认合同 |
| Matrix / Synapse 生态 | [官方客户端服务端规范](https://spec.matrix.org/v1.13/client-server-api/)：sync token、增量状态、timeline gap 补齐 | 适合学习同步 API 与设备状态；本项目是单中心会话服务，暂不需要引入完整联邦与端到端加密协议 |

OpenIM 服务端和 WuKongIM 仓库标注 Apache-2.0。这里复用的是设计原则；若以后直接引入文件或依赖，仍要按具体仓库版本核对许可证和 NOTICE。Matrix 这一行讨论的是协议规范，不代表某个具体服务端的许可证。

## 2. 从外部设计映射到已经实现的代码

### 2.1 三种身份分工，而不是一个 ID 解决所有问题

WuKongIM 官方消息概念区分客户端重试号、服务端消息 ID、channel 内 sequence。Tinode API 也将请求 id 与 topic 内消息 seq 分开。借鉴后的本项目合同如下：

| 身份 | 生成方 | 生命周期 | 实现位置 |
|---|---|---|---|
| client_msg_id | Android/Web 发送方 | 同一次逻辑发送的所有重试保持相同 | [Android store](../../android-app/app/src/main/java/com/peco/sparkim/SparkMessageStore.kt) |
| msg_id | 服务端首次分配 | 全链路关联、消息去重 | [ConversationStore](../../logic/conversation_store.cpp) |
| msg_seq | 会话分配器 | 只在同 session 比较先后 | [SQL reservation](../../logic/message_reservation.cpp) |

这次 SQL reservation 保存首条完整内容，在 Redis 丢失且 Job 尚未写入历史时也能回答同 ID 的重试。**这是本项目为 Kafka/SQL 边界实现的方案**，不能把它描述为上述项目内部已经采用的相同算法。细节见 [可靠消息章节](02-reliable-message-pipeline.md)。

### 2.2 把界面建立在本地数据库之上

OpenIM 的官方介绍把 Local Storage、Connection Management、Listener Callbacks 作为 SDK 的职责。对本项目的具体启发是：连接事件驱动数据入库，界面从库读取；断网时也能展示已保存会话，待发送项也有持久身份。

这次 Android 以 `SQLiteOpenHelper` 实现四张表和事务，不依赖 OpenIM 的 SDK。发送流程是本地提交 → 网络上传/发送 → accepted ACK 合并权威身份 → 原子移除 outbox。接收流程是解析 → 本地提交 → 回执。设计落点与索引消融数据见 [Android 章节](03-android-local-store.md)。

### 2.3 接收、已读与网络确认分开

Tinode 的 `recv` 与 `read` 是不同的自报状态，临时 presence 不等于历史持久消息。WuKongIM 官方消息说明也强调发送确认的边界不能推导为每个设备收到或用户已读。

本项目进一步为设备补偿定义：

```text
accepted_ack    Kafka 持久化事件确认
delivered_ack   接收端 WebSocket 队列接纳
received_ack    设备本地库提交后的前缀 / 稀疏回执
read state      用户阅读语义
```

`delivered_ack` 的含义是本项目现有合同，不能误写成 Tinode recv 的完全同义词。更不能让发送者自己的 self-sync 触发接收者 delivered。

### 2.4 同步是协议，不是只在断线时重新请求一次历史

Matrix `/sync` 以 `next_batch` 作为后续 `since`，timeline 有 gap 时通过历史接口补齐。它提示我们将“当前已同步状态”“增量数据”和“历史分页”分开建模。

Spark Push 没有照搬 Matrix token。这里使用每设备/每会话连续 prefix 加 sparse receipt，以解决 1、3、2 到达、永远未持久的分配空洞和重复下载。Comet 在握手、周期恢复以及有下一页的回执之后推动 sync。读完 [02](02-reliable-message-pipeline.md) 和 [03](03-android-local-store.md)，应能说明为何 `max(seq)` 不能代替连续 prefix。

### 2.5 临时 AI 片段与最终消息采用不同恢复合同

持久聊天消息和 transient 通知分开，是这些协议可对照的共同方向。本项目的 `ai_delta` 是流式界面输出，不写历史、没有可靠消息序号、不生成设备接收回执；最终 `ai_reply` 经校验后走同一 Kafka → SQL → outbox 路径。

这不是承诺流式 token 在重启后逐个恢复。它使最终结果具备普通消息的历史、幂等和设备同步能力，同时避免把每个短片段变成可靠消息。详见 [AI 链路章节](04-agent-and-inference.md)。

## 3. 为什么本次保留现有架构

Spark Push 已有 C++ Comet/Logic/Job、MySQL/Redis/Kafka、Android Kotlin 与 Pi/推理扩展。直接把 Go IM 服务端接入，意味着重做认证、群成员、会话标识、ACK、历史与 Agent 编排的边界，也无法证明迁移后的故障语义与当前一致。

本次选择的是在这些边界上增加可验证机制：SQL 身份预留、原子 message/outbox、租约与 generation fencing、客户端事务、设备游标、队列容量。每项机制都有源码、故障测试和本地实测，而不是以“采用成熟项目”代替验证。

## 4. 后续可继续借鉴，但本次没有实现

- 把 SDK 的网络、存储和界面 API 再封装成稳定接口；当前已有分工，仍可减少 ViewModel 的编排复杂度。
- 增量同步页的批量 receipt 与 watermark 合同，需先明确永久序号空洞、历史保留和设备淘汰策略。
- 大群 fanout、历史清理、outbox 完成项归档和设备长期离线后的恢复策略，需要生产负载与运维合同。
- 若未来需要联邦、完整端到端加密或大型群协作，再单独评估 Matrix 生态；不能认为当前系统已具备这些能力。

这些是基于现有源码与外部规范作出的适配判断，不是外部项目对 Spark Push 的背书，也不是本次交付完成项。性能报告会如实说明新的耐久边界成本，不能拿其他项目的宣传吞吐替代本机数据。
