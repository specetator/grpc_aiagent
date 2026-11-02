# Spark Push 项目学习、八股与求职面试手册

> 更新：2026-11-02。源码入口：本 Git 仓库根目录；语言基线：C++17。
> 产品基线：已发布的 v0.1.0，受检提交 `7f92f3d`；后续文档提交不改变发行制品。
> 源码核对基线：`a3901af`，已提交图文协作与源码整理；当前源码、v0.1.0 发布制品与各阶段验收分别标注。音视频已按用户要求暂缓。
> 使用目标：从“看懂项目”走到“解释原理、定位源码、复现实验、应对追问、写出可信简历”。
> 本次更新：统一 Git 管理，补齐图文协作、来源任务和重构取舍；保留原有学习路线、66 道问答与 10 个 C++17 实验，新增 Q67～Q69。版本管理见 0.11，资料闭环见 10.9，最新文档检查见 14.9。

这份手册面向 C++ 后端岗位，也覆盖 Agent 应用开发岗位。学习时沿着一条真实业务链路，
把网络、并发、存储、权限和故障恢复连起来。第 4 节保留源码深读，第 5、11 节补齐基础原理，
第 10 节讲最新多人协作，第 12 节安排练习，第 7、13 节训练求职表达。
建议把第 11.8 节的连续追问作为学习检查点：每学完一条源码链路，就合上文档回答一轮，再用实验核对。

本文区分四种口径：**当前实现**以源码为准，**实际验收**以带版本的回执为准，
**基础知识**解释通用原理，**扩展设计**用于面试讨论。后两者不能直接写成已经交付的功能。
简历中的第一人称是表达模板，请替换成自己实际参与、能解释和复现的贡献。

<a id="study-roadmap"></a>
## 0. 怎么用这份手册学习

### 0.1 先形成五张图，再读函数细节

1. **架构图**：Client → Comet → Logic → Kafka → Job → Comet，标出 MySQL、Redis 和 Agent 分支。
2. **消息时序图**：标出 SQL 预留、Kafka 确认、连接接纳、历史落库，各阶段失败后从哪里恢复。
3. **线程与所有权图**：谁运行 EventLoop，谁阻塞等待，异步闭包持有什么，谁负责停止和释放。
4. **权限图**：登录身份、正式成员、角色、Agent 安装、工作区授权分别在哪一步检查。
5. **状态图**：消息状态、Agent 执行状态、协作任务状态分开画；不要共用一个“成功”含义。

每读一个模块，写一张学习卡：

```text
业务问题：为什么需要它？
基础原理：涉及哪些 C++ / OS / 网络 / 数据库知识？
实现证据：文件、关键函数、数据结构和不变量。
故障推演：在哪一行前后崩溃，会丢失、重复还是结果未知？
验证方法：输入、预期现象、观测数据、实际结果。
面试表达：30 秒结论 + 2 分钟过程 + 一个限制。
```

“能复述概念”只是起点。合格标准是：关掉文档后，能画图，能找到对应函数，能说明一个失败路径。

### 0.2 按岗位选择主线

| 目标 | 优先掌握（P0） | 第二轮深化（P1） |
|---|---|---|
| C++ 后端 | 第 2～3 节；4.1～4.16；第 5、6、11 节 | 空间权限、任务 CAS、部署恢复、Agent 接入边界 |
| Agent 应用后端 | 消息/权限基础；4.19、4.21～4.23；第 10 节 | 4.24 原生运行时、工具边界、unknown 恢复 |
| 校招或基础薄弱 | 先学第 5、11 节，再回到一条消息的调用链 | 第 12 节手写题与故障练习 |
| 面试前快速复盘 | 第 7 节表达、第 6 节证据、第 13 节模拟面试 | 按追问返回源码，补最弱的两项 |

第 4.24 节是较长的 Pi 深读专题，第一次学习不必从头通读。先理解系统如何保证身份、消息和执行结果，
再研究 SDK 如何组织 Agent Loop；不要因为熟悉某个框架名而跳过 C++ 和数据库基础。

### 0.3 两周学习安排

默认每天 1.5～2 小时；按自己的基础伸缩，不要求在一天内读完一个大文件。

| 时间 | 学习任务 | 当天必须留下的产物 |
|---|---|---|
| 第 1～2 天 | 产品演示、架构、一次发送的完整链路 | 五分钟演示；一张有 ACK 边界的时序图 |
| 第 3～4 天 | RAII、智能指针、条件变量、Reactor | 解释一个异步对象生命周期；完成队列练习 |
| 第 5～6 天 | MySQL 索引/事务、取号、Outbox | 两客户端并发取号推演；SQL 与 Kafka 崩溃窗口表 |
| 第 7～8 天 | Kafka、Redis 路由、连续游标与补拉 | 乱序/重复/缺口样例；能区分 offset、seq、游标 |
| 第 9～10 天 | 空间 RBAC、对象权限、多 Agent 隔离 | 成员撤销链路图；一条消息调用两个 Agent 的身份表 |
| 第 11～12 天 | 真实回执、冷备恢复、性能定位 | 一份练习记录；一页“测过与没测过”清单 |
| 第 13 天 | 简历压缩、贡献核对、三分钟项目介绍 | 3～4 条简历要点，每条附源码和证据 |
| 第 14 天 | 45 分钟模拟面试与复盘 | 录音或逐字稿；按第 13 节评分并修订薄弱答案 |

### 0.4 阅读导航

- 项目与消息主线：第 1～4 节；C++ 基础：第 5 节。
- 测量与验收口径：第 6 节；简历与介绍：第 7 节；源码与术语：第 8～9 节。
- [最新协作空间与多 Agent 案例](#collaboration-cases)：第 10 节。
- [基础知识与面试追问](#interview-foundations)：第 11 节。
- [六组连续追问：把基础题讲回项目](#interview-followups)：第 11.8 节。
- [C++ 工程题与架构取舍](#cpp-engineering-interview)：第 11.9～11.10 节。
- [跨层八股：分配、背压、落盘与 MVCC](#interview-cross-layer)：第 11.11 节。
- [六个易混概念的面试辨析](#interview-discrimination)：第 11.12 节，先作答，再展开参考答案。
- [C++ 代码评审中的基础追问](#cpp-review-foundations)：第 11.13 节，区分容量、对象、异常与时间预算。
- [并发、网络与安全的跨层追问](#interview-concurrency-security)：第 11.14 节，区分 CAS、租约、哈希与传输确认。
- [线程退出与消息完成的连续追问](#interview-shutdown)：第 11.15 节，从 atomic、条件变量、join 讲到业务失败恢复。
- [资料授权的数据库与安全追问](#interview-context-access)：第 11.16 节，从分页讲到对象权限、撤权与来源回执。
- [网络解析与 C++ 边界检查](#interview-framing)：第 11.17 节，把 TCP、长度计算与异步所有权串起来。
- [资源池与异步结果](#interview-resource-pools)：第 11.18 节，回答 RAII 为什么不能防止死锁，以及线程为什么可能都在等待。
- [动手练习与复现方法](#practice-lab)：第 12 节。
- [可运行的 C++17 基础实验](#cpp-interview-experiments)：第 12.8 节，先预测结果再编译。
- [双连接 MySQL 实验](#sql-interview-experiments)：第 12.9 节，观察快照、锁定读和过期版本更新。
- [虚函数与完美转发实验](#cpp-dispatch-experiment)：第 12.10 节，区分类型规则与业务保证。
- [FIFO 去重与差分验证](#fifo-dedup-experiment)：第 12.11 节，把复杂度、淘汰策略和恢复边界一起讲清。
- [CAS、ABA 与任务版本实验](#cas-version-experiment)：第 12.12 节，观察失败时 expected 的变化；第 12.13 节把实验讲回源码。
- [双线程关闭实验](#queue-close-experiment)：第 12.14 节，用同步门控制交错，区分入队、执行与停止。
- [权限过滤与分页实验](#authorized-pagination-experiment)：第 12.15 节，验证空页误判、游标丢项与固定消息上界。
- [增量拆帧实验](#framing-experiment)：第 12.16 节，穷举一条短字节流的切分方式；[现场评审题](#framing-review)见 13.10。
- [future 超时、异常与完成实验](#future-result-experiment)：第 12.17 节；[资源耗尽专题面试](#resource-pool-rehearsal)见 13.11。
- [模拟面试、自测与个人贡献](#interview-rehearsal)：第 13 节。
- [Git 版本管理与文档入口](#git-workspace)：第 0.11 节。
- [图文协作的当前完整链路](#document-collaboration)：第 10.9 节。
- [精简实现与工程取舍](#interview-refactoring)：第 11.19 节，Q67～Q69。
- [官方原理资料](#primary-references)：第 14 节。

Agent 深读顺序：4.24.1～4.24.5 理解对象与循环；4.24.7～4.24.9 理解流式协议；
4.24.6、4.24.10～4.24.14 理解并发、工具、记忆和取消；4.24.15～4.24.17 训练完整讲解。

### 0.5 判断一个知识点是否真的学会

下面每行是一组学习目标，最后一列是可以交给同学检查的产物。先完成 C++ 后端主线，再深入 Agent 专题。

| 项目切入点 | 必须理解的基础 | 能回答的追问 | 学习产物 |
|---|---|---|---|
| 会话提交锁 | RAII、shared/weak、析构顺序、互斥范围 | 锁表里的对象何时销毁？两个 Logic 实例还能互斥吗？ | 画所有权图；解释 11.8-B 的代码 |
| 线程池与 EventLoop | 条件变量、就绪通知、阻塞、排队与背压 | 增加线程能否解决热点会话？队列满后谁收到错误？ | 写停止时序；完成 11.8-C 的容量推演 |
| 消息取号与历史 | 唯一索引、事务、行锁、范围扫描 | 同 ID 改正文怎么办？为什么深分页不使用大 OFFSET？ | 写两个事务交错表；保存自己的 EXPLAIN |
| ACK 与 Outbox | 业务确认、至少一次、幂等、故障窗口 | Kafka 已收但 ACK 丢了，客户端能否换 ID？ | 画 11.8-A 的失败恢复图 |
| 成员与任务 | 认证/授权、TOCTOU、乐观并发控制 | 老连接仍在线怎么办？revision 冲突能否直接覆盖？ | 写撤权用例与双人修改用例 |
| Agent 原生执行 | 状态机、身份隔离、幂等账本、恢复 | unknown 为什么不能直接转 failed 后重试？ | 对照原 request/Run 与工具记录解释恢复 |

复习时先口述，再看答案。答错后记录“错在哪里、源码证据是什么、下次如何验证”，不要只重新抄一遍定义。
初学者每次完成一行即可；面试前用这些产物替代临时背诵整篇文档。

### 0.6 先做一次 20 分钟摸底，再决定从哪里学

每题口述两分钟，再用一分钟记下卡住的位置。不要查答案；不会的题正是学习入口。

| 自问题 | 只背概念时容易漏掉什么 | 回看位置 |
|---|---|---|
| 发消息超时后，重新点击发送为什么可能重复？ | 原请求可能已成功；应沿用业务身份，而非只考虑 TCP 重传 | 11.8-A、4.1～4.2 |
| 把连接放进 shared_ptr，为什么还不能随意跨线程操作？ | 生命周期与对象同步是两个问题 | 5.1、11.8-B |
| 用过 epoll，能否解释一次发送没有写完怎么办？ | 可写是就绪通知，剩余缓冲、写事件与慢连接还要管理 | 11.3、4.16 |
| 数据库有索引，为什么同一个群仍会变慢？ | 范围扫描、锁竞争和外部等待需要分别测量 | 11.8-C/D、6.2 |
| 用户退出群聊后，已有 Agent Session 为什么仍值得检查？ | 服务端新请求权限与 Runtime 已缓存上下文属于不同边界 | 10.2、10.5、11.8-F |
| 如何证明简历里的“优化”和“可靠”？ | 需要同条件对照、失败率、故障点与结果记录 | 6.5、12.7、13.3 |

能说定义但没有项目例子，补源码；能说源码但没有失败路径，补故障推演；能说失败路径却拿不出证据，补实验。
第一次不要求全部答对。将最薄弱的两题安排到下一次学习，第二周再录同一轮回答，比较解释是否更具体。

### 0.7 面试题索引：先学什么，答到哪里才够

P0 是本项目 C++ 后端主线的必修内容，P1 用于第二轮追问；这是学习顺序，不是预测某家公司必考什么。
每题练习“30 秒结论 → 90 秒原理与项目例子 → 一个反例”，不要一次背出整页答案。

| 优先级 | 题目与知识点 | 源码/业务落点 | 自测通过条件 |
|---|---|---|---|
| P0 | Q01～Q07、Q34～Q37：生命周期、并发和异步捕获 | 会话锁、任务队列、连接回调 | 指出谁拥有对象、谁同步状态、谁负责退出 |
| P0 | Q12～Q17、Q39、Q44：TCP、Reactor、超时 | Comet、MessageStream、PushStream | 区分字节传输、业务 ACK、心跳和持久化 |
| P0 | Q18～Q23、Q41、Q47：索引、事务、失败处理 | SQL 取号、历史分页、成员事务 | 画出两连接交错，说明读版本、锁与回滚范围 |
| P0 | Q27～Q32、Q46：Kafka 与消息可靠性 | reservation、Outbox、消费位点 | 推演三个崩溃窗口，沿用原业务 ID 恢复 |
| P0 | 10.1～10.5、11.8-E/F：空间与 Agent | 正式成员、Session、Run、任务 revision | 解释撤权、跨房间隔离和 unknown 的处理 |
| P1 | Q04、Q33、Q42～Q43：接口、构建、模板 | Repository、CMake、任务封装 | 能写 override 和转发实验，解释编译错误 |
| P1 | Q24～Q26、Q45：Redis 与缓存 | token、路由、路由代数 | 用时间线说明陈旧回填，区分缓存与权威数据 |
| P1 | Q09～Q11、Q38～Q40：OS、内存、IO | 线程池、连接池、输出缓冲、恢复 | 用观测证据区分等待、积压、泄漏与落盘 |
| P0 | Q48～Q49：容器容量、异常安全 | fanout 目标列表、有界去重窗口 | 区分 reserve/resize；指出两次写入之间的失败状态 |
| P1 | Q50～Q51：对象表示、时间预算 | Protobuf、连接池等待 | 不把对象内存当协议；不把等待截止当阻塞调用的取消 |
| P1 | Q52～Q53：CAS、ABA、租约与旧执行隔离 | 任务 revision、媒体 worker 的 attempt/token | 分清进程内原子操作、数据库并发条件和服务端拒绝旧写入 |
| P0 | Q54～Q55：密码、身份、HTTP/2 与流控 | security、UserService、双向 gRPC 流 | 不把 hash 当授权，不把 Write 返回当业务完成 |
| P0 | Q56～Q58：内存序、关闭与异常 | NextRequestId、ThreadPool、Comet 析构 | 画出 happens-before；分清拒绝新任务、清队列、取消和 join |
| P1 | Q59～Q61：授权分页、TOCTOU、来源与编码 | MediaContextService、Messages、Record | 构造先 LIMIT 后过滤的反例；区分读回执、送达与模型理解 |
| P0 | Q62～Q63：增量解析、整数边界与缓冲所有权 | HandleWebSocketFrame、TcpConnection::send | 能手写半包状态、拒绝超限长度，解释何时必须拥有正文 |
| P0 | Q64～Q66：死锁、资源池容量、future 与异常传播 | KeyedMutex、MySqlConnectionPool、ThreadPool | 画等待关系，区分借用超时、任务停止、结果就绪与业务成功 |

每行对应一张第 0.1 节的学习卡。完成后把“能背答案”改成“能给出函数名、边界输入和验证方式”，
再进入第 13 节的模拟面试；第 13.6 节用于安排第二天、第三天和第七天的复述。

### 0.8 拿到岗位描述后，怎样挑选这份手册的内容

先圈出岗位要求的两条主线，再各选一个能讲透的项目案例。不要因为岗位列出十种技术，就在简历里认领十种能力。

| 岗位关注点 | 优先准备的项目案例 | 当场可能要求的基础题 | 交付给自己的学习证据 |
|---|---|---|---|
| C++、Linux、网络编程 | Comet 接入与线程退出 | 所有权、Reactor、条件变量、部分写、join | 第 12.14 节实验；一张对象与线程退出图 |
| 数据库、缓存、消息队列 | 同 ID 重试与历史恢复 | 联合索引、事务、隔离、缓存失效、消费位点 | SQL/Kafka 失败窗口表；一条消息的身份记录 |
| 并发与性能分析 | 热点会话锁和队列积压 | 锁粒度、排队、p99、内存预算 | 分段计时方案；有实测才填前后数字 |
| AI 应用与工具集成 | 多 Agent 隔离和 unknown 恢复 | 身份可信边界、幂等、原生会话、工具副作用 | 一个请求的 room/agent/actor/Run 对照表 |

如果还没有个人代码贡献，先用“阅读、复现、分析”描述学习成果，再选择一个有明确验证方法的改进。
基础八股、项目案例与简历句子应指向同一件事：例如写了“并发资源管理”，就应能回答 Q02、Q07、Q57，
而不只是解释 shared_ptr 的定义。第 7.7 节给出口述范例，第 13.8 节用于连续追问。

### 0.9 从一个问题学完整条链：收到字节，为什么还不能处理消息？

这条练习适合正在准备 C++ 网络后端面试的人。已有 TCP 基础可用约 90 分钟完成；否则先学 Q12～Q16，分两次练。
目标不是增加背诵量，而是把一份代码讲清楚、改出一个可观察的错误，再解释测试覆盖到哪里。

| 顺序 | 具体动作 | 完成标准 |
|---|---|---|
| 先答，10 分钟 | 不看答案解释“发送两次，接收端是否恰好 recv 两次？” | 说清字节流、协议边界和业务完成是三个问题 |
| 追源码，20 分钟 | 阅读 4.15、Q62～Q63，定位 HandleHandshake → HandleWebSocketFrame → OnTextMessage | 标出未消费、已消费、拒绝连接三个分支，以及正文何时取得所有权 |
| 手写，25 分钟 | 遮住 12.16 的答案，实现两字节长度前缀解析；先写合同再写循环 | 半头、半正文、连帧、零长度、超限都可解释 |
| 验证，20 分钟 | 跑 256 种切分，再故意破坏一项检查 | 保存一次通过和一次预期失败，说明失败断言保护什么 |
| 面试，15 分钟 | 用 13.10 的代码评审题口述，再写 90 秒介绍 | 按“原理 → 源码 → 反例 → 边界”作答；不把教学协议说成产品 WebSocket |

学习卡只需一页：画一条输入缓冲变化图，贴出关键函数、实际输出和一个失败断言，最后写下一条未验证的风险。
完成后可表述为“阅读 Comet 增量解析路径，并用 C++17 教学解码器验证分段输入与长度边界”；
只有亲自修改并验证了产品实现，才能将其升级为“实现或修复 Comet 协议处理”。第 7.8 节的贡献分层同样适用。

### 0.10 面向求职的三轮读法：每一轮都要能够独立作答

全文适合作为查阅手册，不必顺序背完。第 0.3 节安排时间，这里规定每一轮的通过条件；
没有通过就回到相应源码或实验，不以“看完多少页”衡量进度。

| 轮次 | 只抓这些问题 | 阅读与练习入口 | 合上文档后的通过条件 |
|---|---|---|---|
| 第一轮：能讲完整项目 | 用户发一条消息，谁鉴权、取号、确认、推送、存历史？ | 第 2～3 节、4.1～4.5、7.1 | 3 分钟画完链路，解释两个 ACK，推演一次断线重发 |
| 第二轮：能解释实现选择 | 为什么使用这类锁、容器、索引和线程模型？ | 第 5 节、Q01～Q32；按弱项选 12.8、12.9、12.14 | 每个概念给出源码入口、替代方案、一个失败边界 |
| 第三轮：能应对变题 | 增加用户、缩小连接池、撤销权限或执行结果未知时怎样处理？ | 11.8、11.10、11.18；第 13 节 | 先列已知事实，再提出可验证假设；不把扩容或重试当通用答案 |

每轮留下三样东西：**一张调用/等待关系图、一份自己执行的记录、一段不超过 90 秒的回答**。
如果只能说“用了线程池提升性能”，第二轮尚未通过；应进一步回答任务运行在哪、队列何时拒绝、
哪个资源限制吞吐，以及线程阻塞时谁还能够推进工作。11.18 → 12.17 → 13.11 提供一条完整示范。

面试前只剩 45 分钟时：用 10 分钟画消息主线，15 分钟复述自己最熟的一个实现和故障案例，
10 分钟核对简历中的贡献与数字，最后 10 分钟回答 13.11 的追问。没有亲自验证的内容仍写作学习或设计，
不要因为时间紧而把项目报告改写成个人业绩。

<a id="git-workspace"></a>
### 0.11 只保留一个开发仓库，用 Git 查看历史

日常开发目录是 `/home/peco/cppcode/fenbushi/sparkpush_project`。
本手册统一保存在仓库的 `docs/project-guide-and-resume.md`，随源码提交；父目录中的
`项目指引与简历书写.md` 保留为指向它的符号链接。编辑任一入口都是同一份内容，查看本地源码链接时优先打开仓库内路径。
不在父目录再建立嵌套 Git 仓库，也不把个人 `.env.local`、凭据、工作区或构建产物加入提交。

| 对象 | 查看方式 | 能说明什么 |
|---|---|---|
| 当前开发源码 | `git log -1 --oneline`、`git status --short` | 当前提交与未提交修改 |
| 第一版候选 | `git show v0.1.0-rc.1:README.md` | 候选源码，对应 `48ffd6e` |
| 正式版源码 | `git show v0.1.0:README.md` | 受检产品提交 `7f92f3d` |
| 正式安装包/镜像 | GitHub Release 与 `release.json` | 打包后的文件、镜像 digest 与依赖版本 |
| 当前手册历史 | `git log --follow -- docs/project-guide-and-resume.md` | 手册从入库起的演进；本节以前的本地编辑不伪造为 Git 提交 |

```bash
cd /home/peco/cppcode/fenbushi/sparkpush_project
git status --short
git fetch origin --tags
git tag --list
git log --oneline -8
git diff v0.1.0..main -- logic/services web_demo/static
git show v0.1.0:logic/services/room_agent_service.cpp
```

这组查看命令不切换正在工作的分支。不要为了看旧代码而 reset 当前工作树。
需要实际修改旧版本时再建立独立分支或临时 worktree；只读比较使用 `git show` / `git diff` 即可。
原 `spark-push-v0.1.0` 与 `spark-push-v0.1.0-rc.1` 是安装包解压副本，核对清单后已移入回收站，
不再作为开发目录。安装包里的固定 Compose 镜像配置可能与同提交源码不同，不能把 Git 源码快照直接冒充原安装包。

**面试练习**：解释工作区、暂存区、提交、分支、标签、发布包之间的关系；
再用一次 `git diff` 找出“这次改了什么”，用带版本的回执解释“这个版本实际验证了什么”。
不要把一个测试通过数字复制给没有运行过的新提交。

## 1. 项目定位与简历主线

Spark Push 是基于 C++17 的实时消息与 Agent 协作项目：Comet 处理 WebSocket 连接，Logic 处理会话和消息编排，
Job 消费 Kafka 完成推送与历史持久化；Bridge 将消息接入 Kernel Agent、Hermes 或推理服务。

| 简历主线 | 项目中的具体实现 | 可以延伸的知识 |
|---|---|---|
| 分布式消息可靠性 | SQL 取号、Outbox、两阶段 ACK、断线补拉 | 事务、幂等、至少一次投递、状态机 |
| C++ 网络与并发 | Reactor、双向 gRPC、线程池、连接池 | IO 多路复用、RAII、智能指针、锁与条件变量 |
| 数据结构与算法 | 会话锁表、连续游标、去重窗口、增量重排 | 哈希表、队列、有序树、复杂度与摊销分析 |
| Agent 工程 | Kernel Agent、子任务编排、Session 路由、执行账本、上下文压缩 | Agent Loop、记忆、RAG、任务调度、取消与恢复 |
| 性能分析 | 分层计时、背压、批量刷新、热点会话分析 | 排队论、锁竞争、吞吐与尾延迟 |

当前 v0.1.0 已支持私有协作空间、邀请与角色、通用 Room Agent、持久协作任务和 Docker 单机部署。
登录使用随机 token 和 Redis 会话；正式成员关系是访问房间、历史、附件和 Agent 的权限依据。
面试主线可以从“一个用户发消息”扩展到“多人带权限地协作”，详见第 10 节。

## 2. 架构、数据与线程模型

```text
Client ── WebSocket ─► Comet ── MessageStream ─► Logic
  ▲                      ▲                       ├─ Redis：路由与缓存
  │                      │                       ├─ MySQL：序号与 Outbox
  │                      └── PushStream ── Job ◄──┤
  └──────────── 下推 / ACK / 历史补偿              └─ Kafka：持久化与推送事件

Agent：Logic ─► ai_request ─► Bridge ─► AgentRouter ─► Kernel Agent / Hermes
       Logic ◄── ai_delta / ai_reply ◄────────────┘
       Bridge 也可按配置选择 Inference Gateway
```

| 模块 | 工作内容 | 并发组织 |
|---|---|---|
| Comet | 连接、协议解析、上行与下推 | Muduo main loop + IO worker；有界 RPC 线程池 |
| Logic | 会话、取号、幂等、路由、消息编排 | HTTP/gRPC 工作线程；按会话提交锁；Outbox 后台恢复 |
| Job | Kafka 消费、投递、历史写入 | 按 topic 消费；每 Comet 的流 reader/writer |
| Bridge | 模型调用、流式投影、最终回复发布 | Kafka consumer + HTTP/SSE；可选控制抢占 |
| Kernel Agent Gateway / Router | 会话绑定、执行账本、Agent 调度 | 同会话串行；跨会话按全局/Agent 配额调度 |

Logic 的分层是 `HTTP/gRPC Adapter → Service → Repository/DAO`：Adapter 处理协议，
Service 组织业务步骤，Repository 封装 SQL、Redis 和 Kafka。面试可延伸到依赖注入、接口隔离和可测试性。

### 2.1 标识与存储

| 标识或存储 | 作用 |
|---|---|
| `session_id` | 消息所属会话；单聊由排序后的两个 uid 构造，房间由 room_id 构造 |
| `client_msg_id` | 客户端一次业务发送的身份，重试沿用 |
| `msg_seq` | 会话内递增序号，用于排序和补拉 |
| `msg_id` | `session_id + "-" + msg_seq` |
| `request_id` | 内部请求、应答与执行回放的关联身份 |
| MySQL `message_sequence` | 分配消息序号 |
| MySQL `message_reservation` | 保存首次受理原文、幂等键和 Outbox 阶段 |
| MySQL `message` / receipt / user_session_state | 历史、逐条投递记录、连续游标 |
| Redis | 路由、在线状态、token 与热点缓存 |
| Kafka | 持久化、推送和 Agent 事件日志 |
| Gateway SQLite / Pi JSONL | 执行与路由账本 / Pi 原生会话 |

### 2.2 一条消息的状态

| 阶段 | 实现动作 | 对应状态 |
|---|---|---|
| 预留 | SQL 事务生成序号并保存原文 | reserved |
| 准备 | 将待发布事件序列化后写入 Outbox | prepared |
| 受理确认 | Kafka 返回持久化事件的 delivery report | accepted_ack |
| 连接接纳 | 目标 Comet 的 owner IO 线程接受下推 | delivered_ack |
| 历史持久化 | Job 幂等写入消息表 | persisted |
| 已读 | 客户端上报阅读进度 | read_seq |

实时投递与历史落库可以交错完成，因此系统分别维护投递记录和持久化进度，再计算连续水位。
Agent 的 `ai_delta` 用于实时预览，完整 `ai_reply` 进入正式消息链路。

## 3. 核心调用链

### 3.1 连接接入

客户端取得 token 后发起 WebSocket 握手；Comet 将验证工作放入 RPC 池，完成后切回连接所属的 IO 线程，
注册路由并执行离线补拉。这里重点理解：耗时操作移出 EventLoop、异步回调检查连接状态、跨线程操作回到 owner loop。

### 3.2 一条单聊消息

1. Comet 从连接上下文读取用户身份，通过 MessageStream 发送目标、正文和 client ID。
2. Logic 校验会话与限流状态，取得对应 session 的提交锁。
3. SQL 事务查幂等键、分配序号、保存 canonical 原文；重复输入返回原记录。
4. 构造二进制 Outbox，等待 Kafka 确认后发送 accepted_ack。
5. 查询接收方路由，发布实时推送任务；持久化和推送由不同消费者处理。
6. Job 经 PushStream 请求 Comet 投递，取得同 request_id 的成功应答后完成消费。
7. Comet 的连接线程接纳发送，记录 receipt；Job 幂等写入历史。
8. 客户端按序号排序和去重，发现缺口时分页补拉。

### 3.3 房间与 Agent

房间 fanout 先由 room→Comet 路由定位相关节点，再在节点内找到对应连接，减少无关节点上的工作。
Agent 输入先经普通消息链路取得持久化事件确认，再进入 `ai_request`；此时历史 SQL 仍可能等待 Job 写入。
Runtime 负责模型与工具执行，Bridge 将事件映射为预览和最终回复。
可以把这两条链路分别讲成“分层 fanout”和“异步任务执行 + 结果回流”。

### 3.4 读到同名业务接口时，先核对执行路径

当前 `MessageService::SendLegacyText` 是旧 HTTP 文本发送路径：由 handler 做登录认证，
Service 检查用户后同步写历史，不走正式发送的 Kafka 确认与实时 fanout，也不复用客户端发送幂等键。
因此不能拿它的 HTTP 成功响应证明 `accepted_ack`，也不能用它的耗时替代 WS → Comet → gRPC → `Submit` 的性能。

阅读入口：[SendLegacyText 与 Submit](../logic/services/message_service.cpp)。
学习时给每个测试标注入口、经过的组件和确认边界；“都叫发消息”不表示测的是同一条链路。
本文的 accepted/delivered 是两个业务确认阶段，与分布式事务协议“两阶段提交（2PC）”不是同一个概念。

## 4. 算法实现与面试追问

本节保留关键实现、数据结构、复杂度和执行步骤。每个主题可按“问题 → 结构 → 过程 → 原理”讲解。

### 4.1 SQL 会话取号与 canonical 幂等

入口：[MySqlMessageReservations::Reserve](../logic/message_reservation.cpp)。
两类唯一约束：session/seq，以及 session/sender/client ID；原文按字节保留。

```text
BEGIN
INSERT message_sequence(session,0)
  ON DUPLICATE KEY UPDATE last_seq=last_seq
SELECT last_seq FOR UPDATE
if last_seq==0: floor=max(历史 MAX(seq),session.last_msg_seq)
find reservation(session,sender,client_id)
if found:
    sealed → 拒绝
    type/body 不同 → 拒绝
    COMMIT，返回原 seq/body/timestamp
else:
    检查 int64 上限
    更新 last_seq=floor+1
    INSERT canonical 原文、stage=0
    COMMIT
```

先 UPSERT 再 FOR UPDATE，是为了取得同一会话的排他锁；对该行使用 INSERT IGNORE 可能让并发事务
先取得共享锁、随后互相等待锁升级。会话锁覆盖查幂等、递增和保存原文，避免两个请求都判定“新消息”。

旧 HTTP 空 client ID 使用安全随机内部标识，每次提交独立；WS 要求 ID 1～128 字节。
重试返回原 timestamp 而非重新取当前时间，避免 Job 完整字段比较把合法重试判为冲突。
参数用 prepared statement；client ID/outbox 使用二进制语义，避免字符集排序规则掩盖差异。

成本：索引定位和事务往返；同会话必须串行，不同会话可竞争不同 SQL 行。
热点会话的处理速度主要取决于锁持有时间和 SQL/Kafka IO。可延伸讨论行锁、事务隔离、唯一索引和共享锁转排他锁时的死锁；这里不是指行锁自动升级为表锁。

**追问**：只用 message 唯一键为什么不够？它只能拒绝碰撞，无法找回未落历史消息的原文和序号。

### 4.2 Outbox 状态、重试与清理

入口：[PrepareOutbox/Published/Pending](../logic/message_reservation.cpp)、[ConversationStore](../logic/conversation_store.cpp)。

```text
0 reserved → 1 prepared → 2 Kafka published → 3 history persisted
0 超过 10 分钟、从未 prepare → 4 sealed
```

1. canonical 消息构造 persist protobuf，prepare 把字节写 SQL，已有 payload 必须完全相同。
2. SendAndWait 返回成功后发送 accepted；超时重试沿用原 ID，由幂等记录识别原操作。
3. 后台每秒扫描最多 10 条 stage=1，用原始二进制重发，确认后推进 published。
4. Job 插入/核对历史后标 stage=3；晚到 delivery report 使用 GREATEST(stage,2)，不得回退 stage=3。
5. stage=0 超时后封存；stage=1/2 保留供后台恢复，直到进入完成阶段。
6. stage=3/4 创建满一天后分批清理，单批最多 1000；这里是有限窗口幂等。

Outbox 用本地事务保存待发送内容，再通过后台重试和下游幂等实现最终收敛。
游标计算中，sealed 视作已结算位置，prepared/published 继续等待历史写入。

**延伸**：本地事务与分布式事务、至少一次投递、幂等消费、单调状态机。

**追问**：Logic 在 Kafka 成功、更新 stage 前崩溃？stage=1 会重发原事件，由 Job 幂等收敛。

### 4.3 会话提交锁：跨会话并行

入口：[KeyedMutex](../common/keyed_mutex.h)、[MessageService::Submit](../logic/services/message_service.cpp)。
map<session-key, weak_ptr<mutex>> 只保存弱引用；调用者拿 shared_ptr 保证等待/持锁期间 mutex 存活。

1. 短暂锁住 map，查 weak_ptr；已失效时创建新 mutex。
2. 每 128 次请求扫描并删除失效 weak entry，避免永远保存历史会话锁。
3. 释放 map 锁后 try_lock 对应会话；每 5 ms 重试，总等待最多 8 秒。
4. 取得锁后执行该提交，包括认证、预留、prepare、Kafka 确认与任务生成。

哈希表平均查找 O(1)，周期清扫 O(S)。进程内的 mutex 控制同会话临界区，数据库行锁处理跨实例取号。
锁内包含 Kafka 等待，因此热点优化可以从临界区长度入手。若需要严格 FIFO 准入，可延伸讨论 ticket 队列。

### 4.4 delivered 连续水位：receipt + 安全前缀

入口：[UserSessionStateDao::AdvanceDelivered](../logic/user_session_state_dao.cpp)。
数据结构：用户/会话状态行 + 每条 seq 的唯一 receipt；SQL join 查第一个缺失证明。

```text
safe = min(持久计数 last_seq, 最早 stage<3 的 seq - 1)
BEGIN，锁 user_session_state(user,session)
previous = delivered_seq
INSERT 当前 seq receipt（重复忽略）
first_missing = (previous,safe] 中最小的历史消息且没有 receipt
candidate = first_missing 存在 ? first_missing-1 : safe
delivered_seq = max(previous,candidate)
删除 <= delivered_seq 的 receipt，COMMIT
```

safe 的求值在 SQL 事务内；不存在未结算 reservation 时，以持久计数/兼容历史 MAX 作边界。
GetDelivered 也会重算，所以历史落库后能利用已有高序号 receipt 收敛。

例：seq=1 prepared 尚未落历史，seq=2 已投递；safe=0，receipt(2) 不能推进 delivered。
等 1 持久化但尚未投递时，first_missing=1；只有补投 1 后才能一起推进到 2。
同用户/会话状态行锁串行化 receipt 更新；查询利用索引范围查找首个未投递位置，再推进水位。

**延伸**：连续前缀、TCP 累计确认、滑动窗口；稠密序号可用位图记录，稀疏序号可用集合记录。

**追问**：为什么 read_seq 不代替 delivered_seq？阅读晚于接纳，按已读补推会重复全部未读消息。

### 4.5 历史、Sync 与跨会话 Offline 分页

入口：[ConversationService](../logic/services/conversation_service.cpp)、[MessageDao](../logic/message_dao.cpp)。
历史倒序使用 WHERE session_id=? AND seq<anchor ORDER BY seq DESC LIMIT L，复合索引定位后读一页。
Sync 正序使用 seq>after，先取得 safe，再读取 L+1 条，用多一条判断 has_more。

1. 校验 actor 是会话参与者/房间持久成员。
2. Sync **先读安全前缀、后读历史**，避免查询之间低序号晚插入被错误跳过。
3. 按条数与 2 MiB 估算字节预算截页；单行超过预算返回 413，不推进游标，需 HTTP history。
4. next_seq 是本页最后 seq；safe_seq=min(safe,next_seq)，空页可返回完整 safe。
5. Offline 收集已授权单聊/房间，按 session ID 排序，从 after_session_id/after_seq 继续。
6. 每会话以 max(delivered_seq, continuation) 查询，共用页预算；每页最多 200，继续直到无下一页。

索引分页约 O(log N + L)，Offline 另有会话排序与逐会话查询成本。
`next_seq` 负责“下一页从哪里读”，`safe_seq` 负责“连续游标可推进到哪里”。
例如一页读到 101、103，下一页从 103 之后读取；连续游标结合安全水位和 102 的补拉结果推进。
Offline 使用 `(session_id, seq)` 二级游标：先读完当前会话，再继续下一个会话，晚落库消息由后续补拉收敛。

**延伸**：Keyset Pagination 与 OFFSET 分页、联合索引最左前缀、B+ 树范围扫描、L+1 判断下一页。

### 4.6 客户端消息游标与流式 TreeMap

入口：[Web 游标](../web_demo/static/index.html)、[SparkViewModel](../android-app/app/src/main/java/com/peco/sparkim/SparkViewModel.kt)。
正式消息使用每会话 cursor + seen set，界面消息按 ID 去重：

```text
seen.add(seq)
while seen 包含 cursor+1: 删除它，cursor++
seq > cursor+1 → 请求 Sync(cursor)
sync_end:
  请求 after<=当前连续 cursor 时，才允许 cursor=max(cursor,safe_seq)
  删除 seen 中 <=cursor，再消费连续部分
  has_more → 用 next_seq 翻页；仍有缺口 → 延迟重拉
```

seen 超过 4096 时清空并从连续游标补拉；cursor 按已连续接收的位置推进。
Android 使用 generation 标记连接版本，仅处理当前连接的回调；ai_delta 使用下面的独立重排结构。

Agent 增量另用 TreeMap<delta_index,delta>：

1. index 小于 expected 或 key 已存在，丢弃重复/旧帧。
2. 插入有序树，反复 remove(expected) 并追加 StringBuilder，然后 expected++。
3. 达 256 帧/512 KiB 上限或增量积压时停止预览，靠最终 reply/历史恢复。
4. UI 约 40 ms 合并刷新；Web 使用 requestAnimationFrame 与 256 帧缓冲。

树的插入/删除 O(log W)，正文追加摊销 O(正文长度)；避免每次重建整个字符串造成 O(n²)。
正式 seq 排序、delta_index 重排、连接 generation 是三个不同状态空间。

**追问**：为什么用有序树？按索引插入后可连续取出 expected；若索引范围固定且稠密，可进一步讨论环形缓冲区。

### 4.7 双向 gRPC 流与 deadline

入口：[Comet MessageStream](../comet/comet_server.cpp)、[Job PushStream](../job/service.cpp)。
结构：FIFO send_queue、request_id→pending 哈希表、writer、reader、mutex/cv 与可取消 context。

1. enqueue 生成/携带 request_id，同时记入队时刻的绝对 deadline。
2. writer 串行 Write；reader 按 request_id 查 pending，填结果并唤醒对应 completion，支持应答乱序返回。
3. Write 成功只说明 gRPC 接受数据；Job 收同 ID 的成功 reply 后才认可 callback 完成。
4. 读写失败标 broken，旧 pending 失败后重连；保留业务 ID，让 Kafka/上游安全重试。
5. context 创建、替换与取消共用锁，锁内复核 running；channel/stub 在组件生命周期内稳定。
6. Stop 取消 context、唤醒队列、join reader/writer，随后释放回调依赖的对象。

Comet 每流 queue+pending 最多 512 项，排队数据 8 MiB；8 秒 ACK 期限从入队开始。
独立 100 ms watchdog 过期排队请求并清理 pending，pending 超时还取消 stalled 流。
Job 每 Comet 队列默认 10000，reply 等待默认 2 秒，失败可 Unary fallback。
按 ID 查找平均 O(1)，pending watchdog 扫描 O(P)；网络 Write/Finish 等待不是 CPU 复杂度。

**追问**：writer 堵住还怎么超时？超时由独立 watchdog 执行，不依赖 writer 能继续循环。

### 4.8 request_id 去重：deque + unordered_set

入口：[CometServer 的 Accept/Complete/ForgetPushRequest](../comet/comet_server.cpp)，
调用与错误映射见 [ProcessPushRequest](../comet/comet_grpc_service.cpp)。
当前代码先区分正在处理的 `inflight_push_ids_` 和已完成的 `recent_push_ids_`，共同受 `conns_mu_` 保护。

| 请求状态 | 服务端行为 | 不能误解成什么 |
|---|---|---|
| 新 ID | 预占 in-flight，再执行下推 | 刚登记不代表处理完成 |
| 同 ID 正在处理，或 in-flight 已达上限 | 抛出 PushAlreadyInFlight，adapter 返回业务错误 503 | 后来的请求不能替原请求确认成功 |
| 同 ID 已完成 | 返回成功，delivered_count=0 | 本次没有新增投递，不等于用户已读 |
| 下推异常退出 | Completion guard 调用 Forget，释放 in-flight | 失败身份不能永久留在“完成”窗口 |
| 正常完成 | Complete 移除 in-flight，再登记完成窗口 | 窗口只在本进程有效 |

完成窗口用 unordered_set 平均 O(1) 查 ID，deque 保存完成登记顺序，超过 100000 就 erase(front)+pop_front。
命中不刷新位置，因此是 FIFO 窗口，不是 LRU；空 ID 不记录。Job 默认业务投递 ID 是 msg_id@comet_id。
进程重启或 ID 被淘汰后，窗口不再记得它；持久消息仍需 SQL 唯一键和客户端身份去重。
这些是正常资源条件下的执行路径，不能据此推导复合容器操作在内存分配失败时也有事务保证，见 Q49。

**面试追问**：为什么查重和登记必须在同一把锁下？为什么不能在收到请求时就记为 completed？
如果先执行副作用、再登记时进程崩溃，内存窗口还能保证什么？第 12.11 节只练习完成窗口，生产的 in-flight 和恢复协议仍需另行解释。

### 4.9 Kafka producer、consumer 与 offset

入口：[KafkaProducer](../common/kafka_producer.cpp)、[KafkaConsumer](../common/kafka_consumer.cpp)。

- producer 配置 acks=all 和 idempotence，等待 ISR 内副本确认并处理生产者重试去重。
- Send 异步入 librdkafka；SendAndWait 用 opaque→DeliveryState map、condition_variable 等 DR。
- 等待超时后，状态继续保留到实际 DR 到达；回调再释放 opaque 对应的对象。
- consumer 禁 auto commit，callback 完成后 commitSync；失败最多三次并退避。
- persist/配置了持久死信的 Agent 路径确认 DLQ 后才提交原 offset；DLQ/commit 失败停消费。
- 实时 push/broadcast 使用重试与死信日志，历史持久化链使用持久 DLQ。

进程在处理完成、commit 前崩溃会重放，所以 callback 必须幂等。
Kafka 的日志顺序以 partition 为单位；异步 fanout 的完成顺序由任务调度决定，客户端再按 msg_seq 重排。
producer 队列默认 10000 条/32 MiB；积压不应只看 lag，还要看拒绝、队列年龄与下游容量。

### 4.10 Job 幂等落库与长字段读取

入口：[MessageDao](../logic/message_dao.cpp)、[Job 持久化](../job/service.cpp)。
INSERT 遇唯一键冲突，逐字段比较 session、seq、sender、type、content、client ID、timestamp；
完全相同返回幂等成功，不同返回 sequence collision，保留首次写入的内容。
session.last_msg_seq 使用 GREATEST 避免乱序回退，reservation 标 persisted。

MEDIUMTEXT 读取先绑定 64 KiB 缓冲，reported_length 超容量或 MYSQL_DATA_TRUNCATED 时，
按真实长度分配 string，再 mysql_stmt_fetch_column。成本 O(正文长度)。
这里体现“长度探测 → 按需分配 → 完整读取”的缓冲区处理方式，可延伸到 C API 与 std::string 的内存管理。

### 4.11 ThreadPool：有界 producer/consumer

入口：[ThreadPool](../common/thread_pool.cpp)。queue<Task> 含 function 与字节权重，mutex/cv 保护状态。

```text
Submit:
  锁内检查 started/stopping、项数、bytes<=max_bytes-queued_bytes
  push，更新计数，notify_one；拒绝返回 false
Worker:
  cv.wait(stopping || !queue.empty)
  pop/decrement，解锁执行，捕获异常
Stop:
  标 stopping，选择 drain 或丢未执行项
  notify_all，join 已运行 worker，清队列
```

默认排队 1024 项/提交权重 32 MiB，硬件线程数为 0 时回退至少 1。
入/出队 O(1)，执行时间由任务决定；字节权重是调用方估算，不是进程总内存硬上限。
`queued_bytes_` 在出队时扣减，已运行任务仍可能持有消息和连接，需另计在途内存；详见 11.8-C。
Stop(false) 也需等待已运行任务；阻塞 RPC 必须有自己的 deadline。

注意源码注释与实际控制流的区别：Comet 析构注释把 `drain=false` 简写为“不等在途 RPC”，
但 ThreadPool::Stop 仍会 join 所有 worker。准确说法是“丢弃尚未出队任务，等待已运行任务返回”。
此外，Submit 返回 true 只证明内存队列接纳，不是 accepted_ack；异常被日志捕获也不等于业务已经补偿。
第 11.15、12.14 节将这些边界展开成面试题与教学实验。

### 4.12 MySQL/Redis 池与 RAII

入口：[MySQL pool](../common/mysql_pool.cpp)、[Redis pool](../common/redis_pool.cpp)。
deque<ConnEntry> 记录空闲连接/last_used，mutex/cv 管总数、min/max 和等待。

1. 锁内清过期超额空闲、借出连接，或预留创建名额。
2. 解锁后 connect/健康检查，重新加锁核对停止状态，使慢网络操作与连接队列互斥分离。
3. 失败减计数、关闭坏连接并通知等待者；每 250 ms 尝试恢复 min。
4. 即使关闭自动增长，也允许补回 min，避免所有连接失效后永远没人 Release 来唤醒。
5. 归还健康连接到 deque 尾部；RAII guard 自动归还，Redis guard 禁复制、允许 move。

默认 Acquire 等待 3 秒；绝对期限防多次唤醒重复延长等待。
等待池空位、建立连接和执行 SQL 分别使用对应的超时。Redis 热连接仅空闲超过健康间隔才 PING。
平均借还 O(1)，回收可扫描过期项；池越大不一定越快，需看 DB 能力和锁竞争。

**读代码时校正注释**：MySQL 头文件将四个 `*Unlocked` helper 一概描述为调用方持锁，
实际 `Acquire` 在 `CreateConnectionUnlocked` 和验活前释放池锁；以调用点为准。
`Acquire(0)` 仍可能先建连或验活，不能解释成整个调用一定非阻塞；等待期限也不能强行中断这些操作。
`Stop()` 当前只设置停止标志并唤醒等待者，空闲连接由析构回收，借出连接归还时关闭。
guard 保存池的裸指针，因此池必须活过所有 guard，不能把“调用了 Stop”当作已等待借用者全部退出。见 Q64～Q65。

### 4.13 Redis 路由防迟到删除与短缓存

入口：[RedisStore](../logic/redis_store.cpp)。

```text
上线 Lua：n=INCR generation；SET lease=n:nonce；SADD route comet
下线 Lua：仅当 GET lease == 自己的 n:nonce 才 DEL lease / SREM route
```

generation 处理旧下线晚到；安全随机 nonce 处理 Redis 计数器丢失后同数字复用。
Lua 将“读取版本、比较、删除”合并为原子操作。可延伸到 CAS、ABA 问题，以及带 TTL/心跳的租约设计。

本地 uid→{comets,expire} 缓存用 shared_mutex；命中共享锁复制，未命中读 Redis 后独占写入。
默认 TTL 1 秒，注册/删除主动失效；多 Logic 无广播失效，因此 TTL 是陈旧路由边界。
该缓存按 TTL 判断有效性，SQL 取号独立于路由缓存。可延伸比较 TTL 失效与 LRU 容量淘汰。

### 4.14 令牌桶与背压

入口：[SceneRateLimiter](../common/rate_limiter.cpp)。map<scene:key,Bucket>，一把 mutex 管所有桶。

```text
tokens = min(burst, tokens + elapsed_steady_seconds * rate)
tokens < 1 → 429
否则 tokens -= 1
```

首次填 burst，single 默认 1000/s、burst=200；group 300/60；broadcast 20/5。
时间使用 steady_clock，避免系统时钟跳变；桶是进程级，不是全局分布式限流。
平均定位 O(1)。令牌桶控制入口速率，有界队列控制积压，连接输出上限控制慢消费者占用。
观察整条链：入口→SQL/Kafka→Job queue→gRPC flow control→Comet output→慢客户端。

### 4.15 HTTP/WS 增量解析与握手算法

入口：[HttpContext](../muduo/net/http/HttpContext.cc)、[CometServer](../comet/comet_server.cpp)。
HTTP request-line→headers→body→complete 状态机仅消费已完整确认的数据。
半包时保存解析状态并退出，下一次 onMessage 接着处理；每次循环消费字节、推进状态或等待新数据。
Content-Length 按十进制累积，检查 `length > (maxBody-digit)/10` 防溢出；重复、符号、异常 header 拒绝。
不支持 Transfer-Encoding/chunked，提前拒绝。限制 line 8 KiB、headers 32 KiB、body 8 MiB、未完成 30 秒。

WS 升级计算 `Accept = Base64(SHA1(client_key + GUID))`，随后按帧协议解析。
握手与数据帧共用 TCP 字节流，解析器通过状态和长度字段识别边界。

WS 帧：先读 2 字节，按 7/16/64-bit 取网络序长度；数据未齐不 retrieve；校验 FIN、mask、RSV/opcode，
再 `payload[i] ^= mask[i%4]`。文本负载最多 64 KiB；当前实现拒绝 WebSocket 分片消息和二进制数据帧，但支持同一帧在 TCP 层分多次到达。
控制帧最多 125 字节，Ping 回同负载 Pong，Close 回应并 1 秒后强关；Pong 不作业务消息。
mask/序列化 O(P)。当握手头与第一帧一起到达时，按 header 结束位置拆分两部分分别解析。

**学习连接**：Q62～Q63 解释“不支持 WS 分片”与“必须处理 TCP 分段”的区别，以及长度检查的顺序；
第 12.16 节使用简化协议练习同一思路，不替代真实 WebSocket 协议测试。

### 4.16 连接表、fanout 与跨线程所有权

入口：[TcpConnection](../muduo/net/TcpConnection.cc)、[CometServer](../comet/comet_server.cpp)。
user_conns 是 unordered_map<uid,set<shared_ptr<connection>>>，room_users 保存本机订阅用户。
锁内复制目标指针、锁外排队到各 owner loop；fanout O(目标连接数)，不是 O(消息数)。

跨线程 send/startRead/stopRead 闭包持有 shared_ptr 与消息字节；状态读 atomic，转换归 owner loop。
闭包拥有消息字节和连接生命周期；延迟 close 使用 weak_ptr，执行时通过 lock() 获取临时所有权。
owner loop 检查连接仍在、frame+output 不超过 4 MiB 后才计 delivered；慢连接关闭，不无限积压。
总缓冲占用可按“连接数 × 平均积压字节”估算，单连接高水位限制长尾积压。

资源释放顺序：传输对象先停止/join IO worker，连接表和 callback 依赖之后释放。
RPC 池、流、timer、恢复任务分别停止；取消并不意味着回调已退出，必须 join/等待实际执行收敛。

### 4.17 附件传输：数据与消息分离

入口：[AttachmentService](../logic/services/attachment_service.cpp)。
图片通过 HTTP 上传，消息只携带附件 ID；Bridge 在调用模型时读取文件并构造图片输入。
这样 Kafka/WS 传输的是轻量引用，图片大小由上传层独立管理。

文件写入先生成临时文件，再通过 rename 切换为正式文件；SQL 保存附件路径、大小和所属会话。
同一文件系统内 rename 提供原子的名称切换，持久化顺序还涉及文件写入与元数据更新。

**延伸**：控制信息与大数据分离、引用传递、文件系统原子操作；对象存储可作为进一步的架构选择。

### 4.18 SSE、增量合并与最终 envelope

入口：[SSE parser](../hermes_bridge/sse_parser.cpp)、[Bridge](../hermes_bridge/main.cpp)。

1. Feed 缓冲半行，按换行取 event/data；空行结束一条 event，data 支持跨行拼接。
2. 第一段立即发 delta，随后累计约 48 字节或 50 ms 才 flush，降低 Kafka/UI 每 token 开销。
3. 收到合法成功终态与结束标记后生成最终回复；错误或中断走对应状态分支。
4. 重复 final 要求文本/metadata 一致；最终 envelope.sequence 必须等于 delta_count。
5. delta 异步发送，final SendAndWait；进程内 reply cache 可重发已完成输出，但非永久持久幂等。

parser 预算 16 MiB，扫描 O(输入字节)；不把 JSON chunk 边界当 SSE event 边界。
预览和正式消息分开管理：增量改善实时体验，完整回复负责历史恢复。

### 4.19 Agent 路由、FIFO 与版本化交接

入口：[AgentRouter](../cannbot/scripts/agent_router.py)、[Scheduler](../cannbot/scripts/agent_scheduler.py)、
[Room workspace](../cannbot/scripts/room_agent.py)、[Room Repository](../logic/repositories/room_agent_repository.cpp)。

route_key 由 tenant/channel/conversation/thread 的 canonical JSON 哈希生成，避免串用户/渠道；
request_id 对应固定 input_hash，completed 回放、状态未知拒绝盲执行；SQLite turn/event 账本可恢复。
event 使用 request/sequence 与唯一 event_id，冲突拒绝；只保留有界回放窗口。

FIFO 调度：每 session ticket 列表；只有队首、session 未运行、全局/per-agent 配额满足才能 acquire。
队列计时用 monotonic；finally 只释放自己真正 acquired 的执行权，超时排队者不得解锁正在执行者。
该调度器在单 Gateway 内维护准入状态；多实例调度可以延伸到租约与集中式队列。

协作交接：房间绑定 task/session/workspace；成员可读，房主/显式授权者可开发。
checkpoint 使用 `UPDATE ... WHERE revision=?` 做 CAS，更新成功后递增版本，版本冲突返回 409。
源码按相对路径/真实字节 SHA-256 做内容寻址副本；测试前后版本、runner hash、exit_code 和 timeout
共同关联“哪一份代码执行了哪一次测试”；执行期间内容变化会使版本检查失败。

同 turn 的 write/edit/test 另有互斥；固定 runner 经 bubblewrap/prlimit，不接受任意 shell 命令。
测试状态由固定 runner 的退出码、超时和源码版本共同计算，形成可重放的执行记录。
**延伸**：乐观锁、内容寻址、检查点恢复；把任务进度、源码版本和测试结果分开存储。

### 4.20 推理服务准入、KV 与显存观测

入口：[Inference serving](inference-serving.md)、[WorkerRegistry](../inference/gateway/worker_registry.cpp)。
Gateway 管排队/选择/取消，Worker 管 runtime 和自身并发；模型批处理由 runtime 执行。
Worker 约 1 秒心跳、5 秒 TTL；选择前过滤模型/健康，再按运行/等待/显存状态决策。

默认准入 running=max(心跳 running,Gateway active RPC lease)，避免漏算直连/共享流量。
RPC 完成释放 lease，旧心跳仍可能满载；单 Gateway 独占全部 Worker 流量时可显式 managed-only，
用本地 lease 准入，健康/显存仍来自心跳。多 Gateway/共享 Worker 不满足这个前提。
取消、断连、deadline 要向下关闭模型请求并释放计数，否则会留下无主计算。

Prefill 处理输入，Decode 逐 token；TTFT 含排队/预填充/首 token，TPOT 是后续单 token 成本。
continuous batching 把新请求补进空 slot，服务队列与计算 batch 是两层。
Flash Attention 通过分块计算与算子融合减少注意力中间矩阵的显存读写。

```text
KV bytes ≈ 2 × layers × kv_heads × head_dim × cached_tokens × bytes_per_element
```

KV 量化与模型权重量化不同；prefix cache 重用预填充，是否命中要看 cached_prompt_tokens。
多个 Worker 共用 runtime 时共享 slot；多个 runtime 各自持有权重/KV，容量规划需要同时计算显存和计算资源。
GPU 使用 NVML 按 PCI bus ID 采样，避免 CUDA visible ordinal 重排造成读错卡；回退 Driver 时保存/恢复
线程 current context、retain/release primary context。容量分析同时考虑权重、KV、临时 workspace 和 allocator 开销。

### 4.21 Hermes Native：执行账本、原生终态和控制抢占

入口：[Native Adapter](../cannbot/scripts/hermes_native_adapter.py)、[Kafka 控制循环](../common/kafka_consumer.cpp)、
[Runtime 集成说明](hermes-native-runtime-integration.md)。适配本机 Hermes 0.21.1 Runs API。

1. Logic 已验证的 tenant/channel/conversation/thread/user/agent 路由形成 Native Session 身份，使用独立 API Session，不借用 CLI Session。
2. AgentStore 在唯一一次 `POST /v1/runs` 前持久化准入意图，稳定派生 Idempotency-Key；同 request_id 改输入拒绝。
3. SSE 只投影过滤后的文本、工具与审批进度；仅原生 completed 且输出有效才落成功结果，再进入 `ai_reply`。
4. 中断或重投查询原 Run，已落盘结果直接复用；提交丢响应且无 run_id 时记录 unknown，等待核对原执行结果。
5. `/stop` 与审批通过独立控制池；可选 Kafka 控制窗口越过长任务执行控制，但 offset 只提交连续完成前缀。
6. 审批令牌绑定用户会话、Run、请求、代次及时效，只支持 once/deny；默认关闭，超时或缺少交互支持时拒绝。
7. `/new` 先确认原生 Session 创建，再切本地指针；失败保留旧上下文。终态输出与已确认发布状态不可被迟到恢复覆盖。

这里采用“执行身份 + 持久账本 + 原结果恢复”处理跨服务执行；控制窗口使用有界队列和连续完成前缀。
Agent Loop、工具与 Memory 由 Hermes 执行，Spark 负责身份、调度和结果投递。

**延伸**：Write-ahead intent、幂等键保留期、长任务取消、优先级队列和队头阻塞。

**追问**：已经有 Idempotency-Key，为什么不重发？服务端有保留期；执行过工具但幂等记录过期后重发可能再次执行。

### 4.22 Pi 的会话记忆与上下文压缩

入口：[PiRpcClient](../cannbot/scripts/pi_gateway.py)、[AgentStore](../cannbot/scripts/agent_router.py)。

| 层次 | 数据结构/存储 | 用途 |
|---|---|---|
| 原生会话 | Pi JSONL，按会话与 generation 命名 | 保存用户、助手、工具消息和压缩条目 |
| 当前模型上下文 | 系统提示 + 摘要 + 保留消息 | 每次模型推理的输入 |
| 执行账本 | SQLite route/turn/event | 路由、幂等、结果回放 |
| 知识资料 | Skills、项目文件、CANN RAG | 按任务读取长期维护的知识 |
| IM 历史 | MySQL 消息表 | 展示、查询和断线恢复 |

每轮先绑定对应 JSONL；原生路径变化时才调用 `switch_session`，相同路径复用当前会话，再提交本轮消息。
Pi 由原生会话历史构造上下文，恢复与压缩细节见 4.24.12。
`/new` 使用新的 generation 切换空白会话，旧文件和聊天记录保留。

Pi 0.84.4 的自动压缩默认开启：上下文接近模型窗口时，由模型总结较早消息，按预算保留近期内容，
将摘要和 `firstKeptEntryId` 追加到 Session。默认预留 16,384 tokens，近期内容预算 20,000 tokens。
后续输入由“系统提示 + 压缩摘要 + 保留消息”组成，原始记录继续留在文件中。
这是主会话的默认能力；Kernel 内存子会话显式关闭自动压缩与自动重试。

Router 另外保存最近 24 条有界消息记录，用于状态管理；Pi 的上下文由原生 Session 构建。
当前个人偏好跨会话自动提炼与召回可作为扩展设计，已有实现使用会话持久化、摘要和外部知识。

**追问**：Session、摘要、RAG、KV cache 有什么区别？Session 保存执行历史；摘要缩短文本输入；
RAG 从外部资料检索相关片段；KV cache 复用模型计算的中间张量。

### 4.23 RAG 与工具调用：从检索到引用

入口：[cann-advisor](../cannbot/pi/extensions/cann-advisor.ts)。

```text
问题 → search 定位候选片段 → get / neighbors 读取上下文
     → 片段进入模型上下文 → 生成答案及引用标识 → Spark 渲染引用链接
```

扩展向 Pi 注册只读知识工具。search 提供定位信息，get 按 offset/length 分页读取正文，
`expected_hash` 校验多次读取的是同一内容版本；引用使用本轮工具返回的 `CANN_REF` 标识。
这种设计将检索、证据读取和回答生成拆成可观察的步骤。

**可延伸的算法**：倒排索引与 BM25 做关键词相关性计算；向量检索做语义近邻；混合检索融合多个召回列表，
Reranker 对候选进一步排序。这些是 RAG 的通用扩展知识，当前 Pi 扩展本身调用独立 `cann-rag` 检索服务。

**追问**：RAG 与微调如何选择？RAG 在推理时更新外部知识，便于附带来源；微调通过训练改变模型参数，
常用于任务模式、输出格式和领域行为。两者可以组合。

### 4.24 Kernel Agent：复用运行时与子 Agent 编排

本节包含既有 Kernel/CANN 开发模式和固定版本 Pi 内部机制；其中 write/edit/bash 等能力
不能推断为正式公共 Pi/Hermes 已开放。公共只读模式和当前验收边界见第 10、6.4 节。

入口：[Kernel 子 Agent 扩展](../cannbot/pi/extensions/kernel-subagents.ts)、
[产品与权限边界](kernel-agent.md)、[Codex 源码对照](codex-subagent-reference.md)。

Kernel Agent 是本项目的产品名称，Pi coding agent 是底层运行时。项目通过 Pi SDK 的
`createAgentSession` 装配会话，复用 provider 请求适配、统一模型事件流、Agent Loop 和基础工具；
项目实现 IM 会话绑定、Gateway SSE、权限、领域工具与子任务控制。下文 4.24.1～4.24.17 展开各层实现。
子任务生命周期参考开源 Codex CLI 的 spawn / send / wait / close 机制，并按当前 Pi 环境适配。
保留 `pi` / `pi_rpc`、bot ID、旧状态目录与 `pi_gateway.py`，产品改名不迁移聊天历史。

```text
主 Agent 接收任务
  → 划分可并行研究 / 有依赖的设计与实现
  → spawn_agent：独立 Session、角色、任务上下文、写入范围
  → wait_agent / list_agents：查看状态并收集结果
  → send_input：补充真实测试证据或安排修正
  → 主 Agent 核对代码与测试，close_agent，汇总最终答案
```

1. 子任务按主 turn 归属管理，用独立内存 Session 隔离模型上下文；父模型和思考等级沿用，
   必要事实由任务提示显式传递。隔离上下文不等于隔离操作系统或复制工作区。
2. 工具按“父会话已有能力 ∩ 角色允许能力”收敛。researcher / architect / reviewer / tester 只读；
   developer 只能写 `allowed_writes` 中的文件，并拒绝并发开发者持有重叠范围。
3. 不给子 Agent shell 和继续派生能力。tester 负责检查测试设计与已有证据，主 Agent 执行命令；
   房间子 Agent 仅只读工作区/CANN，房间写入和 runner 继续由可信授权的主 Agent 使用。
4. 子任务数量、交互轮数、执行时间、wait 时长和返回文本均有上限。等待超时只表示本次没有等到，
   不能直接认定子任务失败，更不能自动重跑产生副作用的修改。
5. 父 turn 结束、取消、切换会话或关闭时回收子任务，避免 worker 重用时跨会话泄漏。
   子会话不跨 turn 持久化或自动恢复；主 turn 的 SQLite 账本与子会话生命周期分别管理。

| 并发层次 | 输入对象 | 解决的问题 |
|---|---|---|
| Gateway worker 池 | 不同会话的请求 | 同会话串行、跨会话有界并发 |
| Kernel 子 Agent | 同一个任务的子任务 | 独立上下文、分工与结果收集 |
| 房间协作 | 多人的同一算子任务 | 权限、交接与版本化证据 |

**可讲的算法与工程点**：任务注册表与所有权；状态机和迟到结果处理；有界资源配额；
取消与释放顺序；工具能力集合求交；并行写入冲突检查。子 Agent 报告“通过”后，
主 Agent 仍要检查实际命令、退出码和源码版本，不能把模型自报当成测试回执。
子任务的 CANN 引用也需由主 Agent 重新读取来源，才能进入父 turn 的引用校验账本。

**追问**：为什么不直接把 Codex 的 Rust AgentControl 复制过来？它依赖 Codex 的线程、配置和协议栈；
本项目借鉴并注明生命周期语义，执行部分复用已接入的 Pi SDK，避免再引入一套模型与会话运行时。
这不等于已经实现完整 Codex multi-agent，也不代表 NPU 算子自动优化已完成实机闭环。

**本轮验收（2026-10-30）**：子任务生命周期/权限 53 项、真实 Pi SDK + 受控模型集成 32 项、
名称浏览器检查 6 项通过；独立分支 CTest 为 46 通过 / 3 跳过。
[验收记录](benchmarks/2026-10-30-kernel-agent/validation.json)同时登记已有命令卡片浏览器失败，
不将受控模型验证表述为真实模型或 NPU 性能验收。

#### 4.24.1 面试先讲设计分层，再讲框架选择

下面按本机 **Pi 0.84.4** 源码展开，核对日期为 2026-10-31。阅读时区分两个层次：
Pi SDK 提供通用执行引擎；Kernel Agent 负责把它接入项目，并增加领域工具、权限和子任务控制。
面试可以用“我的系统采用这种分层，我在这一层实现了这些策略”讲设计；源码归属与实际改动范围保持明确。

| 组件 | 维护什么 | 解决什么问题 |
|---|---|---|
| `ModelRuntime` / provider adapter | 模型目录、认证、provider 配置和请求适配 | 不让 Agent Loop 感知不同模型厂商的 HTTP 字段 |
| `Agent` | 当前消息、运行状态、待执行工具、steering/follow-up 队列 | 驱动“模型 → 工具 → 模型”的循环 |
| `AgentSession` | 工具/扩展注册、输入处理、压缩、重试与事件协调 | 把低层执行循环包装成可交互的工作会话 |
| `SessionManager` | 会话条目、分支和持久化文件 | 保存历史，并重建某个分支上的模型上下文 |
| `ResourceLoader` | 系统提示、上下文文件、Skills、扩展和模板 | 将工作区资源按配置装配到会话 |
| Kernel `AgentControl` | 子任务身份、状态、额度、写入范围和清理 | 在同一个主任务内组织可控的独立子会话 |
| 项目 Gateway / Router / Bridge | IM 身份绑定、准入、执行账本和协议投影 | 把 Agent 执行接入可靠消息链路 |

`createAgentSession()` 是这些对象的**装配工厂**。它返回一个会话对象；真正向模型发送任务发生在后续的
`session.prompt(...)`。创建一个 Session 不等于已经发起推理，也不等于复制模型权重或启动独立模型服务。

本项目有两种进入相同 Pi 引擎的方式：

```text
主会话：Python Gateway → Pi CLI 子进程的 JSONL RPC → AgentSession → Agent → ModelRuntime
子会话：kernel-subagents.ts → createAgentSession → AgentSession → Agent → ModelRuntime
```

子会话直接在父 Pi 进程内创建 SDK 对象，拥有独立消息历史，但不是独立操作系统进程。
跨会话 worker 池才会启动多个 Pi RPC 进程；两种并发不能混为一谈。

源码：[SDK 装配][pi-sdk]、[AgentSession][pi-session]、[本项目子会话创建](../cannbot/pi/extensions/kernel-subagents.ts)。

#### 4.24.2 `createAgentSession` 内部究竟做了什么

按源码顺序，可以分成以下步骤：

1. **确定路径**：cwd 优先采用调用者配置，其次是传入的 SessionManager 的 cwd，最后才是进程 cwd；
   agentDir 决定认证、模型配置和资源目录的位置。cwd 是工具工作目录，不自动构成文件系统沙箱。
2. **取得基础依赖**：优先使用注入的 `ModelRuntime`、`SettingsManager`、`SessionManager`，缺省时再创建。
   因而测试可以换成受控模型配置，子 Agent 可以传入纯内存 Session，而不需要另写执行循环。
3. **加载资源**：没有传 ResourceLoader 时创建默认 loader 并 `reload()`；显式传入 loader 时，
   调用者负责先完成加载。Kernel 子会话正是在这里禁止自动发现工作区扩展、Skills、模板与上下文文件。
4. **读取可恢复上下文**：`buildSessionContext()` 取得当前分支的有效消息、模型与思考等级。
5. **决定模型**：显式 `options.model` 优先；否则尝试恢复历史模型并检查可用认证，再按配置选择默认模型。
   创建结果允许携带模型回退提示；没有可用模型时不能据此声称会话能够执行，`prompt` 还会检查。
6. **决定思考等级**：显式参数优先；恢复旧会话时采用历史等级，旧会话缺少记录时采用全局默认；
   新会话再查按模型保存的偏好和全局默认，最后按模型能力裁剪。`thinkingLevel` 是请求配置，
   不是让本地代码执行额外推理算法。
7. **确定工具约束**：一般默认激活 `read/bash/edit/write`；显式 `tools` 和排除项用于约束工具集合。
   函数先构造工具为空的 Agent，随后由 AgentSession 建注册表、包装扩展并设置真正的活跃工具。
8. **构造 Agent**：注入上下文转换、模型流函数、provider 请求/响应钩子、会话 ID、队列与传输配置。
9. **恢复或记录初始状态**：已有历史时恢复 Agent 消息；新会话记录初始模型与思考等级。
10. **构造 AgentSession 并返回**：会话协调层订阅 Agent 事件，建立工具与扩展运行时；返回
    `{session, extensionsResult, modelFallbackMessage}`，调用者可以检查扩展错误和模型回退。

以下是装配思路的伪代码，省略错误处理和细节，**不是可以原样替换 SDK 的实现**：

```typescript
async function createSession(options) {
  const runtime = options.modelRuntime ?? await createModelRuntime();
  const settings = options.settingsManager ?? loadSettings(options.cwd);
  const history = options.sessionManager ?? createPersistentSession(options.cwd);
  const resources = options.resourceLoader ?? await loadDefaultResources();
  const restored = history.buildSessionContext();
  const model = chooseModel(options.model, restored.model, settings, runtime);
  const thinking = clampToModelCapabilities(chooseThinking(options, restored, settings), model);

  const agent = new Agent({
    initialState: {model, thinkingLevel: thinking, tools: [], systemPrompt: ""},
    transformContext: messages => extensionContextHook(messages),
    convertToLlm: messages => convertApplicationMessages(messages),
    streamFn: (model, context, request) => runtime.streamSimple(model, context, request)
  });
  restoreOrInitialize(agent, history, restored);
  return new AgentSession({agent, history, settings, runtime, resources});
}
```

这里的设计价值是**依赖注入与策略解耦**：同一个 Agent Loop 可以服务交互式 CLI、RPC 主会话和内存子会话。
系统提示、工具集合、模型、存储方式都在外面组合，不需要为了每一种业务复制一份循环。

**追问**：为什么不是一个大类同时管理模型、文件、历史和 UI？因为它们有不同的变化原因和生命周期。
模型接口变化应落在 provider；历史恢复落在 SessionManager；IM 权限落在 Logic/Router；
工具是否有资格执行则由本地策略决定，不能靠模型服务商替项目鉴权。

#### 4.24.3 从 `session.prompt` 到一次模型请求

`AgentSession.prompt` 不是直接把字符串交给 HTTP 客户端，它先做输入与会话协调：

1. 处理已注册的扩展命令；某些命令直接产生本地结果，不一定调用模型。
2. 触发 `input` 钩子，扩展可以接管或改写输入；之后才展开 Skill 命令和提示模板。
3. 如果当前正在生成，普通 prompt 不能同时进入同一个循环；要显式选择 steering 或 follow-up 队列。
4. 检查模型与认证，处理必要的压缩，将文本和图片组成 `user` 消息。
5. 触发 `before_agent_start`，加入本轮扩展上下文或修改系统提示。
6. 进入 Agent；在模型调用前再做 `transformContext`，随后 `convertToLlm`，最后调用 `streamFn`。

这三种消息形式不要混淆：

| 形式 | 示例 | 用途 |
|---|---|---|
| 应用消息 | user、assistant、toolResult，以及 custom_message、分支/压缩摘要转换后的消息 | 保存运行历史和应用语义 |
| Pi 统一 LLM 消息 | text/thinking/toolCall 等内容块，关联的 toolResult | 提供与具体 provider 无关的推理输入 |
| provider 请求 | Chat Completions 的 messages/tools，或 Responses 的 input 等 | 适配外部模型协议 |

`transformContext` 可以在一次调用前筛选或补充上下文，`convertToLlm` 再把应用消息转成模型可识别的消息。
这不等于清空原始历史；“持久化了哪些条目”和“本次发送哪些条目”是两个不同的问题。
例如 `model_change`、`thinking_level_change` 和普通 `custom` 条目可以记录配置或扩展状态，
它们并不会全部变成发送给模型的聊天消息。

主 Gateway 不会把 IM 中所有历史回复每次重新塞给模型。它从请求中提取本轮用户输入，绑定对应的 Pi Session，
由 Pi 原生历史提供连续上下文。这可以减少 IM 历史与模型原生工具历史重复注入的问题。

源码：[AgentSession.prompt][pi-session]、[消息转换][pi-messages]、[Gateway.latest_user_text / chat](../cannbot/scripts/pi_gateway.py)。

#### 4.24.4 Agent Loop 是什么，循环条件是什么

普通模型请求通常只有“输入 → 一次推理 → 输出”。Agent Loop 会检查模型输出中的结构化动作，
执行工具，把真实执行结果放回上下文，再继续调用模型。

```text
用户：检查 add.cpp 的错误并修复
  → 模型输出 toolCall(read, {path: "add.cpp"})
  → 本地 read 执行，生成带 toolCallId 的 toolResult
  → 模型看到源码，输出 toolCall(edit, {...})
  → 本地 edit 校验并修改，返回 diff/错误
  → 模型输出验证计划或最终答案
```

Pi 的循环可以简化成两层；真实代码还包含事件与异常分支：

```text
追加本轮输入，发出 agent_start
初始化循环标记，保证至少进入一次模型调用
外层：处理本次运行及待续接的 follow-up
  内层：只要还有工具结果要反馈，或待处理的 steering
    取 steering 消息并加入上下文
    context = transformContext(history)
    llmMessages = convertToLlm(context)
    assistant = 消费一次完整模型流，同时发布增量事件
    将 assistant 加入历史
    如果 error / aborted：结束低层运行
    calls = assistant.content 中的 toolCall
    如果有 calls 且 stopReason 为 length：全部生成失败 toolResult，不执行任何工具
    否则如果有 calls：校验、执行工具，将 toolResult 加入历史
    发布 turn_end
    继续处理工具结果或新的 steering
  如果有 follow-up：加入后再次进入内层
  否则退出
发出 agent_end
会话层处理完重试、压缩等后，再发出 agent_settled
```

模型负责**提出下一步动作**，运行时代码负责**决定该动作是否允许执行及怎样执行**。
工具不是模型在服务器上直接运行的函数；最终调用的是本地注册的 `execute` 实现。

“turn”在不同层含义不同：Agent core 的一次 turn 通常包含一次模型响应及对应工具结果，
而 Gateway 的一个用户请求可能包含多次这样的 turn。一次模型流结束，只说明本次模型调用结束，
不一定说明整个用户任务结束。

若模型没有继续请求工具、也没有排队输入，低层循环可以退出；取消或出错会终止低层运行。
输出截断需要分情况：`length` 携带工具调用时，给这一批全部回填错误，再让模型重发完整请求；
不携带工具调用且没有待处理输入时才退出，不能把它当成正常回答。
**结束不等于成功**：主 Gateway 额外要求最终助手消息为 `stop`、文本非空且会话已 settled。

通用循环也不能依赖“模型最终总会停下来”。本项目主请求有 deadline；子会话另有工具调用数、运行时长、
启动数和交互轮数限制。子 Agent 的“四轮”指初始 prompt 加后续输入的额度，不能误讲成每个任务最多四次模型调用。

源码：[Agent][pi-agent]、[agent-loop][pi-loop]、[项目最终成功判定](../cannbot/scripts/pi_gateway.py)。

#### 4.24.5 工具调用如何从模型输出变成真实副作用

一个工具至少包含名称、说明、参数 schema 和本地执行函数。模型得到的是名称/说明/schema，
不是执行函数的代码或函数指针。例如项目的 `cann_knowledge_search` 用 TypeBox 定义 query、版本与 top_k 参数，
执行函数再调用本地检索命令。

统一消息中的工具请求与结果大致如下，省略时间戳等字段：

```json
{"role":"assistant","content":[{"type":"toolCall","id":"call_1","name":"read","arguments":{"path":"add.cpp"}}]}
{"role":"toolResult","toolCallId":"call_1","toolName":"read","content":[{"type":"text","text":"源码内容"}],"isError":false}
```

`toolCallId` 将结果与具体请求对应起来。它不同于 IM 的 `request_id`、数据库 `msg_seq` 和子 Agent ID，
不能因为名字里都有 ID 就复用同一套幂等语义。

本地执行流程为：

1. 按工具名查找当前工具注册表；不存在或未开放的能力不能凭模型输出临时创建。
2. 做参数预处理和 JSON Schema 校验；这是结构校验，还不是文件权限或业务授权。
3. 执行调用前策略钩子：角色工具集、房间权限、路径范围、写文件名单与调用额度等。
4. 调用 `execute(toolCallId, args, signal, onUpdate, ...)`，可通过更新回调报告中间进度。
5. 捕获执行错误或阻断结果，形成带错误状态的 toolResult；成功结果也会经过结果钩子。
6. 将结果作为下一次模型调用的上下文，让模型基于真实结果继续决策。

错误结果可以让模型改参数或改方案，但不能无限重试。取消信号需要工具实现配合；
对已经写入的文件，即使后续模型调用失败，也不会自动撤销。

**两条边界**：JSON Schema 防止参数形状错误；授权策略防止合法形状的恶意操作。
例如 `{path: "../../.env"}` 完全可能是合法 JSON 和合法字符串，仍必须在路径策略中拒绝。

源码：[工具调度与校验][pi-loop]、[CANN 工具注册](../cannbot/pi/extensions/cann-advisor.ts)、
[子会话调用守卫](../cannbot/pi/extensions/kernel-subagents.ts)。

#### 4.24.6 多个工具怎样并行，为什么单线程也会竞争

Pi 0.84.4 的 Agent 默认工具执行模式是 `parallel`。当前实现先逐一准备本批工具调用，
包括参数校验和调用前钩子；准备阶段完成后，再并发执行可运行的工具，并按原调用顺序发布结果消息。
因此“并发执行完成顺序”和“回填上下文顺序”可以不同。
如果全局选择 `sequential`，或本批任一工具声明 `executionMode = "sequential"`，整批改为串行执行。

```text
本批：read(A), read(B)
prepare A → prepare B → 并发执行 A/B → 按 A/B 顺序回填 toolResult
```

Node.js 的 JavaScript 在事件循环中执行，`await` 会让出控制权；两个异步任务可以交错更新共享状态。
所以单线程并不自动消除“先检查再更新”的竞争。对于子 Agent 数量检查，应在第一个 await 之前
同步预留名额；否则两个 spawn 都可能看到剩余一个名额，然后都开始创建会话。

Pi 的文件修改工具另有 `withFileMutationQueue`：先求目标路径的规范键，为该路径登记 Promise 链，
等前一个修改结束后再进入“读原文 → 校验 → 写入”的临界区；finally 释放后继。
同一文件串行、不同文件仍可并行。该队列是进程内协调，不提供跨进程文件锁或事务回滚。

Kernel 子 Agent 又增加**任务级写范围占用**：两个 developer 不能同时取得相同或冲突路径。
两者目的不同：文件队列避免一次修改交错；任务范围限制避免两个开发者基于不同计划轮流覆盖同一文件。
主 Agent 仍需要遵守同一分工，不能在子 Agent 持有范围时另行修改这些文件。

**设计陷阱**：不要在调用前钩子中持有必须等 `tool_result` 才释放的长期锁。
如果并行执行器先准备全部工具，第二个准备步骤等待第一个工具释放，而第一个工具还没有进入执行阶段，
就可能形成等待环。当前房间 `room-workspace.ts` 的“调用前加锁、结果后释放”与这种调度顺序存在组合风险；
这是本次源码分析发现的待验证点，不能用已有单独 guard 测试推断任意同批写入/测试都不会死锁。

源码：[并行工具调度][pi-loop]、[文件修改队列][pi-file-queue]、
[房间守卫](../cannbot/pi/extensions/room-workspace.ts)。

#### 4.24.7 模型适配层如何消除厂商协议差异

Agent Loop 调用统一的流接口，输入包含模型、系统提示、消息、工具 schema、思考配置与取消信号。
`ModelRuntime` 根据模型/provider 配置选择实现，取得认证信息，再交给具体 provider adapter。

适配层主要做两件事：

- **请求方向**：将统一消息、工具描述和推理配置转换成 provider 支持的字段。
  Chat Completions 使用 `messages` 与函数工具描述；Responses 使用其 input/item 协议。
  工具结果在不同协议里的 role/item 表达不同，由适配层处理。
- **响应方向**：把 provider 原生事件统一为 text/thinking/toolcall 的开始、增量、结束事件，
  并最终构造含内容块、usage 和 stopReason 的 AssistantMessage。

统一事件的用途可以这样理解：

| 事件 | 运行时处理 |
|---|---|
| `text_delta` | 追加可展示文本，向上报告预览 |
| `thinking_delta` | 更新思考状态或该内容块；不应自动当作最终答案 |
| `toolcall_delta` | 拼接工具参数字符串，维护参数预览 |
| `done` | 提交本次调用的完整助手消息和结束原因 |
| `error` | 提交错误/取消状态，不能用之前的部分文本冒充成功 |

例如 Completions 的 `choices[].delta.content` 和 Responses 的 `response.output_text.delta`，
都可以转换成下面的统一形状；这里省略无关字段：

```typescript
{
  type: "text_delta",
  contentIndex: 0,
  delta: "检查输入形状",
  partial: {role: "assistant", content: [{type: "text", text: "先检查输入形状"}], /* ... */}
}
```

`delta` 是本次增量，`partial` 是累计消息；`contentIndex` 区分同一响应里的文本、思考和工具调用块。
适配器按块追加内容、累计 usage、转换结束原因，再把事件推入 `AssistantMessageEventStream`。
它是支持 `for await` 的内存异步队列：有消费者等待时唤醒消费者，否则暂存事件；消费者可用 `result()`
取得完整 AssistantMessage。`done` 与 `error` 都会完成这个结果 Promise，故 `await result()` 没抛异常
仍不能证明模型成功，必须检查返回消息的 `stopReason`。此处的“Stream”是进程内抽象，不是另一个 HTTP 连接。

这里讨论的 OpenAI Completions / Responses adapters 使用客户端 SDK 消费底层协议，再做统一事件转换。
因此不能说项目自己实现了所有厂商的 HTTP/SSE 解码器；项目自己实现的 C++ SSE parser 位于另一段边界。
SDK 还支持其他传输选择，下面以项目常用的 HTTP/SSE 路径说明，不把它描述成所有 provider 唯一的传输方式。

**特别容易混淆**：Gateway 对 Bridge 暴露 `/v1/chat/completions`，只说明项目的对外兼容接口。
Pi 在内部仍可使用配置为 Responses 的模型；外层 URL 不能用来推断内层模型协议。

源码：[ModelRuntime][pi-model-runtime]、[OpenAI Completions adapter][pi-completions]、[Responses adapter][pi-responses]、
[事件队列][pi-event-stream]。

#### 4.24.8 SSE 的字节边界、事件边界与参数增量

SSE 是在 HTTP 响应体中持续发送文本事件的一种格式，常用 `Content-Type: text/event-stream`。
`event:` 可指定事件类型，`data:` 携带内容，**空行结束一条事件**。模型协议再约定 data 中的 JSON 含义。

下面是示意，不是本项目某次真实模型输出：

```text
data: {"choices":[{"delta":{"content":"先检查"}}]}

data: {"choices":[{"delta":{"content":"输入形状"}}]}

data: {"choices":[{"delta":{},"finish_reason":"stop"}]}

data: [DONE]

```

需要严格区分四个单位：

1. **TCP/HTTP 读到的字节块**：可能只有半个 UTF-8 字符、半行、半个 JSON，也可能一次包含多个事件。
2. **SSE 事件**：按协议行和空行边界组合出来。
3. **事件里的 JSON 对象**：完整事件拿到后才按 JSON 协议解释。
4. **模型 token/文本增量**：provider 可以把多个 token 合成一个 delta，delta 也不保证对应一个完整词。

本项目 C++ `HermesSseParser::Feed` 保存未完成行，扫描换行，处理 event/data 字段；遇到空行再组装事件。
多条 data 行按 SSE 规则拼接，达到缓冲上限时失败。不能对每次网络回调的数据直接 `json.parse`。
`[DONE]` 是兼容接口约定的结束标记，不是 SSE 标准要求每个服务都发送的字段；Responses 的终态事件也有自己的名称。

工具参数也可能逐片到达：

```text
工具调用 id=call_1，name=read
arguments delta 1: {"path":"op
arguments delta 2: erator.cpp"}
最后组合：{"path":"operator.cpp"}
```

Pi 可以用 `parseStreamingJson` 得到尽力而为的参数预览，方便 UI 展示，但预览不具有执行资格。
只有完整助手消息收齐，运行时才进入工具准备、schema 校验和执行；输出因 `length` 截断时不会执行这批残缺请求。
即使某个参数前缀碰巧是合法 JSON，也不能据此提前写文件。

**面试故障例子**：网络已经给出一段自然语言，随后连接断开。
这段文字只能算预览，不能视为最终任务成功；模型可能还没发工具参数、完整结论或成功终态。

源码：[provider 增量转换][pi-completions]、[本项目 SSE parser](../hermes_bridge/sse_parser.cpp)。

#### 4.24.9 三段协议怎样连起来，何时才能落最终结果

```text
模型服务
  │ provider 流：Completions/Responses 等，由 Pi adapter 归一化
  ▼
Pi Agent / AgentSession
  │ stdout：一行一个 JSON 的 RPC 事件；stdin：带命令 ID 的 JSONL 命令
  ▼
Python PiRpcClient / Kernel Gateway
  │ HTTP SSE：兼容文本 delta，或通用 agent.event；最终携带引用与元数据
  ▼
C++ Bridge → Kafka → Logic / Job → WebSocket 客户端
  ├─ ai_delta：临时展示与工具进度
  └─ ai_reply：完整回复进入正常消息序号、持久化与投递链路
```

**Pi RPC 是进程间协议，不是模型 SSE。** Gateway 向 stdin 写一条命令，例如
`{"id":"cmd-12","type":"prompt","message":"检查 add.cpp"}`，然后接收两种不同消息：

- `type=response`：以命令 ID 匹配对应等待者，确认命令是否被接受或执行成功。
  对 prompt 而言，这个确认不代表模型回答已经完成。
- `message_update`、`tool_execution_start/end`、`agent_settled` 等执行事件：进入事件队列，
  由当前聊天请求消费并转换成输出。

项目 `PiRpcClient` 用 `pending[id] → 单元素 Queue` 关联命令应答，另一条有界队列接执行事件。
写端持有独立锁，用非阻塞 pipe 和 deadline 处理部分写入；读线程还检查自己属于哪个进程，
防止已被替换的旧 worker 将迟到事件写进新 worker 的会话。

判断成功时，Gateway 会等 `agent_settled`，检查当前运行最近一条完整 assistant 的 `stopReason == stop`，
确认文本非空，再做引用改写。`agent_end` 不够：Session 层可能随后重试或压缩并继续执行。
遇到新的 `agent_start`，当前候选最终消息会清空，避免把前一次失败运行的输出交给用户。
Bridge 的 SSE parser 再检查 `[DONE]`、成功 final 或 `finish_reason=stop`、非空正文是否同时成立；
只收到 `[DONE]` 或连接正常关闭仍不足以宣布成功。

外层 Gateway 把文本合并成小批次，Bridge 再按字节量或时间间隔合并，降低 Kafka 发布和 UI 更新开销。
其中约 40 ms / 50 ms 是收到事件时检查的刷新条件，终态也会 flush；它们不是独立周期定时器，
不能据此保证每一个积压 delta 都在固定毫秒数内发送。

完整最终答案可能经过重试、引用替换，并且前面的预览可能包含中间工具轮次的文字；
所以客户端必须以 final 为准，不能永久把所有 delta 拼接文本当作权威历史。
`ai_delta` 不进入 IM message 历史、不分配正式消息序号，也不推进游标；这不意味着它绝不接触磁盘，
因为 Kafka 与可选 Gateway 事件回放账本仍可能存储这些事件。

源码：[Pi RPC mode][pi-rpc]、[PiRpcClient / SSE 输出](../cannbot/scripts/pi_gateway.py)、
[Bridge 客户端](../hermes_bridge/hermes_client.cpp)、[Bridge 投影](../hermes_bridge/main.cpp)。

#### 4.24.10 read / write / edit / bash 工具如何实现

工具执行器需要解决的是“让模型能够使用工程能力，同时把输入、输出和副作用管住”。

| 工具 | 底层动作与实现要点 |
|---|---|
| read | 解析相对于 cwd 的路径，检查/读取文件；文本按行分页并限制返回量，图片按能力处理 |
| write | 创建需要的父目录，再写入内容；使用文件修改队列，且检查取消状态 |
| edit | 读取原文件，规范化换行后匹配旧文本，检查匹配是否唯一、替换区间是否冲突，再写回并返回差异 |
| bash | 启动 shell 子进程，读取输出，处理超时、取消、退出码和进程树清理；结果受输出预算限制 |

Pi 0.84.4 的 edit 主要使用 `edits: [{oldText, newText}, ...]`，并兼容旧的单次替换输入。
同一批替换针对同一份原文计算，不能把后一个 oldText 默认理解成匹配前一个替换完成后的内容。
“唯一匹配、禁止重叠”和 diff 返回有助于避免模型改错同名代码段，但不能替代编译或行为测试。

输出也必须有限：Pi 通用文本截断默认约 2,000 行或 50 KiB，先达到哪个限制就使用哪个。
read 更关心文件前部/指定范围，bash 常保留输出尾部并在必要时提供完整日志位置。
这些是具体工具的策略，不是所有自定义工具都自动获得的全局额度；项目 CANN 工具另有自己的分页与上下文预算。

**不应扩大声称的保证**：默认文件 write 使用文件 API 写入，不代表整个编辑操作具有数据库事务语义；
Agent 失败不会自动回滚文件。使用独立工作区、提交前 diff、验证回执和版本快照，才是在应用层补足可检查性。

Kernel 子 Agent 没有 bash，开发角色只能使用明确写名单内的 write/edit；房间子 Agent 只有只读工具。
主 Agent 的命令能力与房间固定 runner 的隔离边界不同，不能用“都有工具”推断权限相同。

源码：[read][pi-read]、[edit][pi-edit]、[write][pi-write]、[bash][pi-bash]、[输出截断][pi-truncate]。

#### 4.24.11 Skill、Extension、工具和 RAG 分别放在哪一层

| 概念 | 本质 | 本项目的例子 |
|---|---|---|
| Skill | 可读取的工作流说明、参考资料和脚本入口 | `spark-push-agent` 规定研发分工和验收要求 |
| Extension | 在运行时执行的插件代码，可注册工具、命令和事件钩子 | `kernel-subagents.ts`、`cann-advisor.ts` |
| Tool | 有参数 schema 的可调用操作与本地执行函数 | `spawn_agent`、`cann_knowledge_get`、read |
| RAG | 从外部知识中检索证据并加入当前推理输入 | `cann-rag` 返回片段、文档身份与引用信息 |

Skill 写“审查后再交付”属于行为指引；它自身不是权限隔离器，也不会自动创建子进程。
Extension 的代码才会实际注册 `spawn_agent`，由工具 execute 创建独立 Session。
同样，提示词写“不要读工作区外文件”不是路径授权，必须在工具调用守卫中再检查。

项目 CANN 扩展通过 `pi.registerTool` 暴露四个知识操作，并用 input 钩子处理知识查询意图。
`get` 的 offset/length 与 expected_hash 解决“长文分段读取”和“跨页版本一致”的问题；
Gateway 的本轮引用账本再验证最终答案使用的引用是否来自本轮受信工具结果。
子会话检索只产生子会话证据，父 Agent 必须重新读取要引用的来源，不能直接复制子回复中的引用 token。

子会话使用显式 ResourceLoader 关闭自动发现，再仅加载已知的 CANN 扩展和子工具守卫，
避免工作目录里新增一个插件文件就悄悄扩大子任务能力。代价是子 Agent 不自动拥有主会话的全部 Skills；
必要任务事实和参考路径必须由父 Agent 传入。

#### 4.24.12 Session 持久化与上下文压缩怎样协作

Session 不是“把所有聊天拼成一个字符串”。Pi 使用带 `id/parentId` 的条目构成历史树，
持久化通常是 JSONL：消息、模型切换、思考等级、压缩和分支摘要等分别作为条目记录。
文件的追加顺序可以包含不同分支，`buildSessionContext` 沿当前叶子对应的祖先路径重建有效上下文。
内存结构是条目数组、`byId` Map 和当前 `leafId`：从 leaf 沿 `parentId` 回溯，再逆序得到活动路径。

```text
user(1) → assistant/toolCall(2) → toolResult(3) → assistant(4)
                         └→ 另一条继续路径 ...
```

上图是结构示意；实际条目类型与模型消息类型不是完全相同的一套枚举。
JSONL 的好处是增量记录、便于检查与恢复；它不自动等于带 fsync 保证的数据库 WAL，
也不保证任意外部副作用与消息追加原子提交。

`SessionManager.create()` 默认新建，恢复指定历史用 `open(path)`，续接最近会话用显式 `continueRecent`；
相同 cwd 不会使 SDK 自动找回最近对话。`branch(id)` 先移动内存中的 leaf，下一次追加才形成持久的新分支，
它不撤销旧分支已经修改的文件。完整 `message_end` 才触发消息条目追加，并非每个 token 都落盘；
新会话在首次 assistant 消息出现前延迟创建 JSONL，不能把它描述成用户输入一到就已有完整磁盘记录。

主会话绑定使用由合法 IM session 与上下文代次生成的文件名。相同原生文件已激活时可以复用，
需要切换时才发 `switch_session` 并确认结果；新建上下文换到新的 generation，旧记录不删除。
Kernel 子会话传入 `SessionManager.inMemory`，因此不会拥有主会话 JSONL 那样的重启恢复能力。

长上下文压缩的处理顺序是：

1. 根据模型窗口、最近 usage 和尚未计量消息的估算判断预算；估算值不是逐 token 精确计数。
2. 预留回答空间，选择需要总结的旧消息与保留的近期消息，尽量保持工具调用与结果的逻辑完整性。
3. 将旧内容交给模型生成摘要；摘要成本是一次真实的模型调用，不是免费字符串压缩。
4. 追加包含 summary、`firstKeptEntryId` 等信息的压缩条目；重建输入为摘要加保留消息。
5. 原始旧条目仍保留在 JSONL，压缩改变的是上下文投影，当前模型请求不必再次发送全部原文。

近似触发条件是 `estimatedContextTokens > model.contextWindow - reserveTokens`。
估算优先利用最近有效 assistant 的 usage，再估计其后的消息；没有 usage 时采用字符数等启发式，
文本大约按 chars/4 估计，不能当作中文或代码的精确 tokenizer。
`findCutPoint` 从后向前累计近期预算，选择合法消息边界，避免把 toolCall 与 toolResult 截成无效配对；
超长 turn 还可以拆出 turn prefix 单独总结。摘要使用独立模型请求，成功后才提交 compaction 条目，
失败或取消不会靠删除原始历史来“腾出空间”。自动压缩可以发生在新输入前或工具循环中的模型调用边界，
不一定要等用户整个任务结束。

Pi 默认自动压缩开启，默认预留 16,384 tokens、近期内容预算 20,000 tokens，实际可由设置覆写。
Kernel 子会话显式关闭自动压缩与自动重试，以配合短任务、有限轮次和可解释的错误状态；
不能把主会话默认能力直接写成子会话也支持。

摘要可能漏掉细节，所以它是上下文管理手段，不是对全部历史事实的无损索引。
代码真相仍在工作区，知识证据仍需检索，持久执行身份仍由 Router 的账本管理。
模型服务侧 KV cache 则是计算缓存，保存注意力中间张量，和这里的文本/JSONL 记忆不是一回事。

源码：[SessionManager][pi-session-manager]、[压缩实现][pi-compaction]、[设置][pi-settings]。

#### 4.24.13 子 Agent 编排是怎样建立在这些原语上的

Kernel 的子任务管理器保存一张 `Map<childId, record>`。record 除角色、独立 Session 和状态外，
还维护运行代次、后续输入次数、工具调用量、输出、超时器、清理 Promise 和资源是否真正释放。

```text
starting → running → completed
                  ↘ failed / timed_out / cancelled
completed → send_input → running
close_agent → 停止执行、清理会话 → 释放名额及写入占用
```

关键实现不是“创建 N 个模型请求”这么简单：

1. **先预留后异步创建**：spawn 在首次 await 前登记记录并占用名额；创建失败也留下可解释状态并释放资源。
2. **继承运行配置**：使用父会话选定的模型和思考等级，经公开模型注册接口取得认证/请求头，
   必要的运行时凭据只作内存覆盖；不把密钥放到子任务提示词或工具结果里。
3. **收窄工具**：父会话活跃工具集与角色允许集求交。developer 的精确文件名单再经过路径检查，
   拒绝越界、隐藏路径、符号链接和硬链接等当前策略不支持的路径。
4. **独立上下文**：父任务传入目标、文件、约束和预期交付，子 Session 从空历史开始；不自动复制私聊。
5. **事件收集**：订阅工具事件统计调用量；只有子 prompt 正常结束且最终 assistant 为 stop，才记 completed。
6. **等待与停止分开**：wait 订阅状态变化，任一指定子任务终结即可返回；等待超时不会取消任务。
7. **防止迟到结果覆盖**：中断或关闭时增加 generation；旧运行即使稍后返回，也不能更新新运行的状态。
8. **清理后再释放**：closed 的状态标签不代表旧工具已经退出。abort/dispose 收敛后才释放名额和写入占用，
   防止下一名 developer 与尚未停止的上一名同时操作文件。

同一个 child 的控制操作也需要串行化，避免两个 send_input 同时中断和重启同一个 Session。
运行中的子任务必须先等待，或明确 `interrupt=true`；本项目并没有把普通 send_input 实现成无限排队邮箱。
扩展在父 `agent_settled`、取消、会话切换与关闭时清理归属记录；不能使用过早的低层 agent_end 代替整个父操作结束。

当前边界：默认最多 4 个占用名额、每次父会话操作/turn 最多启动 16 次、每子会话最多 4 次输入、
工具执行预算累计 48 次（超额调用尝试会被守卫阻断）、
默认每次执行 120 秒，输出文本保留约 12,000 个 UTF-16 码元并附截断提示。
这些是代码/配置预算，不是根据 token 自动计算的花费上限；同样也不是跨进程全局资源配额。

**复杂度**：子任务 ID 查询平均 O(1)；名额与写范围冲突检查按已登记记录扫描。
在当前小规模额度下，简单 Map 和数组比引入分布式调度器更便于审计；跨进程恢复或更深层派生是另一个设计范围。

源码：[AgentControl](../cannbot/pi/extensions/lib/kernel-agent-control.ts)、
[SDK 适配与工具](../cannbot/pi/extensions/kernel-subagents.ts)。

#### 4.24.14 超时、取消、重试与幂等分别由谁负责

| 机制 | 作用 | 不承诺什么 |
|---|---|---|
| provider HTTP 超时/重试 | 处理模型请求的暂时失败 | 不保证工具执行只发生一次 |
| Agent AbortSignal / session.abort | signal 发取消通知，session 与控制层另行等待执行收敛 | 不撤销已经完成的文件写入 |
| Gateway 请求 deadline | 约束一次用户请求的等待与执行时间 | 不等于所有外部系统都已回滚 |
| Router request_id + input_hash | 相同已完成请求回放，按记录的路由与输入摘要拒绝冲突 | 不是 SQL、模型和文件系统间的分布式事务 |
| 子任务 generation | 丢弃旧运行的迟到状态更新 | 不删除旧运行已产生的外部副作用 |
| `wait_agent` timeout | 限制这一次等待的时间 | 不取消仍在执行的子任务 |

Pi 的 steering 是在可继续的执行边界注入新输入，follow-up 是当前工作完成后的续接输入；
二者都不能理解成“立即撤回已经提交的工具操作”。Kernel 的 interrupt 路径显式 abort 后再提交后续输入，
并等待前一轮收敛，语义比简单向队列里追加字符串更明确。
当前 Router 摘要中的图片字段只记录数量，没有逐字节覆盖图片内容，不能扩大成完整多模态输入的一致性保证。

项目还区分三类成功：命令被 RPC 接受、Agent 执行成功、最终消息被可靠持久化/投递。
这与 `accepted_ack` / `delivered_ack` 的分层思想一致：一个阶段的确认不能替代下一个阶段的完成证明。

可观测指标也要按层拆分：排队耗时、模型首段文本等待、模型调用次数、工具名/耗时/错误、
重试次数、累计 token、最终状态与结果投递耗时。一次用户任务可能调用模型多轮；
只统计最后一段文本长度，不能解释整个任务的延迟和成本。

#### 4.24.15 用一个完整例子串起设计

以“读取 Add 算子实现、分析边界条件并给修改建议”为例，下面是设计讲解，不是新增的真实模型验收记录：

1. Logic 验证用户与会话权限，将本轮请求送入 Agent 链路；Router 校验路由身份并安排准入。
2. Gateway 绑定主 Pi Session，经 JSONL prompt 进入 AgentSession；它检查输入、模型和认证。
3. 父模型获得工具 schema，选择 `spawn_agent({role: "researcher", message: "具体目标和路径"})`。
4. 本地扩展预留子任务名额，创建独立内存 Session，继承模型并只开放 read/CANN 工具。
5. 子模型返回 read 工具请求；本地守卫确认路径合法，工具读文件，再把 toolResult 交回子模型。
6. 子模型根据源码得出分析，子注册表记录 completed；父模型通过 wait_agent 取得结果。
7. 父模型核对关键文件与引用。私聊需要修改时，可以向 developer 指定精确写范围；房间子 Agent 始终只读，
   实际修改由已授权的主 Agent write/edit 执行。命令验证也由主 Agent 承担，房间使用固定 runner。
8. 任务收敛后，父会话 settled；Gateway 检查最终 stop 与引用，Bridge 发布正式回复。
9. Logic/Job 负责最终消息历史与投递；前端用 final 覆盖临时预览。主操作结束同时清理子 Session。

如果第 5 步读文件失败，模型可以看到工具错误后调整方案；如果第 7 步已经改文件但第 8 步模型断线，
文件可能已经改变，而用户还没有正式答案。这正是需要检查 diff、保留执行状态、避免盲目重放整个任务的原因。

#### 4.24.16 面试表达与追问展开

**约 90 秒的项目讲法**：

> 我把 Kernel Agent 分成模型适配、执行循环、会话、工具权限和消息接入几层。
> 模型适配层把不同 provider 的响应统一成文本、工具调用和终态事件；执行循环在完整工具请求到达后，
> 先校验参数与权限，再执行本地工具，把 toolResult 放回上下文继续推理。
> 会话层负责历史、压缩和重试，和 IM 的消息历史分开管理。
> 这部分采用 Pi 的运行时设计和 SDK。我主要完成了它与 Spark Push 的连接、CANN 证据工具、
> 房间权限与子任务管理：子任务有独立 Session、继承且收窄的工具集、明确写范围、名额预留和取消回收。
> 输出经过模型流、JSONL RPC 和 Gateway SSE 三段转换，预览与最终消息分开；只有完整成功终态才进入正式回复链路。

**面试官追问“不是直接调框架吗”**，可以继续讲具体决策与代码：

> 我没有重复实现 provider 和基础循环，而是确定了它们的输入、输出与失败边界，再把业务策略放到明确接口上。
> 例如创建子会话时我禁用自动资源发现、使用内存历史、继承父模型认证，并对每个工具做角色与文件范围检查；
> 创建前先占名额，关闭时等旧运行退出才释放写权限。接入层也没有把 RPC 接受或流式结束当作任务成功，
> 而是等 settled 后验证最终助手状态。这样可复用底层实现，同时对项目行为承担明确责任。

| 追问 | 应该说到的实现细节 |
|---|---|
| createAgentSession 为什么需要异步？ | 可能要构建模型运行时、加载资源与模型配置；不是因为创建时一定要推理 |
| Agent Loop 是 ReAct 吗？ | 可以按“决策—动作—观察”理解；落地是结构化 toolCall/toolResult 状态循环，不必解析自然语言 Thought/Action |
| 工具参数有 schema 为什么还要 guard？ | schema 约束形状，guard 约束权限、路径与副作用；二者检查对象不同 |
| 为什么不能边收到参数边执行？ | 增量可能不完整、后续会修改，输出可能截断；必须等完整消息并校验 |
| 模型输出了文本是否就结束？ | 同一响应可含工具调用；低层 turn 结束后还可能继续工具轮次、重试或压缩 |
| 单线程为什么要锁或预留？ | await 造成逻辑交错；检查名额和登记必须在同一同步片段完成 |
| 子 Agent 与开几个线程有何区别？ | 子 Agent 有独立消息上下文、任务身份和工具策略；线程只是计算/执行机制 |
| 为什么用内存子会话？ | 任务范围短、上下文干净、清理明确；代价是不能跨 turn/重启恢复 |
| 如何证明你做的权限有效？ | 看 guard 与工具名单，并用真实 SDK 对受控模型产生的越界/写入请求做集成验证 |
| 如何衡量 Agent 效率？ | 按模型轮次、工具调用、token、排队与执行时间拆账；不能只看最终回答速度 |

**简历可以具体写成**：

> 基于 Pi 的分层执行引擎构建 Kernel Agent，完成原生会话与 IM 路由绑定、结构化工具事件投影及 CANN 证据接入；
> 实现独立子会话编排、启动前配额预留、角色工具约束、文件写入范围与取消回收，区分临时流式输出和可靠最终回复。

这里的“设计”可以讲得深入，但贡献动词要落到对应源码：底层库用“采用、适配、扩展”，
自己增加的控制与接入用“实现”。这样面试官继续问函数、状态和错误路径时，每句话都有可追溯的实现。

#### 4.24.17 源码阅读顺序与核对范围

建议按 `sdk.ts → agent-session.ts → agent.ts → agent-loop.ts → provider → session-manager.ts` 阅读，
最后回到本项目 `kernel-subagents.ts → kernel-agent-control.ts → pi_gateway.py → sse_parser.cpp`。
不要先陷进 CLI 界面代码；先理解对象、消息和控制流，再看 UI 如何订阅这些事件。

上游链接固定到 Pi 0.84.4 对应提交 `b79e4cc834970cca69daebffab7df1da7d1e52c4`，本机安装位于
`/home/peco/.local/node/lib/node_modules/@earendil-works/pi-coding-agent/`。
`dist/*.js.map` 中的 sourcesContent 可核对原始 TypeScript；这里的伪代码用于解释控制流，不能当作逐行源码。
本专题的 Pi SDK 受控模型验收范围仍以 2026-10-30 记录为准；产品的后续真实 Runtime 与发行验收见第 6.4 节。
本次学习文档更新只核对材料与教学示例，没有重跑真实模型或 NPU 测试。

[pi-sdk]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/sdk.ts
[pi-session]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/agent-session.ts
[pi-messages]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/messages.ts
[pi-agent]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/agent/src/agent.ts
[pi-loop]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/agent/src/agent-loop.ts
[pi-file-queue]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/tools/file-mutation-queue.ts
[pi-model-runtime]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/model-runtime.ts
[pi-completions]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/ai/src/api/openai-completions.ts
[pi-responses]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/ai/src/api/openai-responses.ts
[pi-event-stream]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/ai/src/utils/event-stream.ts
[pi-rpc]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/modes/rpc/rpc-mode.ts
[pi-read]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/tools/read.ts
[pi-edit]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/tools/edit.ts
[pi-write]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/tools/write.ts
[pi-bash]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/tools/bash.ts
[pi-truncate]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/tools/truncate.ts
[pi-session-manager]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/session-manager.ts
[pi-compaction]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/compaction/compaction.ts
[pi-settings]: https://github.com/earendil-works/pi/blob/b79e4cc834970cca69daebffab7df1da7d1e52c4/packages/coding-agent/src/core/settings-manager.ts

## 5. 从项目展开 C++ 与并发八股

### 5.1 智能指针：谁拥有对象，谁延长生命周期

项目中的会话锁表保存 `weak_ptr<mutex>`，调用者取得 `shared_ptr<mutex>`；连接发送闭包持有连接的 shared_ptr。
这两个例子可以直接解释所有权：锁表负责查找，正在使用的调用者负责让对象活着。

| 类型 | 所有权 | 典型应用 |
|---|---|---|
| `unique_ptr<T>` | 独占，可移动 | 管理一个明确归属组件的 client、context 或资源包装对象 |
| `shared_ptr<T>` | 共享，最后一个所有者释放对象 | 异步任务和连接表共同持有连接 |
| `weak_ptr<T>` | 观察共享对象，lock() 尝试取得所有权 | 定时器、锁表、打断循环引用 |

**常见追问**：shared_ptr 的控制块引用计数支持并发管理，但对象内部字段仍需要锁或其他同步；
多个线程修改同一个 shared_ptr 变量也要同步。`weak.lock()` 将“检查仍存活”和“取得所有权”合成一次操作。
同一对象由同一控制块管理；从裸指针再次构造独立 shared_ptr 会带来重复释放问题。

### 5.2 RAII、移动语义与异常安全

连接池 guard 析构时归还连接，unique_lock 析构时解锁，线程组件退出时停止并 join 工作线程。
资源释放与对象作用域绑定，正常 return 和异常展开走同一条释放路径。

- 独占资源包装通常删除复制构造/赋值，实现移动构造/赋值，把所有权转移给新对象。
- `std::move` 是转换为右值表达式，具体类型的移动操作负责转移资源。
- 标准库类型移动后通常仍可析构和赋值；其具体值遵循该类型的约定。
- Rule of Zero：优先组合标准资源管理类型，减少手写析构与复制/移动逻辑。
- 强异常保证可用“先构造临时结果，成功后提交替换”解释；基本保证关注对象有效和资源释放。

**项目落点**：队列中取出任务后 move 到局部变量，解锁后执行；闭包按值持有需要延长生命周期的数据。

### 5.3 条件变量为什么要带谓词

线程池等待的是“有任务或停止”，而不是某一次 notify。

```cpp
std::unique_lock<std::mutex> lock(mutex);
cv.wait(lock, [&] { return stopping || !tasks.empty(); });
if (stopping && tasks.empty()) return;
auto task = std::move(tasks.front());
tasks.pop();
lock.unlock();
task();
```

wait 原子地释放锁并进入等待，唤醒后重新取得锁，再检查谓词。虚假唤醒、多个 worker 竞争同一任务时，
谓词都能重新判断当前状态；生产者在同一把锁下修改队列，保证条件检查与状态更新协调。
新增一个任务常用 notify_one，关闭线程池用 notify_all。

**追问**：为什么执行任务要解锁？队列锁只保护入队出队；业务执行在锁外进行，其他 worker 才能继续取任务。
定时等待使用绝对 deadline，反复唤醒后继续等待同一个截止时刻。

### 5.4 mutex、atomic 与 happens-before

| 场景 | 同步方式 | 原因 |
|---|---|---|
| 队列、计数与停止状态共同变化 | mutex + 条件变量 | 多字段操作需要保持整体一致 |
| 独立停止标志或计数器 | atomic | 对单个变量做原子读写或更新 |
| 路由缓存读多写少 | shared_mutex | 允许多个读取者并行，更新时独占 |
| IO 连接状态转换 | owner EventLoop | 将主要状态变化集中到所属线程 |

mutex 的 unlock 与随后成功取得同一锁建立同步关系。atomic 的默认顺序是 seq_cst；
release/acquire 可用来发布并观察先前写入；relaxed 提供原子性而不承担跨变量发布协议。
`volatile` 主要约束特定访问优化，线程间共享状态使用同步原语。

**延伸**：false sharing 是不同线程更新同一 cache line 上的不同变量，引发缓存一致性流量；
高频独立计数器可以讨论按缓存行分离与批量累加。

### 5.5 Reactor、线程池与异步回调

Reactor 使用 IO 多路复用等待就绪事件，由 EventLoop 分发回调。项目中连接绑定 owner loop，
耗时 RPC 放到工作线程，结果通过任务队列送回 owner loop。

- epoll 监听 socket 就绪，eventfd 可用于唤醒等待中的 EventLoop；唤醒后处理跨线程提交的任务。
- 非阻塞读写遇到 EAGAIN 表示当前可处理的数据已耗尽，保存状态等待下一次就绪。
- LT 持续报告就绪状态，ET 在状态变化时通知；ET 通常配合非阻塞并读写到 EAGAIN。具体触发模式以 poller 配置为准。
- TCP 是字节流，HTTP/WS/SSE 各自通过长度、分隔符和状态机划分业务记录。

**生命周期追问**：任务闭包拥有连接和消息字节；计时器使用 weak_ptr；退出时依次停止准入、取消阻塞、
唤醒等待者、join 线程，再释放共享数据。取消是通知，join 是等待线程真正结束。

### 5.6 容器选择、复杂度与内存

| 项目场景 | 结构 | 为什么这样选 |
|---|---|---|
| request_id → pending | unordered_map | 平均 O(1) 关联异步应答 |
| 近期去重窗口 | unordered_set + deque | 查重与按插入顺序淘汰各司其职 |
| Agent 增量重排 | TreeMap；C++ 可对应 map | 按序号维护有序数据，插入/删除 O(log W) |
| 待执行任务 | queue | FIFO 调度，入队出队 O(1) |
| 空闲连接 | deque | 两端借还，避免整体搬移 |
| 连接快照与批次 | vector | 连续存储，遍历局部性好 |

unordered_map 的最坏查找可退化到 O(N)，rehash 会使迭代器失效；vector 扩容时迭代器、指针和引用可能失效。
若要跨锁或跨线程保存对象，复制具有所有权的对象或智能指针，而不是依赖容器内部元素地址。

字符串逐次全量拼接可能累计 O(n²) 拷贝；缓冲区追加、预留容量和批量刷新减少重复分配。
延伸可讨论对象池、内存池与零拷贝；项目中的拥有型缓冲区首先解决异步生命周期问题。

## 6. 性能数字与分析方法

### 6.1 简历可引用的数据

2026-10-11，WSL2、12 logical CPU、约 15.52 GiB 内存，客户端与服务同机，单 Kafka broker/partition、RF=1，AI 关闭。
三轮实验记录见 [消息链路压测](benchmarks/2026-10-11-current/README.md)。

| 场景 | 负载 | 三轮吞吐中位数 | 各轮 p99 的中位数 |
|---|---|---|---|
| 单会话 E2E | 每轮 8 个连接 × 20 条；ACK、唯一接收、SQL 均为 160 | 33.52 msg/s | 4723.21 ms |
| 历史查询 | 每轮 400 次查询，页长 50 | 1158.62 QPS | 11.62 ms |
| 认证建连 | 每轮 200 个连接、8 线程 | 1290.94 conn/s | — |
| Kafka→Job→Comet | 每轮 1000 条唯一接收 | 1197.01 msg/s | 811.46 ms |

回答性能问题时先说明测量路径：E2E 包含受理、SQL 与推送；Kafka→Comet 测量异步投递段；历史 QPS 测查询。
这组数据来自 2026-10-11 的三轮短时实验，适合解释测量方法和瓶颈定位，不能当作 v0.1.0 的容量验收。
`1197.01 msg/s` 是特定投递段吞吐，不是完整发送链路吞吐；不要写成“系统支持千级端到端 QPS”。

### 6.2 热点会话为什么排队

同会话 800 条突发时出现 ACK 超时。实现中会话锁覆盖 SQL 预留、Outbox 准备和 Kafka 确认，
后续请求等待该临界区；8 秒 deadline 从入队开始累计，因此突发流量首先体现为等待时间增长。

```text
总延迟 = 入队等待 + 会话锁等待 + SQL 往返 + Kafka 确认 + 下游投递
```

优化讨论按顺序展开：分段计时 → 定位最长等待 → 缩短锁内工作 → 评估批量确认或可靠异步受理 → 控制客户端发送窗口。
每次优化都同时观察吞吐、p99、队列长度和失败率。

### 6.3 可延伸的性能八股

- **Little's Law**：稳态下 `L = λW`，平均在途数 = 到达率 × 平均停留时间。适合分析并发窗口和队列积压。
- **Amdahl 定律**：串行比例为 f 时，N 个执行单元的理想加速比约为 `1 / (f + (1-f)/N)`；热点锁对应串行部分。
- **吞吐与尾延迟**：接近资源饱和时排队时间增长，p99 对突发、慢 SQL 和长任务更敏感。
- **背压**：入口限流、队列容量、消息字节预算和慢连接输出上限，分别控制链路不同位置的积压。
- **批量处理**：减少系统调用、Kafka 发送和 UI 刷新次数；批量窗口同时增加等待时间，需要权衡。

### 6.4 当前发行验收与历史合同测试如何区分

2026-11-01 的 v0.1.0 已发布，受检产品提交为 `7f92f3d`。证据入口：
[最终交付回执](benchmarks/2026-11-01-v0.1.0-release.json)、
[交付跟踪](v0.1.0-delivery.md)、
[正式 Release](https://github.com/specetator/grpc_aiagent/releases/tag/v0.1.0)。

| 证据类型 | 已执行结果 | 能说明什么 |
|---|---|---|
| 本地 CTest | 注册 66 项，62 通过、4 项数据库默认跳过 | 受检源码的单元/合同回归；跳过不算通过 |
| 冻结源码 CI | 59 通过、7 跳过；另行真实 MySQL 2 项、Redis 2 项及 Kafka 持久化/卷检查通过 | 实际构建与独立基础设施回归 |
| 三个独立 Chromium | 基础 IM 11 项；真实 Pi/Hermes 12 项通过 | 账号邀请、群聊、代码读取、任务接手、重连/重启恢复 |
| 真实跨房间 Agent | 16 项通过 | 两房间、两 Agent、原生上下文隔离、撤权与重投 |
| 真实恢复与兼容 | 成套冷备 18 项、IM 冷备 10 项、CANN 文档 14 项、旧 CANN/私聊 11 项通过 | 已定义场景的状态持久化与不重复执行 |
| 干净 Linux 用户空间 | 4 项外层检查 + 11 项 IM 检查通过 | 独立 DinD daemon 匿名安装、十用户及正常恢复 |

这些场景有重叠，不能相加后声称“覆盖了若干互相独立的功能”。Mock、合同、真实服务、真实模型、
真实浏览器属于不同证据层次；必须说清是否真的调用了 Runtime、是否注入故障、是否只验证局部链路。

当前边界：这是 v0.1.0 只读协作交付，不是 MVP V1.0 全部 T01～T14 验收。
本次固定制品没有独立重跑 Native 单任务停止、provider failure、审批拒绝专项；历史阶段证据不能冒充当前制品。
Windows 工具限制不等于 OS 只读挂载沙箱；干净 Linux 验证是 WSL 上的独立用户空间与 daemon，非独立 VM。
unknown 恢复用了真实 Native Run 加受控回执丢失；不代表所有自然故障。Logic 热替换、Android 实机、
Ascend/NPU 数值和长期容量仍未验收，升级/恢复按维护窗口冷启动。

2026-10-28 的 Native 单元 38 项、Gateway 6 项等属于早期合同记录，见
[历史测试回执](benchmarks/2026-10-28-hermes-native.json)。
它们可用于解释故障构造，但不再用“真实 Hermes 尚未验证”概括整个项目。
本次学习文档更新没有重新执行上述产品验收；自行练习时另记环境、版本、命令和退出码。

### 6.5 面试中的量化表达模板

> 在【日期/提交】、【环境/模型是否开启】下，用【负载和持续时间】测试【明确路径】；
> 观测【吞吐、p99、失败率、SQL/接收去重计数】。结果是【真实数字】；
> 另外在【故障点】注入失败，用【序号/Run/工具记录】确认恢复。尚未覆盖【边界】。

没有同条件的前后实验，就不写“性能提升 50%”。功能验收通过也不能推出生产 SLA、海量并发或 exactly-once。

## 7. 简历表述与面试讲解

以下为可选表达模板，按实际贡献选择；“参与实现”“负责实现”“主导设计”对应不同责任范围。
每条必须能补上源码、一个失败例子和验证方法，无法解释的条目先删除。

### 7.1 30 秒介绍

> Spark Push 是面向小团队的 AI 协作即时通讯项目，支持私有空间、多人聊天、持久任务和可选 Pi/Hermes。
> 后端以 C++17、Muduo、gRPC、Kafka、MySQL、Redis 构建，Comet 接入连接、Logic 管业务、Job 做异步消费。
> 我重点参与【选择自己实际负责的两项】，例如消息幂等与断线恢复，或者正式成员权限与 Agent 接入。
> 系统将流式预览和可靠最终消息分开，并通过三个独立浏览器、真实 Runtime 与冷备恢复验证了 v0.1.0 的协作闭环。

展开到三分钟时，第一分钟讲用户场景和架构，第二分钟讲一个故障与实现，第三分钟讲验证、取舍和未完成项。
不要从技术栈清单开始连续报组件名；要让面试官知道每个组件解决什么问题。

### 7.2 C++ 后端方向简历条目

按版面选 3～4 条，每条准备一个能讲清楚的数据结构或执行流程。

- 基于 Muduo/WebSocket、gRPC 双向流与 Kafka 拆分连接接入、业务编排和异步消费，使用 owner EventLoop 与有界线程池组织 IO 和耗时任务。
- 设计 MySQL 会话取号、canonical 原文与二进制 Outbox，在同一业务身份下复用序号和原文，通过后台重试与幂等消费恢复未完成消息。
- 实现 accepted/delivered 两阶段确认、逐条 receipt 和连续游标，结合 Keyset 分页与客户端去重重排完成断线补拉和历史恢复。
- 使用 weak_ptr 会话锁表、RAII 连接池和条件变量任务队列管理资源，设计关闭、取消、唤醒与 join 的线程退出顺序。
- 按连接/会话设置队列和字节预算，结合分段计时分析热点会话锁等待、SQL 往返和 Kafka 确认对吞吐及 p99 的影响。
- 基于服务端正式成员关系实现私有空间邀请、角色管理和跨 HTTP/WS/gRPC 的对象级授权；使用事务行锁处理成员变更，撤权后复查历史、附件和推送资格。
- 交付 Compose 单机部署、增量迁移与冷备恢复，使用固定源码、镜像 digest 和真实验收回执发布 v0.1.0，保留旧消息及 Agent Session。

### 7.3 Agent 工程方向简历条目

- 基于 Pi 分层执行引擎构建 Kernel Agent，完成原生会话与 IM 路由绑定、结构化工具事件投影与 CANN 证据接入；参考 Codex CLI 生命周期实现独立子会话编排、启动前配额预留、角色工具约束、文件写入范围与取消回收。
- 将 Pi/Hermes Runtime 接入同一房间时间线，以结构化 agent_id 路由、确定性 request_id 和独立 Runtime Session 支持多个 Agent，保持跨房间上下文隔离。
- 为批准项目提供受限代码快照读取与持久协作任务，将请求发起者、Agent 身份、任务 revision 和真实工具回执分别保存，支持成员接手与重登恢复。
- 接入房间图文资料库，采用分片续传、异步解析和固定版本来源回执，使授权 Agent 可读取讨论、PDF/DOCX 和图片并关联持久任务；真实三浏览器 PDF 流程由 Hermes 验证，其他模型或格式按各自证据说明。
- 为 Hermes Runs API 实现准入预写、稳定幂等键和结果账本，区分执行恢复与重新执行；以合同测试覆盖丢响应、重复投递、取消及审批，并与真实 Runtime 验收分别记录。
- 为长任务增加可选 Kafka 控制抢占，以连续完成前缀提交 offset，在保留普通任务顺序的同时处理停止和一次性审批。
- 接入只读 CANN RAG 工具，通过检索、分页读取、内容 hash 和引用标识组织模型证据；协作任务以 CAS 检查点、内容寻址版本关联测试结果。

### 7.4 高频追问速答

| 问题 | 回答主线 | 复习入口 |
|---|---|---|
| 为什么同时有消息表和 reservation？ | reservation 保存首次受理身份与原文，消息表保存最终历史 | 4.1～4.2 |
| 消息乱序怎样处理？ | 会话序号、receipt 连续前缀、客户端 seen 集合与补拉 | 4.4～4.6 |
| next_seq 和 safe_seq 如何分工？ | 一个驱动分页，一个驱动连续游标推进 | 4.5 |
| 哈希表查重为什么还要 deque？ | 哈希表负责成员查询，deque 负责有序淘汰 | 4.8 |
| mutex 和 FIFO 有什么关系？ | mutex 实现互斥；排队顺序需要 ticket/队列额外管理 | 4.3、4.19 |
| shared_ptr 就能保证线程安全吗？ | 引用计数、指针变量、被管理对象是三个同步问题 | 5.1 |
| 为什么 cv.wait 要带谓词？ | 等待的是共享状态，唤醒后重新检查 | 5.3 |
| 对象析构时线程还在运行怎么办？ | 停止准入、取消等待、唤醒并 join，再释放依赖 | 4.16、5.5 |
| 为什么扩线程不一定提高吞吐？ | 分析串行锁、IO 等待、CPU 和队列；用 Amdahl 与分段计时解释 | 6.2～6.3 |
| 子 Agent 与多 worker 有何区别？ | 子 Agent 拆同一任务，worker 池承接不同会话；两者有独立资源边界 | 4.24.1、4.24.13 |
| 子任务能跨重启恢复吗？ | 当前子会话只属于主 turn，不能把父账本回放误写成子任务持久恢复 | 4.24.12～4.24.13 |
| createAgentSession 创建了什么？ | 注入模型运行时、历史与资源，构造 Agent，再由 AgentSession 装配工具和提示词；prompt 才开始推理 | 4.24.1～4.24.3 |
| Agent 和普通模型接口有什么区别？ | Agent Loop 检查结构化动作，校验并执行工具，将结果回填后继续推理 | 4.24.4～4.24.5 |
| 工具参数截断了怎么办？ | 不执行该响应中的任何工具，生成失败结果，让模型重新提交完整参数 | 4.24.4、4.24.8 |
| 流式响应怎样转成 UI 输出？ | provider 归一化事件 → JSONL RPC → Gateway SSE → Bridge/Kafka；final 覆盖预览 | 4.24.7～4.24.9 |
| 单线程为什么还有竞争？ | await 造成交错，配额须同步预留；文件修改按路径排队，写范围另行约束 | 4.24.6、4.24.13 |
| 对话很长怎么办？ | JSONL 保留历史，按预算生成摘要并保留近期消息，按需检索外部证据 | 4.24.11～4.24.12 |
| 取消是否撤销已经改过的文件？ | AbortSignal 通知停止，控制层等待收敛，已发生的副作用需要另外处理 | 4.24.14 |
| 工具请求超时为何查询原 Run？ | 工具可能已经产生副作用，通过执行身份恢复原结果 | 4.21 |

讲解一个技术点时，用“业务问题 → 数据结构 → 一次正常执行 → 一个并发或故障例子”组织答案。
例如讲连续游标，直接画出“1 未落库、2 先投递”的时间线，再说明 safe 和 receipt 如何配合。

### 7.5 把“技术栈堆砌”改成可追问的简历

下面是修改方法，动词仍须按自己的贡献替换。每条只保留一个主要问题、关键机制和验证范围。

| 原表述 | 改写模板 | 面试前必须准备 |
|---|---|---|
| 使用 C++、Redis、Kafka 实现高并发聊天 | 【参与实现/复现】消息持久受理与断线恢复，通过 SQL 原文预留、Outbox 和稳定客户端 ID，使重复发送复用原消息；在【实际环境】核对 ACK、唯一历史行和补拉结果 | 一张 SQL/Kafka 故障窗口图；解释不能承诺 exactly-once |
| 使用智能指针，保证线程安全 | 【实现/审查】会话锁表与异步连接所有权，使用 weak_ptr 回收空闲锁、shared_ptr 保持等待期间存活，并将连接操作切回 owner loop | 指出谁拥有 mutex；解释 shared_ptr 不保护对象内部字段 |
| 接入多个大模型，支持智能协作 | 【参与实现/扩展】结构化 Agent 路由，将房间、Agent 与原生 Session 隔离；以持久请求账本恢复未知执行，验证同请求重放时工具未再次运行 | 一个真实请求的身份表；区分框架工具、自己的适配代码和实际回执 |
| 优化系统，性能提升数倍 | 在【日期/提交/硬件/负载】定位【瓶颈】，修改【机制】；同条件下【指标】由【实测前值】变为【实测后值】，同时核对失败率与消息完整性 | 前后原始报告；没有对照实验就删除“提升数倍” |

**一条简历的自查顺序**：我实际做了什么 → 为什么这样做 → 什么情况下会失败 → 怎样证明 → 哪项仍未覆盖。
先写一页证据笔记，再压缩成一两行简历。面试展开时用证据笔记，不把所有源码细节塞进简历。

针对岗位调整顺序：C++ 后端把并发、消息可靠性、存储排在前面；Agent 后端把权限、执行恢复、原生 Runtime 集成排在前面。
两种版本使用同一套事实与指标，不能为了岗位名称改变个人参与程度。

### 7.6 面试前给每条简历配一张证据卡

从 7.2 或 7.3 选三条即可。下面每行是准备方法，不是替你认领贡献；把“当前实现”与“我实际做的部分”分开填写。

| 可选主线 | 30 秒讲清的问题 | 必须能接住的基础追问 | 随身准备的证据 |
|---|---|---|---|
| 消息可靠性 | ACK 丢失后如何恢复同一条消息 | TCP 与业务确认；唯一键；事务；Kafka offset | 一张失败时序图、一个原消息 ID 的恢复记录 |
| C++ 网络与并发 | 慢调用如何不阻塞 IO，异步对象怎样安全退出 | Reactor；shared/weak；谓词 wait；队列背压 | 线程/所有权图、关键函数、边界测试输出 |
| 空间权限与 Agent | 同一时间线如何保留身份和授权边界 | 认证/授权；事务行锁；CAS；幂等与 unknown | 一次撤权路径、room/agent/actor/request 身份表 |
| 发布与恢复 | 本地能跑为何还需验证固定制品 | 进程/存储故障；日志；恢复点；配置依赖 | 带版本的回执、实际失败候选、修复后对照 |

证据卡背面只写三行：“我的具体改动 / 我运行或检查过什么 / 还没有证明什么”。
面试官换成“如果增加一个实例”“如果进程在这里崩溃”“如果调用超时”，应能沿原图改变假设，而不是重复介绍词。
数字只引用第 6 节注明环境和负载的报告；本手册中的小程序运行次数不是项目吞吐或用户规模。

### 7.7 一段可以练习的 90 秒回答：从线程池讲到消息可靠性

问题：“你在这个项目里怎么处理异步任务和安全退出？”下面使用学习者口径；只有确实修改过对应实现，才改成个人研发贡献。

> 我沿 Comet 的 unary 上行分支阅读了线程池，重点复现了任务入队和退出边界。IO 回调把耗时 RPC 交给工作线程，
> 提交时检查队列条数和字节权重；队满会返回拒绝，避免无限积压。worker 在锁内取任务，锁外执行业务，
> 因此一个慢调用不会一直占住队列锁。关闭时先拒绝新提交，再选择处理完队列或丢弃未执行项，最后等待 worker 退出。
> 我用同步门固定“任务 1 已运行、任务 2 仍排队”的交错，验证丢弃模式只影响任务 2，不会中断任务 1。
> 这也让我区分了线程池接纳和消息受理：只有后续持久化事件得到 Kafka 确认，才能发 accepted_ack。
> 已发出 RPC 但结果未知时，需要沿原 client_msg_id 恢复，不能把本地停止理解成远端回滚。

**练习方法**：先按“问题 → 三个实现点 → 一个实验 → 一个限制”复述，再去掉文本录一遍。
范例讲的是 unary 分支；双向流使用自己的队列与 pending 管理，不能把所有上行消息都说成经过同一个线程池。
未亲自运行实验时，将“复现、验证”改成“阅读、推演”；实际完成第 12.14 节后再补上输出。

可以写进学习项目的一句话是：“阅读并复现 C++ 有界任务队列的停止与排空语义，用受控线程交错区分队列接纳、
执行完成和业务确认，并分析同消息身份重试的恢复边界。”这不等于“独立实现生产级线程池”或“解决任意故障下消息不丢失”。

### 7.8 同一个模块，怎样按自己的参与程度写简历

以下以资料授权为例。先判断自己完成到哪一层，再选用对应表达；不要直接把项目总体能力改成“我负责”。

| 自己已有的证据 | 可以采用的表达模板 | 面试前要补齐什么 |
|---|---|---|
| 阅读源码并独立解释链路 | “分析协作空间资料查询的服务分层与对象权限，梳理请求身份、来源代次和读取边界。” | 指出 3 个关键函数，手画一次撤权交错 |
| 亲自运行教学实验 | “复现权限过滤与 keyset 分页的顺序错误，以差分检查验证静态数据下的游标完整性。” | 保存原始输出；解释教学 eligible 与真实授权的区别 |
| 亲自实现并测试对应改动 | “实现【自己修改的部分】，通过【版本/环境/实际用例】验证【明确合同】，保留【未覆盖边界】。” | 提交/diff、失败回归、真实执行回执；占位符必须换成自己的事实 |

**90 秒练习稿（阅读与实验口径）**：

> 我选资料查询学习后端权限，因为这条链同时涉及身份、SQL 和并发。服务端由持久 request 取得房间和发起者，
> 查询把房间、消息上界及有效来源作为资格条件，合格结果再分页，直读也复用条件。
> 我在教学模型里复现了先取两条再过滤导致空页的错误，并检查额外探测行不能成为下一页游标。
> 对读取期间撤权，源码在记录来源前后复查授权，事务还会比较代次和来源范围。
> 我的实验只验证分页算法，真实权限依赖数据库和 Service；已发出的网络内容也不能靠再次鉴权追回。

未运行第 12.15 节时，把“复现、检查”改成“设计了验证输入”。面试官追问性能数字时，
给出测量方案或自己真实的数据；1024 个教学组合不是请求吞吐，也不是生产权限覆盖率。

## 8. 源码复习入口

| 主题 | 阅读入口 |
|---|---|
| 消息原文与 Outbox | [message_reservation.cpp](../logic/message_reservation.cpp) |
| 业务编排 | [message_service.cpp](../logic/services/message_service.cpp) |
| 连续游标与历史 | [user_session_state_dao.cpp](../logic/user_session_state_dao.cpp)、[message_dao.cpp](../logic/message_dao.cpp) |
| C++ 锁、队列与池 | [keyed_mutex.h](../common/keyed_mutex.h)、[thread_pool.cpp](../common/thread_pool.cpp)、[redis_pool.cpp](../common/redis_pool.cpp) |
| 连接与双向流 | [Comet](../comet/comet_server.cpp)、[Job](../job/service.cpp) |
| Kafka | [producer](../common/kafka_producer.cpp)、[consumer](../common/kafka_consumer.cpp) |
| Kernel 子 Agent | [扩展](../cannbot/pi/extensions/kernel-subagents.ts)、[能力与边界](kernel-agent.md)、[Codex 对照](codex-subagent-reference.md) |
| Agent Session 与调度 | [Kernel Agent Gateway](../cannbot/scripts/pi_gateway.py)、[Router](../cannbot/scripts/agent_router.py)、[Scheduler](../cannbot/scripts/agent_scheduler.py) |
| 空间权限与成员事务 | [RoomService](../logic/services/room_service.cpp)、[SpaceRepository](../logic/repositories/space_repository.cpp) |
| 通用 Agent 与任务 | [Catalog/Binding 与任务用例](../logic/services/room_agent_catalog.cpp)、[Repository](../logic/repositories/room_agent_repository.cpp) |
| 工作区、发布与恢复 | [room_workspace.py](../cannbot/scripts/room_workspace.py)、[备份脚本](../scripts/spark_backup.py)、[发行验证](../scripts/verify_release.py) |
| 开发中的资料授权与来源 | [MediaContextService](../logic/services/media_context_service.cpp)、[ContextRepository](../logic/repositories/media_context_repository.cpp)、[阶段合同](room-context-tools.md) |
| 当前验收 | [交付回执](benchmarks/2026-11-01-v0.1.0-release.json)、[Web E2E](../tests/collaboration_web_e2e.py) |
| Native 与 RAG | [Native Adapter](../cannbot/scripts/hermes_native_adapter.py)、[cann-advisor](../cannbot/pi/extensions/cann-advisor.ts) |
| 推理与 KV | [推理架构](inference-serving.md)、[WorkerRegistry](../inference/gateway/worker_registry.cpp) |

## 9. 面试术语速查

### 9.1 消息与分布式系统

| 术语 | 一句话解释 |
|---|---|
| canonical 原文 | 第一次受理后确定的消息内容、身份、序号与时间戳，重试复用 |
| Outbox | 与业务状态一起持久保存的待发布事件，由后台继续投递 |
| 幂等 | 同一业务身份重复执行，结果收敛到同一状态 |
| 至少一次 | 失败恢复中允许重投，消费端通过幂等处理重复 |
| ACK | 对某个处理阶段的确认；项目中分 accepted 和 delivered |
| receipt | 一条消息被目标连接接纳的记录，用于推进连续投递水位 |
| 连续前缀 | 从起点到某个位置的所有必要条件都已满足 |
| safe_seq | 根据持久化进度确定的安全序号范围 |
| next_seq | 当前页结束位置，用于构造下一页查询 |
| offset | Kafka 某个 partition 内的读取位置 |
| consumer group | 协调分区分配并管理消费进度的一组消费者 |
| DR | delivery report，Kafka 对一次生产结果的异步回调 |
| DLQ | dead letter queue，保存多次处理失败的记录以便检查和重放 |
| CAS | 比较旧版本后更新，版本变化则重试或报告冲突 |
| generation | 一次连接、路由或上下文的版本号，用于识别过期操作 |
| lease | 有有效期或明确释放条件的占用记录；不同模块的 lease 语义各自定义 |
| fanout | 将一条消息展开到多个目标节点或连接 |

### 9.2 C++ 与网络

| 术语 | 一句话解释 |
|---|---|
| RAII | 用对象生命周期管理资源申请与释放 |
| UAF | use after free，对象释放后仍被访问 |
| owner loop | 连接所属的事件循环线程，集中执行连接状态变化 |
| Reactor | IO 就绪通知加事件分发的网络编程模型 |
| completion | 一次异步操作的结果及完成通知 |
| deadline | 一次操作的绝对截止时刻 |
| happens-before | C++ 中描述操作可见性和执行约束的先行关系 |
| 虚假唤醒 | 条件变量醒来时业务条件仍可能不成立，因此需重查谓词 |
| false sharing | 多线程修改同一缓存行中不同变量，产生额外一致性流量 |
| 背压 | 根据下游处理能力限制上游流量或在途数据 |
| FIFO / LRU | 按插入先后淘汰 / 按最近使用情况淘汰 |
| 摊销复杂度 | 把偶发扩容或清理成本分摊到一系列操作上 |
| TCP 字节流 | 应用通过协议自行划分消息边界 |
| gRPC 双向流 | 同一 RPC 内双方可持续发送多条消息 |
| SSE | 按文本事件格式从服务端持续推送事件 |

### 9.3 Agent 与推理

| 术语 | 一句话解释 |
|---|---|
| Kernel Agent | 本项目基于 Pi 运行时的算子研发助手，负责领域工具、子任务和协作接入 |
| Subagent | 由主 Agent 派发、具有独立上下文的子任务执行者；当前只属于一个主 turn |
| Agent Loop | 模型决策、工具执行、结果回填，再决定继续或结束的循环 |
| Provider adapter | 把统一消息与工具描述转换为模型厂商协议，再把响应归一化 |
| toolCall / toolResult | 模型提出的结构化工具请求 / 本地执行的真实结果，通过 toolCallId 关联 |
| Steering / Follow-up | 在可继续的执行边界加入输入 / 当前循环完成后的续接输入 |
| agent_settled | AgentSession 完成重试、压缩、续接等处理后的稳定结束事件，仍需检查成功状态 |
| Session | 一段会话上下文；是否可恢复取决于持久化方式，Kernel 子会话当前仅在内存中 |
| Context Window | 模型单次推理可容纳的上下文范围 |
| Compaction | 将较早对话压成摘要并保留近期内容 |
| Skill | 可按任务加载的操作说明、规则和资源 |
| RAG | 检索外部资料后组织上下文，再让模型生成回答 |
| Embedding | 将文本映射为向量，便于语义相似度计算 |
| Reranker | 对初步召回的候选再次排序 |
| Run | 一次具有独立身份、状态与输出的 Agent 执行 |
| 工具副作用 | 文件修改、消息发送等对外部状态的改变 |
| Checkpoint | 可恢复的阶段状态，常配合版本号 |
| 内容寻址 | 用内容摘要标识某份文件或版本 |
| Prefill / Decode | 处理输入并建立缓存 / 逐 token 生成输出 |
| KV cache | 缓存已处理 token 的注意力 Key/Value 张量 |
| TTFT / TPOT | 首 token 延迟 / 首 token 之后的单 token 时间 |
| Continuous batching | 动态给正在运行的计算批次补充请求 |
| Prefix cache | 复用相同前缀的预填充计算结果 |
| 量化 | 使用更少位数存储权重或 KV，权衡精度、带宽与容量 |

<a id="collaboration-cases"></a>
## 10. 用最新多人协作功能串起系统设计

本节对应 v0.1.0 当前实现。阅读时同时打开
[空间权限](collaboration-spaces.md)、
[通用 Agent](generic-room-agents.md)、
[工作区](room-workspaces.md)和
[最终交付回执](benchmarks/2026-11-01-v0.1.0-release.json)。
阶段文档可能保留当时的待办，发行状态以最终回执和当前源码为准。

### 10.1 私有空间：为什么“登录了”还不够

**业务问题**：A 创建私有空间并邀请 B；C 即使知道 RoomID，也不能读历史或下载附件。

认证回答“你是谁”，授权回答“你能对这个对象做什么”。actor 从受信凭据解析，不能用请求正文中的
`user_id` 冒充。成员表决定能否访问房间；角色决定能否管理成员；连接/订阅只决定当前如何投递。
邀请是独立记录，`pending → accepted` 后才建立正式成员，猜中 ID 不等于取得权限。

| 操作 | owner | admin | member |
|---|---|---|---|
| 发言、读历史/附件、调用已安装 Agent | 可以 | 可以 | 可以 |
| 邀请、移除普通成员 | 可以 | 可以 | 不可以 |
| 授予 admin、移除 admin、修改空间属性 | 可以 | 不可以 | 不可以 |
| 安装 Agent、选择批准的项目工作区 | 可以 | 不可以 | 不可以 |
| 随意提交 Runtime URL、密钥、文件根路径 | 不可以 | 不可以 | 不可以 |

owner 当前不能退出、被移除或降级；所有权转让仍未实现。角色是房间内的角色，不是全平台超级权限。
共享任务的代码执行授权也不能由“我是 admin”直接推出。

**实现落点**：[RoomService](../logic/services/room_service.cpp) 与
[SpaceRepository](../logic/repositories/space_repository.cpp)。房间、owner 成员和会话原子创建；
同一房间的邀请接受、移除和角色变化在事务行锁内判断及写入。

**追问**：“先查 owner，再开事务修改角色”有什么问题？检查和修改之间身份可能变化，形成 TOCTOU。
把相关检查和写入放入同一个受锁保护的事务，才能说明权限判定对应哪一版成员状态。

### 10.2 撤权：为什么不能只隐藏前端按钮

移除 B 的成员关系提交后，新的发送、历史、同步、附件和 Agent 请求都应重新检查权威成员关系。
Comet 对加入和下推资格复查，并周期清理旧订阅；页面收到撤销事件后清掉缓存与当前面板。
服务端拒绝访问不能依赖前端自觉，也不能把 Redis 的在线集合当作成员名单。

这里要讲清**时间边界**：数据库撤销提交后的新授权应拒绝；已经完成授权的在途操作可能结束，
已经交给 socket 的字节无法追回。周期扫描还受 RPC 与扫描耗时影响，不承诺“严格一秒清空所有副本”。
授权服务不可用时拒绝受保护访问，不用旧 presence 放行；失败投递不能推进 delivered 游标。

**源码/测试**：[Comet](../comet/comet_server.cpp)、
[AttachmentService](../logic/services/attachment_service.cpp)、
[空间服务测试](../tests/space_service_test.cpp)。
**追问**：上传者退出后能否继续读原附件？房间对象仍需当前房间资格，上传身份不能绕过成员检查。

### 10.3 同一条群消息，为什么需要多个不同的 ID

| 标识 | 负责的范围 | 不应该拿它代替什么 |
|---|---|---|
| `room_id` / IM `session_id` | 公共消息时间线 | Agent 原生上下文 |
| `client_msg_id` | 一次客户端发送及其重试 | 一次新业务操作 |
| `msg_seq` | 会话内排序与补拉 | 身份授权或已读证明 |
| `agent_id` | 稳定的 Agent 机器身份 | 可修改/可重名的展示名称 |
| `bot_user_id` | 消息中 Agent 的发言身份 | 真实任务发起者 |
| `actor_user_id` | 请求发起人和权限审计 | 正文中“我是管理员”的自述 |
| `request_id` | 一次 Agent 请求及恢复 | 新建第二次工具执行 |
| Runtime Session | Agent 的原生历史与执行上下文 | IM 用户看到的完整消息历史 |

Web 的 @ 选择器提交结构化 `agent_mentions`；通用路由不只解析正文显示名。服务端校验成员、Catalog、
房间安装状态和 generation。普通消息不触发模型，Agent 最终回复默认不会触发另一个 Agent。
旧 `@CANN` 保留自己的兼容协议，不能把两种路由规则混用。

同一源消息调用 Pi、Hermes 时，各自得到确定性请求：

```text
request_id = "ra_" + SHA256(compact_JSON(["room-agent-v1", source_msg_id, agent_id]))
```

相同消息与相同 Agent 重试得到同一 ID，不同 Agent 得到不同 ID；规范序列化避免简单字符串拼接的边界歧义。
哈希只提供稳定标识，不能替代授权和执行账本。源码见
[room_agent_catalog.cpp](../logic/services/room_agent_catalog.cpp)。

同一房间同一 Agent 的不同成员可以共享该 Agent 上下文；不同房间或不同 Agent 要隔离。
绑定授权工作区后，Session namespace 还包含安装/工作区代数和项目标识的派生值，切换项目不复用旧上下文。
具体编码见 [room_workspace.py](../cannbot/scripts/room_workspace.py)，不要把裸 `room_id` 当作完整隔离键。

### 10.4 执行状态、协作进度和测试结果是三回事

Agent 请求可以是 queued、running、completed、failed、cancelled、unknown；协作任务则保存标题、步骤、负责人、
进度和交接摘要。模型生成建议后，成员确认保存为任务；模型说“已经完成”不会自动成为测试通过的证据。

任务更新带 `revision`：B、C 都读到版本 7，B 成功更新成 8，C 用版本 7 更新应冲突，而不是覆盖 B 的结果。
这是数据库条件更新的乐观并发控制，和 CPU 的 CAS 指令有相似思想，但不是同一实现。

```sql
-- 教学示意，不是直接复制的生产 SQL
UPDATE task SET summary = ?, revision = revision + 1
WHERE task_id = ? AND revision = ?;
-- 检查是否真正更新，并在同一事务中保存历史快照。
```

**为什么 unknown 必须单独存在？** 请求可能已进入 Native，工具也可能执行过，但本地没收到结果。
这时新建请求重跑会重复副作用。恢复使用原 request_id/Run 查询；不能确认就保留 unknown。
Runtime 不支持停止或审批时明确返回不可用，不把“取消本地等待”包装成“远程工具已停止”。

**源码**：[任务与请求持久化](../logic/repositories/room_agent_repository.cpp)、
[HermesNativeAdapter](../cannbot/scripts/hermes_native_adapter.py)。
当前制品已验证受控回执丢失后的原 Run 恢复；不要用此结果替代第 6.4 节未重跑的控制专项。

### 10.5 代码读取：路径合法为什么仍不等于有权限

管理员配置允许共享的项目与路径，房主选择项目 ID；普通成员不能提交任意根目录、Runtime 地址和密钥。
Connector 从持久请求取得权威 scope，并在执行时向 Logic 复查当前授权，构建有边界的源码快照。

项目实现使用目录描述符、`O_NOFOLLOW` 等路径防护，拒绝符号链接、硬链接和特殊文件，并限制文件数与字节数。
原生 Pi/Hermes 读取工具只能访问相应快照；公共模式不开放任意 Shell，不继承个人 technical Profile。

**追问链**：禁止 `../` 是否足够？还要考虑链接、检查后替换、目录逃逸和授权失效。
文件名黑名单能否防泄密？不能识别源码里所有硬编码秘密，管理员批准共享内容本身就是信任边界。
工具 allowlist 是否等于 OS 沙箱？不是；Windows 共享盘验收没有证明 OS 只读挂载隔离。

**源码**：[工作区服务](../logic/services/room_workspace_service.cpp)、
[快照与授权](../cannbot/scripts/room_workspace.py)。

### 10.6 发布和备份也是后端工程能力

v0.1.0 的基础 IM 用 Compose 启动，统一入口 9010；Agent 是可选组件。数据库做增量迁移，历史成员与消息保留。
迁移可重入不等于全部 DDL 在一个可回滚事务里；升级前仍要备份、停止旧写入并检查版本兼容。

冷备需要让 IM 卷、Agent 执行账本、Native Session 与授权快照处于可解释的同批状态。
单独备份 MySQL，不能证明“工具已执行、消息尚未确认”的 Agent 请求可以正确恢复。
恢复默认关闭 Agent，核对原身份、镜像和配置后按维护流程启用，未知请求继续查询原执行。

可讲的真实工程问题：Kafka 镜像声明的匿名卷在容器替换后遗留；发行包显式平台字段被备份预检拒绝。
修复保留持久数据卷、只把非持久目录设为 tmpfs，并验证备份镜像平台。发行验收重新从固定候选包执行，
没有把旧候选的通过结果迁移到新版本。案例见
[候选修复回执](benchmarks/2026-11-01-release-candidate-fixes.json)。

**面试收束**：镜像 tag 是可变名字，digest 标识内容；测试必须绑定受检代码、镜像、安装包和实际运行实例。
本项目最终 Release 有维护者补足发布权限的过程，不能把失败的 Actions run 说成全绿。

### 10.7 媒体 worker：从当前实现学习分布式恢复

当前源码 `a3901af` 已提交资料上传、隔离解析、派生物发布和持久 worker，适合学习“长任务如何领取、续租、恢复与拒绝旧写入”。
本阶段用户入口开放文本、PDF/DOCX 和图片；音视频实现和历史回执保留，但相关功能及验收暂缓。
这些变化晚于 v0.1.0 固定发行制品，不能宣称已进入该安装包；当前三用户图文协作证据见 10.9。

| 已有材料 | 可以据此学习什么 | 不能据此推出什么 |
|---|---|---|
| [worker 实现与阶段说明](media-worker.md) | 稳定 claim 身份、120 秒租约、一小时硬期限、恢复原 bundle | 所有 Agent 工具都可以自动重跑 |
| [发布协议](media-publication.md) | 暂存与可读发布分离、完整集合校验、SQL 与文件恢复 | 一个 SQL 事务已经原子覆盖全部文件 IO |
| [worker 阶段回执](benchmarks/2026-11-01-media-worker.json) | 阅读真实解析、响应丢失、进程终止与租约失效的证据 | 本次文档更新重新执行了这些测试 |
| [Agent 上下文接入进度](media-agent-context.md) | 请求代次、首次发布检查、背景筛选、独立执行 namespace、资料 API 与已提交的 Pi/Hermes 原生工具接线 | 换了 namespace 就已验证真实 Native 历史、todo 和媒体理解隔离 |
| [上下文代次阶段回执](benchmarks/2026-11-01-media-context.json) | 阅读真实 SQL 锁竞争、旧结果收养、缺迁移失败关闭的阶段证据 | 已有共享删除 API，或本次文档修改重跑了这些产品测试 |
| [资料 API 与来源回执](room-context-tools.md) | canonical request 授权、固定版本读取、事务复验、读取范围与来源身份 | HTTP 返回 JPEG 就证明 Pi/Hermes 理解了图片，或最终任务已校验引用 |

面试表述可采用：“我阅读或实现了哪些长任务恢复环节，如何将资料来源关联到 Agent 请求，以及哪些真实流程已有证据。”
其中“验证”只填写自己实际执行过的范围。媒体 `ready` 表示解析发布完成，不等于模型理解、任务完成或用户已读。
相关基础题见 Q52～Q53；不要把本节新增的学习材料写成个人已经独立交付的产品能力。

### 10.8 用资料查询练习一次完整的后端面试

**学习情境**：用户让房间 Agent 总结讨论并参考 PDF。面试官追问：“你怎样防止它读到隔壁房间，或者读到撤销前缓存的内容？”
这是一道后端系统题，可以按身份、查询、并发、证据四步回答；模型名称不影响这些边界。

```text
受信 Connector 的 key + 持久 request_id
  → RoomAgentService::AuthorizeContext：取得权威 scope 与消息上界
  → MediaContextService::Execute：限制字段、范围和返回体
  → Repository：按房间/资格查询；固定 revision 读取资料
  → 再授权 → Record 事务复验并保存来源 → 最后授权 → 返回
```

| 源码问题 | 联系的八股 | 练习时要指出的具体边界 |
|---|---|---|
| 为什么不收 actor_user_id、room_id？ | 认证、对象级授权、信任边界 | scope 来自持久 request；请求正文不能更换操作身份 |
| 为什么搜索和按序号读取共用查询？ | 关系查询、分页、接口一致性 | 搜索排除的来源不能通过猜 ID 的直读接口绕过 |
| 为什么先资格筛选再 LIMIT？ | 排序、keyset 分页、查询代价 | 旧 Agent 回复不能挤掉前面的有效人类讨论；limit 不保证扫描少 |
| 读到一半被移出房间怎么办？ | TOCTOU、版本检查、事务锁 | 在返回前重新授权；不能把多次检查说成全链路瞬时撤回 |
| 为什么回执要带范围与 hash？ | 幂等、内容寻址、编码单位 | 读了摘录不等于读了全文；hash 不能替代权限判断 |

源码入口是 [AuthorizeContext](../logic/services/room_workspace_service.cpp)、
[MediaContextService](../logic/services/media_context_service.cpp)、
[MySqlMediaContextRepository](../logic/repositories/media_context_repository.cpp)。
读源码时分别标记“取身份”“查询数据”“决定可否返回”，不要只在 HTTP handler 找鉴权代码。

已有[资料 API 阶段报告](benchmarks/2026-11-01-room-context-tools.json)记录：
CTest 注册 77 项，66 通过、11 跳过；隔离 MySQL/Redis 补跑 6 个测试入口，包含上下文 HTTP 的 11 组场景。
这些数字是该阶段的历史证据，本次文档修改没有重新运行。其聊天和 Agent request 由 SQL 夹具建立，
实际 HTTP/数据库锁竞争、文本解析和 JPEG 读取已验证，Kafka、原生模型和三浏览器资料流程未在该阶段验收。
共享删除、最终答案/任务来源校验不在这份早期报告的验收范围。
[Pi 原生工具](../cannbot/pi/extensions/room-context.ts)与
[Hermes 工具接线](../cannbot/scripts/hermes_public_workspace.py)现已提交；
后续真实 Hermes、来源预览和任务保存的证据见 10.9，不能倒填为这份 API 报告的结果。

**学习交付**：先答 Q59～Q61，再运行第 12.15 节，最后用第 13.9 节接受连续追问。
回答至少包含一个正常路径、一个失败交错和一项证据限制，才能把它写成自己的学习成果。

<a id="document-collaboration"></a>
### 10.9 当前图文协作闭环：从文件上传讲到任务交接

当前源码能力与范围见 [图文协作说明](multimodal-collaboration.md)、
[Web 操作](web-collaboration-client.md) 和 [范围调整回执](benchmarks/2026-11-02-document-scope.json)。
可以使用这一条链路连接项目讲解和八股，先讲用户动作，再讲背后的状态与失败点：

```text
成员上传文本 / PDF / DOCX / 图片
  → init 预留身份与容量 → chunk 记录分片 → finish 校验并入队
  → worker 领取任务、续租、解析 → 固定 revision 的文本/图片发布为 ready
  → 成员选择已安装 Agent，以结构化 mention 发起独立 request
  → Runtime 通过 room_search / room_read / room_read_media 读取授权来源
  → 回答进入同一消息时间线；来源可按 request 回查
  → 用户保存为协作任务，其他成员接手、更新进度与交接摘要
  → 刷新、重登后从消息历史和服务端任务记录恢复
```

| 源码入口 | 适合联系的基础题 | 必须能解释的取舍 |
|---|---|---|
| [RoomMediaService](../logic/services/media_service.cpp) | 分片、幂等键、摘要、数据库事务 | 上传重试复用已确认分片；finish 成功不等于解析或模型任务完成 |
| [MediaProcessingService](../logic/services/media_processing_service.cpp)、[worker](../cannbot/scripts/media_worker.py) | 租约、fencing、崩溃恢复 | 超时后核对原任务状态；旧 worker 不能覆盖新 attempt |
| [MediaContextService](../logic/services/media_context_service.cpp) | SQL 分页、对象授权、TOCTOU | request 决定身份和范围；读取内容期间撤权要在返回前发现 |
| [RoomTaskService](../logic/services/room_task_service.cpp) | 乐观并发控制、持久化、关联关系 | 模型输出只是一份草稿；保存、接手和更新由服务端记录与 revision 控制 |
| [room_media.js](../web_demo/static/room_media.js) | 异步 UI、对象生命周期、资源释放 | 切换房间后旧请求结果不能覆盖新视图；关闭预览应回收 Object URL |

现有真实证据分别回答不同问题：

- [PDF 网页验收](benchmarks/2026-11-02-web-media-analysis.json)：7 组真实浏览器检查，覆盖三用户、Hermes 的 PDF 与授权代码分析、图片来源预览、任务保存/接手/交接、整栈重启后的新浏览器恢复及撤权。
- [来源与任务关联验收](benchmarks/2026-11-01-source-links.json)：真实 HTTP/MySQL/Redis 13 组检查；模型消息由夹具建立，不能计作真实模型验收。
- [源码整理提交](https://github.com/specetator/grpc_aiagent/commit/a3901af17ae4af65f6a3d37070df400120447885)：定向 C++ 构建、1 项服务合同、6 项真实数据库/HTTP 集成、29 项模拟 HTTP/WS 浏览器检查通过。

Pi 与 Hermes 的通用房间接入已有独立证据，但 Pi 的历史图像误判仍保留；本节成功的真实图像理解案例是 Hermes。
文档/图片已开放，音视频暂缓；共享资料删除、更广泛的语义准确性及全量 MVP V1.0 验收不从上述局部结果推断。

**90 秒回答练习**：前 20 秒讲用户流程，中间 40 秒选“断线续传”或“成员撤权”画出状态，
最后 30 秒说明任务为何持久、哪份验收支持结论，以及你个人实际做了哪一部分。

<a id="interview-foundations"></a>
## 11. 基础知识：从八股题回答到项目追问

每题先用 30～60 秒回答原理，再用项目举例，最后接受一个反例。涉及当前实现的结论都要回到源码；
“换成更大规模时如何设计”属于扩展，不代表项目已经拥有该能力。

### 11.1 C++：对象、资源、泛型与并发

**Q01：栈、堆、对象生命周期和 RAII 有什么关系？**

栈上局部对象通常随作用域退出而析构，动态对象的存活由其所有者决定。RAII 管理的是资源生命周期，
资源可以是堆内存、锁、socket、数据库连接，不是“所有对象都放栈上”。项目连接池 guard 离开作用域归还连接，
避免多个异常/return 分支逐个释放。追问：进程被强制终止时析构不一定执行，所以可靠恢复不能只靠 RAII。

**Q02：`shared_ptr` 的“线程安全”到底是哪一部分？**

不同 `shared_ptr` 实例共享控制块时，引用计数管理可以并发；同一个普通指针变量的读写、以及对象内部状态仍需同步。
回调捕获 `shared_ptr` 能延长生命，不会自动让 `TcpConnection` 的跨线程修改合法；仍要回到 owner loop。
`weak_ptr::lock()` 一次完成存活检查与取得所有权。追问：捕获裸 `this` 时，回调运行前对象已析构会怎样？

**Q03：`std::move` 是否一定移动？为什么移动构造常标 `noexcept`？**

`std::move` 只是产生可用于移动重载的表达式；实际是否移动取决于类型、const 和重载。
容器搬迁元素时，为维持异常保证，可能选择复制而非可能抛异常的移动。项目任务出队把闭包转移到局部再解锁执行。
追问：不要从已移动对象继续取旧业务值；只按该类型对移动后状态的合同使用。

**Q04：虚函数、虚析构、模板，各解决什么问题？**

虚函数支持运行时多态；常见 ABI 用虚表实现，但“对象一定多一个特定大小的指针”不是跨平台标准保证。
通过基类指针删除派生对象时，普通多态所有权设计应使用虚析构；也可禁止经基类析构。
模板在编译期生成类型相关代码，适合静态组合，但可能增加编译和代码体积。追问：Service 注入 Repository 接口带来什么测试替换点？

**Q05：lambda 按引用捕获为什么在异步任务里危险？**

闭包排队不意味着它引用的局部变量会一起存活。函数返回后，引用捕获的字符串、栈对象或裸指针可能悬空。
按值捕获拥有型数据或合适的智能指针，并明确取消和回调回收顺序。`std::function` 还可能涉及类型擦除和分配成本。
追问：C++17 的 `std::function` 要求可复制目标，捕获 `unique_ptr` 的不可复制闭包不能直接照搬进去。

**Q06：数据竞争、原子性和内存序怎样区分？**

并发冲突访问中至少一方非原子、又缺少适当 happens-before，可能构成数据竞争并导致未定义行为。
单个 atomic 自增不保证“检查队列容量 + 入队 + 更新字节计数”这组操作原子；此处使用同一把 mutex。
release/acquire 用于发布和观察关联数据，relaxed 适合不承担发布协议的独立计数。不要用 volatile 代替线程同步。
原理参考 [C++ 数据竞争规则](https://eel.is/c++draft/intro.races)，项目示例见第 5.4 节。

**Q07：条件变量会丢通知吗？为什么不能只写 `if`？**

条件变量不保存业务事件，业务状态保存在共享谓词中。生产者在锁内更新谓词，等待者带谓词 wait；
即使通知先到，之后检查到条件成立也不用睡眠。唤醒可能虚假，也可能资源先被其他 worker 取走，因此需要循环检查。
追问：停止时 notify_all 只负责唤醒；线程退出还需要正确的停止条件与 join。

**Q08：容器复杂度背后有哪些工程代价？**

vector 连续存储、遍历局部性好，但扩容使引用/指针/迭代器失效；unordered_map 平均 O(1)，有 rehash 和碰撞退化；
map 有序、操作 O(log N)，代价包括节点分配与指针跳转。近期去重用 set + deque，分别负责查重和有界淘汰。
追问：不要把所有队列都称作 LRU；本项目按插入顺序淘汰的窗口是 FIFO。详见 4.8、5.6。

### 11.2 操作系统：线程调度、内存和排障

**Q09：线程为什么比进程轻？线程多了为什么仍会变慢？**

同一进程的线程共享地址空间与文件描述符等资源，各有栈和执行上下文；进程通常有独立地址空间。
线程仍需调度，切换会带来寄存器保存、缓存局部性变化等成本。线程数超过有效并行度后，可能增加竞争和排队。
项目的会话锁若串行等待 SQL/Kafka，多加 worker 只会增加等待者；应先测 CPU、阻塞点和队列，再调线程数。

**Q10：死锁的四个条件是什么？如何落实到代码？**

互斥、持有并等待、不可抢占、循环等待同时成立时可能死锁。工程中规定锁顺序、缩短临界区，避免持锁调用未知回调。
超时有助于退出等待，但不是所有状态都能安全重试，也不能替代一致性设计。
项目练习：画出 map 锁、会话锁、池锁与 SQL 行锁的获取顺序；区分 C++ 互斥死锁和数据库事务死锁。

**Q11：RSS 一直涨，就一定是内存泄漏吗？**

也可能是输出缓冲积压、队列无界、缓存未淘汰、分配器保留内存或真实泄漏。先按连接数、消息字节、队列长度解释增长，
再用采样/堆分析验证；容器内存、进程 RSS、文件页缓存不是一个指标。
虚拟内存大小也不等于物理内存占用；缺页可能只是建立映射，并非每次都读磁盘。
追问：队列只限制条数，为什么大附件仍能耗尽内存？对应 4.11、4.17。

### 11.3 网络：从 TCP 到业务确认

**Q12：TCP 可靠，为什么还要业务 ACK、幂等和重试？**

TCP 管理连接内字节的传输；一次 write 成功不代表对端业务已处理、Kafka 已确认或 MySQL 已提交。
断线后客户端也可能不知道上一次业务是否成功，因此业务请求需要稳定身份，重试必须能识别原请求。
项目 accepted_ack 确认 Kafka 持久化事件，delivered_ack 确认目标连接接纳，两者都不是已读。详见 2.2。

**Q13：粘包、半包、WebSocket 消息边界怎样讲？**

TCP 是字节流，应用写入边界与接收边界不一一对应；协议解析器必须保存未完成状态。
HTTP 有报文规则，WebSocket 有帧和消息分片规则，SSE 也需跨字节块拼出完整事件。
不能假定一个 recv 是一条 JSON，也不能把一个 WebSocket 帧永远当作完整消息。
参考 [WebSocket RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455#section-5)，项目见 4.15、4.18。

**Q14：三次握手、四次挥手、TIME_WAIT 的回答怎么落地？**

握手建立双方连接状态并同步初始序号，处理延迟到达的旧连接请求；关闭则要分别结束两个方向，报文可能合并。
主动关闭方通常进入 TIME_WAIT，用于处理旧报文存活和必要的终止确认重传。
不要把大量 TIME_WAIT 一律当作泄漏，先看连接 churn、端口四元组、主动关闭方和长连接复用。
本项目面向长连接，重点还包括心跳、半开连接、失效路由和重连后的状态恢复。

**Q15：select/poll/epoll、LT/ET、Reactor 有什么关系？**

它们解决“哪些 fd 当前可读写”的等待和通知问题；Reactor 是围绕就绪事件分派回调的组织方式。
epoll 维护关注集合与就绪集合，避免每次由用户传递并遍历完整监听集合，但处理活跃连接仍有实际工作量，不能笼统称全流程 O(1)。
LT 在就绪状态存在时可反复通知；ET 常配合非阻塞 IO，处理至 EAGAIN，并控制公平性以免单连接霸占线程。
本仓库 Muduo 的 Channel 事件没有附加 `EPOLLET`，默认按 LT 使用；面试不要把“知道 ET”说成“项目用了 ET”。
源码：[Channel.cc](../muduo/net/Channel.cc)、[EPollPoller.cc](../muduo/net/poller/EPollPoller.cc)；
原理参考 [epoll 手册](https://man7.org/linux/man-pages/man7/epoll.7.html)。

**Q16：socket 可写时为什么还会发生部分写？怎样处理慢连接？**

就绪只表明当时可能进行 IO，不承诺一次写完整个缓冲区。记录已发送位置，剩余内容保存在有界缓冲区，等待后续可写事件。
不要始终监听可写事件形成空转，也不要用不断积累内存掩盖慢客户端。超过预算时要拒绝、断开或降级，并保留可靠历史恢复路径。
项目落点是 Comet 输出预算、owner loop 和最终消息补拉，而不是无限增大 socket 缓冲。

**Q17：gRPC deadline、超时和取消是不是同一回事？**

timeout 是等待时长，deadline 是截止时刻；跨多个步骤时应传递剩余预算，避免每层重新给满时间造成请求超期。
调用方停止等待不表示服务端已经回滚；取消需要被工作线程与 Runtime 协作观察，已完成的副作用仍要处理。
项目的 pending 表关联 request_id 与回执，迟到结果不能误归给下一次调用。参考
[gRPC deadline 指南](https://grpc.io/docs/guides/deadlines/)，具体实现见 4.7、4.21。

### 11.4 MySQL：索引、隔离与持久化

**Q18：B+ 树、联合索引和最左前缀如何回答？**

B+ 树用较高扇出降低查找层数，叶子有序便于范围扫描；InnoDB 聚簇索引叶子保存行，二级索引通常带主键以回表。
项目历史查询以 `session_id` 等值、`seq` 范围和排序为主，联合索引应围绕这个访问路径设计。
覆盖索引能减少回表，但把大正文塞进索引会增加体积和写成本。应结合真实 SQL、数据分布与 EXPLAIN 判断。
追问：有索引不等于一定走索引；函数、隐式转换、选择性和返回量都可能改变计划。

**Q19：为什么历史分页用 `seq < anchor`，而不是很大的 OFFSET？**

OFFSET 越深，通常需要扫描并跳过更多符合条件的记录；keyset 从锚点继续范围读取，更适合连续翻页。
锚点要有稳定且足够唯一的排序定义；本项目用会话内序号，并区分向前历史与向后增量同步。
并发新消息不会因为插到前面就使旧页整体位移，但权限、已删除记录和未落库缺口仍要单独处理。
追问：keyset 不擅长随意跳到第 N 页；它解决的是顺序翻页，不是所有分页产品需求。

**Q20：MVCC 和 `SELECT ... FOR UPDATE` 分别做什么？**

MVCC 让一致性读按版本可见性规则读取历史版本，减少读写互相阻塞。InnoDB RR 的普通一致性读通常复用事务快照，
RC 的一致性读每次建立新快照；锁定读有不同语义，不能把所有 SELECT 都当成同一种读。
取号需要“读最新值并互斥更新”，所以项目用事务和行锁，不能把普通快照读的结果直接当成独占序号。
参考 [MySQL 隔离级别](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html)与
[锁定读](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html)。

**Q21：行锁真的只锁一行吗？死锁后怎样重试？**

InnoDB 的锁与索引访问有关；唯一键等值、范围扫描和隔离级别会影响记录锁/间隙等范围，不能仅看 SQL 返回行数。
项目按房间行、会话序号行组织竞争，仍要缩小事务、统一访问顺序并处理数据库错误。
死锁受害事务回滚后，应按业务合同重试整个事务；先确保重试沿用幂等身份，不能只补执行最后一条 UPDATE。
追问：为什么 `SELECT MAX(seq)+1` 后直接 INSERT 不够？两个请求可能读到同一 MAX。

**Q22：undo、redo、binlog 与项目 Outbox 是什么关系？**

undo 支持回滚和版本读取，redo 支持 InnoDB 崩溃恢复，binlog 记录用于复制等用途的逻辑变更。
这些是数据库内部与服务层机制；项目 Outbox 是应用表中的待发布事件，用来弥合 SQL 与 Kafka 的提交边界。
SQL 提交成功并不表示 Kafka 已接收，Kafka 确认也不表示 Job 已写历史；因此需要可恢复状态机和幂等消费。
追问：恢复数据卷不等于已验证所有存储故障；持久性仍受部署、刷盘、备份与损坏模型约束。

**Q23：预处理能解决所有数据库安全问题吗？**

参数绑定把值与 SQL 语法分开，名字中有单引号也应作为数据处理；不能靠“去掉危险字符”代替绑定。
表名、排序字段等 SQL 标识符不能一般地用值参数替代，应使用固定代码或白名单。预处理不负责业务授权。
项目修复了房间创建的输入拼接，并把权限规则留在 Service、SQL 留在 Repository。
追问：防了注入，为什么普通成员仍不能请求把自己的 role 改成 owner？这是另一层授权问题。

### 11.5 Redis：缓存、路由和原子操作

**Q24：Redis 单线程，为什么客户端还会有并发问题？**

Redis 命令执行通常按主线程串行组织，但网络 IO、持久化等还涉及其他线程/进程；也不能把所有版本和模块一概而论。
两个客户端执行“GET → 判断 → DEL”是多条命令，可能交错。项目旧连接关闭时需要比较当前路由身份/代数后删除，
避免删除新连接刚注册的路由；原子脚本负责把判断与删除作为一个步骤。详见 4.13。

**Q25：缓存穿透、击穿、雪崩如何区分？本项目怎么用这些知识？**

穿透是反复查询不存在对象，击穿是热点失效时大量请求同时回源，雪崩是大量缓存集中失效或缓存服务故障。
常见办法包括输入校验、短期负缓存、合并回源、过期抖动与限流；每项都要考虑陈旧数据和误拒绝成本。
这些是扩展设计，不能默认写成项目已实现。对成员权限更要先讲正确性：缓存过期或 Redis 故障不能自动授予访问。

**Q26：TTL、淘汰策略、持久化是否可以互相替代？**

TTL 是键的有效期；内存淘汰是在容量压力下选择清除哪些键；RDB/AOF 则属于重启恢复机制。
缓存可重建不意味着 token、路由与限流状态随意丢失也无影响，要逐类解释故障行为。
Redis 的 LRU 实现是近似选择，不能当作自己手写的精确 LRU 链表。参考
[Redis 淘汰策略](https://redis.io/docs/latest/develop/reference/eviction/)。项目正式成员与历史权威仍在 MySQL。

### 11.6 Kafka 与分布式可靠性

**Q27：Kafka 的顺序保证到哪里？一个分区能被多个组消费吗？**

顺序围绕 partition 内日志建立，不是跨所有分区的全局顺序。同一消费组内一个分区通常由一个消费者负责，
不同组可以各自消费该分区，维护各自进度。把相关事件放到同一分区有助于局部顺序，但应用并发处理仍会改变完成顺序。
项目还需要 msg_seq、会话提交锁和客户端重排；扩展多实例时不能把进程内 mutex 当成跨实例发布屏障。

**Q28：`acks=all`、幂等 producer、业务幂等各解决什么？**

`acks=all` 约束当前 ISR 的确认；实际耐久性还取决于副本、min ISR、故障和存储配置，单副本不是多副本容灾。
producer 幂等减少其协议内重试产生的重复，不代表跨 SQL、WebSocket 或外部工具自动 exactly-once。
客户端重提业务和服务重启后的重放，仍需要稳定业务 ID、数据库约束与执行账本。源码见
[KafkaProducer](../common/kafka_producer.cpp)，语义参考 [Kafka 3.7 设计](https://kafka.apache.org/37/design/design/)。

**Q29：先提交 offset 还是先做业务？**

先提交再执行业务，进程崩溃可能漏处理；先执行业务再提交，崩溃可能重复处理。项目选择可重试与幂等收敛，
业务完成后才确认相应消费进度。并发处理时，不能越过未完成的低 offset 直接提交高位置。
追问：低位置是长任务，高位置是停止命令怎么办？控制可提前处理，但提交仍遵循连续完成前缀，详见 4.21。

**Q30：Outbox 为什么不是跨系统原子事务？**

本地事务只能原子保存它所在数据库内的数据与待发事件，发往 Kafka 的动作仍可能成功但回执丢失。
Outbox 通过持久待办、重试和下游幂等使状态收敛，不消除所有重复或提供即时一致性。
面试画两个窗口：SQL 已提交/Kafka 未发；Kafka 已确认/SQL 阶段未更新。分别说明原事件如何重发、谁去重。
Agent 再加一层：工具有副作用且结果未知时，不一定能安全重发执行，应查询原 Run。

**Q31：CAP、最终一致性和幂等可以怎样联系项目？**

CAP 讨论出现网络分区时强一致性与可用性的取舍，不是给三个组件贴标签，也不是“平时只能选两个”。
项目成员授权查询失败时拒绝受保护访问；消息历史允许异步落库，用状态和游标表达尚未完成的部分。
幂等约束重复操作的效果，不自动保证顺序、原子性或新鲜度。回答时先说具体操作要求，别给整个系统草率标一个 CP/AP。

**Q32：重试、去重、超时、取消、补偿怎么区分？**

重试再次尝试同一操作；去重识别已处理身份；超时表示等待预算用尽；取消请求停止后续工作；补偿用新操作处理已发生效果。
给请求换新 ID 是新业务，不是幂等重试。取消成功也不等于已发出的邮件、已写文件全部回滚。
项目把 unknown 留给不可确认执行，完成结果可重复投递到消息链路，但不会因此重新运行工具。

### 11.7 不依赖项目也应会答的补充题

这些题不必硬塞进简历，但面试可能从项目顺着问下去。先独立回答，再找项目里是否真的存在对应机制。

| 题目 | 至少应答到 | 与项目的连接 |
|---|---|---|
| malloc/free 与 new/delete | 分配存储、构造/析构、配对方式、失败处理 | RAII 包装与异常路径；不能混用释放函数 |
| 重载、重写、隐藏 | 选择发生阶段、签名与作用域、override 检查 | C++ 服务接口与实现 |
| 左值/右值、引用折叠、完美转发 | 值类别、模板推导与 std::forward；不要把 move 当作“立即搬内存” | 任务/资源封装；生产代码没有用就不强行认领 |
| static、extern、链接与 ODR | 存储期、链接属性、声明/定义、重复定义 | 头文件与库边界，常量和全局对象初始化 |
| TCP 流量控制与拥塞控制 | 接收方窗口与网络拥塞窗口的不同目标 | 不替代应用队列背压 |
| HTTP 缓存、TLS 与 WebSocket | HTTP 语义、传输保护、升级后的帧协议 | 统一入口和内部 RPC TLS；不宣称自研密码协议 |
| 乐观锁与悲观锁 | 版本冲突重试 vs 加锁互斥；冲突率与临界区 | 任务 revision vs 房间/取号行锁 |
| 布隆过滤器 | 可有误报、正常用法下无假阴性、删除需额外结构 | 可讨论穿透防护，未据此实现成员授权 |
| LRU、堆、滑动窗口 | 数据结构、复杂度、边界与容量限制 | 第 12 节手写；与已实现 FIFO 去重区分 |

<a id="interview-followups"></a>
### 11.8 六组连续追问：从会背答案到能解释代码

每组用 5～8 分钟。先只读问题，写出判断和依据，再展开回答核对。下面的推演和练习不等于新增的实测结果。

#### A. “你的消息可靠，那为什么用户还会看到发送失败？”

**连续追问**：TCP 已保证可靠，为什么还要 ACK？返回 503 是否意味着没写入？ACK 丢了能否重新生成 ID？
消息只展示一次，能否证明服务端只执行一次？

<details>
<summary>回答要点与源码核对</summary>

先区分“调用方知道什么”和“服务端做到了哪一步”。`accepted_ack` 是持久化事件得到 Kafka 确认的业务证明，
而调用超时或失败响应可能发生在副作用之后。因此收到成功 ACK 可以作出限定结论，没有收到 ACK 不能反推从未受理。

| 观察到的现象 | 服务端可能的状态 | 正确处理 |
|---|---|---|
| 客户端断线，尚未收到 ACK | 未受理、已预留、已进入 Kafka 都可能 | 沿用原 client_msg_id 与原正文重试，查询原身份 |
| Kafka 等待超时 | broker 未接收，或已接收但确认未到 | 保留 prepared Outbox，由重试与幂等消费收敛 |
| Kafka 已确认，Published 更新失败 | 事件已存在，SQL 阶段仍待恢复 | 不能认定“什么都没发生”；同事件重投可能出现 |
| accepted 已到但历史暂时没有 | Job 尚未完成异步入库 | 检查消费与 SQL；客户端保持有界补偿机制 |
| 前端最终只显示一条 | 可能只是前端去重成功 | 同时核对 reservation、唯一历史行；Agent 还要核对工具执行账本 |

当前去重约束含 `(session_id, sender_id, client_msg_id)`，不能只在进程内保存一个布尔值。
同 ID 不同正文必须按原文一致性规则拒绝，不能悄悄替换历史；换 ID 则是新的业务发送。
`PersistToTopic` 的返回还受 `Published` 更新结果影响，这也是“没有成功响应不等于没有执行”的具体例子。

源码：[PersistToTopic / Submit](../logic/services/message_service.cpp)、
[原文与 Outbox](../logic/message_reservation.cpp)。
**练习产物**：画出上表每个失败点对应的重试箭头，并指出 SQL 唯一约束与客户端去重各自保护什么。

</details>

#### B. “已经用了 RAII 和智能指针，为什么仍可能访问失效的 mutex？”

**连续追问**：weak_ptr 为什么适合锁表？unique_lock 是否拥有 mutex 的生命周期？下面代码能否合并成一行？
会话互斥为什么还不能代替数据库行锁？

```cpp
// 假设 keys 是当前项目的 sparkpush::KeyedMutex。
auto mutex_owner = keys.Get("room-42");
std::unique_lock<std::mutex> guard(*mutex_owner);
// 在 guard 的生命周期内，mutex_owner 必须继续有效。
```

<details>
<summary>回答要点与错误写法</summary>

锁表的 weak_ptr 不延长 mutex 生命周期，等待者/持锁者保存的 shared_ptr 才负责存活。
`unique_lock` 管理加锁与解锁，不管理被引用 mutex 的分配和销毁。
上例先声明 owner、后声明 guard，退出作用域时 guard 先解锁，owner 随后释放所有权。

不能简化成 `std::unique_lock<std::mutex> guard(*keys.Get("room-42"));`：
临时 shared_ptr 在该完整表达式结束时销毁；若没有其他所有者，mutex 此时也被销毁，guard 随后访问它就不再合法。
这道题同时考资源生命周期、临时对象、析构顺序和“RAII 管的是哪一个资源”。

`KeyedMutex::Get` 还必须在表锁内完成查找、weak.lock 和创建，否则同键可能并发创建两把业务锁。
表锁只保护索引，业务锁保护同一进程内的会话提交；跨 Logic 实例取号仍依赖数据库事务与约束。
进一步扩展多实例发布顺序，需要独立设计跨实例串行策略，不能把已有 mutex 的作用范围放大。

源码：[keyed_mutex.h](../common/keyed_mutex.h)、
[Submit 中 submission_mutex 与 submission_order](../logic/services/message_service.cpp)。
原理：[unique_lock 的生命周期要求](https://eel.is/c++draft/thread.lock.unique)。
**练习产物**：画出锁表、owner、guard 三者的持有关系；写出两次同键 Get 返回同一个存活锁的断言。

</details>

#### C. “线程池最多排队 32 MiB，进程为什么还能占用更多内存？”

**连续追问**：限制条数和字节有何区别？worker 出队后任务是否释放？增加 worker 是否一定改善延迟？
Stop(false) 能不能立即打断执行中的 RPC？

<details>
<summary>回答要点与容量推演</summary>

`queued_bytes_` 只统计还在队列中的提交权重，在 `WorkerLoop` 取任务时扣除。
执行中的闭包、连接输出缓冲、数据库结果、线程栈和分配器开销都不包含在这个数中。
权重还由调用方提供，并不等于对堆占用做了准确测量。

**教学假设**：有 4 个 worker，每个正在执行的任务持有 1 MiB 数据；队列又存了 32 MiB。
仅这些任务数据就可能达到 36 MiB，尚未计算其他内存。这不是项目实测值，而是用来检查预算定义的反例。

队列有界解决准入和积压，不保证吞吐。若任务都等待同一个会话锁，增加线程可能只增加等待者。
应把端到端时间拆成排队、锁等待、连接池等待、SQL、Kafka 等阶段测量，再决定缩短临界区还是增加并行度。
`Stop(false)` 清掉未执行队列后仍要 join；已经执行的阻塞操作依靠自己的 deadline/取消协议退出。

源码：[ThreadPool::Submit / WorkerLoop / Stop](../common/thread_pool.cpp)、
[有界队列用例](../tests/queue_capacity_test.cpp)。
**练习产物**：用 promise/future 阻塞一个 worker，确定其已开始后再填满队列；释放 worker 后验证 drain 与拒绝行为。
不要靠任意 sleep 假设线程恰好已执行到某一行。

</details>

#### D. “历史查询有索引，为什么还可能慢？”

**连续追问**：联合索引怎么排字段？只查 seq 与查正文有什么差别？有索引是否不需要排序？
OFFSET 分页和锚点分页怎样做公平比较？

<details>
<summary>回答要点与 SQL 阅读任务</summary>

当前 `message` 有唯一索引 `(session_id, msg_seq)`，主键是 `id`。
先固定 session，再按 seq 范围读取，符合历史访问模式。只选择索引已包含的字段有机会覆盖查询，
而读取 `content_json` 等字段通常还要取得完整行；大正文的传输、反序列化和前端渲染也可能成为瓶颈。

```sql
-- 在独立练习库中替换为真实会话与锚点；这是查询形状示例。
EXPLAIN
SELECT msg_seq, sender_id, content_json
FROM message
WHERE session_id = 'room_42' AND msg_seq < 100000
ORDER BY msg_seq DESC
LIMIT 50;
```

检查实际选择的 key、访问方式、估算扫描量、是否额外排序，再看真实耗时；不要只看到 possible_keys 就说索引已命中。
小表可能让优化器选择不同计划，查询语句中有 ORDER BY 也不代表必然出现 filesort。
比较深分页时，两个查询应读取同一批消息、同样的列和页长，并固定数据、冷热缓存等条件。
`EXPLAIN ANALYZE` 会实际执行受支持的查询，需与只看计划的 EXPLAIN 区分。

源码：[表与索引](../sql/schema.sql)、[历史/同步查询](../logic/message_dao.cpp)。
原理：[MySQL EXPLAIN](https://dev.mysql.com/doc/refman/8.0/en/explain.html)。
**练习产物**：保存自己的计划与数据规模，解释索引扫描、回表、序列化三个阶段；不要把上面的 SQL 示例当作已跑出的性能结论。

</details>

#### E. “有登录校验和角色表，为什么还会越权或丢更新？”

**连续追问**：B 已被移除，但旧 WS 仍连着，能否读取附件？正文里填 owner 是否有效？
B、C 同时修改任务，最后写入覆盖前者算不算正常？可以通过 Redis 缓存加速权限查询吗？

<details>
<summary>回答要点与两个不同的问题</summary>

认证证明用户身份，授权判断该用户此刻能否操作这个房间/对象；WS 在线只说明有投递通道。
因此历史、附件、Agent 调用与下推资格都要依据服务端成员关系判断，不能让上传身份、旧订阅或模型自述授予权限。
缓存设计需要明确撤权如何失效、查询失败如何处理；不能用“过一会儿会过期”冒充立即撤权保证。

任务修改还需要并发控制。两人读到 revision=7，先提交者成功写到 8；后提交者应看到冲突，
重新读取并由用户决定合并，不应静默拿新 revision 覆盖。授权检查与 revision 校验承担不同职责，缺一不可。
成员角色修改则需把受保护状态的检查与写入放入合适的事务和锁范围，避免检查后身份变化的竞态。

源码：[RoomService](../logic/services/room_service.cpp)、
[成员事务](../logic/repositories/space_repository.cpp)、
[附件授权](../logic/services/attachment_service.cpp)、
[任务 Repository](../logic/repositories/room_agent_repository.cpp)。
**练习产物**：分别写“移除后新请求拒绝”和“双人同版本更新冲突”用例；说明已发送字节不能追回的边界。

</details>

#### F. “Agent 超时了，消息有幂等键，为什么还不能放心再执行一次？”

**连续追问**：消息去重能否保证工具只运行一次？unknown 和 failed 有何区别？
换一个 request_id 重试会怎样？Pi 与 Hermes 为什么不能共用一个原生 Session？

<details>
<summary>回答要点与身份表练习</summary>

消息表的唯一约束只能约束消息身份，不能自动约束外部文件写入或其他工具副作用。
Runtime 已接收但本地没有终态时，unknown 表示证据不足；此时应使用原 request/Run 查询和恢复，
不能直接换 ID 新建执行。明确失败也需按 Runtime 的失败语义处理，不能一概推断“失败前没有任何副作用”。

房间时间线是所有成员共同看到的消息；原生 Session 是某个 Agent 的上下文与执行状态。
路由要保留 room、agent、actor、request 的不同身份，并按房间/Agent/授权代数隔离原生会话。
最终回复重投与模型工具重新执行是两种动作：前者可以从已保存结果恢复，后者必须遵守执行账本合同。

源码：[Router](../cannbot/scripts/agent_router.py)、
[Native Adapter](../cannbot/scripts/hermes_native_adapter.py)、
[Session 与工作区隔离](../cannbot/scripts/room_workspace.py)。
**练习产物**：为“A 在房间 1 同时 @Pi/@Hermes，B 在房间 2 @Pi”列出三组身份，
再给其中一次请求重投，指出哪些字段必须不变；用原工具记录而非聊天气泡数判断是否重复执行。

</details>


<a id="cpp-engineering-interview"></a>
### 11.9 C++ 工程题：从“知道语法”到“能解释构建和运行”

这组题补充 Q01～Q08 的工程细节。先给简短结论，再选择一个分支展开，不要在面试中一次背完所有规则。

**Q33：头文件能找到，为什么还会出现 undefined reference？**

**一分钟回答**：声明让编译器知道函数类型，定义提供实现。包含头文件不代表实现文件已经编译并进入最终链接。
先分清报错发生在预处理、编译、链接还是程序加载阶段，再检查源文件、目标依赖和符号签名。

| 现象 | 首先检查 | 本项目的阅读入口 |
|---|---|---|
| 找不到 `*.pb.h` | protoc 是否生成、生成目录是否进入 include 路径、生成步骤依赖是否建立 | [proto/CMakeLists.txt](../proto/CMakeLists.txt) |
| 方法声明存在，但链接缺符号 | 对应 `.cpp` 是否加入目标；命名空间、参数、const 限定是否一致；依赖库是否链接 | [logic/CMakeLists.txt](../logic/CMakeLists.txt) |
| 多个 `.o` 都定义同一非 inline 函数 | 是否把具有外部链接的普通函数定义放进了头文件 | 声明/定义分离与 ODR |
| 编译通过，运行时找不到共享库 | 实际部署的依赖、加载路径及 ABI 是否匹配 | 构建环境与发行镜像的差异 |

**追问**：头文件加 `#pragma once` 能解决跨 `.cpp` 的重复定义吗？不能，它避免同一翻译单元重复包含，
不负责合并多个翻译单元的普通外部定义。`inline` 涉及符合 ODR 条件的多定义规则，不承诺机器码一定内联。

**回到项目**：`spark_push_logic` 是静态库，`logic_server` 链接它；`spark_push_proto` 导出生成头文件路径和依赖。
CMake 的 `PUBLIC/PRIVATE/INTERFACE` 表达目标依赖与使用要求的传播，不是 C++ 类成员的访问权限。
静态库最终仍需由可执行文件解决链接依赖，不要将 `PRIVATE` 简化成“这个依赖绝不会出现在下游链接命令中”。
原理见 [GCC 编译阶段](https://gcc.gnu.org/onlinedocs/gcc/Overall-Options.html)与
[CMake 目标链接规则](https://cmake.org/cmake/help/latest/command/target_link_libraries.html)。

**学习产物**：在 `cmake --build build-wsl --target logic_server --verbose` 的输出中指出编译命令和链接命令。
增量构建无动作时不会重新打印这些命令；查看已有构建记录，或使用自己的独立练习构建目录，不必清理当前工作树。

**Q34：线程池接受 `std::function<void()>`，是不是任何 lambda 都能提交？**

**一分钟回答**：不能。C++17 的 `std::function` 对保存的目标有可复制要求。
lambda 用初始化捕获接管 `unique_ptr` 后通常只可移动，即使写 `std::move(lambda)`，也不会让这个类型变得可复制。
这是封装接口的约束，不是 lambda 语法不支持独占所有权。参见
[std::function 构造要求](https://eel.is/c++draft/func.wrap.func.con)。

**回到项目**：[ThreadPool::Submit](../common/thread_pool.h) 的参数就是 `std::function<void()>`。
传入闭包时要同时回答三个问题：它能否被该接口保存、引用的数据能活多久、执行时由哪个线程同步访问。
`std::move` 闭包只处理转移表达式，不会自动修复捕获的裸 `this`、引用或 view。

**追问**：改捕获 `shared_ptr` 就行了吗？类型上可以满足复制要求，但业务上可能无意延长对象生命，甚至形成引用环。
要先决定任务应当拥有资源，还是只在资源仍存在时执行。若确实需要只可移动任务，应另行设计相应任务封装，
同时评估取消与销毁语义；本项目 C++17 接口没有因此自动获得 C++23 的 `std::move_only_function`。
动手部分见 12.8-A/B。

**Q35：`string_view`、迭代器和引用都能减少复制，为什么不能随意跨异步边界？**

**一分钟回答**：它们通常不拥有底层对象。复制 view 或捕获引用，只复制访问位置，不延长被访问存储的生命。
还要考虑对象活着但内部存储已被修改或搬迁；例如 vector 扩容会使指向元素的指针、引用和迭代器失效。
参见 [string_view 定义](https://eel.is/c++draft/string.view.template)与
[vector 修改操作](https://eel.is/c++draft/vector.modifiers)。

**反例推演**：请求函数构造局部字符串，把指向它的 view 放进队列，随后函数返回。
worker 再运行时，局部字符串已销毁；“闭包按值捕获了 view”仍然不能救回字符串。
另一个陷阱是原始字符串还在，但处理下一帧时缓冲区被复用，任务读到另一条消息。

**回到项目**：阅读 Comet 排队闭包时，逐个标注“拥有值、共享所有权、弱观察、借用”。
需要独立快照就保存拥有型字符串；需要共享大数据则建立明确的不变性和所有权合同。
`string_view` 也不保证以 NUL 终止，不能只把 `data()` 当作任意 C 字符串 API 的完整输入。
这是一项代码审查练习，不是断言当前 Comet 存在上述缺陷。

**Q36：有了移动语义，为什么 `return std::move(local)` 未必更快？**

**一分钟回答**：返回同类型局部对象时，编译器可能直接在调用方结果位置构造，即 NRVO。
写成 `std::move(local)` 会失去这一具名对象的 NRVO 条件；不能靠多写 move 判断优化。
在 C++17 中，`return T(...);` 的同类型纯右值直接构造与可选 NRVO 也要分开解释。
原理入口：[复制消除规则](https://eel.is/c++draft/class.copy.elision)。

`const T` 经 `std::move` 得到的仍带 const；常见的 `T(T&&)` 不能接管它，若有 `T(const T&)`，可能选择复制。
对不可复制类型则可能直接编译失败。`noexcept` 要与实现一致，不能为让容器更愿意移动而作出不成立的承诺。
容器移动旧元素可能抛异常且没有复制可用时，某些操作的异常保证会减弱；不要笼统说 vector 永远具有强异常保证。

**回到项目**：[MySqlConnGuard](../common/mysql_pool.h) 删除复制、允许移动，转移的是归还连接的责任。
学习时检查移动构造后的源对象和移动赋值时的旧连接，解释为什么不会归还两次；不要把“移动”只理解成字符串优化。
用 12.8-A 验证 const 与重载选择，不依赖编译器日志猜测。

**Q37：Debug 能运行、Release 崩溃，是否说明编译器优化有 Bug？**

**一分钟回答**：先检查未定义行为、对象生命周期、越界、未初始化值和数据竞争。
未定义行为没有规定必须崩溃；“本机运行几次没问题”不能证明代码合法。
另外，`NDEBUG` 会禁用标准 assert，不能把必要的业务副作用写在 `assert(Send())` 之类的表达式中。
定义见 [C++ 未定义行为](https://eel.is/c++draft/defns.undefined)。

**排查方法**：保存相同输入和构建版本，先复现，再用带符号的构建与合适的诊断工具缩小范围。
ASan 主要帮助发现一类内存错误，UBSan 检测部分未定义行为，TSan 用于数据竞争；它们不证明授权正确、没有死锁或业务恰好执行一次。
工具支持取决于编译器和平台，通常分别建立诊断构建，不把所有 sanitizer 开关堆在一起。
基础练习里的 assert 是检查点，产品中的授权和错误处理必须在 Release 仍有效。

### 11.10 系统设计面试：解释为什么选择，也解释何时改变

面试官问“为什么这样设计”，先说规模和合同，再说收益、代价、替代方案与验证方法。
当前项目面向小团队、以单机 Compose 交付；不能用这个结果承诺百万连接或多机高可用。

| 选择题 | 当前选择与收益 | 必须承认的代价 | 条件变化后讨论什么 |
|---|---|---|---|
| 所有逻辑放一个服务，还是 Comet/Logic/Job 分工？ | 连接、业务、异步持久化各自有明确职责 | RPC、部署及跨阶段排障成本增加 | 负载小可以讨论简化；扩容前先测各阶段瓶颈 |
| 直接写历史后推送，还是 Kafka 异步链路？ | 延续既有事件分发、消费与恢复体系 | 历史可晚于 ACK，需要 Outbox、幂等与补拉 | 同步链路也可设计，但要重新定义 ACK 与故障合同 |
| Redis 取号，还是 MySQL reservation 事务？ | 当前 SQL 将序号、首次原文与幂等身份关联 | 热点会话有锁竞争和数据库往返 | 号段等方案涉及空洞、顺序、恢复，不能只比较 INCR 延迟 |
| 只在加入房间时鉴权，还是每个受保护请求鉴权？ | 当前正式成员关系是服务端权威，支持撤权后拒绝新访问 | 查询、复查和订阅失效增加工作 | 缓存方案必须定义陈旧窗口、撤权传播和失败时行为 |
| 群消息共享一个 Agent Session，还是按房间/Agent 隔离？ | 同一时间线协作，运行时上下文与执行身份分别管理 | Session 数量与恢复状态更多 | 先定义授权的上下文交换，不直接把私人 Session 共享出去 |
| 流式 token 全部当正式消息，还是临时 delta + 最终 reply？ | 当前 delta 改善体验，最终回复走持久化和离线恢复 | 断线时预览可能消失，需恢复最终结果/任务状态 | 若要持久化每个事件，需要另设事件序号、保留和恢复合同 |

**两分钟答题示范：“为什么用了 Kafka，还要写 SQL Outbox？”**

> Kafka 的确认解决事件进入 broker 的问题，但不能替我的数据库提交原子地完成另一边的发送。
> 当前系统先在 SQL 保存业务身份和原文，再准备可恢复的事件；如果发出后没有收到确认，或发布阶段更新失败，
> 后台可能再次发布同一事件。因此我需要稳定身份和下游幂等，不能只说 Kafka 开启幂等就全链路 exactly-once。
> 代价是多一次状态维护，以及历史与实时链路可能暂时不同步。客户端要用序号、去重和补拉恢复。
> 验证时我会分别构造发送前崩溃、发送成功但确认丢失、Job 延迟落库，核对原文、序号、唯一历史行和最终展示。
> 如果目标只是极小规模同步聊天，另一种数据库提交后推送方案也值得比较；但它仍需回答提交后未推送时怎样恢复。

以上是设计理解示范。只有亲自完成相关实现或实验，才能把“当前系统”改成“我设计/验证了”。
继续追问“若群人数增长一百倍怎么办”时，先算 fanout、连接输出、热点锁、Kafka 分区和 SQL 的负载，
再提出可测量的改造；不要直接列分库分表、缓存和微服务名词。对应的故障与性能练习见 12.5～12.6。

<a id="interview-cross-layer"></a>
### 11.11 跨层八股：把“底层原理”讲到业务边界

这一组补充第 11.7 节的薄弱项。每题按“结论 → 原理 → 项目落点 → 反例”回答；
项目没有实现的优化，只能作为设计讨论，不能把通用原理直接包装成个人成果。

**Q38：`new/delete` 和 `malloc/free` 有何区别？用了 RAII 还需要懂吗？**

**一分钟回答**：在 C++17 中，普通 `new T(...)` 表达式取得存储并初始化对象，`delete` 负责析构和相应释放；
`malloc/free` 管理原始存储，不调用类的构造、析构函数。普通抛异常形式的 new 分配失败抛 `bad_alloc`，
malloc 失败返回空指针。对象生命周期和存储有效期要分别考虑，不能把“拿到一块内存”当作已构造一个类对象。
参见 [new 表达式](https://eel.is/c++draft/expr.new)和 [delete 表达式](https://eel.is/c++draft/expr.delete)。

**追问链**：placement new 使用已有存储，谁负责释放？单对象与数组的释放形式能否混用？
构造函数抛异常时，哪些已完成构造的成员会被析构？为什么“我手动写了析构函数”还不等于资源类正确？

**项目落点**：[MySqlConnGuard](../common/mysql_pool.h) 管理的是“归还借出的连接”这项责任，
不负责每次 delete 数据库连接。连接来自池，guard 的复制被禁止、移动会转移归还责任。
学习时逐项写出资源的获取者、所有者、借用者、释放动作，避免把所有资源都简单归类成堆内存。

**易错点**：不要用“new 一定调用 malloc”“析构等于释放内存”概括语言合同；也不要对借用的 MYSQL 指针自行 delete。
会写 RAII 的判断标准，是能解释正常返回、提前返回、异常和所有权转移四条路径，而不只是使用智能指针类型名。

**Q39：TCP 已有流量控制和拥塞控制，为什么 Comet 还要限制输出队列？**

**一分钟回答**：接收窗口 rwnd 反映接收方可接收的数据范围，拥塞窗口 cwnd 约束发送方向网络注入的数据量；
未确认在途数据受二者共同约束，可继续发送量还需扣除已在途数据。它们不限制应用继续生成多少待发消息。
这两个窗口不能替代业务队列预算。原理见 [RFC 5681 的窗口定义](https://www.rfc-editor.org/rfc/rfc5681#section-2)。

**项目落点**：慢浏览器可能导致 Comet 待发数据累积，而 Kafka、其他用户和 Agent 仍在产出消息。
阅读 [CometServer](../comet/comet_server.cpp) 的输出预算、关闭连接和恢复路径：
写不进去时需要限制内存，并依靠正式消息历史恢复；临时流式 delta 不具有同样的恢复合同。

**追问链**：把 socket 发送缓冲调大能解决持续过载吗？对方读取 TCP 很快但渲染很慢，传输层能否判断“用户看到了”？
某个房间 fanout 增大时，应该测每条业务消息数、每个接收者的投递数，还是网络字节？答案是分别记录，不能混为一个吞吐指标。

**易错点**：TCP ACK、accepted_ack、delivered_ack 和已读是不同层次的信号。
回答“用 TCP 保证消息可靠”后，仍需说明断线重发身份、持久化阶段和客户端补拉。

**Q40：`write` 返回、`flush`、`fsync` 与数据库提交分别保证什么？**

**一分钟回答**：常规缓冲文件 IO 的 write 成功不等于数据已到稳定存储；语言层 flush 常用于把用户态缓冲交给下一层。
Linux fsync 请求同步文件数据和相关元数据，调用结果仍需检查；创建或重命名文件时，目录项的耐久性还需要考虑目录同步。
具体语义见 [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)。

**项目落点**：MySQL 通过自己的日志和刷盘策略实现事务恢复，应用不能用一个文件 flush 代替数据库提交。
redo 用于 InnoDB 崩溃恢复，但它不会替应用发送 Kafka 事件，因此仍需要 reservation/Outbox。
数据库原理见 [InnoDB redo log](https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html)。

**追问链**：数据文件页尚未落盘，事务为什么可能已经提交？只有日志落盘，为什么仍需后台刷脏页？
复制了某个目录，能否保证 MySQL、Kafka 与 Runtime 账本属于同一个恢复点？
最后一个问题应回到项目的冷备流程、服务停写和恢复范围，而非只回答“用了 Docker volume”。

**易错点**：进程重启、容器重建、宿主机断电、存储损坏是不同故障模型。
一次正常重启测试不能证明断电耐久性，也不能把单 broker 的确认解释成跨主机容灾。

**Q41：MVCC 让读取不阻塞写入，为什么长事务仍然有代价？**

**一分钟回答**：一致性读可能需要旧版本；只要旧版本仍可能被活跃快照使用，就不能随意清除相关版本信息。
长事务还可能持有锁、占用连接并延长资源保留时间。MVCC 减少某类读写冲突，不代表所有查询无锁、无成本。
参见 [InnoDB 多版本机制](https://dev.mysql.com/doc/refman/8.0/en/innodb-multi-versioning.html)。

**项目落点**：读历史与串行分配 seq 的要求不同。前者需要明确分页和可见性，后者要在短事务内保护共享状态。
阅读 [message_reservation.cpp](../logic/message_reservation.cpp)，画出 SQL 事务结束与 Kafka 等待的位置；
不要为了“让链路看起来原子”而建议跨整个模型调用保持 SQL 事务。

**追问链**：RR 下第一次普通 SELECT 得到 revision=7，另一连接提交为 8 后，再次普通 SELECT 与 FOR UPDATE 各读到什么？
“我刚才读到 7”是否意味着 `UPDATE ... WHERE revision=7` 必然成功？
第 12.9 节直接实验：快照读取的值不是独占所有权，更新必须检查条件和受影响行数。

<a id="interview-discrimination"></a>
### 11.12 六个易混概念：用反例检验八股是否真正理解

这组题承接前面的基础问答。先用两分钟写下判断和理由，再展开答案；最后回到源码解释一个具体决策。
题目中的假设场景用于练习，不能据此声称项目发生过相应线上事故。

**Q42：重载、重写、名字隐藏怎么区分？Repository 接口为什么写虚析构和 override？**

先判断：基类声明 `virtual int Role(int) const`，派生类写 `int Role(int)`，是否重写成功？
通过基类引用调用时会执行谁？如果派生类额外声明 `Role(double)`，基类同名重载是否自动都可见？

<details>
<summary>参考答案、源码落点与追问</summary>

重载选择依赖编译期的候选函数与参数；虚函数重写要求符合签名规则，成员的 const 限定不能随意删掉。
题目中的非 const 版本没有重写基类函数；加上 `override` 后会由编译器报错。派生类同名声明还会影响名字查找，
需要时用 `using Base::Role` 引入基类重载。名字可见性与运行时虚派发是两个步骤。
规则见 [C++ 虚函数](https://eel.is/c++draft/class.virtual)。

项目的 [SpaceRepository / SpaceTransaction](../logic/services/space_repository.h)
有虚析构，SQL 实现的方法使用 override。若采用 `unique_ptr<Base>` 默认删除器持有派生对象，
基类应支持正确的多态析构；虚析构不代表基类指针必须拥有对象。
[RoomService](../logic/services/room_service.h) 保存的是注入的裸指针，生命周期由组合层保证，
Service 不应自行 delete 它。

继续追问：对象切片会丢掉什么？默认实参由静态类型选择还是动态类型选择？
构造/析构阶段的虚调用为什么不能当作派生对象已完整可用？先答本题的调用选择，再展开这些分支，避免只背“有虚表”。
第 12.10 节提供可编译的签名与派发实验。

</details>

**Q43：`T&&` 为什么有时能接收左值？`std::forward` 与 `std::move` 能否互换？**

先判断：在 `template<class T> void relay(T&& value)` 内，表达式 `value` 是左值还是右值？
传入普通左值、const 左值、临时值时，`T` 分别推导成什么？把 value 丢进异步队列是否就安全了？

<details>
<summary>参考答案、推导表与工程边界</summary>

这里的 T 来自本次调用的类型推导，`T&&` 是转发引用；有名字的参数表达式 value 本身仍是左值。
`std::forward<T>(value)` 按 T 保留调用方的值类别，`std::move(value)` 则转换为可供移动重载选择的表达式。
它们均不会单独执行资源转移。规则见 [模板调用推导](https://eel.is/c++draft/temp.deduct.call)。

| 调用参数 | 推导得到的 T | 折叠后的参数类型 | forward 后的表达式 |
|---|---|---|---|
| `std::string s` 的 s | `std::string&` | `std::string&` | 左值 |
| `const std::string s` 的 s | `const std::string&` | `const std::string&` | const 左值 |
| `std::string("hi")` | `std::string` | `std::string&&` | 右值 |

引用折叠中，只要组合涉及左值引用，结果就是左值引用；两个右值引用折叠为右值引用。
`const T&&` 不属于这里的转发引用，类模板中已确定的 T 对应的普通 `T&&` 也不能直接套用本表。

**项目连接**：[ThreadPool::Submit](../common/thread_pool.h) 使用 C++17 的 `std::function`，
接口没有因为调用方使用 forward 就支持不可复制闭包。转发也不会延长被借用数据的生命，
异步任务仍需明确拥有型数据、共享所有权或弱观察。第 12.10 节验证重载选择，第 12.8 节验证生命周期；两项都要成立。

</details>

**Q44：TIME_WAIT、CLOSE_WAIT 和“心跳还活着”各能证明什么？**

先判断：收到对端 FIN 后，本地还有缓冲数据，能否立刻声称整条业务已经结束？
大量 CLOSE_WAIT 能否通过缩短 TIME_WAIT 超时解决？一次 Pong 能否证明某条消息已经写入 SQL？

<details>
<summary>参考答案与排障顺序</summary>

CLOSE_WAIT 表示已收到对端的关闭方向通知，本端等待应用完成自己的关闭；TCP 两个方向可以分别结束。
TIME_WAIT 属于关闭过程的另一阶段，用于处理旧报文及终止确认等问题。持续积累 CLOSE_WAIT 时应查本地关闭、
回调和资源释放路径；TIME_WAIT 数量则要结合连接 churn、端口与主动关闭方解释。
状态定义见 [RFC 9293 状态机](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.3.2)。

心跳用于观察某个层次的活性；TCP keepalive、HTTP/2 PING、WebSocket Ping/Pong 和业务请求涉及的处理层次不同。
收到某层回应不能推出模型执行完毕、Kafka 已确认或 SQL 已落库，排障时要先标出回应来自哪里。

阅读 [CometServer](../comet/comet_server.cpp) 的连接关闭与 NotifyUserOffline，
再阅读 [RemoveRouteGeneration](../logic/redis_store.cpp)：旧连接关闭携带原代数和 token，
不能删除新连接的路由。TCP 连接状态、Redis 路由状态、MySQL 成员资格分别承担不同职责。

**练习**：画“旧连接断开 → 新连接注册 → 旧清理迟到”的顺序，解释为何仅按 user_id 删除会出错。
说明路由清理为何不能顺便删除正式成员；实际重连仍需消息补拉，心跳本身不补历史。

</details>

**Q45：先更新数据库再删缓存，为什么仍然可能读到旧值？延迟双删能否保证强一致？**

先画出两个请求的交错。把“数据库”换成 Redis、把“缓存”换成本地路由缓存，再考虑一次迟到回填。

<details>
<summary>一个反例、项目对应与设计边界</summary>

下面是 cache-aside 的并发反例推演，不是本轮实测结果：

| 时间 | 读请求 R | 写请求 W |
|---|---|---|
| 1 | 缓存未命中，读到后端旧值 v1，暂停 | |
| 2 | | 后端提交 v2，删除缓存 |
| 3 | 将早先读到的 v1 回填缓存 | |

删除发生后，迟到读仍可能重新填入旧值。延迟双删是否覆盖这次竞态取决于延迟和请求耗时；任意慢请求、
进程失败或通知丢失都需要另定义恢复机制，不能从“多删一次”推导强一致。
可讨论的设计有短 TTL、带版本的拒绝旧回填、串行化或可靠失效传播；每种方案都要说明允许陈旧多久以及失败时如何处理。

当前 [RedisStore::GetUserRoutes](../logic/redis_store.cpp) 在锁外查询 Redis，再写入本地短缓存；
AddRouteGeneration / RemoveRouteGeneration 会使本进程缓存失效。源码并没有把所有进程的缓存变成强一致目录，
也不能将路由 generation 自动等同于缓存回填的版本校验。路由负责定位下推节点，正式房间权限仍由权威成员关系决定。

**易错点**：Redis 在项目中不只有“可丢弃缓存”一种角色，token 会话、路由和热点缓存的丢失后果不同。
设计前先分类：丢失后能否安全回源、是否使用户重新登录、是否允许陈旧；权限不能因缓存失效而默认放行。

</details>

**Q46：MySQL 有 redo 与 binlog 协调，为什么还需要 SQL Outbox？**

先回答两个范围：数据库内部需要让什么一致？应用还需要把哪个外部动作完成？

<details>
<summary>参考答案与故障窗口</summary>

开启 binlog 的典型 InnoDB 事务提交，需要协调引擎提交与 binlog 的事务记录，避免崩溃后两边对事务是否提交产生分歧。
可以用 prepare、记录事务、完成 commit 理解其逻辑阶段；实际实现涉及刷盘设置和组提交，不能背成每条事务固定几次磁盘 IO。
参见 [MySQL binlog 与 InnoDB 协调](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)。

这份数据库内部协议没有替 Logic 向 Kafka 发事件。`SQL COMMIT 成功 → 进程崩溃 → 尚未发 Kafka`
仍是应用必须处理的窗口；`Kafka 已接收 → SQL 发布阶段未更新` 则可能导致重投。
本项目的 [message_reservation.cpp](../logic/message_reservation.cpp)
和消息 Service 维护可恢复事件与稳定身份，下游承担幂等收敛；不是把外部工具也纳入数据库事务。

**继续追问**：如果使用 CDC 读取 Outbox，会改变谁负责发布与恢复，但仍需确认重复、顺序和消费幂等。
这是候选方案，本项目不能因使用 MySQL 就声称已接入 CDC。不要把“数据库内部两阶段提交”、
“accepted/delivered 两阶段确认”和“跨系统原子事务”当作同一种机制。

</details>

**Q47：数据库死锁与锁等待超时，是否都会把整个事务自动回滚？COMMIT 断线又怎么办？**

先判断：事务里第一条 UPDATE 已成功，第二条 UPDATE 等锁超时，继续 COMMIT 是否安全？
如果 COMMIT 已发出但响应丢失，能否直接换 ID 重做整个业务？

<details>
<summary>参考答案、RAII 边界与重试原则</summary>

InnoDB 检测到死锁时会回滚选中的受害事务；默认锁等待超时通常只回滚当前语句，
整事务回滚行为受 `innodb_rollback_on_timeout` 设置影响。因此不能统一解释成“数据库已经帮我撤销所有步骤”。
规则见 [InnoDB 错误处理](https://dev.mysql.com/doc/refman/8.0/en/innodb-error-handling.html)。

对“全成或全不成”的业务用例，遇到中途失败应停止继续提交，并明确结束这次事务。
项目 [SqlTransaction](../common/mysql_statement.h) 对未成功 Commit 的路径在析构时尝试 ROLLBACK；
Repository 还要检查 active、SQL 结果和提交结果。RAII 保证清理路径会运行，不保证断开的连接上回滚请求一定送达。

COMMIT 的响应丢失属于提交结果不确定，不能仅凭本地失败就断言服务端没有提交。
应通过业务幂等键与权威记录核实，恢复时保持原业务身份；执行外部 Agent 工具还需查询原 Run。
这也是 Q21“整个事务重试”的前提：确认回滚范围、限制次数与退避、避免重复外部副作用，
而不是给每个异常无条件套一层无限重试。减小事务和一致锁顺序可降低死锁概率，不能承诺绝不死锁。

**练习产物**：列三行“死锁 / 等锁超时 / COMMIT 响应丢失”，分别填写数据库可能状态、应用动作和恢复身份。
第 12.9 节实测的是快照与版本冲突，没有实测这三种故障；完成推演后要单独记录验证范围。

</details>

<a id="cpp-review-foundations"></a>
### 11.13 C++ 代码评审题：从基础规则判断一段代码是否可靠

这组题补充项目面试中常见的短代码判断。每题先给结论，再指出一处源码，最后讲失败边界。

**Q48：vector 的 reserve 与 resize 有什么区别？预留容量后能直接写 v[0] 吗？**

空 vector 执行 `reserve(100)` 后，size 仍为 0，不能通过下标访问第一个元素；reserve 预留容量，
resize 改变元素数量。可以用 push_back/emplace_back 增加元素，也可以先 resize 再访问有效下标。
扩容可能使已有引用、指针和迭代器失效；reserve 也不使 vector 自动支持并发写入。
规则见 [vector 容量接口](https://eel.is/c++draft/vector.capacity)。

项目 [ProcessPushRequest](../comet/comet_grpc_service.cpp) 先按 targets 数量 reserve，
再只把合法 user_id push_back 进列表。这里容量可以大于最终 size，这是正常的；若改成 resize 后仍 push_back，
就会保留一批默认初始化的元素，再追加目标用户，改变业务输入。

**继续追问**：容量预留减少的是哪些分配？持有元素引用后再追加是否一定安全？
不要回答“vector 每次按固定两倍增长”，增长策略不应当作跨实现的业务合同。

**Q49：两个容器各自异常安全，组合起来是否就异常安全？**

先看 `seen.insert(id); order.push_back(id);`：第一步成功，第二步分配失败，seen 可能记住一个从未进入淘汰队列的 ID。
RAII 能释放局部资源，但不会自动撤销第一步已经提交的业务状态；catch 后直接返回成功会掩盖不一致。

基本保证要求失败后对象仍有效、保持约定的不变量；强保证要求操作失败时逻辑状态不变；不抛异常保证另有适用条件。
必须结合具体操作和 Hash、比较器、分配器等条件，不能把保证扩展到任意模板实例。
参考 [unordered 容器异常规则](https://eel.is/c++draft/unord.req.except)与
[deque 单元素尾插规则](https://eel.is/c++draft/deque.modifiers)。

第 12.11 节的教学版在队列追加失败时撤销刚插入的 set 项，并在新记录建立后才淘汰旧项。
这只处理完成窗口的本地逻辑状态；不会回滚已经发到 socket 的字节，也不会恢复进程强杀后的内存。
生产 [CompletePushRequest](../comet/comet_server.cpp) 还有 in-flight 状态，不能用教学类替换它，
也不能因使用标准容器就声称该路径已经验证过 bad_alloc 恢复。

**继续追问**：为什么“先淘汰旧项，再尝试插入新项”不满足失败后保持原窗口？
析构函数里执行可能分配内存的完成登记，异常如何影响进程？这是代码审查问题，本文没有对生产路径做故障注入或修复。

**Q50：结构体有内存对齐，把 sizeof(struct) 个字节直接发出去能代替 Protobuf 吗？**

对象表示可能包含填充字节，布局还涉及平台和 ABI；包含 string 或指针的对象内部表示更不能当成其业务内容。
即使一个类型可平凡复制，也不等于它有跨平台、跨版本的稳定 wire format。
`sizeof`、`alignof` 可以观察本次编译环境，不能据此承诺另一种架构上的协议布局。
参见 [C++ 对象表示](https://eel.is/c++draft/basic.types)。

项目通过 [spark_push.proto](../proto/spark_push.proto) 定义字段和消息，使用生成代码完成序列化；
Protobuf wire format 以字段号和 wire type 组织记录，而非复制 C++ 对象地址空间。
详见 [Protobuf 编码说明](https://protobuf.dev/programming-guides/encoding/)。
序列化还不等于认证或授权；反序列化成功的 room_id、actor 或工具参数仍需服务端检查。

**继续追问**：为什么不能用 memcmp 比较两个业务对象是否相等？为什么“相同字段值”不能直接推导任意序列化结果字节相同？
项目 canonical 原文与 request_id 的身份规则，应回到消息合同解释，不能用一段内存 hash 代替。

**Q51：计算超时为什么常用 steady_clock？选对时钟是否就能严格限时？**

steady_clock 的时间点不随物理时间前进而倒退，适合进程内测量耗时；日历时间用于展示和持久时间戳，职责不同。
不能将单调时钟的任意 epoch 数值当作通用 UTC 时间，也不能直接拿另一台机器的数值比较到期。
规则见 [steady_clock](https://eel.is/c++draft/time.clock.steady)。

项目 [MySqlConnectionPool::Acquire](../common/mysql_pool.cpp) 用 steady_clock 计算等待截止，
周期唤醒时取当前剩余预算；这避免每次被唤醒都重新获得一整段等待时间。
但函数在锁外建连或校验连接，这些阻塞调用还有自身超时；仅看队列的 deadline，不能证明 Acquire 整体严格在同一毫秒截止。
回到 Q17：完整请求预算需要覆盖排队、连接、RPC 和重试，各层能否中止要分别验证。

**练习**：列出“客户端等待结束 / 服务端执行停止 / SQL 回滚确认 / Agent 工具无副作用”四个事实。
指出一次 timeout 最多直接证明哪一个，其他事实要向谁查询；不要把它们统称为“请求失败了”。

<a id="interview-concurrency-security"></a>
### 11.14 跨层追问：把 C++ 并发、网络和安全放回业务

每题按“先判断 → 解释原理 → 指向源码 → 改变一个条件”练习。不要用一个底层名词解释所有层的可靠性。

**Q52：CAS 为什么也会出错？C++ compare_exchange 和任务 revision 是一回事吗？**

对本节使用的整数，CAS 比较当前值与 expected：相同则写入 desired，否则失败并把观察值写回 expected。
`compare_exchange_weak` 允许伪失败；`strong` 不允许这种伪失败，但仍可能因值不同而失败。
CAS 只比较这一次看到的状态，不记录过去：值从 A 变 B 再变回 A，旧 expected=A 仍可能匹配，这就是 ABA 的基本问题。
规则参见 [原子比较交换](https://eel.is/c++draft/atomics.types.operations)；第 12.12 节只使用 C++17 已支持的整数接口。

| 层次 | 原子边界 | 需要额外保证什么 |
|---|---|---|
| `std::atomic` CAS | 当前进程中的一个原子对象操作 | 关联数据发布、对象生命周期、算法的并发正确性 |
| SQL 条件 UPDATE | 数据库执行的更新与事务 | 授权、受影响行数、冲突响应、历史记录的一致提交 |
| Agent/工具调用 | 外部执行系统的合同 | 请求身份、执行账本、原生结果恢复；不能靠一条内存 CAS 覆盖 |

项目 [SaveTask](../logic/repositories/room_agent_repository.cpp) 更新时带 `task_id + room_id + revision` 条件，
并在同一事务里写历史。两个用户都读到 revision=7，先提交者更新到 8，后提交者不能覆盖它。
[UpdateTask](../logic/services/room_task_service.cpp) 另做成员与角色检查；版本号相同不代表用户有权修改。

**容易答错的追问**：CAS 失败后 expected 已变为 8，直接再写旧摘要是否合理？
语言层面可以再尝试，但业务上应重读并决定如何合并；不能将计数器的自动重试模板套在人工协作内容上。
版本号需要避免复用/回绕；带版本也不自动解决无锁链表节点的内存回收。用了 atomic 不代表整个算法无锁，更不代表性能一定更好。

**Q53：worker 一直发心跳，为什么还需要 fencing？本地文件锁能替代它吗？**

先推演下面的调度：A 因暂停或网络故障错过续租，服务端允许 B 领取新 attempt；随后 A 恢复，仍拿着旧结果。
不能要求 A 自觉停下作为唯一保障，受保护存储必须拒绝 A 的旧身份写入。这里将这种服务端拒绝旧执行的机制称为 fencing。

| 顺序 | worker A | 服务端与 worker B |
|---|---|---|
| 1 | 持有 attempt=1、token=A，开始只读解析 | job 记录当前租约 |
| 2 | 暂停，续租未成功 | 租约到期，不据此断言 A 已停止 |
| 3 | 仍未恢复 | B 领取 attempt=2、新 token，旧 revision 失效 |
| 4 | 恢复，尝试暂存旧结果 | 校验当前 attempt/token、状态与期限，拒绝旧写入 |

另一个分支是 A 已经完成正式发布，但 Complete 响应丢失：此时应按原身份与摘要恢复原回执，
而不是把“恢复已提交决定”当成“允许过期 worker 新写入”。它与上表 B 重新领取的分支分别推演。

源码入口是 [Claim / Lock / Live / Stage](../logic/repositories/media_processing_repository.cpp)、
[派生物 Stage / Complete](../logic/repositories/media_artifact_repository.cpp) 与
[LeaseKeeper](../cannbot/scripts/media_worker.py)。服务端检查发生在受保护操作中；不能只在 worker 开始时查一次。
这里使用当前 attempt 与随机 token 的相等性检查、事务锁和期限判断，不能误讲成“随机 token 本身具有单调大小”。

心跳用于续租和发现失联；本地 `flock` 防止同一状态目录被重复操作；服务端 fencing 拒绝旧执行的受保护写入。
它们不是同一个锁。两个 worker 使用不同状态目录时，本地文件锁不能协调数据库任务所有权。

**继续追问**：租约过期是否证明文件没被写过、工具没调用过？不能。已发生的副作用不能靠到期撤销。
当前媒体解析可在限定条件下重新执行，是因为它读取受控源并写入隔离暂存区；
不能把这条恢复策略套到 unknown 的 Agent shell、付款或任意外部写操作上。该工作树阶段边界见 10.7。

**Q54：SHA-256、密码哈希、HMAC、随机 token 和 TLS 分别解决什么？**

| 机制 | 回答中的用途 | 不能替代什么 |
|---|---|---|
| SHA-256 摘要 | 对照可信摘要检查文件/内容一致性 | 不是加密；攻击者能同时改内容和摘要时，不能证明来源 |
| 带 salt 和成本参数的密码派生 | 提高校验密码及抵抗离线猜测的成本 | 不保证弱密码不可破解；salt 通常无需保密 |
| HMAC | 持有共享密钥者验证消息认证码 | 不提供内容机密性，也不自动防重放；见 [RFC 2104](https://www.rfc-editor.org/rfc/rfc2104.html) |
| CSPRNG 随机 token | 生成难以预测的会话凭据 | 服务端仍需检查到期、撤销、用户状态和对象权限 |
| TLS | 保护传输并按配置认证通信端点 | 不能代替房间成员检查；见 [TLS 1.3](https://www.rfc-editor.org/rfc/rfc8446.html) |

项目 [HashPassword / VerifyPassword / GenerateSecureToken](../common/security.cpp) 使用随机 salt、
PBKDF2-HMAC-SHA256 和 OpenSSL `RAND_bytes`；[UserService](../logic/services/user_service.cpp)
负责发行和校验会话 token。`RAND_bytes` 失败应返回错误，不能回退为时间戳或普通随机数；见 [OpenSSL 文档](https://docs.openssl.org/3.0/man3/RAND_bytes/)。
HMAC 在上表是原理对比，不是声称本项目把所有请求都改成了 HMAC 协议。

**源码与推荐的差异**：当前 `kPbkdf2Iterations` 为 120000，附近“符合 OWASP 推荐下限”的注释不适合作为面试依据。
2026-11-01 核对的 [OWASP Password Storage 指南](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html#pbkdf2)
列出 PBKDF2-HMAC-SHA256 的 600000 次建议。本文记录差异，没有修改产品参数，也没有实测修改后的登录吞吐。
实际升级应结合成本测试、登录限流、旧编码兼容与 `needs_rehash` 迁移设计，不能口头宣称“已经达到推荐值”。

**继续追问**：拿到某个媒体文件的正确 hash，是否可以读取它？不可以；hash 校验内容，正式成员关系校验访问资格。
同样，客户端连接经过 TLS 也不能让正文中的 `actor_user_id` 成为可信身份。

**Q55：HTTP/2 已经多路复用，gRPC 为什么还会阻塞？Write 成功为什么不算业务成功？**

HTTP/2 可在同一连接交错传输不同 stream 的帧，缓解 HTTP/1.1 管线化的应用层队头阻塞；
它仍运行在 TCP 上，不能消除底层 TCP 有序交付带来的队头阻塞。流级和连接级流控也会限制发送。
参见 [RFC 9113](https://www.rfc-editor.org/rfc/rfc9113.html#section-1)。

一次双向 streaming RPC 自身也不是无限并行执行器：该流上的消息仍有顺序，应用队列、串行 writer、
接收端处理速度和业务锁都可能成为等待点。增加 stream 或线程必须先测瓶颈，不能只凭协议名推断加速。

gRPC 的 Write 把消息交给框架，不证明对端业务已完成；流控可能使同步写等待。
若双方大量写入却不读取，还可能无法取得进展。见 [gRPC Flow Control](https://grpc.io/docs/guides/flow-control/)。
项目 [Comet 的 StreamWriterLoop](../comet/comet_server.cpp) 按 request_id 保存待答复回调，
[Job 的流读写处理](../job/service.cpp) 则等待 Comet 的响应；成功且 delivered_count 大于零才安排投递 ACK。

**一分钟作答结构**：先画“应用队列 → gRPC → TCP → 对端业务”，再标 Write 返回与业务响应两个位置，
最后用本项目 accepted/delivered 的含义说明为什么还要 request_id、超时与恢复。
不要把“框架接纳写入”“对端连接接纳下推”“历史入库”和“人已阅读”合并成一个成功。

<a id="interview-shutdown"></a>
### 11.15 从线程退出讲到消息完成：三道需要画时间线的题

这一组将第 5 节的语言原理连到实际控制流。先给 30 秒结论，再读源码，最后改变一个前提。

**Q56：request_id 用 relaxed 原子递增，为什么发布任务却不能只靠它？**

`fetch_add` 对该原子计数器执行不可分割的读改写；在不回绕等前提下，并发调用取得不同的旧值。
relaxed 不提供关联普通数据的跨线程发布关系。[NextRequestId](../comet/comet_server.cpp)
用于编号，不负责把消息正文、pending 表或队列安全地交给另一个线程；它也不是跨进程、跨重启的持久业务幂等键。

考虑一个仅发布一次的教学场景：生产者先写普通变量 payload，再对 ready 做 release store；
消费者的 acquire load 若读到该次发布的值，才通过这条同步关系观察之前写入的 payload。
若只把 ready 换成 relaxed，又没有锁、join 等其他同步关系，不能据此安全地并发读写 payload。
依据见 [C++ 内存顺序](https://eel.is/c++draft/atomics.order)。

**项目落点**：ThreadPool 的 mutex 共同保护队列、停止状态和字节计数，随后解锁/加锁建立可见性。
把其中一个 bool 改成 atomic，并不会自动让 queue 安全，也不会让“检查容量 + 入队 + 加计数”变成整体原子操作。

**追问**：“把所有字段都改成 seq_cst 是否就正确？”不够。单字段原子性、复合状态一致性和对象生命周期必须分别证明。
不要把“刷新 CPU 缓存”当成完整回答；请画出哪一次写入通过哪条同步关系先于哪一次读取。

**Q57：Stop(false) 能立即返回吗？notify_all、取消、join 各做了什么？**

先假设一个 worker：任务 A 已经出队并阻塞在 RPC，任务 B 还在队列，主线程此时调用 Stop。

| 操作 | 对任务 B 的影响 | 对正在运行的 A 的影响 |
|---|---|---|
| 设置 stopping，拒绝新 Submit | 取决于随后选择 drain 还是丢弃 | 不会自动打断 A |
| drain=true | 保留 B，worker 后续执行 | 等 A 返回后继续处理队列 |
| drain=false | 丢弃尚未出队的 B | A 仍需返回，Stop 才能完成 join |
| notify_all | 唤醒等队列的 worker，重新检查谓词 | 无法唤醒正在另一个 RPC 内阻塞的 A |
| 对相应 RPC 发取消或设置 deadline | 不负责队列策略 | 影响该 RPC 的等待/执行合同，不能据此宣称远端回滚 |
| join | 不决定 B 是否执行 | 等待对应线程结束；不是杀线程指令 |

源码见 [ThreadPool::Stop / WorkerLoop](../common/thread_pool.cpp) 和
[CometServer 析构](../comet/comet_server.cpp)。join 的等待与同步合同见
[C++ thread 成员](https://eel.is/c++draft/thread.thread.member)；谓词等待见
[condition_variable](https://eel.is/c++draft/thread.condition.condvar)。

**生命周期追问**：不要让 worker 在自身任务里调用会 join 自己的 Stop；也不要在其他线程仍调用 Submit/Stop 时销毁池。
当前代码有顺序重复停止的保护，但不能从这一点推出任意并发 Start/Stop/析构都安全；生命周期需要由外部明确组织。
detach 只解除线程对象关联，后台线程仍可能访问已释放的 this、队列或连接，不能用它跳过所有权问题。

**口述标准**：说清停止准入、队列处理、正在执行的取消/超时、线程退出、依赖释放这五步。
若说“设个 atomic 就安全退出”，请补充：睡在条件变量里的线程如何醒来？阻塞 IO 如何返回？谁保证对象活到 join？

**Q58：worker catch(...) 之后继续工作，能否认为任务已安全失败？**

[WorkerLoop](../common/thread_pool.cpp) 在任务调用边界捕获异常并记录日志。
从线程入口逃出的异常会调用 terminate，规则见 [C++ 线程启动](https://eel.is/c++draft/thread.thread.constr)。
捕获普通 C++ 异常可以保住 worker 的执行路径；它不恢复任意内存破坏，不撤销外部副作用，也不向调用者自动交付业务结果。

将“SQL 预留成功 → Kafka 已接收 → 本地后续步骤失败”画出来：即使外层捕获异常，前两步也可能已经发生。
恢复要查询原身份和持久状态，沿 reservation/Outbox 的合同处理；不能重新生成 client_msg_id 再当作新消息执行。
同理，Agent 工具结果未知必须查询原 Run，不得用 catch 把 unknown 改造成“确认未执行”。

**一分钟回答的结构**：异常边界保护线程 → Service 决定错误语义 → 持久状态负责恢复 → 协议返回可解释结果。
后续若设计 future/回调，也要定义被拒绝、被丢弃、执行异常时如何通知调用方；这是扩展设计，当前 ThreadPool 接口并没有返回任务 future。
第 12.14 节只验证队列关闭，不模拟 C++ 异常、数据库提交或 RPC 取消，不要混用证据。

<a id="interview-context-access"></a>
### 11.16 资料授权：把数据库、安全与协议串成连续追问

本节对应第 10.8 节的资料查询模块。通用原理可独立用于面试；当前实现与验收范围必须分别说明。
先答题，再展开参考答案。给自己的回答圈出“身份来自哪里、数据在哪里过滤、失败时返回什么”三处。

**Q59：先查最新 20 条再过滤权限，与在 SQL 中过滤后 LIMIT 20，有什么区别？用了预处理就够安全吗？**

<details>
<summary>参考回答：区分结果资格、分页与参数安全</summary>

假设最近 20 条都是旧代次 Agent 回复，第 21 条是仍有效的人类讨论。
先 LIMIT 再过滤会得到空页，若把空页当结束，就永远读不到第 21 条；扩大固定候选数也不能普遍解决。
如果把未过滤的正文交给模型再筛选，数据已经离开服务端授权边界。即便始终留在服务端，短页和 has_more 也可能算错。

正确思路是让资格条件成为查询结果定义的一部分，在合格结果上排序并限制数量。
这里说的是 SQL 的结果语义，不是要求优化器按 WHERE 文本从左到右执行，更不能把某个谓词当成异常短路保证。
MySQL 的 LIMIT 及预处理占位符规则见 [SELECT 文档](https://dev.mysql.com/doc/refman/8.0/en/select.html)。

本项目 [Messages](../logic/repositories/media_context_repository.cpp) 同时限制房间、
触发 request 的 message_seq 上界、before_seq，以及 Agent 原 request 的房间/bot/代次。
再按 msg_seq 降序取 limit+1 条合格结果，额外一条只判断 has_more；游标取**最后一条实际返回的记录**。
直接读取某条 msg_seq 也复用这套查询，不能只写 `WHERE msg_seq=?`。

**继续追问：有 keyset 分页，是否一定快、一定不漏？**

- `(session_id, msg_seq)` 可帮助缩小会话序号范围，但 JSON 来源判断、关联和正文子串匹配仍有成本。
  limit 限制返回数量，不保证只扫描 limit 行；应看真实 EXPLAIN、候选/匹配比例、耗时与数据分布。
- 固定消息上界可排除触发请求之后的新消息；它不是跨多次 HTTP 请求的完整数据库快照。
  如果旧序号稍后才落历史表，或资格发生变化，分页仍要按其具体合同解释，不能承诺任意并发写入下的快照遍历。
- 权限变化必须重新检查。游标只描述位置，不是访问凭证，也不能沿用上一页已失效的授权。

预处理把**值**与 SQL 语法分开，用户输入中的引号不会变成 SQL 控制结构，见
[MySQL Prepared Statements](https://dev.mysql.com/doc/refman/8.0/en/sql-prepared-statements.html)。
但它不会自动加房间条件，不会判断角色，也不会把 `LIKE ?` 中的 `%`、`_` 自动变成普通字符。
本模块用 `LOCATE(?, text)` 做字面子串搜索；匹配规则仍受字符串类型/排序规则影响，不能默认等于逐字节比较。
可对照 [LOCATE 与字符串函数](https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_locate)。
动态表名、排序方向等 SQL 结构只能由受控代码选择；不能把“所有值都绑定了”说成“任意 SQL 拼接都安全”。

**一分钟回答标准**：先讲空页反例，再讲合格结果上的 limit+1/游标，最后区分防注入、对象授权和查询性能。

</details>

**Q60：读之前已经查过成员，为什么返回前还要查？查三次能否保证撤权瞬间零泄漏？**

<details>
<summary>参考回答：画交错顺序，再说明检查能覆盖的边界</summary>

TOCTOU 是“检查时成立，使用时可能已改变”。下面是必须能手画的一组交错：

| 时间 | 读取请求 R | 管理操作 W |
|---|---|---|
| t1 | 查到 actor 是正式成员，取得 scope | — |
| t2 | 读取受保护资料，暂存在服务端局部结果 | — |
| t3 | — | 移除成员并提交 |
| t4 | 再授权发现失权，丢弃响应内容 | — |

只在 t1 鉴权会漏掉这类变化。[MediaContextService::Execute](../logic/services/media_context_service.cpp)
在读取前、记录来源前和记录后复查授权，并比较同一 request、collaboration、消息上界与 bot 集合。
输出参数先清空，成功到最后才赋值；错误返回不能把先前读到的局部数据顺手带出去。
逐请求和逐对象检查的通用原则见 [OWASP 授权指南](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html)。

来源 [Record 事务](../logic/repositories/media_context_repository.cpp) 按 room → context → request 加锁，
比较当前代次和 canonical request，再复验来源版本、范围与摘要，最后提交整批回执。
相同 request 的来源限额也在这个事务中检查；两个并发请求争最后一个名额不能都按旧 count 放行。
锁顺序、事务回滚与唯一键各自有职责，不能仅靠单进程 mutex 保护多个 Logic 实例。

**但三次检查不是瞬时撤回协议**：最后一次检查与响应实际发出之间仍有边界；已授权进入网络或队列的数据不能追回。
本实现没有新增 Kafka/Comet 已排队帧的同步撤销屏障，也不能由“禁止返回”推出“Native 工具已停止”。
锁住一行只会与遵循相应锁/事务协议的操作协调，不会自动覆盖文件 IO、网络发送或 Runtime 内存。
更强的撤销语义必须先定义线性化点、在途读和已发内容的处理范围，再讨论实现；当前阶段未交付共享资料删除入口。

**异常追问**：代次表不存在，能否按 0 处理以兼容旧库？不能。
“查到没有初始化行”和“查询失败/迁移缺失”含义不同；后者应返回不可用，不能因为基础设施故障扩大授权。
同样，临时存储故障不代表原工具肯定没执行，不能顺便把 unknown 改成可安全重跑。

**一分钟回答标准**：给出 t1～t4 → 指出事务与复查位置 → 主动说清最后一次检查之后的边界。
只回答“加锁就行”时，继续追问锁住谁、其他路径是否遵循同一顺序、跨进程和外部 IO 如何处理。

</details>

**Q61：source_id、SHA-256 和“工具成功”各能证明什么？为什么读了 100 个字符也要定义单位？**

<details>
<summary>参考回答：身份、内容、传输和推理分别取证</summary>

本模块的 source_id 是 request 与规范 reference 的确定性摘要，数据库保存相应来源回执。
同一 request 重读同一范围复用身份，不同 request 即使读取相同内容也有不同身份。
它不是 bearer 凭据；持有 ID 不授予读取权。hash 用于检测规范内容是否一致，也不证明来源作者可信或推理正确。
原文和引用结构见 [资料 API 来源合同](room-context-tools.md)。

| 观测结果 | 能支持的结论 | 仍需另外证明 |
|---|---|---|
| `media` / `image_metadata` 回执 | 服务端观察了资产或图片元数据 | 实际正文/图像是否读取 |
| `fragment` / `message` 回执 | 服务端观察了固定片段或字符范围 | 客户端是否收到、模型是否消费 |
| `image` 回执与 JPEG hash | 服务端验证并读取了实际图像字节 | 原生工具是否保留图像类型、provider 是否收到图像 |
| 模型产生回答 | 该次调用返回了文本 | 是否依据正确来源、结论是否正确、任务是否持久保存 |

因此不能把 base64 作为普通文本传给模型后声称已完成视觉接入；工具层有图片字段也不等于模型端未降级。
早期资料 API 回执只覆盖服务端读取。后续真实 PDF/Hermes 网页验收已覆盖图像进入原生请求、引用预览和来源任务恢复，见 10.9；
它仍不证明所有模型的视觉语义都正确，也不替代每次任务的实际完成回执。

**编码追问**：UTF-8 的一个 Unicode 码点可以占 1～4 字节，见 [RFC 3629](https://www.rfc-editor.org/rfc/rfc3629)。
`std::string::size()` 在这里计 UTF-8 字节；消息 read 的 offset/length 合同计 Unicode 码点，
不是 UTF-16 单元，也不是用户眼中的组合字形。比如 `A中🙂` 是 3 个码点、8 个 UTF-8 字节；
`e` 加组合重音可以显示成一个字形，却包含两个码点。不要直接用前端字符串索引推算后端读取范围。

代码用 MySQL 的字符范围读取，并记录**实际返回**的长度与范围 hash；读到 EOF 返回零长度，不能记成“全文已读”。
字符数上限也不能代替响应字节上限：JSON 包装、转义、base64 和每项 source_id 都需要空间。
当前 Service 为每项回执预留字节，并对最终序列化结果复查；超限要求缩小范围，不静默删掉来源字段。
练习时可检查 60 个结果的字段开销，而不要只测试一条短英文。

**一分钟回答标准**：区分“可定位、内容一致、有权读、已送达、已理解、结论正确”六件事，任选一个反例讲清楚。

</details>

<a id="interview-framing"></a>
### 11.17 网络解析：把 TCP 八股讲成一段可靠的 C++ 代码

本节接续 Q13、Q16 和 Q35，不要求自研网络库。面试重点是：能否说明一次回调里有哪些有效字节、
哪些内容已经完整、由谁拥有，以及下一次回调从哪里继续。

**Q62：TCP 已经可靠有序，为什么还需要拆帧？“不支持 WebSocket 分片”是否意味着不能处理半包？**

<details>
<summary>参考回答：区分 TCP 字节流、协议帧和业务消息</summary>

TCP 保持字节流的有序传输，不保留应用每次 send 的边界；接收端一次读取可以得到部分帧、一帧或多帧。
“粘包/半包”是在描述应用读取边界与协议边界不一致，不是 TCP 把业务消息损坏了。
[标准 RFC 9293](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.7)明确区分传输分段与应用缓冲边界。
给 recv 更大的缓冲区、关闭 Nagle，都不能建立业务消息边界。

在 Linux 非阻塞 TCP socket 上，用非零长度缓冲区调用 recv 时，可按以下分支思考：

| 结果 | 表示什么 | 解析器应该怎样处理 |
|---|---|---|
| 正数 | 本次取得这些字节，不保证一条消息齐全 | 累积输入；循环处理完整帧；保留未完整尾部 |
| -1，EAGAIN/EWOULDBLOCK | 当前没有可读取的数据 | 保留状态，交回事件循环；不要忙等，也不当成 EOF |
| -1，EINTR | 此次调用被信号打断，尚未取得数据 | 根据停止状态与时间预算决定是否重试 |
| 0 | 对端正常结束发送方向 | 已有完整帧仍可处理；若留下半帧，报告截断；是否继续写由半关闭策略决定 |

该表限定为上述 TCP 调用情境，不能套用到零长度读取或 UDP 零长度报文；系统规则见
[Linux recv 手册](https://man7.org/linux/man-pages/man2/recv.2.html)。项目使用 Muduo 管理 socket，
这张表用于理解底层行为，不是在声称 Comet 直接按这段伪代码调用 recv。

[HandleWebSocketFrame](../comet/comet_server.cpp) 在帧不完整时返回并保留 Buffer；
完整后复制解码正文、retrieve，再继续检查后续帧。它拒绝 FIN=0/continuation 等 WebSocket 消息分片形式，
但同一个合法完整帧在 TCP 层分多次到达是正常输入。TCP 分段和 RFC 6455 的 WebSocket fragmentation 不是一层概念。
握手成功后还要主动处理 Buffer 中可能已经到达的第一帧，不能总是假定还有下一次可读事件来提醒。

**30 秒口述**：“可靠字节流不提供消息边界；我用协议状态与长度判断完整性，未完整不消费，完整才分发。
项目不接受 WS 消息分片，但支持 TCP 分次到达。连接关闭后的半帧是截断，完整消息仍需独立业务鉴权和确认。”

</details>

**Q63：`buffer.size() >= header + length` 为什么未必安全？知道数据完整后，能直接把 string_view 扔进线程池吗？**

<details>
<summary>参考回答：先验证整数与资源边界，再判断所有权</summary>

长度来自网络，不能先把它用来分配内存或构造范围。审查顺序应是：

1. 头部是否齐全；按协议字节序解码，先把 char 转成 unsigned char，避免负值参与移位。
2. 长度是否合法，是否超过业务限额，能否被目标类型表示；必要时拒绝，不把截断后的值当原值。
3. 先证实 `available >= header`，再判断 `length <= available - header`，避免未经约束的加法回绕。
4. 只有完整且合法才复制/解码和消费；区分 NeedMore 与 Invalid，非法长度不能无限等待正文。

无符号算术按模数回绕，有符号算术溢出是未定义行为；“使用 size_t”不等于自动安全。
例如最大 size_t 加 2 会得到 1，可能让错误的长度检查通过。依据见
[C++ 整数规则](https://eel.is/c++draft/basic.fundamental)。先对受信范围作证明后再相加也是可行的，
不能脱离前置检查，看到加号就断言存在漏洞。

项目 [HandleWebSocketFrame](../comet/comet_server.cpp) 先把 payloadLen 限制为 64 KiB，
再检查有限头部、mask 和负载的总长度，完整后才 resize 正文。这个顺序比“先按用户长度分配，再判断超限”更重要。
单帧限额也不等于整体资源有界：还需要输入积压预算、慢发送超时、业务队列容量和事件循环公平性。
这些应分别核对，不能仅凭这一处长度限制宣称已实现完整抗拒绝服务方案。

string_view 只借用字节。即使按值捕获 view，后续 Buffer 的 retrieve、整理、复用或扩容也可能改变内容或使其失效。
异步交接通常需要复制成拥有型 string，或明确共享不可变存储的所有权；shared_ptr 保住连接对象，也不自动固定其缓冲字节。
项目跨线程 [TcpConnection::send](../muduo/net/TcpConnection.cc) 捕获 `bytes = message.as_string()`，
同时持有连接；握手路径也把 token/key 拷贝后交给工作线程。生命周期原理见 Q35；不要将这种必要复制随意“优化”掉。

**追问练习**：画出“创建 view → 排队 → Buffer 复用 → 回调执行”的时间线。
然后分别回答：数据边界在哪里，长度在哪一步可信，谁拥有正文，过载时拒绝哪一层请求。

</details>

<a id="interview-resource-pools"></a>
### 11.18 资源池与异步结果：为什么线程安全了，业务仍可能不推进？

这组题接续 Q07、Q10、Q17、Q34 和 Q57。先区分四件事：**对象是否有效、访问是否同步、
工作能否继续、结果是否成功**。智能指针、mutex、调度与业务状态分别回答不同问题。
以下反例用于学习和评审，不表示已经在当前产品中复现对应事故。

**Q64：全部使用 RAII 锁，为什么仍然会死锁？`scoped_lock` 能解决全部问题吗？**

**一分钟回答**：RAII 保证已取得的锁随守卫生命周期释放，不保证取得锁的过程没有循环等待。
线程 T1 持 A 等 B，T2 持 B 等 A，即使都使用 `lock_guard`，也可能永远到不了析构解锁的位置。
对同一组多个互斥量，C++17 的 `std::scoped_lock(a, b)` 使用多锁算法获取，再以 RAII 管理释放；
它不负责解决数据库连接、worker 名额、RPC 或回调之间的依赖环。参见
[scoped_lock 规则](https://eel.is/c++draft/thread.lock.scoped)。

**项目寻踪**：阅读 [KeyedMutex::Get](../common/keyed_mutex.h)，区分保护锁表的 mutex 和返回的会话 mutex。
Get 返回时锁表守卫已析构，调用者随后才获取会话锁；不要把“锁表内创建锁对象”说成“同时锁住所有会话”。
再看 [MySqlConnectionPool::Acquire](../common/mysql_pool.cpp)：先在锁内预占容量，
释锁建连，重新加锁核对状态；预占是为了防止多个创建者同时越过容量上限，释锁是为了让其他借还操作能够继续。

**追问链**：把阻塞操作搬到锁外后，哪些状态可能变化？当前实现为什么还要复查 stopping？
若改成持会话锁调用一个未知回调，回调又要取得同一非递归 mutex，会发生什么？
改成 `recursive_mutex` 只处理同线程递归获取，不会消除跨线程的 A/B 环，也不能证明重入时业务不变量成立。

**白板输出**：画两类边：任务“等待某资源”和资源“由某任务占用”。找到环后，指出在哪里改变依赖顺序、
缩小持有范围或异步返回。没有找到环时，仍可能是慢 IO 或饥饿，不要仅凭低 CPU 宣布死锁。
多实例、多容量资源不能只凭图中有环就下结论，必须同时检查剩余容量和是否还有可推进的任务。

**Q65：线程池开 64 个线程，MySQL 池有 4 个连接，吞吐会提升 16 倍吗？**

**一分钟回答**：不会。线程数决定某一执行层的并行名额，数据库连接数限制同时占用连接的操作数，
SQL 锁、CPU、IO 和热点会话还可能先成为瓶颈。增加线程可能只增加等待连接的任务和内存占用。
先测借用等待时间、连接持有时间、SQL 时间、队列长度与失败率，再判断该改容量还是缩短持有时间。

**纸上估算**：假设每个请求只借一次连接，平均持有 20 ms，4 个连接持续繁忙，其他资源都不限制，
那么这一资源的理想服务能力约为 `4 / 0.020 = 200 次借用/秒`。这不是项目实测吞吐，也不是 64/4 的加速比。
如果平均持有时间因等锁增为 100 ms，理想值降到 40 次/秒；实际分布、突发和排队还会影响尾延迟。
一次请求多次借用时，要把借用次数和同时持有情况重新计入模型。

**必须会画的反例**：池上限为 2，T1、T2 各持有一个连接，再各自调用会二次借连接的 helper。
若两者都无限等待、只有得到第二个连接后才归还第一个，就没有空位能够出现。RAII 此时不会提前析构外层 guard。
有借用超时可使调用返回，但仍需结束外层事务、归还资源并给出失败，不能直接带着旧事务循环重试。
这是假设性的容量耗尽案例，不是声称当前 DAO 已有嵌套借用缺陷。

| 源码中的动作 | 解决什么 | 不保证什么 |
|---|---|---|
| 锁内增加 total_conns_，释锁建连 | 防止并发创建突破容量 | 建连必然成功或在请求剩余时间内返回 |
| guard 析构时 Release | 正常作用域退出时归还责任 | 嵌套借用不会堵塞、池一定比 guard 长寿 |
| Acquire 的等待 deadline | 限制等待循环的预算 | 直接取消正在进行的 connect/ping 或业务 SQL |
| connect/read/write 超时配置 | 给客户端网络操作设置限制 | 业务已回滚，或者整个请求恰好在同一毫秒截止 |

MySQL 超时选项按各自 API 合同生效，不能简单合成为一次业务总期限；参考
[MySQL 8.0 客户端选项](https://dev.mysql.com/doc/c-api/8.0/en/mysql-options.html)。
源码中还要核对配置单位转换、获取失败时判空、事务结束位置，以及是否在等待外部服务时仍持有连接。
调整池大小属于待验证方案，不能从这个纸上估算推出“已经完成数据库性能优化”。

**Q66：`future` 已 ready，任务就成功了吗？在 worker 内等待子任务有什么风险？**

**一分钟回答**：future 就绪表示共享状态中已有可取的值或异常。`get()` 可以返回结果，也可以抛出保存的异常；
`wait_for` 超时只说明此次等待没有取得就绪结果，不会自动停止任务。`packaged_task` 可以把调用结果或异常写入共享状态，
但它不会自动获得线程；仍需某个执行者调用它。参见
[future 共享状态](https://eel.is/c++draft/futures.state)。

**线程池饥饿推演**：固定 N 个 worker，N 个父任务各占一个 worker，将子任务提交到同一个池后立即阻塞等待。
如果池不做等待期间的协助执行，子任务只能留在队列中，父任务又不让出 worker；即使队列锁已经释放也无法前进。
加一个线程可能让这次样例跑通，却不能保证下一层嵌套或下一批父任务安全。
可讨论 continuation、返回后再接续，或独立且有界的执行资源；选择时也要保留背压、取消与身份关联。

**回到当前实现**：[ThreadPool](../common/thread_pool.cpp) 保存 `std::function<void()>`，
Submit 只返回是否接纳，WorkerLoop 在锁外执行并记录异常，没有隐含的 future 结果通道。
因此它既不会替调用方等待子任务，也不会把“已记错误日志”变成“上游已收到失败”。
本项目的业务结果需要继续沿 RPC 回执、消息持久化或 Agent 状态协议核对。

| 观察到的信号 | 目前可以知道 | 仍不能推出 |
|---|---|---|
| Submit 返回 true | 内存队列已接纳 | 已开始、已持久化或必定执行完成 |
| future wait_for 返回 timeout | 此次等待结束时未取得就绪结果 | 工作已取消、远端未执行 |
| future 返回 ready | 已有值或异常可取 | 没有异常、业务成功、外部副作用恰好一次 |
| get 返回一个值 | 正常取到了该共享状态的结果 | 返回值代表成功，或数据库/Kafka 已完成其他阶段 |
| worker 捕获异常并继续循环 | 工作线程可继续处理其他任务 | 本次调用的资源、补偿与错误通知都已完成 |

**面试收束**：用 12.17 亲眼观察一次 timeout 后的成功和一次 ready 后的异常，
再回到 accepted_ack、delivered_ack、Agent unknown，分别说清各自确认的事实。
不要把 C++ future 状态当成持久任务状态；进程重启恢复仍依赖服务端账本和原生执行核实。

<a id="interview-refactoring"></a>
### 11.19 精简实现：怎样减少重复，又保留核心合同？

**Q67：看到三份类似的代码，就应该抽象成一个通用框架吗？**

<details>
<summary>参考回答：先确认输入、输出与失败语义是否相同</summary>

本次三个媒体服务重复实现非负整数解析、允许字段检查和 64 个十六进制字符的摘要格式检查。
它们都是不访问数据库、没有权限副作用的纯函数，因此合并到
[media_json.h](../logic/services/media_json.h)；各服务仍解释自己的操作、身份和错误响应。
`Fields` 只拒绝多余字段，不自动要求所有字段存在；`Hex` 只检查表示形式，不验证实际内容摘要。

迁移时原 context 的参数顺序是 `(fallback, maximum)`，其余两个是 `(maximum, fallback)`。
只替换函数名会悄悄改变分页默认值；还要逐项保留 context 的 JavaScript 安全整数上界。
调用方继续把异常映射为原业务错误，HTTP handler 不因此增加业务规则。

另一处复用是 [appendImage](../web_demo/static/room_media.js)：统一 JPEG Blob、URL 登记、图片创建；
资料预览和固定来源预览各自的异步版本检查留在调用点。关闭时通过原有 clearPreview 回收 URL。
这种抽取有具体的两个以上使用者和共同变化原因，不需要再造策略系统、继承层级或全局框架。

**追问**：为什么不把三个服务的鉴权也一并统一？先列出 user bearer、worker key、持久 Agent request
三种信任入口及不同权限；只有确实相同的规则才复用，不能按“函数看起来相似”消除差异。

</details>

**Q68：哪些检查是冗余，哪些检查即使出现多次也必须保留？**

<details>
<summary>参考回答：看两次检查之间是否可能改变状态</summary>

对同一个局部、不可变 JSON 反复解析通常可以合并；在数据库/文件 IO 前后复查成员权限却处于不同时间点。
调用期间可能发生撤权、切换上下文代次或卸载 Agent。把后一轮检查删掉，可能让过期内容进入响应。
这也是第 11.16 节 TOCTOU 交错的实际应用。

审查时问三件事：检查依据是否会变化？失败会造成哪项具体业务错误？现有正常/失败路径能否证明等价？
保留成员权限、固定来源、幂等决定、租约和最终提交边界；减少重复的字段解析与渲染代码。
对新层次的收益应说明它减少哪一类重复修改，不能用“更安全”代替具体故障说明。

还有一种不能随意改的“重复”：媒体 worker 将自身源码纳入 pipeline 指纹。
即使只是抽取完成请求的重复几行，也会影响已批准配置及旧 pending 状态恢复；本次没有为这点收益进行配置换代。
这是兼容性取舍，不代表所有重复都必须永久保留。

**追问**：测试如何选？纯图片预览检查两条 UI 路径及资源回收；字段解析迁移检查默认值、边界与业务错误；
只有协议、SQL 或可靠性行为变化才扩大到相关真实集成。Mock 结果和真实环境结果分别报告。

</details>

**Q69：Git 标签、镜像 digest、测试回执都有了，为什么还不能说“最新版本全部通过”？**

<details>
<summary>参考回答：源码版本、运行制品和验收输入需要对应</summary>

Git 提交标识一份受版本管理的文件树，分支指向可继续更新的提交；本项目的发行标签用于定位受检源码。
构建后的镜像、安装包及外部模型/Runtime 配置另外影响运行行为，因此 release.json 记录镜像与文件摘要。
历史回执必须保留原环境、输入和范围；把修复后的主分支推到 GitHub，不会自动更新已发布 v0.1.0 的镜像。

本例应分开说：`v0.1.0` 的受检产品提交是 `7f92f3d`；图文协作和整理进入 `a3901af`；
这次手册更新又是一次文档提交。29 项浏览器合同使用模拟 HTTP/WS，不能代替 7 组真实 PDF/Hermes 网页验收。
同样，文档示例编译通过不等于服务消息恢复通过，真实 HTTP 通过也不能推出模型结论准确。

**练习**：选简历里的一条“支持断线恢复”，写下提交、命令、观察到的消息/任务结果和未覆盖场景。
然后解释若只更改手册，为什么不需要重跑模型；若改了消息游标，又为什么要回到持久化和重连链路验证。

</details>

<a id="practice-lab"></a>
## 12. 动手练习：让“学会了”有可检查的结果

下面是学习任务，不代表已经替你运行过一轮产品验收。先做纯内存练习，再做合同测试，最后按现有 harness
建立独立测试环境；数据库、Kafka 重放和备份练习只操作明确属于练习的实例与卷。

### 12.1 一次练习的最小记录

```text
日期 / Git 提交 / 运行环境：
我要验证的命题：
输入、命令和实际退出码：
正常路径观察：
故障或并发路径观察：
源码解释（文件 / 函数）：
尚不能由本实验推出的结论：
60 秒面试复述：
```

复述模板：“我先假设……，用……构造了……，看到……；代码中……解释了结果。
这能证明……，但没有覆盖……”。它比“测试没报错”更能体现工程能力。

### 12.2 六道与项目直接相关的手写题

| 题目 | 输入与预期 | 结构/复杂度 | 必须检查的边界 |
|---|---|---|---|
| 有界 FIFO 去重 | 容量 3，A/B/A/C/D；第二个 A 重复，D 进入时淘汰最早 A | unordered_set + deque，均摊 O(1) | 容量 0；重复不悄悄改为 LRU；并发是否加锁 |
| 连续确认游标 | 先收 3、1、2，连续游标依次 0、1、3 | set 或窗口 bitmap；按窗口选择 | 缺口、重复、safe 上限、整数边界 |
| 线程池 | 多 producer/consumer、有界队列、停止时可 drain | mutex + cv + queue | 满队列拒绝；异常任务；停止后提交；不要锁内执行业务 |
| 精确 LRU | get/put 命中移动到头部，容量满淘汰尾部 | list + unordered_map，平均 O(1) | 容量 0；更新已有键；保存迭代器有效性 |
| 增量协议解析 | 把同一个事件按每个可能字节位置分段输入，最终解析一致 | 缓冲区 + 状态机 | 半个长度头、多个帧同块、超长输入、非法 UTF-8 边界处理 |
| revision 条件更新 | 两人读到 7；第一人更新成功，第二人冲突后重读 | 版本比较，持久化事务 | 不只在前端比较；失败不生成成功历史；授权先校验 |

一个可编译的**教学版**连续游标练习：

```cpp
#include <cassert>
#include <cstdint>
#include <set>

std::uint64_t Advance(std::uint64_t cursor, std::uint64_t safe,
                      const std::set<std::uint64_t>& receipts) {
    while (cursor < safe && receipts.count(cursor + 1) != 0) {
        ++cursor;
    }
    return cursor;
}

int main() {
    std::set<std::uint64_t> receipts;
    receipts.insert(3);
    assert(Advance(0, 3, receipts) == 0);
    receipts.insert(1);
    assert(Advance(0, 3, receipts) == 1);
    receipts.insert(2);
    assert(Advance(0, 3, receipts) == 3);
    assert(Advance(0, 0, receipts) == 0); // 接收早于持久安全水位
    receipts.insert(2);                  // 重复不改变集合
    assert(Advance(1, 2, receipts) == 2); // 不能超过 safe
    assert(Advance(3, 2, receipts) == 3); // 已确认游标不倒退
}
```

本例 O(k log N) 扫过本次可推进的 k 个位置，空间 O(N)。它只用于理解前缀：
生产实现还要按用户/会话隔离、在事务内计算安全范围、处理 sealed 空洞、淘汰 receipt 并复查权限，
不能直接用这个循环替换 SQL 算法。编译后再尝试加入最大整数、空集合与随机乱序测试。

### 12.3 源码寻踪：每条链只交一页笔记

| 路线 | 打开哪些文件 | 找到什么才算完成 |
|---|---|---|
| 一条消息 | Comet → MessageService → reservation → producer → Job | 谁产生 seq，何时 ACK，失败由谁重试 |
| 一个连接关闭 | comet_server.cpp → RedisStore → owner loop | 为什么旧连接不能删除新路由，谁让回调安全退出 |
| 一次成员移除 | RoomService → SpaceRepository → Comet / AttachmentService | 正式成员、在线订阅、附件授权分别怎么失效 |
| 一次 @Pi/@Hermes | room_agent_catalog.cpp → Router → Scheduler → Adapter → 最终回复 | actor、agent、request、Runtime Session 如何关联 |
| 一次任务交接 | tasks/update adapter → RoomAgentService → Repository | revision 冲突在哪检查，历史在哪持久化 |
| 一次结果未知 | Native adapter → AgentStore → recover | 是否新建 Run、是否重复工具、恢复如何更新状态 |

建议先搜索符号再读上下文，示例命令从仓库根目录执行：

```bash
cd /home/peco/cppcode/fenbushi/sparkpush_project
rg -n 'Reserve|PrepareOutbox|SendAndWait|accepted_ack' logic common
rg -n 'CheckRoomAccess|room_revoked|AdvanceDelivered' comet logic
rg -n 'Mentions|request_id|generation|revision' logic/services/room_agent_catalog.cpp
rg -n 'recover_turn|room_control|NativeUnknown' cannbot/scripts/hermes_native_adapter.py
```

### 12.4 从轻量验证逐步到真实环境

已有 `build-wsl` 构建目录时，先列出测试，确认名称和依赖，再选择相关项执行：

```bash
ctest --test-dir build-wsl -N
ctest --test-dir build-wsl --output-on-failure \
  -R '^(queue_capacity_test|message_acceptance_test|space_service_test|room_agent_service_test|generic_room_agent_test|release_contract_test)$'
```

这些选项中包含单元或受控依赖测试，不能拿来宣称真实模型 E2E 通过。没有构建目录时，先按 README 的构建步骤操作；
只想演示产品则使用发行包，不必为运行基础 IM 安装主机 C++ 构建环境。

| 练习 | 现有入口 | 验收观察 |
|---|---|---|
| 并发取号、重复原文、SQL 特殊字符、角色与附件权限 | [run_mysql_review_tests.py](../tests/run_mysql_review_tests.py) | 返回身份/原文一致，越权失败；真实 SQL 用例与 mock 分开 |
| 新旧路由竞争 | [run_redis_review_tests.py](../tests/run_redis_review_tests.py) | 旧连接退出不能删除新 generation |
| Kafka 容器替换 | [run_kafka_volume_test.py](../tests/run_kafka_volume_test.py) | 原消息和消费位点仍在；确认测到的卷身份 |
| 三人 Web 与实际 Agent | [collaboration_web_e2e.py](../tests/collaboration_web_e2e.py) | 独立浏览器、同序消息、真实工具、接手、重启与撤权 |
| 双房间或整批冷备 | [public_room_runtime_e2e.py](../tests/public_room_runtime_e2e.py) | Session/工具记录隔离、同一个原 Run 恢复 |
| 固定制品安装和证据 | [发行指南](releasing.md) | 源码、镜像、包哈希、实际容器与原始回执对应 |

运行前查看入口的参数说明和 fixture 前提。真实 Agent 测试需要专用公共 Runtime、授权模型、浏览器依赖和独立工作目录，
不能把个人 Hermes Profile 直接公开。备份/恢复测试按现有要求串行执行，不能在有业务流量时随意暂停服务。

### 12.5 六个故障推演题

先在纸上写预期，再使用现有测试入口或隔离环境验证；“推演正确”与“已经实测”分别记录。

| 注入位置 | 预期原则 | 应收集的证据 |
|---|---|---|
| SQL reservation 已提交，Kafka 尚未确认 | 同 ID 恢复原文/序号；已 prepared 的事件可重投 | reservation 状态、原始 payload、client_msg_id |
| Kafka 已确认，accepted_ack 丢失 | 客户端同 ID 重试，不创造第二条业务消息 | 最终 seq、历史唯一行、客户端去重 |
| Job 落库延迟、客户端刚重连 | 空同步不应永久漏掉稍后进入 SQL 的尾消息 | 暂停/恢复点、后续补拉、最终有序唯一时间线 |
| 在线成员被移除 | 新授权失败，旧订阅失效，附件和 Agent 同样拒绝 | API/业务码、推送资格、刷新后行为 |
| Native 已接收，SSE/状态回执丢失 | unknown，恢复查询原 Run，不新建执行 | 原 Run ID、POST 次数、工具 transcript 指纹 |
| 容器与 Bridge 冷重启后重投 | 只恢复未确认发布，已完成工具不重复执行 | SQL 历史、执行账本、Kafka 位点、结果身份 |

不要只数“前端出现一条消息”：前端去重可能掩盖服务端重复执行。至少同时观察历史、执行次数和原始身份。

### 12.6 Linux 排障练习：先收集证据，再选优化手段

选自己启动的练习实例，记录时间、进程、构建版本和负载。下面是排查顺序，不是宣称项目已经完成的压测。

| 现象 | 先观察什么 | 需要排除的其他解释 | 回到哪里读源码 |
|---|---|---|---|
| CPU 不高但请求慢 | 线程状态、队列长度、分段耗时、数据库等待 | 会话锁排队、池耗尽、网络等待，未必 CPU 不够 | Submit、ThreadPool、连接池 |
| 一个线程 CPU 很高 | 该线程的采样栈、活跃连接/事件数量 | 反复可写通知、解析循环、序列化，不能仅凭 CPU 判断死循环 | EventLoop、协议解析、消息编码 |
| 内存随请求增长 | RSS 趋势、队列权重、输出缓冲、在途数 | 积压、缓存、分配器保留，未必泄漏 | WorkerLoop、Comet 输出预算 |
| 只有一个大群变慢 | 同会话与跨会话负载对照、锁等待 | 全局瓶颈与热点串行化是两种问题 | KeyedMutex、取号、Kafka 等待 |
| ACK 正常但历史慢 | 同 msg_id 的 Kafka 消费与 SQL 入库时间 | ACK 边界本来早于历史落库 | PersistToTopic、Job persist consumer |

以下只读命令使用练习进程 PID，不读取环境变量或凭据；工具与可见字段取决于本机安装和权限：

```bash
# 把 12345 换成你已确认的练习进程 PID。
SPARK_STUDY_PID=12345
ps -L -p "$SPARK_STUDY_PID" -o pid,tid,stat,pcpu,wchan:32,comm
rg '^(VmRSS|VmSize|Threads|voluntary_ctxt_switches|nonvoluntary_ctxt_switches):' \
  "/proc/$SPARK_STUDY_PID/status"
```

`wchan` 是等待位置的线索，不能单靠一个 futex 名称就判定死锁；正常条件变量等待也可能睡在同类位置。
`/proc/PID/status` 中的上下文切换计数对应该任务，不能直接作为整个线程组的汇总；需要时逐个查看 `/proc/PID/task/TID/status`。
计数要比较时间区间内的差值，CPU 和内存也应在同一负载窗口采样。字段含义见
[Linux /proc 文档](https://docs.kernel.org/filesystems/proc.html)。

这轮练习至少留下一个被证据排除的假设。例如“怀疑 CPU 不够，但多数请求时间耗在同会话锁与外部确认等待，
下一步先加分段计时，而不是直接把线程数翻倍”。尚未测得的阶段占比保持未知。

### 12.7 把一次练习整理成面试材料

选择 11.8 的任一组，交付四件东西：一张时序或所有权图、一段你能逐行解释的代码、
一份包含失败路径的实验记录、一段 90 秒口述。四者必须讲同一个问题。

口述顺序可用：“用户遇到什么 → 我怎样缩小范围 → 修改或验证了什么 → 证据支持什么 → 还不能保证什么”。
若只是阅读与复现，直接说“我通过源码和实验确认了……”。只有自己实际修改并验证后，才改成“我修复了……”。
随后按 7.5 压缩为简历条目，并放入 13.3 的个人贡献表，形成可持续补充的求职材料。

<a id="cpp-interview-experiments"></a>
### 12.8 C++17 小实验：先预测，再编译，最后讲回项目

两段程序各自保存为一个 `.cpp` 文件，只依赖标准库，在自己新建的练习目录执行。
它们验证语言与所有权规则，不连接生产服务，也不代表跑过整个项目的并发回归。

#### A. move、const、返回值与不可复制闭包

保存为 `move_lab.cpp`。先预测复制/移动计数，并回答：为什么 `std::move(c)` 没选移动构造？

```cpp
#include <cassert>
#include <memory>
#include <type_traits>
#include <utility>

struct Probe {
    inline static int copies = 0;
    inline static int moves = 0;
    int value;
    explicit Probe(int n) : value(n) {}
    Probe(const Probe& other) : value(other.value) { ++copies; }
    Probe(Probe&& other) noexcept : value(other.value) {
        other.value = -1; // 本教学类型自己约定的移动后状态
        ++moves;
    }
};

Probe Make() { return Probe(7); }

int main() {
    Probe a(1);
    Probe b(std::move(a));
    const Probe c(2);
    Probe d(std::move(c));
    Probe e = Make();
    assert(a.value == -1 && b.value == 1);
    assert(c.value == 2 && d.value == 2 && e.value == 7);
    assert(Probe::copies == 1 && Probe::moves == 1);

    auto resource = std::make_unique<int>(42);
    auto task = [p = std::move(resource)] { return *p; };
    static_assert(!std::is_copy_constructible_v<decltype(task)>);
    static_assert(std::is_move_constructible_v<decltype(task)>);
    assert(!resource && task() == 42);
}
```

```bash
c++ -std=c++17 -Wall -Wextra -Werror -pedantic move_lab.cpp -o move_lab
./move_lab
```

**预期**：程序正常退出；复制 1 次、移动 1 次。`Make` 返回同类型纯右值，不额外产生这里的复制/移动调用。
`Probe` 的 `-1` 是示例自行规定的，不能推广成所有类型的移动后状态；`unique_ptr` 转移后源指针为空则是其类型合同。

**故意制造编译失败**：添加 `<functional>`，在 `task` 定义后添加
`std::function<int()> wrapped = std::move(task);`。在 C++17 下应因目标不可复制而失败，
而不是“运行后可能出错”。解释这个结果后，对照项目 ThreadPool::Submit 的参数类型，说明你会如何设计任务所有权。
不要为了消除报错就机械改成 shared_ptr，更不要用裸指针绕过资源生命期。

#### B. 按值捕获、弱观察和共享所有权各保证什么

保存为 `callback_lab.cpp`。这里用手动调用模拟“稍后执行”的顺序，不启动线程，避免用随机调度掩盖生命周期问题。

```cpp
#include <cassert>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

int main() {
    std::function<bool()> has_snapshot;
    {
        std::string text = "hello";
        has_snapshot = [text] { return text == "hello"; };
        text = "changed";
    }
    assert(has_snapshot()); // 闭包拥有自己的字符串副本

    auto owner = std::make_shared<int>(9);
    std::weak_ptr<int> weak = owner;
    auto observe = [weak] {
        auto locked = weak.lock();
        return locked ? *locked : -1;
    };
    std::function<int()> keep_alive = [owner] { return *owner; };
    owner.reset();
    assert(observe() == 9 && keep_alive() == 9);
    keep_alive = {}; // 最后一个共享所有者在此释放
    assert(observe() == -1);

    std::string storage = "hello";
    std::string_view borrowed(storage);
    storage[0] = 'j'; // 未改变长度、未使存储失效
    assert(borrowed == "jello"); // view 不是内容快照
}
```

```bash
c++ -std=c++17 -Wall -Wextra -Werror -pedantic callback_lab.cpp -o callback_lab
./callback_lab
```

**预期**：所有断言通过。第一个回调持有值；第二个只在 lock 成功时使用对象；第三个延长生命；view 借用原字符。
这四种关系应能画在一张所有权图中。weak.lock 成功后，局部 shared_ptr 保持本次访问期间的对象存活，
但这段顺序实验没有证明对象字段可被多个线程同时修改。

**继续追问**：将 `has_snapshot` 改成引用捕获会怎样？在原字符串析构后调用它会发生悬空访问，不能要求一个固定输出。
先用生命期规则判错；如自行做 sanitizer 练习，单独保留故意错误的程序，不把“偶然仍输出 hello”当成功。
最后回到 Comet：即使连接被 shared_ptr 保活，操作仍应遵守 owner EventLoop 的线程约束。

**实验交付**：记录编译器版本、命令和退出码，再分别用 60 秒解释“为什么复制”“谁拥有资源”“哪种修改会失效”。
本轮文档校验的实际结果见文件末尾；它与产品 CTest、数据库及真实 Runtime 验收分别计数。

<a id="sql-interview-experiments"></a>
### 12.9 双连接 MySQL 实验：亲眼看到快照、锁与版本冲突

这是一组 **MySQL 8.0 / InnoDB 教学实验**。在独立练习数据库中打开 A、B 两个连接，
按下面顺序执行；不要使用产品业务库。表名有 `interview_` 前缀，字段精简，不是项目 migration。
练习环境可使用独立 MySQL 容器；本节不要求启动 Spark Push、Kafka 或模型。

**准备数据（任一连接执行一次，完成后两边均选择同一个练习库）：**

```sql
CREATE TABLE interview_task (
    task_id BIGINT PRIMARY KEY,
    revision BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL
) ENGINE=InnoDB;
INSERT INTO interview_task VALUES (1, 7, 'queued');
```

**实验 A：普通一致性读和锁定读是否相同？**

先在连接 A 执行，保持事务未提交：

```sql
SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
START TRANSACTION;
SELECT revision, status FROM interview_task WHERE task_id=1;
-- 预期：7 / queued。这个普通 SELECT 建立本次一致性读快照。
```

再在连接 B 执行并明确提交：

```sql
START TRANSACTION;
UPDATE interview_task SET revision=8, status='running'
WHERE task_id=1 AND revision=7;
SELECT ROW_COUNT();
-- 预期：1。
COMMIT;
```

返回连接 A：

```sql
SELECT revision, status FROM interview_task WHERE task_id=1;
-- 预期：7 / queued，仍是旧快照。
SELECT revision, status FROM interview_task WHERE task_id=1 FOR UPDATE;
-- 预期：8 / running，读取最新已提交行并加锁。
COMMIT;
```

**回答重点**：同一个 RR 事务里，不同读取形式可能得到不同结果；不要把“可重复读”理解成所有语句永远返回同一版本。
本例 B 已先提交，因此 A 的锁定读无需等待 B；若 B 未提交，A 的锁定读通常要等待冲突锁释放。
原理见 [一致性非锁定读](https://dev.mysql.com/doc/refman/8.0/en/innodb-consistent-read.html)与
[锁定读](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html)。

**实验 B：旧 revision 能否直接覆盖新进度？**

上个实验完成后，在任一连接执行：

```sql
START TRANSACTION;
UPDATE interview_task SET revision=revision+1, status='completed'
WHERE task_id=1 AND revision=7;
SELECT ROW_COUNT();
-- 预期：0，过期版本未匹配，不能向用户报告更新成功。
SELECT revision, status FROM interview_task WHERE task_id=1;
-- 预期：8 / running。
ROLLBACK;
```

重读后先由用户/业务决定是否仍要完成任务，再以 revision=8 提交新更新；不要机械去掉 WHERE revision 条件。
产品还必须先校验正式成员和任务权限，并在同一事务记录新 revision 与历史。
对应源码：[Room Agent Repository](../logic/repositories/room_agent_repository.cpp)。
`ROW_COUNT()` 要紧接 UPDATE 读取；本例匹配时 revision 必然变化，避免“匹配了但值未变化”干扰判断。

**实验 C：让历史分页有明确的排序锚点。**

```sql
CREATE TABLE interview_message (
    id BIGINT PRIMARY KEY,
    session_id VARCHAR(32) NOT NULL,
    msg_seq BIGINT NOT NULL,
    body VARCHAR(64) NOT NULL,
    UNIQUE KEY uk_session_seq (session_id, msg_seq)
) ENGINE=InnoDB;
INSERT INTO interview_message VALUES
    (1,'group_1',1,'a'), (2,'group_1',2,'b'),
    (3,'group_1',3,'c'), (4,'group_1',4,'d'),
    (5,'group_2',1,'other-room');

EXPLAIN SELECT msg_seq, body FROM interview_message
WHERE session_id='group_1' AND msg_seq<4
ORDER BY msg_seq DESC LIMIT 2;

SELECT msg_seq, body FROM interview_message
WHERE session_id='group_1' AND msg_seq<4
ORDER BY msg_seq DESC LIMIT 2;
-- 预期：3/c、2/b；下一页锚点用最后一条的 msg_seq=2。
```

先解释联合索引顺序，再看实际计划；这里只有 5 行，优化器选择扫描也不能据此断言索引无效。
比较真实性能需扩充数据并固定负载，记录扫描量、返回量和延迟，不能拿这个微型样例证明性能提升。
产品查询入口是 [MessageDao](../logic/message_dao.cpp)，正式成员授权由上层用例完成，SQL 的 session 条件本身不构成授权。

**实验交付**：保留两个连接的执行顺序、SELECT 输出、受影响行数和 EXPLAIN；用两分钟解释
“为什么先读到 7 又读到 8”“为什么更新 0 行不是成功”“为什么索引不负责权限”。
本文末尾将实际执行范围单列，教学实验不计入产品集成验收。

<a id="cpp-dispatch-experiment"></a>
### 12.10 C++17 实验：虚派发、名字查找与完美转发

先写出每个调用的预期值，再把下面完整程序保存到独立练习目录中的 `dispatch_forward.cpp`。
它没有产品服务依赖，也不修改业务库。

```cpp
#include <cassert>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

struct Repository {
    virtual ~Repository() = default;
    virtual int Role(int) const { return 1; }
    int Role(double) const { return 2; }
};

struct SqlRepository final : Repository {
    using Repository::Role;
    int Role(int) const override { return 3; }
};

int Kind(std::string&) { return 1; }
int Kind(const std::string&) { return 2; }
int Kind(std::string&&) { return 3; }

template<class T>
int Relay(T&& value) {
    return Kind(std::forward<T>(value));
}

template<class T>
int Named(T&& value) {
    return Kind(value);
}

int main() {
    static_assert(std::has_virtual_destructor_v<Repository>);
    SqlRepository concrete;
    Repository& base = concrete;
    assert(base.Role(1) == 3);        // 虚派发到派生实现。
    assert(concrete.Role(1.5) == 2); // using 保留基类 double 重载。
    auto owned = std::unique_ptr<Repository>(std::make_unique<SqlRepository>());
    assert(owned->Role(1) == 3);     // 经基类拥有，销毁需符合多态合同。

    std::string s = "message";
    const std::string cs = "history";
    assert(Relay(s) == 1);
    assert(Relay(cs) == 2);
    assert(Relay(std::string("temporary")) == 3);
    assert(Named(std::string("temporary")) == 1);
    assert(Relay(std::move(cs)) == 2); // const 没有因 move 消失。
}
```

```bash
g++ -std=c++17 -Wall -Wextra -Werror -pedantic dispatch_forward.cpp -o dispatch_forward
./dispatch_forward
```

不带 `-DNDEBUG` 编译；程序正常完成退出 0，没有标准输出。这里的 Kind 只选择重载、不转移字符串，
因此调用 `std::move` 的现象不能被误读成已经发生内存搬迁。

做两次有目的的改动，每次从原始示例重新开始：

1. 只删掉派生 Role 的 const、保留 override：预期编译失败，诊断指出没有重写。不要通过删除 override 掩盖问题。
2. 将 Relay 的 `std::forward<T>(value)` 改成 `value`：编译仍可通过，但临时字符串的断言失败。
   解释为什么“有名字的参数表达式是左值”，以及哪些调用不受这次改动影响。

项目映射：Repository 的运行时多态决定调用哪个实现；模板转发决定如何传递值类别；
对象是否存活、闭包能否保存、线程间是否同步仍是额外问题。能分别回答，才算把语言八股用到了工程判断中。
故意失败的实验在独立目录运行；预期失败是学习结果，不计入产品测试通过数。

<a id="fifo-dedup-experiment"></a>
### 12.11 C++17 实验：有界 FIFO 去重，怎样证明不是 LRU？

这是**单线程、只保存已完成 ID 的教学窗口**。Remember 返回 true 表示该 ID 此刻不在窗口，false 表示重复；
它不执行请求，也不预占 in-flight。空 ID 和容量 0 都选择放行且不记忆，这是一项明确的示例策略。
保存为独立练习目录中的 `fifo_dedup.cpp`。先预测容量 3 时 A、B、A、C、D、A 的返回结果和队列内容。

```cpp
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <deque>
#include <random>
#include <string>
#include <unordered_set>

class RecentIds {
public:
    explicit RecentIds(std::size_t capacity) : capacity_(capacity) {}

    bool Remember(const std::string& id) {
        if (capacity_ == 0 || id.empty()) return true;
        const auto inserted = seen_.insert(id);
        if (!inserted.second) return false; // 命中不改变 FIFO 顺序。
        try {
            order_.push_back(id);
        } catch (...) {
            seen_.erase(inserted.first);   // 撤销本次登记，不吞掉异常。
            throw;
        }
        if (order_.size() > capacity_) {
            seen_.erase(order_.front());
            order_.pop_front();
        }
        return true;
    }

    std::size_t size() const { return order_.size(); }

private:
    std::size_t capacity_;
    std::unordered_set<std::string> seen_;
    std::deque<std::string> order_;
};

// 刻意用线性查找的简单模型作对照，不复制待测类的双容器实现。
bool Reference(std::deque<std::string>& ids, std::size_t cap,
               const std::string& id) {
    if (cap == 0 || id.empty()) return true;
    if (std::find(ids.begin(), ids.end(), id) != ids.end()) return false;
    ids.push_back(id);
    if (ids.size() > cap) ids.pop_front();
    return true;
}

int main() {
    RecentIds recent(3);
    assert(recent.Remember("A"));
    assert(recent.Remember("B"));
    assert(!recent.Remember("A")); // 仍是 A、B，A 没移到尾部。
    assert(recent.Remember("C"));
    assert(recent.Remember("D")); // 淘汰 A，留下 B、C、D。
    assert(recent.Remember("A")); // A 已被淘汰，允许再次记忆。
    assert(recent.size() == 3);
    assert(recent.Remember(""));
    assert(recent.size() == 3);

    const std::string binary_id("req\0x", 5);
    RecentIds binary(2);
    assert(binary.Remember(binary_id));
    assert(binary.Remember("req"));
    assert(!binary.Remember(binary_id)); // string 的长度参与身份。
    RecentIds restarted(2);
    assert(restarted.Remember(binary_id)); // 新对象不会继承旧窗口。

    std::mt19937 rng(20261101);
    for (const std::size_t cap : {0u, 1u, 3u, 32u}) {
        RecentIds actual(cap);
        std::deque<std::string> expected;
        for (int i = 0; i < 10000; ++i) {
            const auto n = rng() % 41;
            const std::string id = n == 0 ? "" : "id-" + std::to_string(n);
            const bool observed = actual.Remember(id);
            const bool wanted = Reference(expected, cap, id);
            assert(observed == wanted);
            assert(actual.size() == expected.size());
            assert(actual.size() <= cap);
        }
    }
}
```

```bash
g++ -std=c++17 -Wall -Wextra -Werror -pedantic fifo_dedup.cpp -o fifo_dedup
./fifo_dedup
```

正常退出 0、没有输出；不要加 `-DNDEBUG`，本练习依赖 assert。4 种容量各比较 10000 次，属于确定种子的差分验证，
不是随机压测。它核对正常输入下的策略、容量和返回值；没有模拟 bad_alloc、并发访问、进程崩溃或网络故障。
字符串内含零字节仅用于检查容器的身份比较，不表示产品允许这种 request_id。

**口述验收**：能说出两个容器保存同一组 ID 的不变量；在 ID 长度有界时，哈希操作平均常数时间，
碰撞与 rehash 会改变单次成本，实际还要计入字符串长度；窗口保存 O(capacity) 个 ID，字节量取决于长度。
代码固定使用 string 和标准哈希，不能把它的异常处理推导到会抛异常的任意自定义 Hash。

**三轮追问**：把命中移到尾部为何变成另一种策略？多线程时为什么要保护整次操作而不是分别保护两个容器？
窗口记住一个 ID，为何不能证明对应的外部工具已经且仅执行一次？最后一问应回到 in-flight、持久账本和 unknown。

<a id="cas-version-experiment"></a>
### 12.12 C++17 实验：CAS 失败、ABA 与协作任务版本

先预测四个结果：冲突是否成功、失败后的 expected 是多少、值恢复为 7 后旧 CAS 是否成功、附带代次后是否仍成功。
保存为独立的 `cas_version.cpp`；这是确定性交错的教学模型，不启动产品，不模拟真实数据库或线程调度。

```cpp
#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>

constexpr std::uint64_t tagged(std::uint32_t generation, std::uint32_t value) {
    return (std::uint64_t{generation} << 32) | value;
}

int main() {
    std::atomic<int> revision{7};
    int stale = revision.load();              // B 记住版本 7。
    int current = 7;
    const bool first = revision.compare_exchange_strong(current, 8); // A 提交。
    assert(first);

    const bool conflict = revision.compare_exchange_strong(stale, 9);
    assert(!conflict);
    assert(stale == 8);                       // 失败会更新 expected。
    assert(revision.load() == 8);
    // 不把旧业务正文拿来直接再提交；调用方需要重读并解决内容冲突。

    std::atomic<int> value{7};
    int old = value.load();
    value.store(8);                           // 模拟其他执行者的两次修改。
    value.store(7);
    const bool aba = value.compare_exchange_strong(old, 9);
    assert(aba);                             // 只看值，检测不到 7 -> 8 -> 7。

    std::atomic<std::uint64_t> versioned{tagged(1, 7)};
    auto before = versioned.load();
    versioned.store(tagged(2, 8));
    versioned.store(tagged(3, 7));
    const bool accepted =
        versioned.compare_exchange_strong(before, tagged(4, 9));
    assert(!accepted);                       // 值相同，但代次不同。
    assert(before == tagged(3, 7));
    std::cout << "conflict=" << conflict << " expected=" << stale
              << " aba=" << aba << " tagged=" << accepted << '\n';
}
```

```bash
g++ -std=c++17 -O2 -Wall -Wextra -Werror -pedantic -pthread cas_version.cpp -o cas_version
./cas_version
```

预期输出：`conflict=0 expected=8 aba=1 tagged=0`。示例使用默认 seq_cst，目的是观察比较和版本身份，
没有演示弱内存序发布、指针回收或无锁性能。也不保证所有平台的 64 位 atomic 都无锁；示例代次假定不回绕。
操作放在 assert 外，避免把断言关闭时消失的表达式当成实际执行；教学验证时仍须开启断言。

**反例练习**：仅把 `versioned.store(tagged(3, 7))` 改为 `versioned.store(tagged(1, 7))`，
等于复用旧代次；预期 `assert(!accepted)` 失败。这说明版本有效性依赖整个更新协议，不能只给字段起名为 version。

**讲回项目**：找出 SaveTask 的条件 UPDATE 和受影响行数检查，再解释为什么 HTTP 冲突不能直接换成最新 revision 重发旧正文。
该 SQL 并发条件并非 `std::atomic` 的跨进程用法；两者共享的是“带前提修改、冲突可观察”的思路。
成员授权仍是独立条件，版本匹配不会授予权限；这个程序也不能充当产品撤权、数据库并发或重启验收。

### 12.13 一次 90 分钟源码学习：交付一页图和三段口述

不再从头通读三千多行文档。选一个主题，按 15 分钟原理、25 分钟源码、20 分钟推演/实验、
15 分钟口述、15 分钟复盘安排一次学习。下面每行可以独立完成，首次优先选 A。

| 主题 | 源码阅读顺序 | 必须主动改变的条件 | 本次合格产物 |
|---|---|---|---|
| A. 两个人修改同一任务 | UpdateTask → SaveTask → room_collaboration_task_history | 两人同 revision；一个人已被移除；更新响应丢失 | 事务交错图；指出冲突与授权的检查位置；注明响应丢失后的合同仍要单独核实 |
| B. gRPC 写成功却没有业务确认 | Comet StreamWriterLoop → Job 流读写 → pending/request_id | Write 成功后断线；对端读得慢；同请求重投 | 四层等待图；说明每一层成功能证明什么 |
| C. 旧 worker 恢复后继续提交 | Claim → Lock/Live → Stage/Complete → LeaseKeeper | 旧租约过期；新 attempt 已领取；Complete 回执丢失 | 两执行者时间线；区分新写入与原终态回执恢复 |

从仓库根目录使用只读定位命令，打开命中的函数而不是只背路径：

```bash
rg -n 'UpdateTask|SaveTask' logic/services/room_task_service.cpp logic/repositories/room_agent_repository.cpp
rg -n 'StreamWriterLoop|StreamReaderLoop|delivered_count|pending' comet/comet_server.cpp job/service.cpp
rg -n 'Claim\(|Live\(|Complete\(|class LeaseKeeper' logic/repositories/media_processing_repository.cpp logic/repositories/media_artifact_repository.cpp cannbot/scripts/media_worker.py
```

结束时合上文档，分别录制三段：30 秒解释问题、90 秒说明实现与取舍、60 秒描述反例和证据。
每段按 0～2 分自评：0＝概念混淆，1＝原理正确但没有项目依据，2＝能给函数、失败点和验证边界。
有一段为 0 就回看对应代码；总分仅供自测，不代表招聘通过率。

本轮仅阅读就写“源码推演”；跑了上面的 C++ 程序就写“教学实验”；实际运行依赖测试后才填写相应结果。
将这一页和第 7.6 节的简历证据卡放在一起：面试时讲自己完成的闭环，尚未执行的故障注入留在下一步计划。

<a id="queue-close-experiment"></a>
### 12.14 C++17 双线程实验：入队成功后，任务一定执行吗？

这次使用真实的两个线程，但用 promise/future 控制关键交错，不靠 sleep 猜测 worker 已运行到哪里。
保存为 `queue_close.cpp`。这是整数任务的教学队列，只讨论容量、关闭与排空，不替代项目线程池。

先预测：队列容量为 1，任务 1 已被 worker 取走但暂未完成，此时能否接受任务 2？
关闭后任务 3 能否入队？关闭选择 drain 或 discard 时，前两个已接纳任务分别完成几个？

```cpp
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

class IntQueue {
public:
    explicit IntQueue(std::size_t capacity) : capacity_(capacity) {}

    bool TryPush(int item) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_ || items_.size() >= capacity_) return false;
            items_.push(item);
        }
        cv_.notify_one();
        return true;
    }

    std::optional<int> WaitPop() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return closed_ || !items_.empty(); });
        if (items_.empty()) return std::nullopt;
        const int item = items_.front();
        items_.pop();
        return item;
    }

    void Close(bool drain) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) return; // 首次关闭策略固定，后续调用不改变它。
            closed_ = true;
            if (!drain) {
                std::queue<int> empty;
                items_.swap(empty);
            }
        }
        cv_.notify_all();
    }

private:
    const std::size_t capacity_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<int> items_;
    bool closed_{false};
};

void CheckClose(bool drain) {
    IntQueue queue(1);
    const bool first = queue.TryPush(1);
    const bool full = queue.TryPush(9);
    assert(first && !full);

    std::promise<void> entered, release, done;
    auto entered_future = entered.get_future();
    auto release_future = release.get_future();
    auto done_future = done.get_future();
    std::vector<int> executed; // 仅 worker 写，主线程在 join 后读取。
    std::thread worker([&] {
        while (const auto item = queue.WaitPop()) {
            if (*item == 1) {
                entered.set_value();
                release_future.wait(); // 门尚未打开，任务 1 无法完成。
            }
            executed.push_back(*item);
        }
        done.set_value();
    });

    entered_future.wait();             // 确认任务 1 已出队。
    const bool second = queue.TryPush(2);
    assert(second);
    queue.Close(drain);
    queue.Close(drain);
    const bool after_close = queue.TryPush(3);
    assert(!after_close);
    const auto state = done_future.wait_for(std::chrono::seconds(0));
    assert(state == std::future_status::timeout); // 关闭并未停止任务 1。

    release.set_value();
    worker.join();                    // 先收回线程，再检查结果和析构队列。
    const std::vector<int> expected = drain ? std::vector<int>{1, 2}
                                           : std::vector<int>{1};
    assert(executed == expected);
    std::cout << "drain=" << drain << " accepted=2 completed="
              << executed.size() << '\n';
}

int main() {
    IntQueue zero(0);
    const bool accepted = zero.TryPush(1);
    assert(!accepted);
    zero.Close(true);
    const auto empty = zero.WaitPop();
    assert(!empty);
    CheckClose(true);
    CheckClose(false);
}
```

```bash
g++ -std=c++17 -O2 -Wall -Wextra -Werror -pedantic -pthread queue_close.cpp -o queue_close
timeout 10s ./queue_close
```

预期输出如下；断言须保持开启。timeout 是 Linux 教学检查的外部看门狗，不是队列提供的取消能力。

```text
drain=1 accepted=2 completed=2
drain=0 accepted=2 completed=1
```

**为什么实验有意义**：同步门保证任务 2 在关闭时仍未执行，因此两个分支的差异来自关闭策略，不是碰巧的调度顺序。
容量只计算排队项，任务 1 出队后释放一个槽位；运行中的资源仍占内存。notify 不保存任务，任务存在 items_ 中。
主线程在 join 后读 executed，避免与 worker 的写入竞争；整个对象存活到 worker 退出。

**改错练习**：把 WaitPop 中的 `if (items_.empty())` 改成 `if (closed_)`。
此时 drain=true 也会跳过任务 2，结果断言应失败。反例验证的是退出条件，不能据此证明任意线程调度都正确。

**回到产品的三项作业**：找出 ThreadPool::Submit 的四个准入条件；指出 queued_bytes_ 何时扣减；
沿 Comet 的 unary 分支找到真正发送 accepted_ack 的位置。另画双向流分支，不把教学 queue 当成消息持久化队列。
本实验没有覆盖多生产者压力、内存分配失败、异常任务、并发析构或真实网络取消；这些应另设验证。

<a id="authorized-pagination-experiment"></a>
### 12.15 C++17 实验：先过滤还是先分页，会影响哪些结果？

这是**纯内存教学模型**，用于验证结果集合与游标算法，不是 MySQL 执行计划、真实鉴权或并发撤权测试。
Scope 假设已由服务端授权，Row 的 eligible 是预先算好的资格；实际产品必须从 canonical request 和权威来源计算。
输入按 seq 严格降序，实验期间不修改已有行；消息上界不能被误称为任意动态数据集的快照。

先预测三件事：最近两条都不合格时，第一页是否应为空？多读的一条用作游标会不会漏数据？
另一个房间的记录和上界之外的新消息能否通过按 ID 读取？再将原文保存为 `/tmp/authorized_page.cpp`：

```cpp
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <stdexcept>
#include <vector>

struct Row { int room; int seq; bool eligible; };
struct Scope { int room; int ceiling; };
struct Page { std::vector<Row> rows; bool more = false; int before = 0; };

bool Allowed(const Row& row, const Scope& scope) {
    return row.room == scope.room && row.seq > 0 &&
           row.seq <= scope.ceiling && row.eligible;
}

Page ReadPage(const std::vector<Row>& rows, Scope scope,
              int before, std::size_t limit) {
    if (limit == 0 || limit > 20) throw std::invalid_argument("limit");
    Page page;
    for (const auto& row : rows) {
        if (row.seq >= before || !Allowed(row, scope)) continue;
        page.rows.push_back(row);
        if (page.rows.size() == limit + 1) break;
    }
    page.more = page.rows.size() > limit;
    if (page.more) page.rows.pop_back(); // 额外一条只用于判断 more。
    page.before = page.rows.empty() ? before : page.rows.back().seq;
    return page;
}

bool ReadOne(const std::vector<Row>& rows, Scope scope, int seq) {
    return std::any_of(rows.begin(), rows.end(), [&](const Row& row) {
        return row.seq == seq && Allowed(row, scope);
    });
}

int main() {
    const Scope scope{1, 101};
    const std::vector<Row> rows{{1, 103, true}, {2, 102, true},
        {1, 101, false}, {1, 100, false}, {1, 99, true},
        {1, 98, false}, {1, 97, true}, {2, 96, true}, {1, 95, true}};

    // 故意错误的策略：按房间/上界取两条，再过滤来源资格。
    std::vector<Row> wrong;
    for (const auto& row : rows) {
        if (row.room != scope.room || row.seq > scope.ceiling) continue;
        wrong.push_back(row);
        if (wrong.size() == 2) break;
    }
    wrong.erase(std::remove_if(wrong.begin(), wrong.end(),
        [](const Row& row) { return !row.eligible; }), wrong.end());
    assert(wrong.empty());

    const auto first = ReadPage(rows, scope, 102, 2);
    assert(first.rows.size() == 2 && first.more);
    assert(first.rows[0].seq == 99 && first.rows[1].seq == 97);
    const auto second = ReadPage(rows, scope, first.before, 2);
    assert(second.rows.size() == 1 && second.rows[0].seq == 95 && !second.more);
    assert(ReadOne(rows, scope, 99));
    assert(!ReadOne(rows, scope, 103)); // 未来消息。
    assert(!ReadOne(rows, scope, 96));  // 其他房间。
    assert(!ReadOne(rows, scope, 101)); // 不合格来源。
    bool rejected = false;
    try { (void)ReadPage(rows, scope, 102, 0); }
    catch (const std::invalid_argument&) { rejected = true; }
    assert(rejected);

    // 8 条记录的全部资格组合；与“一次过滤全量”的参考结果比较。
    for (unsigned mask = 0; mask < 256; ++mask) {
        std::vector<Row> sample;
        std::vector<int> expected;
        for (int seq = 8; seq >= 1; --seq) {
            const bool eligible = (mask & (1u << (seq - 1))) != 0;
            sample.push_back({1, seq, eligible});
            if (eligible) expected.push_back(seq);
        }
        for (std::size_t limit = 1; limit <= 4; ++limit) {
            std::vector<int> actual;
            int before = 9;
            for (;;) {
                const auto page = ReadPage(sample, {1, 8}, before, limit);
                for (const auto& row : page.rows) actual.push_back(row.seq);
                if (!page.more) break;
                assert(!page.rows.empty() && page.before < before);
                before = page.before;
            }
            assert(actual == expected);
        }
    }
    std::cout << "wrong=0 first=99,97 second=95 exhaustive=1024\n";
}
```

```bash
g++ -std=c++17 -O2 -Wall -Wextra -Werror -pedantic \
  /tmp/authorized_page.cpp -o /tmp/authorized_page
/tmp/authorized_page
```

这里断言保持开启，不加 `-DNDEBUG`。输出中的 1024 是 256 种资格组合乘 4 个页面大小，
它验证静态数据下的分页集合，不代表穷举了产品并发或所有 SQL 行为。
算法每页从头扫描 vector，空间 O(limit)、最坏扫描 O(N)；这不是数据库索引访问的性能实现。

完成后做三次口述或修改：

1. 把“空页”当成“没有授权内容”，指出最早被错过的 seq=99；解释错误来自运算顺序，而非 vector 本身。
2. 故意用额外探测行作为 before，观察 seq=95 被跳过；说明 cursor 必须以最后一个实际交付项为准。
3. 删除 ReadOne 的 Allowed 检查，说明为何“搜索没泄漏”不能证明直读安全；按 Q60 再画一次读取中的撤权。

实验结束只需交一页：预期/实际输出、一个失败断言、产品对应函数、尚未验证的边界。
如继续查数据库，使用独立测试实例核对真实 SQL、EXPLAIN 和锁等待；不能把本实验的通过替代现有 HTTP/MySQL 用例。

<a id="framing-experiment"></a>
### 12.16 C++17 实验：相同字节流换 256 种到达方式，结果还能一样吗？

先写合同，再遮住参考代码自己实现。教学协议只有“2 字节大端无符号长度 + 正文”，正文最多 16 字节，允许零长度与 NUL。
它不是 Spark Push 的 WebSocket 协议，不含 mask、opcode、UTF-8 或鉴权；目的只是练习增量解析与 C++ 边界。

- `NeedMore`：缺头部或正文，消费 0 字节；`Invalid`：长度越界，消费 0 字节并使该解码器永久拒绝后续输入。
- `Ready`：交付一个拥有正文的结果，只消费这一帧；前面已交付的完整帧不会因后面的非法帧而回滚。
- 输入结束时只在没有半帧、没有错误的情况下通过完整性检查；零字节的空流有效。
- 输入按字节推进，使解析缓冲最多为 18 字节；收集结果的 vector 由调用方持有，**没有**因此变成有界业务队列。

保存为 `/tmp/frame_decoder.cpp`。先预测“一条半帧随后 EOF”和“一个合法帧随后非法长度”分别会留下什么。

```cpp
#include <cassert>
#include <climits>
#include <cstddef>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

static_assert(CHAR_BIT == 8, "This exercise uses 8-bit octets");
constexpr std::size_t kHeader = 2;
constexpr std::size_t kMaxBody = 16;
enum class State { NeedMore, Ready, Invalid };
struct Result {
    State state = State::NeedMore;
    std::size_t consumed = 0;
    std::string body;
};

Result ParseOne(std::string_view input) {
    if (input.size() < kHeader) return {};
    const auto hi = static_cast<unsigned char>(input[0]);
    const auto lo = static_cast<unsigned char>(input[1]);
    const std::size_t length = (std::size_t{hi} << 8) | std::size_t{lo};
    if (length > kMaxBody) return {State::Invalid, 0, {}};
    if (input.size() - kHeader < length) return {};
    return {State::Ready, kHeader + length,
            std::string(input.data() + kHeader, length)};
}

class Decoder {
public:
    bool Feed(std::string_view chunk, std::vector<std::string>& output) {
        if (failed_) return false;
        for (char byte : chunk) {
            pending_.push_back(byte);
            assert(pending_.size() <= kHeader + kMaxBody);
            auto result = ParseOne(pending_);
            if (result.state == State::Invalid) {
                failed_ = true;
                return false;
            }
            if (result.state == State::Ready) {
                // 按字节推进，因此此刻缓冲内恰好只有一个完整帧。
                assert(result.consumed == pending_.size());
                output.push_back(std::move(result.body));
                pending_.clear();  // output 拥有正文，不借用这里的存储。
            }
        }
        return true;
    }
    bool CompleteAtEof() const { return !failed_ && pending_.empty(); }
private:
    std::string pending_;
    bool failed_ = false;
};

std::string Encode(std::string_view body) {
    assert(body.size() <= 65535);  // 编码助手允许构造超过业务上限的测试输入。
    std::string frame;
    frame.push_back(static_cast<char>((body.size() >> 8) & 0xff));
    frame.push_back(static_cast<char>(body.size() & 0xff));
    if (!body.empty()) frame.append(body.data(), body.size());
    return frame;
}

int main() {
    const std::vector<std::string> expected{"A", std::string("\0B", 2), ""};
    const auto stream = Encode(expected[0]) + Encode(expected[1]) + Encode(expected[2]);
    assert(stream.size() == 9);
    assert(Encode({}) == std::string(2, '\0'));
    const std::string_view input(stream);
    const std::size_t cases = std::size_t{1} << (stream.size() - 1);
    for (std::size_t cuts = 0; cuts < cases; ++cuts) {
        Decoder decoder;
        std::vector<std::string> output;
        std::size_t start = 0;
        assert(decoder.Feed({}, output));
        for (std::size_t i = 1; i <= input.size(); ++i) {
            const bool end = i == input.size();
            if (end || (cuts & (std::size_t{1} << (i - 1)))) {
                assert(decoder.Feed(input.substr(start, i - start), output));
                start = i;
            }
        }
        assert(decoder.CompleteAtEof() && output == expected);
    }

    const auto complete = Encode("XYZ");
    for (std::size_t n = 0; n < complete.size(); ++n) {
        const auto result = ParseOne(std::string_view(complete).substr(0, n));
        assert(result.state == State::NeedMore && result.consumed == 0);
        Decoder decoder;
        std::vector<std::string> output;
        assert(decoder.Feed(std::string_view(complete).substr(0, n), output));
        assert(output.empty() && decoder.CompleteAtEof() == (n == 0));
    }
    auto first = ParseOne(stream);
    assert(first.state == State::Ready && first.consumed == 3 && first.body == "A");
    std::string owned = Encode("own");
    auto copied = ParseOne(owned);
    owned[2] = 'X';
    assert(copied.body == "own");

    Decoder boundary;
    std::vector<std::string> output;
    assert(boundary.Feed(Encode(std::string(kMaxBody, 'x')), output));
    assert(boundary.CompleteAtEof() && output == std::vector<std::string>{std::string(16, 'x')});
    for (const auto& bad : {Encode(std::string(17, 'x')).substr(0, 2), std::string("\xff\xff", 2)}) {
        Decoder decoder;
        output.clear();
        assert(decoder.Feed(Encode("ok"), output));
        assert(!decoder.Feed(bad, output));  // 仅收到非法头部就拒绝，不等正文。
        assert(!decoder.Feed(Encode("later"), output));
        assert(!decoder.CompleteAtEof() && output == std::vector<std::string>{"ok"});
    }
    const auto wrapped = std::numeric_limits<std::size_t>::max() + std::size_t{2};
    assert(wrapped == 1);  // 无符号回绕是定义好的行为，错误检查仍可能被绕过。
    std::cout << "segmentations=" << cases << " boundary/eof/ownership=ok\n";
}
```

```bash
g++ -std=c++17 -O2 -Wall -Wextra -Werror -pedantic \
  /tmp/frame_decoder.cpp -o /tmp/frame_decoder
/tmp/frame_decoder
```

预期输出：`segmentations=256 boundary/eof/ownership=ok`。保持断言开启，不加 `-DNDEBUG`。
9 字节之间有 8 个可切分位置，每处“切/不切”共 256 种；包含逐字节和一次全部到达。
这是对这条短流的切分穷举，**不是**对任意网络输入、时序或 WebSocket 协议的穷举。

在分配成功的前提下，算法对输入总字节数是 O(N)，正文各复制一次，解析暂存 O(最大帧长)，
调用方保存的所有结果另计。它没有模拟内存分配失败、真实 socket、并发回调、超时或生产队列背压。
实际产品可以批量追加与扫描以降低调用开销，但先要维持相同的消费、所有权和预算合同。

做两个**有定义行为的错误修改**，不要通过悬空访问来追求“必然崩溃”：

1. 将半正文分支的 `return {};` 改为 `return {State::Ready, input.size(), {}};`。
   观察完整流的输出对比或前缀断言失败：丢弃半帧会破坏下一次解析。
2. 删除 `length > kMaxBody` 的拒绝分支。观察仅有超限头部时 `!decoder.Feed(bad, output)` 失败：
   程序错误地把非法输入当成需要更多字节；本测试不会继续提供巨量正文。

最后回到 Comet：解释它在哪里限制负载、何时 retrieve、何时拥有 payload，以及为什么本实验通过不代表 WebSocket 鉴权和消息恢复通过。

<a id="future-result-experiment"></a>
### 12.17 C++17 实验：等待超时以后，任务还能成功或者抛异常吗？

**先预测**：worker 已开始、但被同步门挡住时，`wait_for(0ms)` 返回什么？打开门后，
成功和抛异常两个任务的 future 是否都会 ready？调用 get 后还可不可以再次读取同一个 future？

这个程序使用真实 std::thread、promise 和 packaged_task，故意不用 sleep 猜测线程先后。
started 通知保证任务已经进入，gate 保证第一次检查时任务还不能完成；零等待只检查状态，不做超时精度测量。
它是标准库实验，不使用产品线程池，也不连接 MySQL、Kafka 或模型服务。

```cpp
#include <cassert>
#include <chrono>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

void observe(bool fail) {
    std::promise<void> started;
    auto entered = started.get_future();
    std::promise<void> release;
    auto gate = release.get_future().share();

    std::packaged_task<int()> task([&started, gate, fail] {
        started.set_value();
        gate.wait();
        if (fail) throw std::runtime_error("controlled failure");
        return 42;
    });
    auto result = task.get_future();
    std::thread worker(std::move(task));

    entered.wait();
    const auto before = result.wait_for(std::chrono::milliseconds(0));
    assert(before == std::future_status::timeout);
    release.set_value();  // timeout 并没有取消任务，显式打开同步门。
    worker.join();
    const auto after = result.wait_for(std::chrono::milliseconds(0));
    assert(after == std::future_status::ready);

    int value = 0;
    bool caught = false;
    try {
        value = result.get();
    } catch (const std::runtime_error& error) {
        caught = true;
        assert(std::string(error.what()) == "controlled failure");
    }
    assert(caught == fail);
    assert(fail || value == 42);
    assert(!result.valid());  // 不再对这个 future 调用 get/wait。
    std::cout << "timeout -> ready -> "
              << (caught ? "exception" : "value=42") << '\n';
}

int main() {
    observe(false);
    observe(true);
}
```

将代码单独保存为 `/tmp/future_result_demo.cpp`，再编译运行；不需要改动仓库构建文件：

```bash
g++ -std=c++17 -O2 -Wall -Wextra -Werror -pedantic -pthread /tmp/future_result_demo.cpp -o /tmp/future_result_demo
timeout 5s /tmp/future_result_demo
```

预期输出：

```text
timeout -> ready -> value=42
timeout -> ready -> exception
```

**如何解释结果**：`packaged_task` 在调用中保存值或异常，因此第二次任务抛出异常不会作为未捕获异常逃出线程入口；
异常在主线程 get 时重新抛出。换成直接执行且不捕获异常的普通 std::thread 函数，不会自动获得这种行为。
任务闭包按值持有 gate，started 被引用捕获，但主线程在局部对象销毁前已 join；逐个解释这些生命周期。

**再做一个错误变体**：只删除 `if (fail) throw ...` 这一行，重新编译；第二次 observe 应在 `caught == fail` 处失败。
这证明实验会检查失败结果的传播，不是只检查线程是否结束。保留断言，禁止加 `-DNDEBUG`；
不要把 `release.set_value()` 或其他必须执行的动作塞到 assert 表达式里。

**讲回项目**：对照 Q66 的五种信号，写出“入队 → 开始 → 本地结果 → 业务受理 → 持久化”的区别。
本实验没有模拟进程崩溃、线程池嵌套等待或远端副作用，不能用它证明产品取消、死锁恢复或 exactly-once。

<a id="interview-rehearsal"></a>
## 13. 模拟面试、复盘与个人贡献

### 13.1 一场 45 分钟模拟面试

| 时间 | 面试官任务 | 候选人必须讲清 |
|---|---|---|
| 0～3 分钟 | 介绍项目和个人工作 | 用户场景、负责范围、两项核心决策 |
| 3～11 分钟 | 画正常发送与断线恢复 | 两种 ACK、SQL/Kafka 边界、连续游标 |
| 11～19 分钟 | 追问 C++ 与网络 | 所有权、条件变量、owner loop、部分读写 |
| 19～26 分钟 | 追问数据库与消息队列 | 行锁、幂等键、Outbox、offset 提交窗口 |
| 26～33 分钟 | 设计私有空间与多 Agent | 正式成员、结构化 ID、Session 隔离、unknown |
| 33～38 分钟 | 手写连续游标或有界去重 | 正确性、复杂度、容量/重复/缺口边界 |
| 38～43 分钟 | 一个真实 Bug 和测试证据 | 现象、假设、修复、验证、未覆盖范围 |
| 43～45 分钟 | 总结取舍和下一步 | 哪个风险优先解决，为什么不是先加更多功能 |

让同学随机挑题，自己不要事先指定只问熟悉的章节。录音后检查：是否混淆身份、是否把框架能力认成自研、
是否只讲正常路径、是否回避失败和边界。

### 13.2 四个值得准备的深挖故事

每个故事写成“现象 → 假设 → 定位 → 修复 → 验证 → 边界”，不要只背 STAR 的标签。

**故事 A：已经重连，为什么最后一条消息仍然不见？**

实时投递、Kafka 确认和 SQL 入库不是同一时刻。客户端重连时若历史暂为空，不能据此永远停止补偿。
结合真实暂停 Job 的用例，解释尾消息如何在 SQL 稍后完成后自动恢复，以及如何检查只有一份且顺序一致。
证据入口：[Web 协作回执](benchmarks/2026-11-01-web-collaboration.json)。

**故事 B：用户已被移除，为什么在线 WebSocket 仍是风险点？**

持久成员、内存订阅和 IO 队列属于不同状态。既要在新请求处拒绝，也要复查下推资格、清理订阅和丢弃已观察到撤权后的旧队列帧。
最后说明 socket 已发送字节不可追回，避免承诺超出实现的同步撤销屏障。证据与源码见 10.1～10.2。

**故事 C：Agent 没有回复，为什么不能直接重试一遍工具？**

模型/工具可能已执行完成，只是结果回执丢失。讲清 request_id、Native Run、unknown、原生 GET 与持久结果恢复，
再说明如何比较工具 transcript，证明恢复的是同一次执行。证据见最终交付回执的 `team_backup`，不是用 mock 代替实际模型。

**故事 D：本机都通过，为什么发行包还会失败？**

环境与制品也是输入。候选实际发现 Kafka 匿名卷和平台字段预检问题；只有使用固定安装包、精确镜像和独立 daemon 才暴露这些边界。
讲清为什么失败候选不覆盖、为什么修复后重新验收、为什么发布工作流提交与产品提交分开记录。详见 10.6。

### 13.3 个人贡献核对表

| 简历句子 | 我实际做了什么 | 我能定位的源码 | 我亲自看过/运行过的证据 | 面试前还需补什么 |
|---|---|---|---|---|
| 消息幂等与恢复 | 自行填写：设计 / 实现 / 排障 / 复现 | 函数与提交 | 命令、回执、失败样例 | 待补 |
| C++ 并发与资源管理 | 自行填写 | 所有权和停止路径 | 单测/诊断记录 | 待补 |
| 空间权限与多 Agent | 自行填写 | Service、Repository、Router | 越权/隔离/恢复结果 | 待补 |
| 部署和发行 | 自行填写 | Compose、迁移、备份脚本 | 固定制品报告 | 待补 |

使用 AI 辅助开发可以如实说明：AI 协助检索、实现或补测试，你负责哪些需求边界、审查、真实验证与取舍。
如果主要工作是学习和复现，使用“复现、阅读并扩展、参与修复”；不要把仓库全部功能写成个人独立设计。
面试官更容易从故障定位、测试反例和具体修改中判断你真正理解的部分。

### 13.4 自测评分与复习优先级

每个维度 0～3 分：0＝说不清；1＝能背定义；2＝能连到源码；3＝能给故障反例与验证方法。

| 维度 | 达到 3 分应做到 |
|---|---|
| 业务与架构 | 解释每个组件的必要性，并说出一个可简化的条件 |
| C++ 与网络 | 画出线程/所有权，指出悬空引用、阻塞或关闭风险 |
| 存储与可靠性 | 推演 SQL/Kafka/ACK 窗口，知道幂等的身份和保留边界 |
| 权限与 Agent | 区分 actor/Agent/Session/Run，说明撤权与 unknown |
| 测试与性能 | 区分合同/E2E，读懂失败率和 p99，不偷换压测路径 |
| 个人表达 | 说清自己做了什么、用到哪些框架、哪项还没验证 |

总分 18；可把“至少 14 分且没有 0 分”作为自己进入模拟面试的学习门槛，不是招聘标准或录用保证。
每次复盘只修最薄弱的两项，并重新录一段三分钟介绍，避免无目标地从头背文档。

### 13.5 面试前一天的检查清单

- [ ] 不看笔记画出消息链路，并解释 accepted、delivered、persisted、read 的区别。
- [ ] 用 C++ 讲清一个资源所有权和一个并发不变量，能写正确的谓词 wait。
- [ ] 解释一个索引、一处事务行锁、一个幂等键和一个重试窗口。
- [ ] 区分正式成员、在线连接、Agent 安装、工作区和工具执行权限。
- [ ] 能推演同消息多 Agent、跨房间 Session、结果未知和旧请求重放。
- [ ] 熟悉两项真实 Bug，以及各自验证没有扩大哪些结论。
- [ ] 简历中的数字有日期、负载、路径和结果报告；没有把历史性能当当前容量。
- [ ] 能在“我不会/没测过”之后给出下一步验证方法，不编造成功。

### 13.6 间隔复习与错题卡：检查自己是否只记住了答案

每次选两道薄弱题，先关闭答案口述，再按下面四项检查。第二天、第三天和第七天换一组输入或故障点复述，
不要只重复第一次的句子。

| 复习时间 | 任务 | 通过条件 |
|---|---|---|
| 当天 | 读源码与原理，完成一张卡 | 能给结论、文件/函数和一个反例 |
| 第 2 天 | 不看文档，90 秒回答 | 不混淆定义，能解释为什么 |
| 第 3 天 | 同学追问或自己改变前提 | 能说出原答案哪里不再适用 |
| 第 7 天 | 把知识点放回项目故事 | 连起问题、实现、验证与限制 |

```text
题号 / 日期：
我第一次的错误判断：
正确边界（只写两三句话）：
源码或官方原理依据：
能推翻错误判断的最小输入 / 事务交错：
验证层级：仅推演 / 教学实验 / 产品合同测试 / 真实 E2E
下一次换什么前提追问：
第 2 / 3 / 7 天结果：未掌握 / 需提示 / 独立完成
```

示例：Q47 第一次答“等锁超时自动撤销整笔事务”；修正为“默认通常只回滚等待中的语句，应用仍需处理整笔业务失败”。
反例是第一步写入成功、第二步等锁超时；下一轮问“COMMIT 响应丢失时能否判定未提交”。
先把它标为原理推演，真实构造并记录后才能改成实测。

改简历也使用同一规则：写“设计幂等恢复”前，先讲出结果不确定时的恢复身份、一个失败窗口和你实际看过的证据。
如果只完成了阅读与教学实验，可以写成学习项目中的复现和分析成果；通过复习把贡献范围讲实，比增加更多技术名词更有用。

### 13.7 十分钟白板题：把八股组织成一个有边界的方案

题目：三名用户在同一私有房间聊天，其中一人 @Pi；期间一名成员被移除，发起者断网重连。
说明消息、权限和 Agent 请求怎样恢复。先按本项目小团队规模回答，不主动引入新数据库或多机架构。

| 时间 | 白板上写什么 | 面试官可能改变的条件 |
|---|---|---|
| 0～1 分钟 | 用户场景、身份、明确的成功含义 | “收到 ACK”是否代表所有人已读？ |
| 1～3 分钟 | Comet/Logic/Kafka/Job 链路和 SQL 权威状态 | Kafka 已确认，Job 还没落库怎么办？ |
| 3～5 分钟 | client_msg_id、msg_seq、request_id 的对应与重试 | 客户端同 ID 换了正文；多实例同时提交 |
| 5～7 分钟 | 成员授权、订阅复查、room/agent Session 隔离 | 原连接仍在线；模型正文要求提升权限 |
| 7～9 分钟 | delta/最终消息、unknown、历史补拉与任务状态 | 工具做完但回执丢了；进程此刻重启 |
| 9～10 分钟 | 两个验证用例、一个代价、一个尚未覆盖项 | 要求“零丢失”“任意故障下 exactly-once” |

完成后沿图标注用到的基础：C++ 所有权与互斥、TCP 与业务 ACK、事务和唯一键、Kafka 重投、缓存与权威数据。
每个基础点必须解释它解决哪一步的问题；例如 mutex 保护当前进程的临界区，不能代替跨实例的数据库约束。

自测时让同学只改一个前提。若暂时不会，明确“当前实现到这里，新增条件需要验证这里”，再给出最小验证方法。
交付一页图和两分钟录音即可；这道题的推演不是一次实际三人 E2E，不加入测试通过计数。

### 13.8 从一个项目回答连续八问，而不是切换八套背诵稿

背景：“A 发消息后立即关浏览器，服务端也开始停止；A 重登后发现消息已经在历史里。”
先口述一个可能发生的时序，不假定所有失败都发生在同一层。让同学按下表逐步追问，每题控制在 60～90 秒。

| 面试官追问 | 答案必须落到的事实 | 需要避免的跳跃 |
|---|---|---|
| 浏览器关闭后，服务端能立刻知道吗？ | 正常关闭与断网不同；连接检测与业务状态分开 | 断线就表示请求没执行 |
| 异步闭包还拿着 conn，是否安全？ | shared_ptr 延长生命周期；连接操作仍遵守所属 EventLoop | 活着就等于线程安全 |
| 任务已入队，是否能返回成功？ | 本地准入与 Kafka 确认是两个阶段 | Submit(true) 等于 accepted_ack |
| 停止标志已经置位，为何进程仍未退出？ | 队列等待、在途 RPC、join 分别有退出条件 | atomic 或 notify 能中断所有 IO |
| catch 住错误后，数据库写入会消失吗？ | 取决于事务状态与已发生副作用；需要原身份查询 | 捕获异常等于业务回滚 |
| 重登补发为什么不能换 ID？ | 原请求可能已成功；沿原文与原 client_msg_id 恢复 | 超时等于确认未受理 |
| SQL 只有一条，是否说明 Kafka 只投递一次？ | 唯一约束与消费幂等可吸收重投 | 最终一条证明全链路恰好一次 |
| 如果内容是 @Pi，能否照原样再跑工具？ | 区分消息发布幂等和原生执行幂等，查询原 request/Run | 聊天去重自动覆盖外部工具副作用 |

参考路径：第 3 节定位链路，第 4.1～4.4 节解释恢复，第 11.15 节解释线程退出，第 4.21 节解释 Native 执行。
这组回答应能反复使用同一张图：网络边界、线程边界、SQL/Kafka 边界、原生工具边界各画一条线。

每题给自己 0～2 分：0＝混淆边界，1＝原理正确，2＝原理正确且能指出一个函数/状态及反例。
错两题以上就选一条最短路径复查源码；不要通过背熟范例掩盖不会解释的部分。
本节是模拟面试，没有实际运行该故障场景；可将它改写成测试计划，但执行后才能记录为验证结果。

### 13.9 一轮 15 分钟的资料授权专题面试

拿第 10.8 节的链路练习，不打开参考答案。每两分钟只改变一个条件，最后三分钟复盘证据与贡献。

| 追问顺序 | 至少要答到的点 | 回查 |
|---|---|---|
| “用户已登录，也属于两个群，为什么不能跨群查资料？” | 人的成员身份与本次 request 的房间范围都要满足；对象归属单独检查 | 10.8、Q60 |
| “最近一页都无效，那返回空列表不就行了吗？” | 合格集合上的分页；短页不能推断没有更早的有效结果 | Q59、12.15 |
| “已有联合索引，为什么仍慢？” | 扫描候选数、过滤选择性、JSON/关联/正文匹配；用 EXPLAIN 与时间数据定位 | 11.8-D、Q59 |
| “查询后被移除，能否用一个 mutex 解决？” | 多进程权威状态、读前后复查、数据库锁协议；最终网络边界仍要说明 | Q60 |
| “有 source_id，是否可以直接复用旧答案？” | ID 不授予权限，旧来源和代次要复验；历史显示与新派生内容区分 | Q61、10.7 |
| “你亲自完成了什么，怎样证明？” | 分开源码分析、教学实验、产品测试；说出自己的输出、一次反例和未测项 | 7.8、14.6 |

每题 0～2 分：0＝结论错误或只有术语，1＝原理正确，2＝能回到函数/数据并给出反例。
分数用于选择下次学习内容，不代表招聘评价标准。错题卡记录面试官改变的前提，避免只记一段固定答案。

<a id="framing-review"></a>
### 13.10 十分钟现场代码评审：先指出合同，再动手改

下面是**故意有问题的教学伪代码**，不是当前产品源码，也不要运行其中的越界或悬空路径。
假设 `TakeLength` 尝试解析网络头部，`pool.Submit` 可能排队失败：

```cpp
std::size_t length = TakeLength(buffer);
if (buffer.size() >= 2 + length) {
    std::string_view body(buffer.data() + 2, length);
    buffer.erase(0, 2 + length);
    pool.Submit([body] { Handle(body); });
}
```

先独立找问题，再查看下面的评审要点。满分不是“找最多危险词”，而是能给出明确输入和修正顺序。

| 面试官追问 | 应指出什么 | 不能停留在哪个答案 |
|---|---|---|
| 一个头部字节就到达了？ | TakeLength 必须返回 NeedMore，不能未检查就访问第二字节 | “TCP 可靠，所以头一定完整” |
| 长度很大或非法？ | 字节序、整数可表示范围、业务上限、减法前提；Invalid 与 NeedMore 分开 | “换成 unsigned 就不会有问题” |
| 正文何时失效？ | erase 已可能覆盖 view，后续扩容或销毁也会失效；先取得拥有型数据再消费 | “lambda 按值捕获就安全” |
| 一次有三帧？ | 循环处理完整帧，保留半帧尾部，定义每轮处理预算 | “把 recv 缓冲调大” |
| Submit 拒绝之后呢？ | 定义失败回复、关闭或受控暂停策略，不能已经消费却静默丢消息；业务重试沿用身份 | “换成无限队列” |
| 如何证明修正有效？ | 切分输入、非法头、EOF 截断、结果所有权；真实业务另测鉴权与 ACK/恢复 | “正常字符串跑了一遍” |

**90 秒回答示例（学习与复现口径）**：

> 我先把协议完整性和业务完成分开。TCP 只传字节，所以解析器要保留未完整尾部，并给非法长度一个独立错误状态。
> 接着检查 C++ 边界：先限长，再验证可用字节，不能让长度加法回绕。跨异步边界的正文需要所有权，
> 按值捕获 string_view 仍然借用 Buffer。项目 Comet 先限长和解码，跨线程发送也会复制消息字节。
> 我用简化协议跑过 256 种短流切分，并故意破坏半帧或超限分支观察断言失败。
> 这证明教学解码合同；真实 WebSocket、线程池拒绝和业务消息恢复还要分别验证。

没有实际执行实验时，把最后两句改为“我设计了这些测试输入，下一步准备执行”；不要直接照抄成个人已完成成果。
能答完本题后，将 Q13、Q16、Q35、Q62～Q63 合并成一张学习卡，不必再背五套互不关联的答案。

<a id="resource-pool-rehearsal"></a>
### 13.11 一轮 20 分钟面试：请求变慢，为什么不能先加线程？

**题目**：聊天请求 p99 上升，CPU 使用率不高，部分 worker 等待数据库连接。面试官要求你定位问题，
并问：“是不是把线程池从 8 改成 64、连接池从 4 改成 32 就好了？”这是一道假设题，没有预设产品事故结论。

| 时间 | 面试官的连续追问 | 回答中必须出现的内容 |
|---|---|---|
| 0～3 分钟 | 先查什么？为什么 CPU 低仍然可能慢？ | 分段延迟、排队长度、连接借用等待/持有时间、SQL 锁等待；等待型瓶颈不要求 CPU 满 |
| 3～6 分钟 | 线程数与连接数应该相等吗？ | 两者控制不同资源；给出 Q65 的假设和估算，承认它不是实测容量 |
| 6～9 分钟 | 项目用了 RAII，还会卡住吗？ | guard 只能在退出作用域时归还；画两个持有者再次借连接的依赖关系 |
| 9～12 分钟 | 连接超时了，重试是不是就行？ | 判断是否拿到连接、是否发出操作、提交结果是否已知；检查事务结束和原幂等身份 |
| 12～15 分钟 | 子任务放到同一线程池，父任务 get 等它，有何问题？ | N 个父任务占满 N 个 worker 的推演；有队列空间不等于有执行名额 |
| 15～18 分钟 | future ready 能否标成业务 completed？ | 值与异常、返回值语义、业务持久化分别检查；举 12.17 的两条输出 |
| 18～20 分钟 | 你会怎样验证方案、写进简历？ | 同条件对照吞吐/p99/错误率和资源使用；只描述亲自完成的改动与回执 |

**90 秒回答示范（学习与排查方案口径）**：

> 我会先定位时间花在哪一层，CPU 不高不能排除锁、连接池或外部 IO 等待。在线程池里，队列接纳只说明可以排队，
> 不代表有数据库资源可用。连接池里则要分开测等待和持有时间；持有时间变长时，单纯增加 worker 可能只扩大积压。
> 源码中建连前预占容量、在锁外做网络操作，随后复查停止状态，这些细节分别保护容量和并发访问，不能合并成一句“加了锁”。
> 我会用等待关系图排查嵌套借用或父子任务互等；超时后根据请求是否已执行决定恢复，不能自动换 ID 重试。
> 我用标准库实验观察到 timeout 后仍能完成，而且 ready 后 get 可能抛异常，因此结果状态还必须回到业务协议核对。
> 如果尝试调整容量，会保留同条件前后数据和失败率；目前的纸上估算不能写成已经优化了吞吐。

尚未亲自运行实验时，将“我用标准库实验观察到”改为“我准备通过标准库实验验证”。
自评共 10 分：问题分层、源码定位、失败推演、验证设计、贡献边界各 0～2 分。
只说名词得 0 分，解释原理得 1 分，给出具体源码或可复现输入及限制得 2 分。
低于 8 分时只补最弱的两项，再录一遍；不要靠增加技术栈名词来延长回答。

<a id="primary-references"></a>
## 14. 原理资料与继续学习

以下官方资料用于核对基础语义，查阅日期为 2026-11-01；项目行为仍以仓库固定版本为准。
C++ 草案页面会更新，阅读时只把本手册涉及的 C++17 已有机制用于项目，不直接移植后续标准接口。

| 原理 | 资料 | 阅读时要回答的问题 |
|---|---|---|
| C++ 并发 | [标准草案：数据竞争与同步](https://eel.is/c++draft/intro.races) | 什么情况下产生数据竞争，谁建立 happens-before？ |
| C++ 发布与退出 | [内存顺序](https://eel.is/c++draft/atomics.order)、[thread 成员](https://eel.is/c++draft/thread.thread.member)、[条件变量](https://eel.is/c++draft/thread.condition.condvar) | 原子编号是否发布正文，notify、取消与 join 分别完成哪一步？ |
| C++ 比较交换 | [atomic 操作](https://eel.is/c++draft/atomics.types.operations) | 失败为何改变 expected，值相同是否说明中途没变？本文实验限定为 C++17 整数 CAS |
| C++ 锁与生命周期 | [unique_lock](https://eel.is/c++draft/thread.lock.unique) | 锁的 RAII 与 mutex 对象的所有权有什么区别？ |
| C++ 任务与所有权 | [function 构造约束](https://eel.is/c++draft/func.wrap.func.con)、[string_view](https://eel.is/c++draft/string.view.template) | 可复制的闭包是否一定拥有它访问的数据？ |
| 协议长度与读取 | [C++ 整数运算](https://eel.is/c++draft/basic.fundamental)、[Linux recv](https://man7.org/linux/man-pages/man2/recv.2.html) | 长度检查的加减法是否已证明安全，EAGAIN、EOF 与半帧分别怎么处理？ |
| C++ 移动与容器 | [复制消除](https://eel.is/c++draft/class.copy.elision)、[vector 修改规则](https://eel.is/c++draft/vector.modifiers) | 哪次复制可以省略，哪些引用会失效？ |
| C++ 容器与复合状态 | [vector 容量](https://eel.is/c++draft/vector.capacity)、[unordered 异常规则](https://eel.is/c++draft/unord.req.except)、[deque 修改规则](https://eel.is/c++draft/deque.modifiers) | reserve 是否创建元素，两次容器更新如何保持不变量？ |
| 时间预算与协议 | [steady_clock](https://eel.is/c++draft/time.clock.steady)、[Protobuf 编码](https://protobuf.dev/programming-guides/encoding/) | 哪种时间可比较，哪些字节构成跨进程协议？ |
| C++ 构建 | [GCC 编译阶段](https://gcc.gnu.org/onlinedocs/gcc/Overall-Options.html)、[CMake 目标链接](https://cmake.org/cmake/help/latest/command/target_link_libraries.html) | 错误发生在哪一阶段，哪个目标负责提供实现？ |
| C++ 对象与存储 | [new 表达式](https://eel.is/c++draft/expr.new)、[delete 表达式](https://eel.is/c++draft/expr.delete) | 分配、初始化、析构、释放分别由谁负责？ |
| C++ 派发与推导 | [虚函数规则](https://eel.is/c++draft/class.virtual)、[调用参数推导](https://eel.is/c++draft/temp.deduct.call) | 名字查找与重写是否一致，T&& 是否为转发引用？ |
| C++ 多锁与异步结果 | [scoped_lock](https://eel.is/c++draft/thread.lock.scoped)、[future 共享状态](https://eel.is/c++draft/futures.state) | 自动释放为何不等于无死锁，结果就绪为何仍可能抛异常？ |
| Linux IO | [epoll(7)](https://man7.org/linux/man-pages/man7/epoll.7.html) | 就绪是什么，LT/ET 如何影响读取与公平性？ |
| 文件耐久性 | [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html) | 写入返回和稳定存储的边界在哪里，目录项如何处理？ |
| Linux 排障 | [/proc 文件系统](https://docs.kernel.org/filesystems/proc.html) | 哪些字段是进程/任务状态，如何结合时间区间解释？ |
| TCP 窗口 | [RFC 5681](https://www.rfc-editor.org/rfc/rfc5681#section-2) | rwnd、cwnd 与应用层排队各约束什么？ |
| TCP 关闭 | [RFC 9293 状态机](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.3.2) | CLOSE_WAIT 与 TIME_WAIT 各在等待什么？ |
| WebSocket | [RFC 6455](https://datatracker.ietf.org/doc/html/rfc6455#section-5) | TCP 字节、帧与消息分别在哪划边界？ |
| gRPC | [Deadlines](https://grpc.io/docs/guides/deadlines/) | 客户端停止等待后，服务端还需做什么？ |
| HTTP/2 与 gRPC 流控 | [RFC 9113](https://www.rfc-editor.org/rfc/rfc9113.html)、[Flow Control](https://grpc.io/docs/guides/flow-control/) | 多路复用消除了哪层等待，Write 返回证明到哪一步？ |
| 密码与凭据 | [OWASP 密码存储](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)、[OpenSSL 随机数](https://docs.openssl.org/3.0/man3/RAND_bytes/) | 当前源码参数与推荐是否相同，生成失败如何处理？ |
| MySQL 8.0 | [隔离级别](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html)、[锁定读](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html) | 快照读和取号所需的互斥读有什么区别？ |
| MySQL 执行计划 | [EXPLAIN](https://dev.mysql.com/doc/refman/8.0/en/explain.html) | possible_keys 与实际 key 有何区别，查询成本在哪里？ |
| MySQL 客户端超时 | [mysql_options](https://dev.mysql.com/doc/c-api/8.0/en/mysql-options.html) | 借用等待、建连、网络读写与业务总期限有什么区别？ |
| SQL 查询与输入 | [SELECT/LIMIT](https://dev.mysql.com/doc/refman/8.0/en/select.html)、[预处理语句](https://dev.mysql.com/doc/refman/8.0/en/sql-prepared-statements.html) | 合格结果上的分页与固定候选过滤有何区别，参数安全覆盖哪些问题？ |
| 对象授权与字符范围 | [OWASP 授权](https://cheatsheetseries.owasp.org/cheatsheets/Authorization_Cheat_Sheet.html)、[UTF-8 RFC 3629](https://www.rfc-editor.org/rfc/rfc3629) | 已知对象 ID 是否有权读，字符数与响应字节数如何分别限制？ |
| MySQL 日志与失败 | [binlog 协调](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)、[InnoDB 错误处理](https://dev.mysql.com/doc/refman/8.0/en/innodb-error-handling.html) | 内部提交覆盖什么，死锁与等锁超时的回滚范围是否相同？ |
| Kafka 3.7 | [设计与投递语义](https://kafka.apache.org/37/design/design/) | Kafka 内部保证为何不能自动覆盖外部数据库和工具？ |
| Redis | [淘汰策略](https://redis.io/docs/latest/develop/reference/eviction/) | TTL、内存淘汰与数据恢复各自解决什么？ |

继续学习按“发现薄弱点 → 查原理 → 回源码 → 构造反例 → 录制回答”循环推进。
下一阶段的重点应来自当前真实边界，例如更严格的 Runtime 隔离、原生控制实测、热替换恢复和容量分析；
这些是可讨论的演进方向，完成之前不要加入简历的已实现清单。

### 14.1 前次学习手册更新与校验记录

2026-11-01 的前次记录对应本次修改前的文档 SHA-256 `61c660889df0dbad7e9ed76fecb877df23a50a2bdcb536009d13d2d62b5f67f3`。
当时手册已形成“学习路线 → 源码与原理 → 连续追问 → 实验 → 简历与模拟面试”的结构，
包含 41 道编号问答、6 组连续追问、6 项架构取舍、3 个独立 C++17 程序和双连接 MySQL 实验。
该次补充 Q38～Q41 的对象存储、网络背压、落盘与 MVCC 追问，以及第 12.9 节的可执行 SQL，保留原有源码深读。
第 0 节用于安排学习，第 11 节用于回答原理和追问，第 12 节用于验证，第 7、13 节用于简历和面试表达。

| 前次记录的检查 | 结果 | 证明范围 |
|---|---|---|
| 文中 3 个含完整 main 的独立 C++ 程序 | GCC 11.4.0，C++17，`-Wall -Wextra -Werror -pedantic`，编译与运行全部退出 0 | 连续游标教学程序和 2 个语言实验；没有产品运行依赖 |
| move-only lambda 转 `std::function` 的反例 | 编译退出 1，诊断命中目标必须可复制 | 预期编译失败被实际验证，不算成功程序 |
| 第 12.9 节的 6 段 SQL 原文 | 独立 MySQL 8.0.46 容器，两个真实连接；旧快照 7、锁定读 8、过期更新 0 行、分页序号 3/2 均符合预期 | 教学事务与分页；没有启动产品服务，没有使用业务库；测试容器已清理 |
| 教学 EXPLAIN | 本次使用 `uk_session_seq`，range / Backward index scan | 仅证明这次微型数据集的执行计划，不代表性能提升或所有数据分布 |
| 142 处本地文件链接、10 处内部导航、代码围栏与折叠块 | 检查通过；41 个题号无重复 | 路径存在、导航与结构完整；不代表自动核验每个历史报告的结论 |
| 项目工作树对照 | 已有源码文件内容与 Git 状态保持一致 | 该次修改学习手册，保留原有开发改动 |

该次没有重新执行产品 CTest、产品 MySQL/Redis/Kafka 集成测试或 Pi/Hermes E2E；上述真实 MySQL 实验仅用于教学。
第 6.4 节仍引用其注明版本的历史验收证据；新增学习题不能扩大那些证据的覆盖范围。

### 14.2 上一轮增补与实际校验（2026-11-01）

该轮保留已有项目深读、简历模板和验收边界，补充第 0.7 节优先级索引、第 11.12 节 Q42～Q47 辨析题、
第 12.10 节 C++17 实验和第 13.6 节复习卡。当时全文有 47 道编号问答、4 个完整独立 C++ 程序。
新增题目按“先作答 → 展开答案 → 回源码 → 构造反例”学习；缓存一致性与提交失败案例明确标为推演。

源码核对涵盖 SpaceRepository、RoomService、RedisStore、ThreadPool、SqlTransaction 与消息 reservation；
基础规则另对照 C++ 草案、TCP RFC 9293 和 MySQL 8.0 官方文档，链接已放在对应题目旁。

| 本次实际检查 | 结果 | 证明范围 |
|---|---|---|
| 文中全部 4 个完整 C++ 程序 | GCC 11.4.0，C++17，`-Wall -Wextra -Werror -pedantic`；编译和运行均退出 0 | 教学示例按文中原文可运行 |
| 派生方法去掉 const、保留 override | 编译退出 1，诊断包含 `does not override` | 签名错误会被 override 检出 |
| move-only 闭包转 std::function | 编译退出 1，诊断包含 `copy-constructible` | 重验既有 C++17 接口约束反例 |
| 转发实验删除 std::forward | 编译退出 0；临时字符串断言触发 SIGABRT | 预期失败实际发生，验证有名参数表达式的值类别 |
| 链接、导航、题号与结构 | 150 处本地文件链接、12 处内部导航有效；Q01～Q47 连续唯一；代码围栏和折叠块闭合 | 文档结构完整，文件入口存在 |
| 工作树保护 | 修改前后仓库 Git 状态及已有脏文件内容校验一致 | 本次只修改父目录的这份学习手册 |

本次未重跑第 12.9 节的 MySQL 教学实验，也未运行产品 CTest、MySQL/Redis/Kafka 集成测试或 Pi/Hermes E2E。
前次记录和发行报告保留原有日期与范围；本次通过的是文档与教学示例检查，不是新的产品验收。

### 14.3 前一轮容器与异常安全增补记录（2026-11-01）

本轮以修改前 SHA-256 `ca38da9226337491f546d297256a785b28c1289e73e53b6c75cfc5acd659ef10` 为基线，
保留原有源码深读与学习安排，新增 Q48～Q51、FIFO 去重实验、简历证据卡和十分钟白板题。
该轮完成时共有 **51 道编号问答、5 个完整独立 C++17 程序**，学习顺序仍以第 0 节为入口。

源码核对还修正了 4.8：Comet 的处理中重复返回可重试业务错误，已完成重复才返回成功且不增加投递计数。
Q51 明确区分连接池等待预算与底层阻塞调用的超时；没有将“用了单调时钟”写成严格全链路截止保证。
新增原理已对照相邻的 C++ 标准草案和 Protobuf 官方链接；教学扩展不改变产品协议。

| 本轮实际检查 | 结果 | 证明范围 |
|---|---|---|
| 从文档原文提取全部 5 个完整 C++ 程序 | GCC 11.4.0；C++17，`-Wall -Wextra -Werror -pedantic`；编译与运行均退出 0 | 文中教学程序可运行，不依赖产品服务 |
| FIFO 差分验证 | 4 种容量，各 10000 次，与线性参考模型的返回值及窗口大小一致 | 正常输入、空 ID、容量边界、重复与淘汰策略 |
| 故意将 FIFO 改为命中刷新 | 编译退出 0，运行触发 SIGABRT | 预期失败实际发生，断言能够识别 LRU/FIFO 差异 |
| 文档链接、导航与结构 | 155 处本地文件链接、14 处内部导航有效；Q01～Q51 连续唯一；围栏与折叠块闭合 | 文档结构和源码入口有效 |
| 现有工作保护 | 仓库 989 个既有普通文件 SHA-256 未变，Git 状态与修改前一致 | 只修改本手册，保留现有开发内容和配置 |

本轮没有运行产品 CTest、MySQL/Redis/Kafka 集成测试、Pi/Hermes E2E 或双连接 SQL 教学实验，
没有模拟内存分配失败，也没有将内存窗口练习计作持久幂等验收。14.1～14.2 与发行报告均保留各自的历史验证范围。

### 14.4 前一轮跨层面试学习更新与校验（2026-11-01）

以修改前 SHA-256 `d8a19089cc267d91027bc84450d17dad9b258e128d36ccf0915d5533cea3efbb` 为基线，
保留已有项目深读、学习计划、简历证据卡和模拟面试；新增 Q52～Q55、第 10.7 节开发案例、
第 12.12 节 CAS 实验与第 12.13 节源码学习作业。该轮完成时有 **55 道编号问答、6 个完整独立 C++17 程序**。

新增题目核对了任务 Service/Repository、媒体租约与发布、Comet/Job 流读写、密码和 token 实现。
原理另对照 C++ 原子接口、HTTP/2 RFC、gRPC 流控、OpenSSL 与 OWASP 官方资料。
Q54 明确记录密码派生成本注释与当前建议不一致；此次只更新学习文档，没有更改密码实现或个人配置。

| 本次实际检查 | 结果 | 证明范围 |
|---|---|---|
| 文档原文提取的全部 6 个完整 C++ 程序 | GCC 11.4.0；C++17、O2、Wall/Wextra/Werror/pedantic、pthread；编译与运行均退出 0 | 教学示例可运行，断言启用；不代表产品并发验收 |
| 新增 CAS/ABA 实验 | 输出 `conflict=0 expected=8 aba=1 tagged=0` | 比较失败更新 expected；裸值 ABA 与代次检查的差异 |
| 故意复用旧代次 | 编译退出 0，运行触发 SIGABRT，命中 `!accepted` 断言 | 反例确实能破坏预期，未模拟多线程调度或弱内存序 |
| 文档结构 | 168 处本地文件链接、16 处内部导航有效；Q01～Q55 连续唯一；代码围栏和折叠块闭合 | 路径与导航完整，不等于重新验收所有引用报告 |
| 工作树保护 | 仓库 1011 个既有普通文件内容 SHA-256 未变，Git 状态与修改前一致 | 仅修改父目录指定手册；已有开发改动与配置保留 |

本机教学检查日志保存在临时目录 `/tmp/spark-interview-study-wl9dx6r3`，不作为长期发行回执。
本次没有运行产品 CTest、MySQL/Redis/Kafka 集成测试、Pi/Hermes E2E 或双连接 SQL 教学实验；
第 10.7 节只引用已有阶段报告。源码推演、教学实验、真实依赖测试与完整产品验收保持分开计数。

### 14.5 前一轮求职主线与线程退出学习更新（2026-11-01）

以修改前 SHA-256 `f7256ea1975f02925e4c44e74c391418134c2c8eb2a63868a442f7ea4177b984` 为基线，
补充岗位选题、90 秒口述范例、Q56～Q58、双线程关闭实验和连续八问。该轮完成时有 **58 道编号问答、7 个完整独立 C++17 程序**。
第 4.11 节按实际 Stop/join 控制流澄清了 Comet 析构注释；第 10.7 节按当前源码与阶段报告更新上下文代次进度。
保留全部原有 C++ 示例、项目深读与简历模板，没有修改产品实现。

| 本次实际检查 | 结果 | 证明范围 |
|---|---|---|
| 从文档原文提取全部 7 个完整 C++ 程序 | GCC 11.4.0；C++17、O2、Wall/Wextra/Werror/pedantic、pthread；编译与运行均退出 0 | 教学程序可以按原文运行，断言开启 |
| 双线程关闭实验 | drain 模式接纳 2、完成 2；discard 模式接纳 2、完成 1 | 同步门固定交错，验证容量、关闭拒绝与排空策略；没有真实 RPC |
| 故意把出队退出条件改成 closed | 编译退出 0；运行触发 SIGABRT，命中 `executed == expected` | 反例破坏 drain 语义，断言能识别；不等于并发穷举 |
| 链接、题号和结构 | 173 处本地文件链接、18 处内部导航有效；Q01～Q58 连续唯一；代码围栏与折叠块闭合 | 源码入口与学习导航可用 |
| 现有工作保护 | 仓库 1021 个既有普通文件内容 SHA-256 未变，Git 状态与修改前一致 | 仅修改父目录指定手册，已有开发改动与配置保留 |

本机教学检查记录位于临时目录 `/tmp/spark-interview-shutdown-7gkwt995`；它不是发行制品或产品验收报告。
本次没有运行产品 CTest、MySQL/Redis/Kafka 集成测试、Pi/Hermes E2E 或双连接 SQL 教学实验。
源码注释差异只在手册中澄清；代次功能只引用既有阶段回执，没有将未完成的资料工具、共享删除和 Native 媒体验收写成已交付。

### 14.6 前一轮资料授权与求职学习更新（2026-11-01）

以修改前 SHA-256 `125b059f03dcb5b69b4f34ac34cbb75d15366d2e2a5ab6d649fe6916e8c105a4` 为基线，
保留原有消息链路、C++/网络/存储八股、学习计划、简历模板和全部教学程序。
新增第 10.8 节源码案例、Q59～Q61、第 12.15 节分页实验、第 7.8 节贡献分层表达与第 13.9 节专题面试。
该轮完成时共有 **61 道编号问答、8 个完整独立 C++17 程序**；导航与题目优先级同步更新。

本次核对 AuthorizeContext、MediaContextService、ContextRepository 和预处理封装，
将“服务端资料 API 已实现、原生工具接线待完成”更新到学习案例，避免沿用上一阶段的进度。
通用原理核对 MySQL 8.0、OWASP 授权指南和 UTF-8 RFC，链接位于题目附近。

| 本次实际检查 | 结果 | 证明范围 |
|---|---|---|
| 提取文档中全部 8 个完整 C++ 程序 | GCC 11.4.0；C++17、O2、Wall/Wextra/Werror/pedantic、pthread；编译与运行均退出 0 | 原文教学程序可运行，断言开启 |
| 新增分页实验 | 输出 `wrong=0 first=99,97 second=95 exhaustive=1024` | 空页反例、固定上界、跨房间/不合格直读拒绝、1024 组静态分页差分检查 |
| 故意用探测行作为游标 | 编译退出 0，运行触发 SIGABRT | 跳过 seq=95 的错误被第二页断言识别 |
| 故意移除直读的资格检查 | 编译退出 0，运行触发 SIGABRT | 超出 request 消息上界的读取被断言识别 |
| 文档结构与入口 | 185 处本地文件链接、20 处内部导航有效；Q01～Q61 连续唯一；围栏与折叠块闭合 | 路径与学习导航完整，不代表重验所有历史结论 |
| 资料 API 报告的源文件指纹 | 29 个 SHA-256 与当前文件一致 | 本节引用的阶段报告对应这些源码；没有重新执行该报告的产品测试 |
| 现有工作保护 | 仓库 1039 个既有普通文件内容 SHA-256 未变，Git 状态与修改前一致 | 只修改父目录指定手册，保留产品开发改动和配置 |

本机检查输出位于 `/tmp/spark-interview-context-bd1zr4bt`，用于这次文档练习核对，不是长期发行回执。
本次未运行产品 CTest、MySQL/Redis/Kafka 集成测试、真实 Pi/Hermes、Web E2E 或双连接 SQL 教学实验。
第 10.8 节引用既有阶段结果；本次新增的 C++ 实验只证明教学算法，不能扩大产品验收和个人贡献口径。

### 14.7 前一轮网络解析与面试学习更新（2026-11-01）

以修改前 SHA-256 `c6bedce14555d9adf11b5bcaff76e91e3f1281ee2dea2272506697f633311ca9` 为基线，
保留原有源码深读、八股、简历模板和实验。本次新增 0.9 学习闭环、Q62～Q63、12.16 拆帧实验与 13.10 现场评审，
全文现有 **63 道编号问答、9 个完整独立 C++17 程序**。

源码核对 HandleHandshake、HandleWebSocketFrame 和 TcpConnection::send，澄清 TCP 分段与 WebSocket 消息分片，
并将“锁升级”明确为共享锁转排他锁的讨论。原理对照 TCP RFC、Linux recv 手册与 C++ 整数规则，链接在题目旁。
资料模块只更新为“工作树已有 Native 工具接线代码”，没有把资料 API 历史回执扩写为原生模型或完整产品验收。

| 本次实际检查 | 结果 | 证明范围 |
|---|---|---|
| 文档原文中的 9 个完整 C++ 程序 | GCC 11.4.0；C++17、O2、Wall/Wextra/Werror/pedantic；全部编译、运行退出 0 | 教学示例可按原文运行，断言开启；最后修改的第 9 个程序已重新提取复验 |
| 新增拆帧实验 | `segmentations=256 boundary/eof/ownership=ok` | 9 字节短流全部切分、零长度/NUL、半帧 EOF、16 字节边界、超限头部、结果所有权 |
| 故意提前消费半正文 | 编译退出 0，运行触发 SIGABRT | 受控错误破坏后续流解析，断言能够检出；没有运行悬空访问 |
| 故意删除长度上限 | 编译退出 0，运行触发 SIGABRT | 仅收到非法头部时未拒绝被断言识别；不提供巨量正文 |
| 新实验 UBSan | 编译与运行退出 0，无诊断 | 本次已执行输入未触发 UBSan 检出的错误，不证明任意输入安全 |
| 文档链接与结构 | 190 处本地文件链接、23 处内部导航有效；Q01～Q63 连续唯一；围栏与折叠块闭合 | 学习入口与文档结构完整 |
| 现有工作保护 | 仓库 1046 个既有普通文件内容 SHA-256 未变，Git 状态与修改前一致 | 仅修改这份父目录手册，保留已有产品开发改动与配置 |

临时校验记录位于 `/tmp/spark-interview-framing-vxu1bg2e`，含修改前副本、示例、构建/运行输出与错误变体；不是长期发行回执。
本次没有运行产品 CTest、真实 socket/三服务集成、MySQL/Redis/Kafka、Pi/Hermes 或浏览器 E2E。
教学测试、源码阅读和历史报告分别注明范围；新增练习只能写成自己的阅读/复现成果，不能代替个人产品开发贡献。

### 14.8 前一轮资源池与异步结果学习更新（2026-11-01）

本次修改前文档 SHA-256 为 `f633f3e3fb346ea34b933ab2f3175b741c68c2b5ae7e31047e44b54e6ffd6852`。
保留原有源码讲解与学习内容，新增 0.10 三轮复习路线、Q64～Q66、12.17 标准库实验、13.11 模拟面试，
并在 4.12 校正连接池注释与实际控制流的差异。全文现有 **66 道编号问答、10 个完整独立 C++17 程序**。

源码核对范围为 KeyedMutex、ThreadPool、MySqlConnectionPool 与 guard 生命周期；
原理对照 C++ 多锁/共享状态规则和 MySQL 客户端超时文档。嵌套借用及父子任务互等均标明为教学反例，
没有把它们描述成已复现的产品事故，也没有把容量估算写成性能成绩。

| 本次实际检查 | 结果 | 证明范围 |
|---|---|---|
| 从文档原文提取 10 个完整 C++ 程序 | GCC 11.4.0；C++17、O2、Wall/Wextra/Werror/pedantic、pthread；全部编译、运行退出 0 | 保留断言的教学示例可运行，不是产品回归 |
| 新增 future 实验 | 依次输出 `timeout -> ready -> value=42` 和 `timeout -> ready -> exception` | 同步门下的就绪检查、等待不取消、异常传播和 get 后状态 |
| 删除抛异常语句的错误变体 | 编译退出 0，运行触发 SIGABRT；符合预期 | 断言能发现“应该失败却返回了值”，没有将预期失败计为正常通过 |
| 新实验 UBSan | 编译、运行退出 0，无诊断 | 已执行路径未触发此检查器报告，不证明任意线程交错无误 |
| 文档链接与结构 | 193 处本地源码/文档链接、26 处内部链接有效；Q01～Q66 连续唯一；围栏与折叠块闭合 | 阅读入口和编号一致 |
| 已有工作保护 | 仓库 1089 个既有普通文件内容 SHA-256 未变，Git 状态不变 | 只修改父目录中的本手册，未改产品源码和现有配置 |

临时复核材料：`/tmp/spark-interview-pools-uej1c_5p`，包括修改前副本、提取的程序、编译/运行日志、错误变体、
结构检查和文件哈希记录。此目录是本次本机学习校验材料，不是长期发行制品。
本次未运行产品 CTest、真实 MySQL/Redis/Kafka、Pi/Hermes 或浏览器 E2E；文中既有产品回执仍保留原日期与范围。

### 14.9 本次 Git 归档与图文协作更新（2026-11-02）

以父目录完整手册 SHA-256 `7685bcd6d54c68969b2e38567d1d8a596ba901403f519478a55326e9e4435a45` 为内容基线，
合并到已受 Git 管理的 `docs/project-guide-and-resume.md`，替换仓库中较旧的 2026-10-30 版本。
父目录原路径保留为符号链接；源码只有一个日常开发仓库，旧发行源码通过原 Git 标签查看。

保留原 66 道问答和 10 个完整 C++17 程序；新增 Git 管理入口、图文协作闭环与 Q67～Q69，全文为 69 道编号问答。
修正“原生工具尚未提交”“媒体理解仍是下一阶段”等旧结论，按各自回执明确真实 PDF/Hermes、来源任务和恢复范围。
新增的简历条目仍是表达模板，应按个人实际开发或复现经历改写，不能把仓库全部能力当成个人贡献。

本轮只检查文档本地链接、显式锚点、问答编号、围栏/折叠结构、知识来源与 Git 差异；
原 10 个 C++ 示例代码块逐字保留，不重新编译；本轮不运行产品 CTest、数据库、Kafka 或模型调用。
第 10.9 节的测试数字引用先前实际记录，不记为本次文档更新重新通过。

实际文档检查通过：206 处本地文件链接、29 处内部导航、69 道连续唯一问答；13 个 C++ 代码块逐字保持，其中 10 个为完整程序。
围栏及折叠块闭合，208 项知识来源校验通过；同时校正三个旧导航锚点的位置。
