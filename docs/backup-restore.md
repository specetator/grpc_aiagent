# 单机 IM 冷备与恢复

`scripts/spark_backup.py` 使用 Python 标准库、Docker 和 Compose，备份官方单机部署。
它把 MySQL、Redis、Kafka、附件、管理员密钥和服务密钥六个命名卷保存为同一集合。
先停止应用写入，再正常停止数据服务，归档保留卷内文件所有者和权限。
不支持在线备份、任意自定义 Compose、共享卷、服务副本或跨平台数据库搬迁。

## 备份

安排独占维护窗口：备份期间不要运行其他 Compose、启动容器或修改源目录；
使用与实际部署一致的项目名、Compose 文件、profile 和环境文件。

同一 Docker daemon 上的备份/恢复验收须串行执行。工具会检查所有容器的卷占用；
其他测试同时删除临时容器时，这项检查可能保守失败。此时命令返回非零，不能把
未完成目录当作成功备份；停止并行维护活动后重新运行。

默认基础 IM：

```bash
python3 scripts/spark_backup.py backup --project sparkpush \
  --env-file .env --output "$HOME/spark-backups/before-upgrade"
python3 scripts/spark_backup.py verify "$HOME/spark-backups/before-upgrade"
```

输出目录必须不存在且不在源卷或 bind 目录内。默认在成功或归档失败后恢复原先运行
的服务，原先停止的服务保持停止。增加 `--leave-stopped` 可让源部署保持停机，
适合升级前备份。若恢复原服务健康状态失败，命令返回非零，需要查看 Compose 状态。
当前不支持含 `$` 的 bind 源路径或恢复目录；配置中的字面 `$` 密码和命令保留
Compose 导出的规范形式，不进行二次转义。

使用可选 Connector 的部署应同时传入 `-f docker-compose.yml
-f docker-compose.agents.yml --profile agents`。外部 Gateway/Native Runtime
必须先由运维停止；本工具无法停止它们，也不备份它们的目录。

挂载公开 CANN 原文的部署还须传入 `-f docker-compose.cann.yml`，并保留实际
`SPARK_PUSH_CANN_DATA_DIR`。该路径必须是 [公开文档导出器](cann-docker.md)
生成的独立副本：仅含 `current.json` 和当前 generation 的 documents，目录 0755、
文件 0644。工具在停服前检查固定只读挂载及文档树，拒绝整个知识库、索引、额外
generation、链接和身份不一致的文档。清单以 `kind: cann_documents` 记录 generation、
文档数和内容摘要；复制后复核源文件身份和内容。维护期间不要改动该副本。

备份包含密码、证书、附件、用户数据和已解析的配置。目录权限为 0700，文件 0600；
将整个集合保存在受控存储中，不提交 Git。SHA-256 用于发现损坏，不证明备份来源。
仅恢复自己可信的备份，校验和恢复期间不得修改归档。不要把其他人的归档当成安全导入格式。

`manifest.json` 记录完整标记、每个文件的长度和 SHA-256、镜像实际 ID/digest/平台。
`application_revision` 仅来自部署镜像的 OCI revision；没有标签时为 null。
`tool_checkout_revision` 是备份脚本所在 checkout，不能当作部署镜像源码版本。
MySQL 停机后留下的固定 `mysql.sock` 兼容链接不属于数据库状态，由原镜像启动时重建；
排除项显式登记在清单，其他符号链接、硬链接、设备和 set-id 文件均拒绝。

## 恢复到新项目

只支持使用备份的原始镜像和平台恢复，不在恢复过程中升级。缺少本地镜像时，
工具尝试清单中的原始 registry digest；没有 digest 的本地构建镜像须自行
通过 `docker save` / `docker load` 保存和导入。备份本身不包含镜像层。

```bash
python3 scripts/spark_backup.py restore \
  --backup "$HOME/spark-backups/before-upgrade" \
  --project sparkpush-restored --output "$HOME/spark-restored" \
  --bind 127.0.0.1 --port 9010
docker compose -p sparkpush-restored -f "$HOME/spark-restored/compose.json" ps
```

源部署应保持停止；如仅在隔离测试中同时保留两个项目，须选择不同端口。工具在写入前
验证所有文件和 Compose 计划，拒绝已有的任何目标卷或目标容器（包括已停止的容器）。
它不覆盖、不合并、不自动删除已有数据。目标资源名通过原子容器名锁避免两个恢复命令竞争。

公开 CANN 文档从备份恢复到 `bindings/<绑定名>/content`，Logic 只读挂载这个新路径，
不依赖原机器上的文档目录。恢复前除 SHA 外还验证完整目录结构、文档/片段归属和
读取权限；改过结构的归档即使重新计算校验和也会拒绝。无 CANN 的旧备份无需转换。
这只恢复公开引用原文，不包含 CANN 检索索引、知识库程序或外部 Gateway 状态。

启动失败时保留恢复目录、卷和 `restore-state.json` 的未完成状态供排查，命令返回非零。
不要对原数据库执行降级迁移。恢复到新的空项目，确认账号、消息、附件和权限正常后，
再安排切换访问地址。不要同时启动两个共享同一数据库或 Runtime 的副本。

**当前范围是 IM-only。** SQL 中已有任务、Agent 绑定和最终聊天消息仍随数据库保存，
但外部 AgentStore、Runtime 原生 Session、工作区快照及源项目未包含在六个卷中。
恢复配置移除 Bridge 和授权回连服务，并关闭 Logic 的 Agent 总开关。
旧 Kafka Agent 请求保留在队列中；在完整核实外部 Runtime 状态和幂等记录之前，
不要手动重新启用 Agent。未知执行不得当作新任务再次运行。
公共 Runtime 的成套工具见下文，不能用本轮 IM-only 测试代替它的完整验收。

## 包含公共 Runtime 的整批备份

`scripts/spark_team_backup.py` 复用上面的六卷冷备，以及公共 Runtime 的原生进程锁。
真实整批恢复的 17 项检查已通过，见 [阶段回执](benchmarks/2026-11-01-team-backup-restore.json)。
此流程只支持同一机器、原生平台、
原 home / snapshot / Profile / endpoint 路径，不提供跨主机或个人 Agent 搬迁。
Windows Hermes 必须由 Windows Python 归档、恢复和持锁，WSL 文件锁不能代替它。

先复制 `deploy/agents/backup-plan.json.example` 到自己的私有配置目录，按实际部署
填写一个 Gateway 和 Catalog 中的全部 Native。已停用 Native 也可能有历史执行账本，
不能因此遗漏。纯 Pi 部署使用仅含 Pi 的 Catalog 和一个 Gateway 条目。

每个条目的 `config_root` 是管理员明确指定的公共配置目录。工具只收集正式 launcher
配置及其已知文件引用，不递归复制整个目录；所有这些文件必须位于对应 root 内。
Native 的 `credential_file` 是 Gateway Catalog 引用的专用 `API_SERVER_KEY=...` 文件，
必须与该 Native 的 `api_key_file` 一致。共享配置只能引用相同内容，已有文件不会改写。
Pi 模型文件须仅包含该公共服务选中的 provider/model，环境文件只含受支持的模型变量；
请先自行准备专用文件，备份器不会复制或整理私人模型目录。

WSL 计划中的 Windows 条目使用 `platform: "nt"`，`python` 填 WSL 可执行路径，
如 `/mnt/f/hermes/hermes-agent/venv/Scripts/python.exe`；该条目的 `config`、`config_root`
和 `credential_file` 使用 Windows 原生绝对路径。工具会通过 `wslpath` 核对共享快照的
物理路径。外部 Python、Pi、Hermes 源码和依赖需要保持原版本，指纹不符时拒绝恢复。

安排独占维护窗口，正常停止所有公共 Gateway/Native（见
[受控停机](optional-agent-connector.md#停机与维护锁)）。随后执行：

```bash
python3 scripts/spark_team_backup.py backup \
  --runtime-plan /srv/spark-public/backup-plan.json --project sparkpush \
  --env-file .env -f docker-compose.yml -f docker-compose.agents.yml \
  --profile agents --output "$HOME/spark-backups/team-before-upgrade"
python3 scripts/spark_team_backup.py verify "$HOME/spark-backups/team-before-upgrade"
```

工具先核对身份、实际端口、共享快照及原生停机状态，同时持有全部 Runtime 锁后，
归档完整公共 home（包括 SQLite/WAL、请求账本和原生 Session）、Gateway 创建的
快照及专用配置，再冷备 IM。原生锁文件不属于恢复数据。外层 `batch.json` 只有在
所有组件完整并校验成功后才标记 complete；缺包、损坏、helper 退出或配置不一致均失败。
**源 IM 与 Runtime 保持停机，不自动恢复运行。** 失败包不能用于完整恢复。
目录和文件权限仅按原生文件模式保存；不迁移 Windows ACL，也不宣称已验证 Windows OS 沙箱。

源项目源码、Runtime 程序/虚拟环境、私人 Profile 不在包中；项目 Catalog 和用于既有
请求的授权快照会保留。后续新任务仍依赖管理员恢复原项目源码路径。
备份含公共模型凭据与用户数据，须按私有密钥材料保管，不提交 Git。

## 恢复完整公共批次

确保源服务停止、原 Runtime home 和快照路径为空缺。需要保留旧目录时，先在相应
原生 OS 下把它们移到另外的隔离路径；工具不会帮你覆盖或删除旧状态。专用配置文件
可以保留，但必须与备份 SHA 相同；缺失文件会从包中恢复。不得修改 Profile、Agent ID、
原 endpoint 或 Session 路径来绕过检查。恢复输出必须与全部 Runtime 路径分开。

```bash
python3 scripts/spark_team_backup.py restore \
  --backup "$HOME/spark-backups/team-before-upgrade" \
  --project sparkpush-restored --output "$HOME/spark-restored" \
  --bind 127.0.0.1 --port 9010
```

全套预检通过后，工具在新建的原 home 中立即持有原生锁，再填充和校验；未完成目录
保留标志并拒绝 launcher 启动。全部锁持续到 IM 恢复完成。默认 `compose.json` 仍关闭
Agent，只提供恢复后的普通 IM。`team-restore.json` 和 CLI 回执中的
`requires_cann_admission_verification: true` 表示公共启用尚未验证；生成
`compose.agents.json` 不等于可以启动公共 Runtime。

核对 complete、登录、历史和任务后，先保持 Runtime/Bridge 停止，用以下脚本验证
当前 Logic。使用已有空间成员的普通账号，密码交互输入；脚本正常登录、自动选择
一个已授权空间，不需要 RoomID，不创建任务或启动 Agent：

```bash
python3 scripts/verify_public_agent_mode.py --origin http://127.0.0.1:9010
```

必须成功返回布尔 `cann_enabled: false`。旧镜像缺少字段、返回 true、类型错误、
鉴权失败均返回非零，公共启用保持待处理。写入环境变量不能升级旧二进制；此时
可继续使用恢复后的普通 IM，先另行验证升级，再重新执行这两次检查。恢复工具
仍使用原镜像，不自动换镜像或改写旧备份。

第一次检查只证明该二进制提供此契约，还需在实际启用配置下检查一次。先在维护
窗口停止恢复项目，**不删除数据卷**：

```bash
docker compose -p sparkpush-restored -f "$HOME/spark-restored/compose.json" down --remove-orphans
```

以 `compose.agents.json` 冷启动 IM 部分，暂不启用 `agents` profile，也不启动
Gateway/Native。清除继承的 profile 并使用空环境文件，避免提前启动 Bridge：

```bash
env -u COMPOSE_PROFILES docker compose --env-file /dev/null -p sparkpush-restored \
  -f "$HOME/spark-restored/compose.agents.json" up -d --wait
python3 scripts/verify_public_agent_mode.py --origin http://127.0.0.1:9010
```

第二次检查也必须成功；失败则保持公共 Runtime 和 Bridge 停止。两次均通过后，
才用原配置启动 Gateway/Native，再给**同一份配置**增加 `agents` profile：

```bash
docker compose -p sparkpush-restored -f "$HOME/spark-restored/compose.agents.json" \
  --profile agents up -d --wait
docker compose -p sparkpush-restored -f "$HOME/spark-restored/compose.agents.json" \
  exec -T comet python3 /app/deploy/health.py comet
```

`compose.agents.json` 是完整配置，不需要与 `compose.json` 合并。它和备份一样可能含
已解析的私有环境值，文件权限为 0600。恢复后的 Logic 使用恢复目录里的 Catalog 副本；
如果批次含公开 CANN 文档，关闭和开启 Agent 的两份配置都使用恢复后的文档副本。
该配置显式关闭新 legacy CANN 接纳；实际生效以两次在线检查为准。脚本只检查
Logic 接纳契约，不验证模型可用性或 Runtime 执行结果。
日后改 Catalog 时仍须同步 Gateway 使用的对应配置。不要同时启动源项目和恢复项目，
它们可能使用相同的外部 Runtime 和端口。
不要对仍运行的 disabled 栈直接执行 enabled `up`。实际测试中，热替换 Logic 的
DNS 空窗曾让旧 Comet 流未就绪并使 WS 返回 502；本流程通过整栈冷启动避开该切换方式，
不宣称解决了任意热替换后的连接恢复。

恢复器不改写 completed/unknown、不生成新 request_id、不启动模型。重新开启原链路后，
仍由已有 Router/Native 的恢复协议决定重放或查询；unknown 不能通过清空账本解决。
如果 Bridge 已经耗尽重试并将原事件送入 `ai_request.dlq`，恢复不会自动重放死信。
管理员须核对原始单条 `sparkpush.dead_letter.v1` 记录与请求，再使用镜像内已有工具：

```bash
# 将已核对的单条记录放在 Bridge 容器 /tmp/record.json，权限 0600。
docker compose -p sparkpush-restored -f "$HOME/spark-restored/compose.agents.json" \
  exec -T agent-bridge dlq_replay --file /tmp/record.json --topic ai_request
# 仅在 dry-run 校验通过、原 Runtime 恢复可用后显式提交：
docker compose -p sparkpush-restored -f "$HOME/spark-restored/compose.agents.json" \
  exec -T agent-bridge dlq_replay --file /tmp/record.json --topic ai_request \
  --apply --brokers kafka:29092
```

此操作保留原 request_id 和 DLQ 记录；Native unknown 通过已有 run_id 查询结果，
不会因为恢复而创建替代 Run。不要编辑事件身份或用新请求掩盖未知状态。
中途失败时保留本批创建的目录和未完成标志，默认不会启用 Agent；按失败原因检查后，
仅清理本批未完成目标，再从完整包恢复，不能把部分组件当作完成。

## 验证

```bash
python3 tests/backup_contract_test.py
# 使用已构建的候选镜像；测试环境 Python 3.11，实测 websockets 17.4。
python3.11 tests/backup_restore_e2e.py --image spark-push:web-collaboration \
  --out /tmp/spark-backup-result.json
# 合成状态的文件/锁契约，不调用模型：
python3 tests/runtime_archive_test.py
# 真实 Windows 原生锁/启动器；helper 死亡用例仅替换 IM 边界，不调用模型：
python3.11 tests/runtime_archive_native_e2e.py --out /tmp/spark-native-archive-result.json
# 真实 Compose + Pi/Hermes；需要本机已配置的隔离 Runtime 环境：
python3.11 tests/public_room_runtime_e2e.py --team-backup \
  --image spark-push:team-backup --out /tmp/spark-team-backup-result.json
```

第一条是预检契约测试；第二条创建独立真实 MySQL/Redis/Kafka/IM 栈并在结束后
清理自己的资源，不调用模型。它检查冷备、全新项目恢复、成员和附件权限、在途 Kafka
消息只落库一次、序号继续增长、损坏归档和目标冲突拒绝，以及备份失败后的原服务状态。
执行结果以版本化验收回执为准；没有真实 Runtime 或跨主机测试的结果不能算作相应验收通过。
2026-11-01 同机真实恢复 10 项、预检契约 18 项已通过，完整构建和相关 CTest
通过；此前失败尝试及修复见 [阶段回执](benchmarks/2026-11-01-im-backup-restore.json)。

公共 Runtime 整批测试使用实际 Pi/Hermes 和三个独立 HTTP/WS 客户端，17 项通过；
Windows 原生归档/恢复锁专项 5 项通过，其中 helper 死亡用例明确替换 IM 边界。
归档文件与锁契约 12 项通过。完整 CTest 59 项通过，4 项默认跳过的数据库测试
已在独立真实 MySQL/Redis 补跑通过。成功与失败尝试、当前源码指纹和未覆盖项见
[整批恢复回执](benchmarks/2026-11-01-team-backup-restore.json)，不代表完整 v0.1.0 发行验收。
