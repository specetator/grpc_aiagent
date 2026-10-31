# 单机 IM 冷备与恢复

`scripts/spark_backup.py` 使用 Python 标准库、Docker 和 Compose，备份官方单机部署。
它把 MySQL、Redis、Kafka、附件、管理员密钥和服务密钥六个命名卷保存为同一集合。
先停止应用写入，再正常停止数据服务，归档保留卷内文件所有者和权限。
不支持在线备份、任意自定义 Compose、共享卷、服务副本或跨平台数据库搬迁。

## 备份

安排独占维护窗口：备份期间不要运行其他 Compose、启动容器或修改源目录；
使用与实际部署一致的项目名、Compose 文件、profile 和环境文件。下面是默认基础 IM：

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

启动失败时保留恢复目录、卷和 `restore-state.json` 的未完成状态供排查，命令返回非零。
不要对原数据库执行降级迁移。恢复到新的空项目，确认账号、消息、附件和权限正常后，
再安排切换访问地址。不要同时启动两个共享同一数据库或 Runtime 的副本。

**当前范围是 IM-only。** SQL 中已有任务、Agent 绑定和最终聊天消息仍随数据库保存，
但外部 AgentStore、Runtime 原生 Session、工作区快照及源项目未包含在六个卷中。
恢复配置移除 Bridge 和授权回连服务，并关闭 Logic 的 Agent 总开关。
旧 Kafka Agent 请求保留在队列中；在完整核实外部 Runtime 状态和幂等记录之前，
不要手动重新启用 Agent。未知执行不得当作新任务再次运行。
完整公共 Runtime 的成套备份恢复属于下一项交付，不以这轮 IM 结果代替。

## 验证

```bash
python3 tests/backup_contract_test.py
# 使用已构建的候选镜像；测试环境 Python 3.11，实测 websockets 17.4。
python3.11 tests/backup_restore_e2e.py --image spark-push:web-collaboration \
  --out /tmp/spark-backup-result.json
```

第一条是预检契约测试；第二条创建独立真实 MySQL/Redis/Kafka/IM 栈并在结束后
清理自己的资源，不调用模型。它检查冷备、全新项目恢复、成员和附件权限、在途 Kafka
消息只落库一次、序号继续增长、损坏归档和目标冲突拒绝，以及备份失败后的原服务状态。
执行结果以版本化验收回执为准；没有真实 Runtime 或跨主机测试的结果不能算作相应验收通过。
2026-11-01 同机真实恢复 10 项、预检契约 18 项已通过，完整构建和相关 CTest
通过；此前失败尝试及修复见 [阶段回执](benchmarks/2026-11-01-im-backup-restore.json)。
