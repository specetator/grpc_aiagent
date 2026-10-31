# Docker 中查看 CANN 引用原文

适用：保留原 `@CANN` Gateway 的 Linux/WSL 部署。基础 IM 和公共 Pi/Hermes
启动器不依赖 CANN 数据；此 overlay 只为现有知识原文 API 提供文档，不启用模型。

`KnowledgeService` 允许已登录用户按合法的 doc/chunk ID 读取公开知识资料。
这里的数据不是私有空间附件，不能把私人项目文件或模型配置放入该目录。
RAG 仍使用管理员配置的 `CANN_KNOWLEDGE_ROOT`，Logic 只需对应 generation 的
`current.json` 和 `documents`，不需要检索索引、知识库源码或个人 Profile。

## 导出与挂载

原知识库文件可能为 0600，Logic 容器以 UID 10001 运行，直接挂载会导致引用返回 404。
在知识库的已发布 generation 保持不变的维护窗口中，导出独立的公开副本：

```bash
# 父目录由部署者创建并控制；下面两个路径替换为自己的实际路径。
python3 scripts/export_cann_documents.py \
  --knowledge-root /srv/cann-agent-knowledge \
  --output /srv/spark-public/cann-data
```

输出目录必须不存在。脚本只复制当前 generation 文档，保持原始字节；源数据的
内容和权限不变。它检查 ID、generation、chunk 归属、文件大小和前后文件状态，
拒绝链接、已有输出和导出期间变化的源文件。目标目录为 0755、文件为 0644，供
非特权 Logic 读取；不要递归放宽个人目录的权限。目标父目录本身不需要挂入容器。

在部署专用 `.env` 中设置：

```dotenv
SPARK_PUSH_CANN_DATA_DIR=/srv/spark-public/cann-data
```

启动时增加 overlay；已有 Agent Connector 部署须保留原来的文件和 profile：

```bash
# 仅 IM + 公开文档 API，不启动 Agent：
docker compose -f docker-compose.yml -f docker-compose.cann.yml up -d

# 已配置可选 Connector 的部署：
docker compose -f docker-compose.yml -f docker-compose.agents.yml \
  -f docker-compose.cann.yml --profile agents up -d
```

只读挂载固定到 `/run/spark-cann/data`，Logic 的 root 为 `/run/spark-cann`。
未指定数据目录或目录不存在时，部署会失败，不自动创建一个空知识库。
更新知识库后导出到新的不存在目录，核对 generation，再在维护窗口切换挂载；
不要原地修改正在提供服务的导出副本。

这不会把 legacy CANN 配置安装进正式公共 Catalog，也不会把私人 Hermes Profile
变成共享 Agent。原协作任务与开发权限见 [CANN 协作](room-agent-collaboration.md)，
公共只读部署见 [可选 Connector](optional-agent-connector.md)。

升级旧 CANN-only 部署也需执行增量迁移
[`20261101_cann_publications.sql`](../sql/migrations/20261101_cann_publications.sql)。
Compose bootstrap 会自动执行；WSL 手动部署按现有迁移流程执行。缺少发布表时，
Logic 禁用可选 Agent 能力并记录配置错误，基础 IM 保持可用。

该表复用通用 Room Agent 的首次发布决定：Bridge 重启重投时，流式投影变化不会
改变最终历史。旧已完成请求通过原结果和原消息重建；短期 reservation 已清理时，
从持久消息恢复原序号。正文、引用、工具回执冲突或旧证据不足会拒绝，不自动重跑。

## 验证与当前边界

```bash
python3 tests/cann_document_export_test.py
# 需要真实 Pi、公共 CANN 知识库、授权的测试模型及隔离 Windows Hermes；
# 路径前提由 tests/legacy_private_runtime_fixture.py 明确指定。
docker build -t spark-push:legacy-gc .
python3.11 tests/legacy_private_runtime_e2e.py \
  --image spark-push:legacy-gc --out /tmp/spark-legacy-result.json
```

第一条是文件边界契约；第二条使用实际 Compose、模型、RAG、沙箱编译和原私聊
链路。它从 verified citation 读取原文，检查 generation、doc/chunk、UID 10001、
未登录 401、坏 ID 400 和不存在的文档 404；并检查任务交接、私聊隔离和重放恢复。
主机 C++ 程序运行回执不等于 Ascend/NPU 验证或独立数值精度测试。

旧格式恢复还有真实 MySQL 契约。`tests/run_mysql_review_tests.py build-wsl`
使用一次性数据库验证发布后清理、原序号恢复、并发补建和冲突拒绝；可通过
`SPARK_TEST_CANN_LEGACY_FIXTURE=/绝对路径/旧采样.json` 增补真实旧版本采样。
采样格式为 `sparkpush.cann_legacy_fixture.v1`，保存原 `request_json`、`result_json`、
`original_message_json`、`persist_payload_hex` 和 `replay_reply_json` 字符串。
测试直接使用原 protobuf 内容作为比较基线；含会话内容的采样应保存在受限目录，
不作为公开文档或仓库测试数据发布。

2026-11-01 最终候选完成 11 项真实 HTTP/WS 检查，包含原 Pi/Hermes 私聊、
CANN 编译、Bridge 重启、Kafka 重投及整栈冷重启。构建和 60 项 CTest 通过，
4 项默认跳过的数据库测试已在真实 MySQL/Redis 补跑通过。详见
[阶段回执](benchmarks/2026-11-01-legacy-cann-private.json)，其中保留此前失败、
独立旧数据恢复证据和未覆盖事项；这不是最终发行验收。

**备份兼容仍待完成。** 当前 `spark_backup.py` 的 bind 白名单会拒绝这个挂载；
不能套用公共 Runtime 成套备份通过的结论。发行前需为公开知识依赖增加明确的
身份记录与恢复校验。不要移除 overlay 来制作一个声称完整的 CANN 部署备份。
