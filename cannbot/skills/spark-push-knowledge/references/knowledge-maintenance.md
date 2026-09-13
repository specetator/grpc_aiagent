# 知识层维护

## 当前机制

项目采用两层 Hermes 原生入口：

1. 根目录 `AGENTS.md` 为从项目目录启动的 Hermes 提供稳定工程约束；
2. `spark-push-agent` 负责编排角色，`spark-push-knowledge` 通过
   `architecture-map.md` 和安装快照渐进披露项目知识。

这两层均保存在业务仓，可随代码评审和版本回退。运行时不依赖 CANNBot Toolkit、OpenCode、
GitCode 登录或官方 `cannbot-skills` 仓。

## 新增参考文档

1. 将文档放入 `docs/`，内容包含适用范围、事实来源和验证日期（如果内容会过期）。
2. 在 `cannbot/knowledge-sources.json` 登记唯一 `id`、相对路径、authority 和 topics。
3. 在 `architecture-map.md` 的对应领域增加入口；不要在 `SKILL.md` 复制正文。
4. 如果文档改变了长期有效的工程约束，再更新根目录 `AGENTS.md`；一次性报告不要写入规则。
5. 校验并重新同步 Hermes：

   ```bash
   python3 cannbot/scripts/validate_knowledge.py --strict
   python3 cannbot/scripts/sync_hermes_skills.py --install --replace
   ```

## Authority 取值

- `code`：协议、schema 或直接决定运行行为的源码；
- `canonical`：随主线维护的设计或运行文档；
- `snapshot`：带环境/日期的测试和性能结果；
- `historical`：教学演进、旧实现或背景材料。

## 修改检索机制

先以可复现问题驱动修改：记录问题、期望命中的来源、当前错误命中和答案差异，再调整登记表、
路由或 source topics。不要因为单个回答不理想就把所有文档塞入始终加载的 `AGENTS.md`。

当文档规模大到 Skill 渐进加载无法稳定召回时，再增加独立 RAG/MCP 层。建议保持三个接口：

- `DocumentSource`：读取 Git 文档、外部 URL 或数据库；
- `Chunker`：按 Markdown 标题/代码符号切分并保留来源元数据；
- `Retriever`：关键词或向量召回，可选 reranker，返回可引用的路径和片段。

自定义 RAG 必须有增量更新、删除传播、索引版本、权限过滤和离线评测；在这些能力完成前，
不要替换当前简单、可审计的项目内知识层。
