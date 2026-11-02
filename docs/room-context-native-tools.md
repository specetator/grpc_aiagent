# 原生房间资料工具

状态：2026-11-01，开发工作树，未包含在 v0.1.0 制品中。Pi/Hermes 工具接线与图像降级保护已实现；
当前真实测试中 Hermes 的三图识别和原生会话恢复通过，Pi 同图识别仍有失败。
**本阶段不宣称视觉语义或完整多模态产品验收通过。** 命令、失败图片、原始结果与范围见
[阶段回执](benchmarks/2026-11-01-room-context-native.json)。

## 调用链与身份

复用现有 AgentRouter、AgentStore、SessionScheduler、Pi RPC 与 Hermes Native Runs API。
不新增 Agent Loop、Memory、模型连接、工具执行器或任务数据库。

```text
持久 Room Agent request / 受信 admission scope
  → Pi Extension 或 Hermes Native toolset
  → 前置 authorize → /internal/room-agent/context/* → 后置 authorize
  → 验证来源/版本/范围/hash → 原生文字或图片 content
  → 现有 Runtime Session → 现有 Bridge/最终消息链路
```

三个工具共用 [JSON 定义](../cannbot/room-context-tools.json)：`room_search`、`room_read`、`room_read_media`。
模型只能给查询词、资料身份和有界范围，不能提供 request/actor/room、凭据、Runtime URL、工作目录或权限。
request_id 来自运行时捕获的受信 scope；服务端从已持久 request 再次解析成员、绑定和代次。
API、来源回执和分页的详细合同见 [资料 API](room-context-tools.md)。

工具在读取前后校验同一权威 scope，拒绝跨房间、错误版本/范围、消息上界之外的正文和无效 JPEG。
固定 HTTP 路径，不使用重定向；响应最多 2 MiB，错误不暴露内部地址、key 或原始错误正文。
源数据是上下文，不是权限。已有读取回执只证明服务端读取，不证明模型收到或理解。

## Pi

[PublicRoomPiFactory](../cannbot/scripts/room_agent.py) 对显式 context_generation 的公共请求加载
[room-context.ts](../cannbot/pi/extensions/room-context.ts)。不要求配置代码项目；代码读取仍须另行批准快照。
继续禁用内置工具、用户 context files 和私人工作区，使用独立 home/session/cwd。
旧缺失代次的 request 不被隐式迁入新媒体 Session。
普通私人 CLI 的同步安装器排除该受限扩展；它只由已授权公共 worker 显式加载，不能自动发现后要求私人会话提供房间凭据。

模型由管理员的公共启动配置选择。`room_read_media` 从 SDK 当前 ExtensionContext 读取模型 input 能力；
文字模型或能力未知时，在任何媒体 HTTP 读取之前返回明确的视觉不可用错误。该声明只是准入条件，
不能靠手动增加 `image` 标志证明模型能理解图片，仍需实际 provider 输入与回答验收。
文字搜索、文档片段和转写读取不要求视觉模型。

成功读取输出独立 `{type:"image", mimeType:"image/jpeg", data:...}` 内容块；base64 不混进文字摘要或 details。
source_id/asset/revision/锚点保留在相邻结构化文字中。流式和最终消息沿现有链路，不改变 ACK/游标。

## Hermes 显式启用

媒体能力需要**两个管理员配置同时启用**：公共 Native 启动器配置，以及 Connector registry 对应 Agent，
都设置整数 `room_context_contract: 1`。已有 `room_workspace_contract: 1`、公共只读策略和内部 authority 配置继续保留。
禁止普通成员在消息或工具参数中配置这些字段。

启用后的工具集严格为：

```yaml
platform_toolsets:
  api_server: [todo, spark_workspace, spark_room_context]
```

新增集合必须恰好解析出三个 room 工具；保留 `workspace_read` 与原生 `todo_list`，不允许 terminal、MCP 等额外工具。
memory/user profile、tool search、checkpoint 等公共隔离限制保持原样。
未启用该字段的旧代码协作配置继续使用原来的两组工具，不因新 request 都有 context_generation 而自动升级。

启动器不会覆盖现有 home/config.yaml。不匹配时返回可操作、无私密路径的错误，要求管理员协调
**现有公共**启动配置与公共 config.yaml 的工具集；不要删除 home、复制私人 Profile 或清空旧 Session。
新 home 由启动器生成匹配策略。该开关不代表已经通过真实视觉验收。

## 原生转换、恢复与边界

[Hermes 适配](../cannbot/scripts/hermes_public_workspace.py) 注册独立 `spark_room_context`，使用现有
`_multimodal` / `image_url` 协议。可信 scope 与实例 converter 仅经 ContextVar 传递，退出时清理；
不会写入聊天正文、权限 JSON 或跨请求全局状态。

原生模型能力、provider 限制、已学到的拒图状态都由 Native 原方法判断。实例包装只检查实际结果：

- 已知不支持、图片 URL 丢失或被替换：返回 `success:false, code:501`，不携带成功摘要、元数据或 base64。
- 首次 provider 拒图或载荷恢复时，Native 可能直接去掉消息内图片；包装保留原生重试与 learned-state 行为，
  将实际去图后的工具内容改为明确不可用。保留消息角色、call ID、顺序和原方法布尔返回。
- 去图发生在 `tool.completed` 之后时，不能撤回已发送的成功读取事件；它只能证明读过资料，后续模型输入会说明图片不可用。

当前安装版本另有 todo 恢复兼容差异：工具名为 `todo_list`，旧 hydration 配对助手只认识 `todo`。
适配仅在公共实例的 hydration 输入深拷贝里改结构化 assistant function 名称，仍由 Native 原方法核对 call ID、
角色边界与 revision 并恢复原 TodoStore。原历史、原生安装源、私人 Profile、API 响应和实际工具调用均不修改。
已支持新名字的 Native 版本保持原样；未知接口拒绝启用。真实重启恢复和房间/代次隔离已单独观测。

## 可重复验证与当前失败

```bash
env PYTHONDONTWRITEBYTECODE=1 python3 cannbot/scripts/test_room_context.py
node tests/room_context_test.cjs
# 真实模型，临时公共 home；需操作者已授权的 MODEL 凭据和已安装 Native 环境。
python3 tests/room_context_runtime_probe.py --isolation --out /tmp/native-room.json
# 复用前次保存的相同图片；不覆盖旧结果，不降低检查标准。
python3 tests/room_context_runtime_probe.py --runtime pi \
  --fixtures-report /tmp/native-room.json --out /tmp/native-room-replay.json
# 真实 Logic/SQL/Redis/解析，并接入两种工具客户端；仍没有真实模型或 Kafka。
SPARK_TEST_CONTEXT_IMAGE=spark-media:development \
SPARK_TEST_MEDIA_MODEL=/path/to/approved-asr-model \
SPARK_TEST_CONTEXT_PI_NODE=/path/to/node \
python3 tests/run_mysql_review_tests.py build-wsl
```

`room_context_native_policy_test.py` 还可由已安装 Native Python 执行，传 `--source` 与 `--out`。
它使用真实 Native converter/strip，能力谓词受控，不调用 provider，不能算模型 E2E。

当前同图复跑中 Pi 仍错误识别第二图的右侧绿色三角形。观测确认原生工具与 provider 输入都含真实 JPEG，
function call → asset ID → reference hash → 图片 hash 一致，因此不能将接线检查通过说成视觉答案正确。
一次更早的 Pi 精确记忆回答检查也失败，后续完整观测的实际历史与回答通过；所有失败均保留。
Hermes 对同组图片回答正确，原生 todo 在清洁重启后恢复，其他房间及新代次没有旧 todo/history 标记。
这些模型测试仍使用 HTTP authority 夹具；真实服务权限测试另行计数，二者相加不等于完整产品 E2E。

后续真实服务链路的已知成功与失败见[服务链路报告](multimodal-service-e2e.md)。
Web 资料流及任务来源复验已接入，见[来源 API](room-context-tools.md#成员回查与任务来源)。
仍需完成最终消息引用、共享删除，以及三用户的 20 分钟音频决策到持久任务和视频画面与音轨联合分析验收。
旧 @CANN、私人 Pi/Hermes、accepted/delivered、msg_seq、outbox 和未知执行恢复合同保留。
