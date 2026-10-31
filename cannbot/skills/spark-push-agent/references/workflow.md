# Kernel Agent 子 Agent 工作流

## 何时启用

满足任一条件时启用：跨两个以上运行组件；改变 ACK、游标、幂等、历史或重连语义；修改 Pi
工具/会话/模型路由；用户要求独立审查或完整验收。纯问答、小型文档修正和明确的局部改动由主
Agent 直接完成。

## 顺序

1. 主 Agent 加载 `spark-push-knowledge` 并确认实时源码是否可访问。
2. 通过 `spawn_agent` 派发 architect 子任务，只读输出事实、计划、风险和测试矩阵。
3. 主 Agent 核对计划；存在会改变用户目标的选择时先请求用户决定。
4. 用 `wait_agent` 收集计划后，主 Agent 实施或派发 developer；输入包含计划与 `allowed_writes`。
   多个 developer 的写入文件必须不重叠，主 Agent 同时避免修改它们持有的文件。
5. Developer 完成后，派发 reviewer / tester 独立只读检查。Tester 检查测试覆盖与证据；
   它没有 shell，构建和测试命令由主 Agent 执行，再用 `send_input` 补充实际输出。
6. 有阻塞问题时，把具体证据交给 Developer 修复，再重新审查/测试；最多三轮。
7. 主 Agent 读取最终 diff、复核测试证据；确认每个子任务已收集，`close_agent` 释放会话后交付。

只有依赖独立的研究、审查或互不修改同文件的开发任务可并行。子 Agent 使用独立内存上下文，
不自动继承整段主聊天；必要事实与证据应显式传入。扩展未加载时由主 Agent 执行并说明限制，
不得把同一会话角色切换当作独立子 Agent。房间子 Agent 只读；房间授权的写入和测试由主 Agent 执行。

## 委派上下文最小字段

```text
project_root: <绝对路径>
goal: <本角色唯一目标>
relevant_files: <明确路径>
known_facts: <来自代码/协议的事实>
constraints: <可靠性、权限、禁止项>
allowed_writes: <只读或明确文件范围>
validation: <要执行的命令和成功标准>
expected_output: <结构化交付内容>
```

Spark Push 聊天 Bot 经 Kernel Agent gateway 调用时，cwd 就是项目根目录，允许使用工具修改和验证代码。
只有确认当前进程看不到该工作树时，才把 `project_root` 标为不可访问并保持只读分析。
