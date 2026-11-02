# 媒体处理服务启停

媒体 worker 继续在受信宿主机运行；解析器仍在禁网、只读、有限资源的 Docker 容器中执行。
`scripts/sparkctl.sh media` 管理这个已有 worker，不新增处理循环，不把 Docker socket 挂进 Logic 或 Agent。
需要 Linux、Docker 权限、Python 3 和已运行的 systemd 用户管理器。没有用户管理器时使用 `media run` 前台运行。

## 配置一次，独立运行

先完成[媒体迁移与内部入口配置](multimodal-service-e2e.md)，预置不可变 parser 镜像与本地 ASR 模型。
由服务账号保存独立的 worker key，文件权限 0600；其值须与 Logic 的 `SPARK_PUSH_MEDIA_WORKER_KEY` 一致。
不要使用登录 token 或 Connector 授权 key 代替。

复用已有命令计算当前指纹：

```bash
python3 cannbot/scripts/media_worker.py \
  --image-id sha256:ADMIN_APPROVED_IMAGE_ID \
  --model-dir /srv/spark-media/models/approved-small --describe
```

将 `deploy/media-worker.json.example` 复制到新的私有配置文件，权限设为 0600，并填写绝对路径与上述指纹。
配置只接受声明的字段，不加载 `.env.local`、私人 Agent Profile、聊天参数或未指定的模型目录。

| 字段 | 含义 |
|---|---|
| `schema` | 整数 `1` |
| `image_id` | 已批准的 `sha256:...` 镜像 ID，不接受可变 tag |
| `model_dir` | 已预置的完整模型绝对目录 |
| `origin` | 内部 HTTP(S) origin；可选 Compose overlay 默认为 `http://127.0.0.1:9011` |
| `key_file` | 服务账号拥有的 0600 凭据文件绝对路径 |
| `state_dir` | 本实例独占的持久状态目录，沿用现有锁与恢复规则 |
| `pipeline_sha256` / `parser_sha256` | 当前 worker `--describe` 的批准指纹；同时配置到 Logic 对应环境变量 |

启动器会重新计算实际模型和代码指纹。与文件中的批准值不同则在读取执行状态或连接后端之前失败，
不会替管理员更新批准值。原来的单独 CLI 参数仍兼容，不能与 `--config` 混用。

```bash
bash scripts/sparkctl.sh media describe --config /srv/spark-media/worker.json
bash scripts/sparkctl.sh media start --config /srv/spark-media/worker.json
bash scripts/sparkctl.sh media status
bash scripts/sparkctl.sh media logs
bash scripts/sparkctl.sh media stop
```

`start` 创建 `spark-media-worker.service` 临时用户服务。它等待 Python 成功执行，不宣称 Logic 授权或解析任务已经成功。
实际处理结果看 worker 日志和房间资料状态；仅有 systemd `active` 不能证明 key、迁移和 Logic 批准值正确。
重复启动同名服务会被进程管理器拒绝，不替换正在运行的配置。

多个实例可用 `--unit spark-media-worker-名称`，每个实例必须有独立 `state_dir`。
启动命令可用 `SPARK_PUSH_MEDIA_PYTHON` 指定已安装 Python 的绝对路径；服务命令中只保存配置文件路径，不保存 key 内容。
这些服务不自动开机启动；宿主机或用户管理器重启后由管理员重新启动。基础 IM 的 `up/down` 不隐式管理这些独立实例。

## 停止与恢复

- 进程管理器负责 PID 身份和信号，不使用宽泛 `pkill`。`stop` 校验服务描述归属当前项目，拒绝操作同名外来服务。
- 异常退出后等待 10 秒再启动；120 秒内最多允许启动 3 次。持续配置错误会进入失败状态，不无限拉起。
- 正常 `stop` 发送 SIGTERM，最长等待 90 秒；worker 继续使用现有取消、Docker 回收和持久状态恢复协议。
  超过期限才由进程管理器强制结束；之后必须按未确认执行处理，不能称作正常清理成功。
- 停止不会删除状态、原文件、派生物、SQL 记录或旧 Agent Session。服务再次启动仍使用原 `state_dir`。
  既有 worker 在 SIGTERM 后返回退出码 1，systemd 日志可能显示 `exit-code`；`stop` 成功只确认进程已停止，
  不代表待处理任务已完成。恢复状态仍由下次启动与服务端租约核实。
- 更新代码、模型或镜像前，应停止接收新处理任务并完成/核实旧领取。指纹改变时不能删除 pending 状态绕过原恢复校验；
  新配置也不会自动修复旧管线的未知执行。

前台运行使用 `bash scripts/sparkctl.sh media run --config /srv/spark-media/worker.json`。
服务端仍决定成员权限、处理租约与最终发布；启动器只是运维入口，不能授予房间 Agent 新工具权限。

## 定向验证

```bash
python3 tests/media_worker_test.py
SPARK_TEST_MYSQL_WORKER_ONLY=1 SPARK_TEST_MEDIA_WORKER_TEST=1 \
SPARK_TEST_MEDIA_SYSTEMD=1 SPARK_TEST_MEDIA_IMAGE=spark-media:development \
SPARK_TEST_MEDIA_MODEL=/path/to/approved-model \
SPARK_TEST_MEDIA_FIXTURES=/path/to/synthetic-fixtures \
SPARK_TEST_MEDIA_WORKER_REPORT=/tmp/media-ops.json \
python3 tests/run_mysql_review_tests.py build-wsl
```

该入口只跑媒体 worker 相关真实 HTTP/MySQL/Redis/解析用例，可选验证独立 systemd 用户服务。
不启用 `SPARK_TEST_MEDIA_LONG_AUDIO` 时不运行长录音强杀用例；Native Agent、Kafka、Web 和整体验收另行记录。

2026-11-01 的[实际回执](benchmarks/2026-11-01-media-operations.json)：9 项契约检查、15 组真实 worker 检查通过；
另用实际临时服务验证了同名外来描述的停机拒绝。首次运行因测试参数与 systemd 249 不兼容失败，修正后通过，失败记录保留。
测试服务与容器已清理，没有启动正式部署或更改私人 Agent 配置。
