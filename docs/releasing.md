# 发布 Spark Push v0.1.0

维护者流程：固定提交 → CI → 不可变候选镜像和安装包 → 实际验收 → 原样推广。
当前提供流程和测试工具；是否已发布及最终结果以 GitHub Release 的实际回执为准。
目标平台为 Linux/amd64，适用于 Linux 主机与 WSL2；基础 IM 不需要模型或 GPU。

## 构建候选

先提交并推送全部待发行改动，保持 `main` 在该提交直到验收结束。运行：

```bash
gh workflow run release.yml -f operation=candidate -f candidate=v0.1.0-rc.1
```

工作流复用 CI 的构建、CTest、MySQL/Redis/Kafka 与知识校验。成功后构建一个镜像，
设置 OCI source/revision/version，发布 rc 和完整提交标签；拒绝覆盖已有候选。
Prerelease 中的安装包只读取该 Git 提交的文件，不读取工作区、个人 `.env` 或 Runtime。
应用和四种依赖镜像都按 digest 固定，发行 Compose 没有 build fallback。
`release.json` 记录提交、registry digest、linux/amd64 manifest/config digest 和文件 SHA；
`SHA256SUMS` 校验外层包。`builder_image_id` 仅作构建机诊断：classic 与 containerd
store 的本地 ID 表示可能不同。验证保留 registry/平台/OCI 来源，并将容器与同一
daemon 实际检查到的镜像相对应，不跨机器比较这个本地 ID。
[Docker 的存储说明](https://docs.docker.com/engine/storage/containerd/)。

候选发布不代表实际验收已通过。新 GHCR 包默认可能为 private；在包设置中将
**这个发行包**设为 public，再以空 Docker 客户端配置验证匿名拉取。不要更改私人
Profile、凭据或其他包的可见性。仓库公开不等于包已允许匿名拉取。
[GitHub 的包可见性说明](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)。

## 验证固定安装包

```bash
gh release download v0.1.0-rc.1 --dir /absolute/new/candidate-assets
cd /absolute/new/candidate-assets
sha256sum --check SHA256SUMS
tar -xzf spark-push-v0.1.0-linux-amd64.tar.gz
cd spark-push-v0.1.0
python3 scripts/verify_release.py files --directory .
python3 scripts/verify_release.py images --directory . --pull
cp .env.example .env
docker compose pull
docker compose up -d --no-build --wait
```

访问 `http://localhost:9010`。检查实际容器身份可运行：

```bash
python3 scripts/verify_release.py running --directory . --project sparkpush
```

不要在运行验收时修改安装包内的受检文件。Agent 目录、运行状态、模型凭据与测试
回执放在独立目录；源码开发继续使用 Git checkout 和原构建方式。

## 实际验收

从解压后且已校验的目录运行现有测试，`--image` 统一使用 `release.json` 中的
`application.reference`。测试会核对实际容器 ID、镜像和 OCI revision，而非只看配置。
Python 3.11+ / websockets、Playwright 与真实模型仅是验收工具所需；基础安装不需要它们。

| 回执名 | 从安装包运行的命令（另加 `--image <digest>` 和 `--out <回执名>.json`） |
|---|---|
| `web_im` | `python3 tests/collaboration_web_e2e.py` |
| `web_agents` | `python3 tests/collaboration_web_e2e.py --agents` |
| `cross_room` | `python3 tests/public_room_runtime_e2e.py --cross-room` |
| `team_backup` | `python3 tests/public_room_runtime_e2e.py --team-backup` |
| `legacy_private` | `python3 tests/legacy_private_runtime_e2e.py` |
| `im_backup` | `python3 tests/backup_restore_e2e.py` |
| `cann_documents` | `python3 tests/backup_restore_e2e.py --cann-documents` |
| `clean_linux` | `python3 tests/clean_linux_release_e2e.py --bundle /absolute/candidate-assets/spark-push-v0.1.0-linux-amd64.tar.gz --out /absolute/results/clean_linux.json`（不加 `--image`） |

每条命令必须保存真实退出码到同名 `.exit` 文件，失败不能改写成 0。示例：

```bash
python3 tests/backup_restore_e2e.py --image '<完整 digest 引用>' --out /absolute/results/im_backup.json
result=$?
printf '%s\n' "$result" > /absolute/results/im_backup.exit
test "$result" -eq 0
```

同一 Docker daemon 的备份测试须串行运行，并使用独占维护窗口。真实 Runtime
前提见现有 fixture 和 [公共部署](optional-agent-connector.md)、[legacy 部署](cann-docker.md)。
测试只使用授权模型的专用临时配置，不能将个人 Hermes Profile 直接共享。
`legacy_private` 的 CANN 知识源目前从安装包父目录的 `cann-agent-knowledge` 读取；
这是既有本机回归 fixture 的路径要求，不是普通用户安装要求。

`clean_linux` 创建一个临时 privileged Docker-in-Docker 容器和空卷；不挂宿主 socket、
Docker 登录目录、既有数据卷或模型文件。它匿名拉取镜像，复用现有十用户 IM、
成员/附件权限、断线恢复和正常重启测试，最后只清理自己的资源。准确范围是
“WSL 宿主上的干净 Linux 用户空间和独立 daemon”，不是独立 VM/内核或浏览器验收。

运行结束后，用原候选 assets 目录和八份回执生成验收汇总：

```bash
python3 scripts/verify_release.py collect --directory /absolute/candidate-assets \
  --evidence /absolute/results/acceptance.json
```

只有全部实际退出码、源码指纹、提交、镜像、场景及容器身份对应时才生成文件。
Mock/契约测试不能放入此集合；它们由 CI 和单独阶段报告记录。公开上传前检查回执，
仅上传汇总和这八份 JSON；不要上传 `.private.log`、凭据或 Runtime 数据库。

## 原样推广

把 `acceptance.json` 和八份场景 JSON 附加到同一个候选 prerelease（不覆盖原安装包），
再运行：

```bash
gh workflow run release.yml -f operation=promote -f candidate=v0.1.0-rc.1
```

工作流要求候选 revision 等于当前提交，校验包及全部回执，先验证匿名拉取，再把
同一个 manifest 标记为 `v0.1.0` 并比较 digest。它不重新构建镜像或重打安装包；
正式 Release 附上原包和证据。已有 stable 标签只允许与候选相同，不覆盖别的镜像。
[Docker 的 manifest 复制行为](https://docs.docker.com/reference/cli/docker/buildx/imagetools/create/)。

任一产品或制品变化都必须产生新提交和新候选；不能把旧测试结果迁移成新版本通过。
网络或发布步骤失败时保留已经产生的候选，先核对实际 digest/标签状态；不强推或
覆盖旧候选来恢复流水线。

升级、回滚和卸载遵循 [安装指南](install.md) 与 [整批备份恢复](backup-restore.md)：
先冷备并停机，保留旧镜像与 Runtime 指纹。恢复使用原镜像，Agent 默认关闭；
两次实际能力检查通过后才启用公共 Runtime。`docker compose down` 保留数据；
只删除解压的程序目录也不删除命名卷。完全删除卷属于另一个明确的卸载动作。
