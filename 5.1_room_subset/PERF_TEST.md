### 目标

目前只压测HTTP
- **部署1个comet，1个logic，1个job**
- **设置2个用户**
 
---

### 一键启动/停止（含 web_demo）

先编译：

```bash
mkdir -p build && cd build
cmake .. && make -j$(nproc)
```

启动（comet + logic + job + web_demo）：

```bash
bash scripts/start_all.sh
```

停止：

```bash
bash scripts/stop_all.sh
```

查看状态：

```bash
bash scripts/status_all.sh
```

日志与 PID：

- 日志目录：`run/logs/*.log`
- PID 目录：`run/pids/*.pid`

---
 

### wrk 压测（HTTP 单聊 send vs send-fast）

如果你只想快速对比 **HTTP 单聊接口**的吞吐/延迟（`/api/message/send` vs `/api/message/send-fast`），推荐直接用 wrk：

- 文档与命令：`perf_cpp/wrk测试.md`
- Lua 脚本：`perf_cpp/wrk_single_send.lua`
- 一键脚本（自动注册/登录拿 token）：`scripts/wrk_single_send.sh`

---

### 服务端统计（以服务端为准）

logic/job/comet 会每 5 秒输出一组 `PERF_SUM ...` 行到各自日志中，用于观测：

- count（近似吞吐）
- err（错误数）
- avg_ms（平均耗时）
- buckets_ms（延迟分桶）

建议观测：

- logic：`logic.http.single_send` / `logic.http.room_send` / `logic.grpc.upstream.*`
- job：`job.kafka.parse` / `job.process_push_request` / `job.rpc.push_to_comet`
- comet：`comet.grpc.push_to_comet`


下面给你一套 **纯手动 curl** 的验证流程（能确认 `/api/message/send` 和 `/api/message/send-fast` 都真的打通，并且返回 `code=0`）。

> 默认 logic HTTP：`http://127.0.0.1:9101`（可改 BASE_URL）

---

### 1) 准备：注册两个用户（sender + target）并拿到 token/uid

```bash
BASE_URL="http://127.0.0.1:9101"
SEED="$(date +%s)"

SENDER_ACCOUNT="curl_sender_${SEED}"
TARGET_ACCOUNT="curl_target_${SEED}"
SENDER_PASS="pass_${SEED}_a"
TARGET_PASS="pass_${SEED}_b"

md5() { python3 -c 'import hashlib,sys;print(hashlib.md5(sys.stdin.buffer.read()).hexdigest())'; }

SENDER_PASS_MD5="$(printf "%s" "$SENDER_PASS" | md5)"
TARGET_PASS_MD5="$(printf "%s" "$TARGET_PASS" | md5)"

sender_reg="$(curl -sS -H 'Content-Type: application/json' \
  -d "{\"account\":\"${SENDER_ACCOUNT}\",\"password\":\"${SENDER_PASS_MD5}\",\"name\":\"${SENDER_ACCOUNT}\"}" \
  "${BASE_URL}/api/register")"

target_reg="$(curl -sS -H 'Content-Type: application/json' \
  -d "{\"account\":\"${TARGET_ACCOUNT}\",\"password\":\"${TARGET_PASS_MD5}\",\"name\":\"${TARGET_ACCOUNT}\"}" \
  "${BASE_URL}/api/register")"

SENDER_TOKEN="$(python3 -c 'import json,sys;print(json.loads(sys.argv[1])["data"]["token"])' "$sender_reg")"
TARGET_UID="$(python3 -c 'import json,sys;print(json.loads(sys.argv[1])["data"]["user_id"])' "$target_reg")"

echo "SENDER_TOKEN=$SENDER_TOKEN"
echo "TARGET_UID=$TARGET_UID"
echo "sender_reg=$sender_reg"
echo "target_reg=$target_reg"
```

如果你担心“注册失败但脚本没报错”，可以立刻检查：

```bash
python3 -c 'import json,sys;print("sender_code=",json.loads(sys.argv[1]).get("code"))' "$sender_reg"
python3 -c 'import json,sys;print("target_code=",json.loads(sys.argv[1]).get("code"))' "$target_reg"
```

---

### 2) curl 验证 `/api/message/send`（必须返回 `code=0`）

```bash
curl -sS -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${SENDER_TOKEN}" \
  -d "{\"msg_type\":\"text\",\"target_type\":\"single_chat\",\"target_id\":${TARGET_UID},\"content\":\"hello send\",\"client_msg_id\":\"curl-send-${SEED}\"}" \
  "${BASE_URL}/api/message/send"
```

你应该看到类似（关键是 `code:0`）：

```json
{"code":0,"message":"ok","data":{"msg_id":"...","msg_seq":1,"session_id":"s_..."}}
```

---

### 3) curl 验证 `/api/message/send-fast`（必须返回 `code=0`）

```bash
curl -sS -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${SENDER_TOKEN}" \
  -d "{\"msg_type\":\"text\",\"target_type\":\"single_chat\",\"target_id\":${TARGET_UID},\"content\":\"hello send-fast\",\"client_msg_id\":\"curl-fast-${SEED}\"}" \
  "${BASE_URL}/api/message/send-fast"
```

同样应返回 `code=0`，并有 `msg_id/msg_seq/session_id`。

---

### 4) 一句话判定“是否真的成功”

- **成功标准**：HTTP 返回体里 **`"code":0`**（wrk 脚本也是用这个判定 app_ok 的）
- **常见失败**：
  - `401 unauthorized`：token 没带/无效
  - `500 redis failed: ...`：Redis/Lua 相关问题或 Redis 不可用
  - `400 ...`：`target_type/target_id/content` 不符合要求

如果你把第 2/3 步的输出贴出来（尤其是 `send-fast` 的返回体），我也可以帮你快速定位失败原因。