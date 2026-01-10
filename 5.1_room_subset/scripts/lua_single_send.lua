-- wrk 压测脚本：/api/message/send (single_chat)
--
-- 用法（建议配合 scripts/wrk_single_send.sh 一键脚本）：
--   export SPARKPUSH_WRK_TOKEN="tk-..."
--   export SPARKPUSH_WRK_TARGET_ID="1002"
--   wrk -t8 -c200 -d10s --latency -s perf_cpp/wrk_single_send.lua http://127.0.0.1:9101
-- 注：下面所有注释均为中文，解释脚本在 wrk 环境下的行为与设计要点。

-- 从环境变量读取必要参数：
-- SPARKPUSH_WRK_TOKEN: 由外部脚本（如 wrk_single_send.sh）导出，用于 Authorization 头
-- SPARKPUSH_WRK_TARGET_ID: 目标用户 id（整数），写入请求体中的 target_id
-- SPARKPUSH_WRK_DEBUG: 非空时打印首个响应的状态与 body，便于调试
-- SPARKPUSH_WRK_PATH: 可覆盖请求路径，默认 /api/message/send
local token = os.getenv("SPARKPUSH_WRK_TOKEN") or ""
local target_id = tonumber(os.getenv("SPARKPUSH_WRK_TARGET_ID") or "0")
local debug = os.getenv("SPARKPUSH_WRK_DEBUG") or ""
local path = os.getenv("SPARKPUSH_WRK_PATH") or "/api/message/send"

-- 校验必需环境变量，缺失则立即报错并停止 wrk 线程
if token == "" then
  error("missing env SPARKPUSH_WRK_TOKEN")
end
if not target_id or target_id <= 0 then
  error("missing/invalid env SPARKPUSH_WRK_TARGET_ID")
end

-- 设置 HTTP 方法与请求头（所有请求都使用同一 token）
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
wrk.headers["Authorization"] = "Bearer " .. token

-- 为每个线程设置随机数种子（基于时间），用于生成随机的 client_msg_id
math.randomseed(os.time())

-- 说明 wrk 的执行模型与脚本状态：
-- - init(args)、request()、response() 在每个线程的线程状态（thread state）中执行
-- - setup(thread) 在每个线程调用一次，可以把 thread 对象保存到全局表，方便 done() 在主状态聚合
-- - done(summary, latency, requests) 在主状态（main state）执行，用于打印聚合统计

-- threads: 保存所有线程的 thread 对象，done() 时迭代聚合每线程的统计值
local threads = {}

-- setup(thread): 每个线程调用一次，接收一个 thread 对象，保存以便 done() 聚合
function setup(thread)
  table.insert(threads, thread)
end

-- init(args): 在线程启动时执行一次，用于初始化线程局部变量（这些变量由 thread:get()/set() 可见）
function init(args)
  -- 这些变量是线程局部的计数器：
  -- app_ok: 应用级成功（响应 body 中 code==0）计数
  -- app_err: 应用级错误计数（响应 body 非 code==0）
  -- http_err: HTTP 层面的非 200 状态计数
  -- first_status/first_body: 保存线程看到的第一个响应，便于 debug 输出
  app_ok = 0
  app_err = 0
  http_err = 0
  first_status = nil
  first_body = nil
end

-- request(): 由 wrk 调用以生成每次要发送的 HTTP 请求
-- 设计要点：生成一个包含 client_msg_id 的 JSON body，避免服务端基于消息 id 做幂等或去重影响压测
function request()
  -- client_msg_id: 随机 int 转为字符串，确保每次请求唯一
  local cmid = tostring(math.random(1, 2147483647))
  local body = string.format(
    '{"msg_type":"text","target_type":"single_chat","target_id":%d,"content":"hello","client_msg_id":"wrk-%s"}',
    target_id, cmid
  )
  -- wrk.format(method, path, headers, body) 传回一个完整的请求给 wrk
  return wrk.format(nil, path, nil, body)
end

-- response(status, headers, body): 每个请求返回时会被调用（线程状态）
-- 逻辑：记录首个响应以便 debug；统计 HTTP 层面与应用层面的成功/失败
function response(status, headers, body)
  -- 记录线程见到的第一个响应（用于 debug 输出），只保存第一个线程内的首个响应
  if first_status == nil then
    first_status = status
    first_body = body
  end

  -- 非 200 直接视为 HTTP 层错误
  if status ~= 200 then
    http_err = http_err + 1
    return
  end

  -- 判定应用级别成功：兼容不同 JSON 序列化风格，检查 body 是否包含 "code":0
  -- 注意：这里没有做严格的 JSON 解析，是为了减少依赖并提升性能
  if body and (string.find(body, '"code"%s*:%s*0') or string.find(body, '"code":0', 1, true)) then
    app_ok = app_ok + 1
  else
    app_err = app_err + 1
  end
end

-- done(summary, latency, requests): 主状态执行，聚合各线程的统计并输出
-- 说明：threads 表中的每个 thread 都保存了线程局部变量，使用 t:get("var") 读取
function done(summary, latency, requests)
  local ok = 0
  local app_e = 0
  local http_e = 0
  local fs = nil
  local fb = nil

  -- 聚合每个线程的计数器
  for i, t in ipairs(threads) do
    ok = ok + (t:get("app_ok") or 0)
    app_e = app_e + (t:get("app_err") or 0)
    http_e = http_e + (t:get("http_err") or 0)
    -- 保存第一个线程的首个响应，便于 debug
    if i == 1 then
      fs = t:get("first_status")
      fb = t:get("first_body")
    end
  end

  -- 输出聚合统计：应用级成功/失败与 HTTP 错误
  io.write(string.format("app_ok=%d app_err=%d http_err=%d\n", ok, app_e, http_e))
  -- 如果开启 debug 环境变量，则打印首个响应状态与 body，便于定位问题
  if debug ~= "" then
    io.write(string.format("first_status=%s\n", tostring(fs)))
    io.write(string.format("first_body=%s\n", tostring(fb)))
  end
end


