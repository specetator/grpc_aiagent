-- wrk 压测脚本：/api/message/send (single_chat)
--
-- 用法（建议配合 scripts/wrk_single_send.sh 一键脚本）：
--   export SPARKPUSH_WRK_TOKEN="tk-..."
--   export SPARKPUSH_WRK_TARGET_ID="1002"
--   wrk -t8 -c200 -d10s --latency -s perf_cpp/wrk_single_send.lua http://127.0.0.1:9101

local token = os.getenv("SPARKPUSH_WRK_TOKEN") or ""
local target_id = tonumber(os.getenv("SPARKPUSH_WRK_TARGET_ID") or "0")
local debug = os.getenv("SPARKPUSH_WRK_DEBUG") or ""
local path = os.getenv("SPARKPUSH_WRK_PATH") or "/api/message/send"

if token == "" then
  error("missing env SPARKPUSH_WRK_TOKEN")
end
if not target_id or target_id <= 0 then
  error("missing/invalid env SPARKPUSH_WRK_TARGET_ID")
end

wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"
wrk.headers["Authorization"] = "Bearer " .. token

math.randomseed(os.time())

-- wrk 的 done() 在“主 state”执行，response() 在“线程 state”执行；
-- 要在 done() 聚合统计，需要通过 setup(thread) 保存 thread 对象，并用 thread:get 取回线程内变量。
local threads = {}

function setup(thread)
  table.insert(threads, thread)
end

function init(args)
  -- 这些变量存在于“每线程 state”，done() 里需要用 thread:get() 聚合
  app_ok = 0
  app_err = 0
  http_err = 0
  first_status = nil
  first_body = nil
end

function request()
  -- client_msg_id 仅用于链路追踪/回执关联；HTTP 路径不做去重，随机即可
  local cmid = tostring(math.random(1, 2147483647))
  local body = string.format(
    '{"msg_type":"text","target_type":"single_chat","target_id":%d,"content":"hello","client_msg_id":"wrk-%s"}',
    target_id, cmid
  )
  return wrk.format(nil, path, nil, body)
end

function response(status, headers, body)
  if first_status == nil then
    first_status = status
    first_body = body
  end
  if status ~= 200 then
    http_err = http_err + 1
    return
  end
  -- 兼容无空格与有空格两种 JSON 序列化风格
  if body and (string.find(body, '"code"%s*:%s*0') or string.find(body, '"code":0', 1, true)) then
    app_ok = app_ok + 1
  else
    app_err = app_err + 1
  end
end

function done(summary, latency, requests)
  local ok = 0
  local app_e = 0
  local http_e = 0
  local fs = nil
  local fb = nil

  for i, t in ipairs(threads) do
    ok = ok + (t:get("app_ok") or 0)
    app_e = app_e + (t:get("app_err") or 0)
    http_e = http_e + (t:get("http_err") or 0)
    if i == 1 then
      fs = t:get("first_status")
      fb = t:get("first_body")
    end
  end

  io.write(string.format("app_ok=%d app_err=%d http_err=%d\n", ok, app_e, http_e))
  if debug ~= "" then
    io.write(string.format("first_status=%s\n", tostring(fs)))
    io.write(string.format("first_body=%s\n", tostring(fb)))
  end
end


