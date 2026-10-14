#pragma once

#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
// Actor IDs are resolved by trusted adapters, never copied from client JSON.
//
// 译：actor（操作者 id）必须由可信的适配层解析出来，绝不能从客户端 JSON 里拿。
// 这是全工程权限模型的基础假设，具体落地在 HTTP 适配层：
// handler 先调 requireAdmin(req, resp) 校验 Authorization: Bearer 里的
// 管理员 token（走 UserService::AuthenticateAdmin），**通过之后**才把
// 常量 kAdminUserId 当 actor 传给 service。actor 从来不读请求体，
// 因此客户端写 {"user_id": 1} 或 {"actor": 1} 都没有任何作用。
// gRPC 侧用的是另一套信任模型：AuthorizeInternalRpc 验内部 RPC 密钥，
// 确认对端是可信的 Comet/管理端之后才接受它声明的身份。
//
// 新增管理接口时必须保持这个形状：先鉴权、再代入 actor。
// 若图方便直接把 body 里的 user_id 传进来，权限模型就当场失效。
//
// 写成 inline 函数而不是宏或常量：它返回的是完整的 ServiceStatus，
// 调用方可以直接 `if (auto s = RequireAdministrator(actor); !s.ok()) return s;`，
// 避开忘设错误文本或忘设状态码。
inline ServiceStatus RequireAdministrator(int64_t actor) {
    return actor == kAdminUserId ? ServiceStatus{}
                                 : ServiceStatus{403, "administrator required"};
}
}  // namespace sparkpush
