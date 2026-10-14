#pragma once

#include <string>

namespace sparkpush {

// Business errors, not HTTP status codes or grpc::Status. Adapters map these
// to the existing wire contract. The reason preserves legacy login behaviour
// without coupling services to HTTP response classes.
//
// 译：这是**业务层**的错误表达，不是 HTTP 状态码也不是 grpc::Status。
// 由适配层（http_server.cpp / grpc_service.cpp）把它映射到具体协议。
//
// 为什么要单独搞一个类型而不是直接用 int：service 层同时被 HTTP 与 gRPC
// 两个适配器调用，若直接返回 HTTP 码就把业务逻辑绑死在一种传输上了。
//
// code 的约定：0 = 成功（而不是 200）。这让 ServiceStatus{} 默认构造就是成功，
// 写 `return {};` 比 `return {200, "ok"}` 不容易出错。
// 非 0 值习惯上用 HTTP 语义的数字（400/403/404/409/429/500），
// 但这只是约定，gRPC 适配器会另行映射。
//
// reason 字段存在的唯一理由：有些业务区分无法用 code + message 表达。
// 例如 kMembershipLookupFailed 与普通 403 都是「无权访问」，
// 但前者是「查成员关系时数据库出错」，需要拼上详细错误文本；
// 用枚举而不是比对 message 字符串，避开把控制流建在可变文案上。
enum class ServiceReason { kNone, kInactiveAccount, kMembershipLookupFailed };
struct ServiceStatus {
    int code{0};
    std::string message{"ok"};
    ServiceReason reason{ServiceReason::kNone};
    bool ok() const { return code == 0; }
};

}  // namespace sparkpush
