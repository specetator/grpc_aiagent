#pragma once

#include <string>

#include <nlohmann/json.hpp>

namespace sparkpush {

// 只保留满足公共 schema、长度上限和 cannkb URI 绑定关系的引用。
//
// 为什么需要这个函数：citations 是 **Agent（模型）生成的内容**，
// 而模型输出必须当成不可信输入。它可能被提示注入误导，
// 也可能单纯幻觉出一个不存在的文档。这些引用会直接展给用户，
// 并可能被客户端拿去调 KnowledgeService 拉原文，因此不验就会出两类问题：
//   1. 展示层：title/uri 里控字符或超长文本污染前端
//   2. 请求层：伪造的 doc_id/chunk_id 被当成合法 id 去拉文件
//
// 返回值是一个新数组（只含通过校验的项），warning 回人类可读的拒绝原因。
// 校验失败**不报错**而是静默丢弃并记 warning：一条不合格引用不应该
// 让整条 Agent 回复失败。
nlohmann::json ValidateHermesCitations(const nlohmann::json& input,
                                       std::string* warning = nullptr);

}  // namespace sparkpush
