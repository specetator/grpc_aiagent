#pragma once

#include <string>

#include <nlohmann/json.hpp>

namespace sparkpush {

// 知识库目录（catalog）的只读访问层。整个模块的核心设计原则是：
//
//   **调用方只能传稳定 ID，绝不能传路径。**
//
// 文件系统路径由本模块内部拼出来，外部无从干预。这是路径穿越的
// 唯一防线：一旦开放「传相对路径」的接口，"../../etc/passwd" 之类的
// 输入就会把服务器上的任意文件泄露给客户端。
//
// ID 形态被锁死成 `doc_` / `chk_` + 恰好 24 位小写十六进制字符。
// 这个字符集里天然不含 '/'、'\\'、'.'，所以校验通过就已经排除了穿越。
// 同一个规则在 citation_validation.cpp 里被复制了一份（见那里的注释），
// 两处必须保持同步。
//
// 数据在磁盘上的组织形式是「按 generation 分代 + 按文档切分」：
//   <root>/data/current.json                              当前代指针
//   <root>/data/generations/<generation>/documents/<doc_id>.json
// 分代的好处是索引重建（RAG 重新切块）时可以整代切换、旧代仍可回滚，
// 且客户端引用里的 doc_id 在不同代之间保持稳定。

// ID 格式校验。只判形状，不查存在性——存在性由 LoadKnowledgeDocument
// 读文件时决定。两者分开是为了让调用方能对「格式非法」返回 400、
// 对「格式合法但内容不存在」返回 404（见 KnowledgeService::Document）。
bool IsValidKnowledgeDocumentId(const std::string& value);
bool IsValidKnowledgeChunkId(const std::string& value);

// 从当前 generation 的按文档视图读取内容。调用方只能传稳定 ID，不能传路径。
//
// chunk_id 为空 -> 返回整份文档；非空 -> 额外附带那一个 chunk。
// 输出形如 {"generation":..., "document":{...}, "chunk":{...}}。
//
// 失败时 error 里的文本会**一路透传到 HTTP 响应体**
//（KnowledgeService::Document 返回 {404, error}），因此实现里所有
// 错误串都是刻意写得不含文件系统路径的固定文案，别往里拼 path。
bool LoadKnowledgeDocument(const std::string& knowledge_root,
                           const std::string& doc_id,
                           const std::string& chunk_id,
                           nlohmann::json* output, std::string* error);

}  // namespace sparkpush
