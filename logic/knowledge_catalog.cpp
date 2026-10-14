#include "knowledge_catalog.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>

namespace sparkpush {
namespace {

// 稳定 ID 的形状校验：前缀 + 恰好 24 个小写十六进制字符。
//
// 为什么连长度都要精确卡死（而不是只查字符合法）：
// 一个合法字符集但长度任意的 id 仍然可以很长，拼进路径后可能触发
// 文件系统名长限制（Linux 单个分量 255 字节），报错会从「文档不存在」
// 变成「路径非法」，日志里就多了噪声。固定 24 位同时也让它能与
// citation_validation.cpp 里的规则字面对齐。
//
// rfind(expected, 0) == 0 是 C++17 下“starts_with”的惯用写法（std::string
// 直到 C++20 才有 starts_with）。写成 find(...) == 0 也等价，但写成
// find(...) != npos 就错了——那会把 "xdoc_..." 也当成合法前缀。
bool HexId(const std::string& value, const char* prefix) {
    const std::string expected(prefix);
    if (value.size() != expected.size() + 24 ||
        value.rfind(expected, 0) != 0) {
        return false;
    }
    // 只收 [0-9a-f]：大写十六进制也拒。这不是美观问题而是幂等问题：
    // doc_id 会被拼成文件名，如果大小写都放行，同一个文档就有两个
    // 可用 id，客户端去重与引用校验都会失效。
    // lambda 形参写成 unsigned char 是必须的：std::isdigit 对负值（即
    // 高位为 1 的 char）是未定义行为，不转就会在非 ASCII 输入上出事。
    return std::all_of(value.begin() + expected.size(), value.end(),
                       [](unsigned char c) {
                           return std::isdigit(c) || (c >= 'a' && c <= 'f');
                       });
}

// 带体积上限地读一个 JSON 文件。
//
// 先查 file_size 再打开，而不是读完后看长度：知识库文档可能很大
//（下面调用时上限给到 8MB），先 stat 一下就能把异常大文件在
// 分配内存前挡掉，避免一次请求把进程内存打爆。
//
// 三个失败分支的 error 文案都不带 path：这个串最终会进 HTTP 响应体，
// 带上绝对路径就等于把服务器目录结构告诉客户端。
//
// 局限（诚实标注）：size 检查与实际读取之间存在 TOCTOU 窗口，
// 而 nlohmann 的 `input >> *output` 会一直读到 EOF，并不受 max_bytes 约束。
// 因此这个上限是“正常情况下的快速失败”，不是硬隔离。真要硬隔离得改成
// 定长 read + 逐段解析。当前调用方读的是自己生成的目录，不是外部上传，
// 风险可控。
bool ReadJsonBounded(const std::filesystem::path& path, size_t max_bytes,
                     nlohmann::json* output, std::string* error) {
    std::error_code ec;
    // 传 ec 而不抛异常：文件不存在是**正常业务分支**（客户端给了一个
    // 形式合法但已下树的 doc_id），不应该让它变成异常风暴。
    // 附带好处：path 指向目录时 file_size 也会置 ec，统一当“找不到”。
    const auto size = std::filesystem::file_size(path, ec);
    if (ec) {
        if (error) *error = "knowledge document not found";
        return false;
    }
    if (size > max_bytes) {
        if (error) *error = "knowledge document exceeds size limit";
        return false;
    }
    // binary 模式：避免 Windows/WSL 下把 \r\n 转成 \n 而改变字节数，
    // 也让 size 检查与实际读入的字节数对得上。
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        if (error) *error = "knowledge document cannot be opened";
        return false;
    }
    try {
        // nlohmann 的解析失败是靠异常报的（parse_error），而本项目其余
        // 部分统一用 bool+error 返回值。在这里把异常翻译成返回值，
        // 是为了不让异常穿透到 muduo 的回调里（那里没人接）。
        input >> *output;
    } catch (const nlohmann::json::exception&) {
        if (error) *error = "knowledge catalog JSON is invalid";
        return false;
    }
    return true;
}

// generation 是从 current.json 里读出来的，**属于文件内容而非调用方入参**，
// 但仍然要校：它会被直接拼成目录名，而 current.json 是由外部
//（cann-agent-knowledge 的 Python 端）写入的，不在本进程的控制下。
//
// 三条规则各自的用途：
//   长度 16..80   排除空串与异常长值（空串会拼出 ".../generations//documents/..."）
//   不含 ".."     直接拒掉相对上跳（其实下一条已经足以拦住，这里是双重保险）
//   只允许 alnum 和 '-'  真正起作用的一条：排除了 '/'、'\\'、'.' 与空字节
//
// 上游实际生成的形态是 "%Y%m%dT%H%M%SZ-" + sha256 前 12 位
//（见 cann-agent-knowledge/cann_rag/engine.py 的 build），共 29 字符，
// 落在 16..80 区间内且只含 alnum 与 '-'。
bool ValidGeneration(const std::string& value) {
    if (value.size() < 16 || value.size() > 80 ||
        value.find("..") != std::string::npos) {
        return false;
    }
    return std::all_of(value.begin(), value.end(), [](unsigned char c) {
        return std::isalnum(c) || c == '-';
    });
}

}  // namespace

bool IsValidKnowledgeDocumentId(const std::string& value) {
    return HexId(value, "doc_");
}

bool IsValidKnowledgeChunkId(const std::string& value) {
    return HexId(value, "chk_");
}

// 读取一份文档（可选地只取其中一个 chunk）。
//
// 流程分五步，前两步是安全门，后三步是一致性门：
//   1. 入参形状校验（id 字符集 -> 排除路径穿越）
//   2. generation 校验（目录名同样不能带穿越字符）
//   3. 读文件（带体积上限）
//   4. **内容与请求交叉校验**（generation / doc_id 必须对得上）
//   5. chunk 查找（同时比 chunk_id 和 doc_id）
//
// 第 4、5 步是本函数最值得看的地方：文件名是由 doc_id 拼出来的，
// 但光靠文件名不够——还要让文件**自己声明的** generation 与 doc_id
// 与请求一致。这拦住了两类错误：建索引时写错了目录（旧代文件残留在
// 新代里）、以及人工拷文件时拷错位置。两者都会导致“拿 A 的 id 读到 B
// 的内容”，而客户端引用栏里展示的却是 A。
bool LoadKnowledgeDocument(const std::string& knowledge_root,
                           const std::string& doc_id,
                           const std::string& chunk_id,
                           nlohmann::json* output, std::string* error) {
    // knowledge_root 为空时也走这个分支：未在配置里开启知识库的部署
    // 应该得到干净的失败，而不是拿空串当根目录去读相对路径
    //（那会变成相对于进程 cwd，结果不可预测）。
    if (!output || knowledge_root.empty() ||
        !IsValidKnowledgeDocumentId(doc_id) ||
        (!chunk_id.empty() && !IsValidKnowledgeChunkId(chunk_id))) {
        if (error) *error = "invalid knowledge document or chunk id";
        return false;
    }
    nlohmann::json current;
    const std::filesystem::path root(knowledge_root);
    // current.json 只是个 {"generation": "..."} 指针，4KB 上限已经极宽松。
    // 它由 Python 端用 os.replace 原子写入，所以本进程读到的要么是旧代
    // 要么是新代，不会是一半。
    if (!ReadJsonBounded(root / "data" / "current.json", 4096, &current,
                         error)) {
        return false;
    }
    const std::string generation = current.value("generation", "");
    if (!ValidGeneration(generation)) {
        if (error) *error = "invalid knowledge generation";
        return false;
    }
    nlohmann::json stored;
    // 8MB：一份文档连同它全部 chunk 的正文。上限开得比 current.json 大三个
    // 数量级，因为文档正文长度不可控，而指针文件长度完全可控。
    const auto path = root / "data" / "generations" / generation /
                      "documents" / (doc_id + ".json");
    if (!ReadJsonBounded(path, 8 * 1024 * 1024, &stored, error)) return false;
    // 交叉校验（上面注释的第 4 步）。每个条件各自拦一类错误：
    //   !is_object                       文件被写成了数组/标量
    //   generation 不一致              旧代残留文件被当成当代内容返回
    //   缺 document / 不是对象        结构不完整，下面取字段会抛
    //   document.doc_id != doc_id      **最关键的一条**：文件名说自己是 A，
    //                                  内容却说自己是 B，宁可拒也不猜
    //   缺 chunks / 不是数组          chunk 查找会无法遍历
    if (!stored.is_object() || stored.value("generation", "") != generation ||
        !stored.contains("document") || !stored["document"].is_object() ||
        stored["document"].value("doc_id", "") != doc_id ||
        !stored.contains("chunks") || !stored["chunks"].is_array()) {
        if (error) *error = "knowledge document failed catalog validation";
        return false;
    }

    // 注意输出里**不拷 chunks 数组**，只拷 document：一份文档可能有几十个
    // chunk，全部回传会把响应体撑得很大，而客户端拉原文时只要正文。
    // 需要具体某一段时由下面的 chunk_id 分支单独附上。
    nlohmann::json result = {
        {"generation", generation},
        {"document", stored["document"]},
    };
    if (!chunk_id.empty()) {
        auto found = std::find_if(
            stored["chunks"].begin(), stored["chunks"].end(),
            [&](const nlohmann::json& chunk) {
                // 同时比 chunk_id **和** doc_id：如果只比 chunk_id，一个属于
                // 文档 A 的 chunk 就能被“文档 B + A 的 chunk_id”这个组合拉出来。
                // 这在引用场景下是真问题：citation_validation 把 uri 锁死为
                // "cannkb://document/<doc_id>?chunk=<chunk_id>"，如果两者能乱配，
                // 模型就可以造出一条“形式合法、内容驴唇不对马嘴”的引用。
                return chunk.is_object() &&
                       chunk.value("chunk_id", "") == chunk_id &&
                       chunk.value("doc_id", "") == doc_id;
            });
        // 找不到是 404 而不是静默退化成“只返回整份文档”：客户端拿着
        // 一个失效的 chunk_id 时，应该知道它失效了，而不是以为拿到了那一段。
        if (found == stored["chunks"].end()) {
            if (error) *error = "knowledge chunk not found in document";
            return false;
        }
        result["chunk"] = *found;
    }
    *output = std::move(result);
    return true;
}

}  // namespace sparkpush
