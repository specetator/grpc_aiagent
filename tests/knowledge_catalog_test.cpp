#include "knowledge_catalog.h"

#include <filesystem>
#include <fstream>
#include <iostream>

int main() {
    const auto base = std::filesystem::temp_directory_path() /
                      "spark_push_knowledge_catalog_test";
    std::error_code ec;
    std::filesystem::remove_all(base, ec);
    const std::string doc = "doc_0123456789abcdef01234567";
    const std::string chunk = "chk_89abcdef0123456789abcdef";
    const std::string generation = "20260901T000000Z-0123456789ab";
    const auto documents = base / "data/generations" / generation / "documents";
    std::filesystem::create_directories(documents);
    {
        std::ofstream out(base / "data/current.json");
        out << nlohmann::json({{"generation", generation}});
    }
    {
        std::ofstream out(documents / (doc + ".json"));
        out << nlohmann::json({
            {"schema_version", 1}, {"generation", generation},
            {"document", {{"doc_id", doc}, {"title", "t"}, {"content", "body"}}},
            {"chunks", nlohmann::json::array({{{"doc_id", doc}, {"chunk_id", chunk},
                                                {"content", "piece"}}})},
        });
    }
    nlohmann::json output;
    std::string error;
    if (!sparkpush::LoadKnowledgeDocument(base.string(), doc, chunk,
                                          &output, &error) ||
        output["chunk"].value("content", "") != "piece") {
        return 1;
    }
    if (sparkpush::LoadKnowledgeDocument(base.string(), "../../etc/passwd", "",
                                         &output, &error)) {
        return 1;
    }
    if (sparkpush::LoadKnowledgeDocument(
            base.string(), doc, "chk_000000000000000000000000",
            &output, &error)) {
        return 1;
    }
    std::filesystem::remove_all(base, ec);
    std::cout << "knowledge catalog tests passed\n";
    return 0;
}
