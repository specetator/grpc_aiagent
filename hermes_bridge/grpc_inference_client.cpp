#include "grpc_inference_client.h"

#include <chrono>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "inference.grpc.pb.h"

namespace sparkpush {

bool GrpcInferenceClient::Chat(const nlohmann::json& messages,
    const HermesChatOptions& options, HermesChatResult* result,
    std::string* err_msg) const {
    return ChatStream(messages, options, [](const std::string&) {}, result, err_msg);
}

bool GrpcInferenceClient::ChatStream(const nlohmann::json& messages,
    const HermesChatOptions& options,
    const std::function<void(const std::string&)>& on_delta,
    HermesChatResult* result, std::string* err_msg) const {
    if (!result || options.request_id.empty() || !messages.is_array()) {
        if (err_msg) *err_msg = "invalid inference request";
        return false;
    }
    // 图片只在 Pi HTTP 路径上支持。这里直接失败，不把图片静默丢掉，
    // 否则用户会收到一个没看过图的回答。
    if (!options.images.empty()) {
        if (err_msg) *err_msg = "gRPC inference backend does not support images";
        return false;
    }
    inference::GenerateRequest request;
    request.set_request_id(options.request_id);
    request.set_session_id(options.session_id);
    request.set_model(options.model.empty() ? config_.inference_model : options.model);
    request.set_max_tokens(config_.inference_max_tokens);
    request.set_temperature(config_.inference_temperature_milli / 1000.0f);
    (*request.mutable_metadata())["cache_prompt"] = config_.inference_cache_prompt ? "true" : "false";
    for (const auto& message : messages) {
        if (!message.is_object() || !message.contains("content") ||
            !message["content"].is_string()) continue;
        const auto content = message["content"].get<std::string>();
        request.add_messages(content);
        // prompt 被每次覆盖，循环结束后只剩最后一条。完整多轮在 chat_turns 里。
        // 不要删掉 set_prompt：旧 worker 仍读这一个字段。
        request.set_prompt(content);
        auto* turn = request.add_chat_turns();
        turn->set_role(message.value("role", "user"));
        turn->set_content(content);
    }
    if (request.prompt().empty()) {
        if (err_msg) *err_msg = "inference prompt is empty";
        return false;
    }
    auto stub = inference::InferenceGateway::NewStub(grpc::CreateChannel(
        config_.inference_gateway, grpc::InsecureChannelCredentials()));
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(config_.request_timeout_ms));
    // 每次调用新建 channel。停机时 gRPC 的阻塞 Read 不会自己返回，
    // 所以用一个监视线程在 running_ 变 false 时 TryCancel。
    // 监视线程必须在 Finish 之后 join，否则 context 先析构。
    std::atomic<bool> done{false};
    std::thread monitor([&] {
        while (!done.load()) {
            if (running_ && !running_->load()) {
                context.TryCancel();
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });
    auto reader = stub->Generate(&context, request);
    inference::GenerateChunk chunk;
    bool finished = false;
    uint64_t sequence = 0;
    while (reader->Read(&chunk)) {
        // 序号必须从 0 连续递增，且 request_id 对得上。
        // 错位说明串流了别的请求，立刻取消，不把那些 token 拼进本次回答。
        if (chunk.request_id() != options.request_id || chunk.sequence() != sequence++) {
            context.TryCancel();
            break;
        }
        if (!chunk.error().empty()) {
            if (err_msg) *err_msg = chunk.error();
            context.TryCancel();
            break;
        }
        if (!chunk.text_delta().empty()) {
            result->text += chunk.text_delta();
            ++result->stream_chunk_count;
            on_delta(chunk.text_delta());
        }
        if (chunk.finished()) {
            finished = true;
            result->finish_reason = chunk.finish_reason();
            result->metadata["usage"] = {{"prompt_tokens", chunk.prompt_tokens()},
                {"completion_tokens", chunk.completion_tokens()},
                {"cached_prompt_tokens", chunk.cached_prompt_tokens()}};
            result->metadata["worker_id"] = chunk.worker_id();
        }
    }
    const auto status = reader->Finish();
    done.store(true);
    monitor.join();
    // 流正常结束但没见到 finished 块，也是失败。
    // 否则半截回答会被当成完整回复写进 ai_reply。
    if (!status.ok() || !finished) {
        if (err_msg && err_msg->empty())
            *err_msg = status.ok() ? "inference stream ended without terminal chunk"
                                   : status.error_message();
        return false;
    }
    return true;
}

}  // namespace sparkpush
