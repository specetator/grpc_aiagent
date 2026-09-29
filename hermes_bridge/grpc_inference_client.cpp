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
    if (!options.images.empty()) {
        if (err_msg) *err_msg = "gRPC inference backend does not support images";
        return false;
    }
    inference::GenerateRequest request;
    request.set_request_id(options.request_id);
    request.set_session_id(options.session_id);
    request.set_model(options.model.empty() ? config_.inference_model : options.model);
    for (const auto& message : messages) {
        if (!message.is_object() || !message.contains("content") ||
            !message["content"].is_string()) continue;
        const auto content = message["content"].get<std::string>();
        request.add_messages(content);
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
        }
    }
    const auto status = reader->Finish();
    done.store(true);
    monitor.join();
    if (!status.ok() || !finished) {
        if (err_msg && err_msg->empty())
            *err_msg = status.ok() ? "inference stream ended without terminal chunk"
                                   : status.error_message();
        return false;
    }
    return true;
}

}  // namespace sparkpush
