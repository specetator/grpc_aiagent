#include "hermes_client.h"
#include "sse_parser.h"
#include "agent_event.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <functional>
#include <sstream>

#include "logging.h"

namespace sparkpush {
namespace {

struct ParsedUrl {
    std::string host;
    std::string port;
    std::string path;
};

struct HttpResponseHeaders {
    int status{0};
    bool chunked{false};
    bool has_content_length{false};
    size_t content_length{0};
    std::string initial_body;
};

bool ParseHttpUrl(const std::string& url, ParsedUrl* parsed,
                 std::string* err_msg) {
    if (!parsed || url.rfind("http://", 0) != 0) {
        if (err_msg) *err_msg = "Hermes bridge only supports http:// URLs";
        return false;
    }
    const std::string authority_and_path = url.substr(7);
    const size_t slash = authority_and_path.find('/');
    const std::string authority = authority_and_path.substr(0, slash);
    parsed->path = slash == std::string::npos
                       ? "/v1"
                       : authority_and_path.substr(slash);
    if (authority.empty()) {
        if (err_msg) *err_msg = "Hermes URL host is empty";
        return false;
    }

    // 第一阶段不支持 IPv6 字面量，Windows 本机/虚拟机场景使用 IPv4。
    const size_t colon = authority.rfind(':');
    if (colon != std::string::npos && authority.find(':') == colon) {
        parsed->host = authority.substr(0, colon);
        parsed->port = authority.substr(colon + 1);
    } else {
        parsed->host = authority;
        parsed->port = "80";
    }
    if (parsed->host.empty() || parsed->port.empty()) {
        if (err_msg) *err_msg = "Hermes URL host/port is invalid";
        return false;
    }
    if (parsed->path.empty()) parsed->path = "/v1";
    if (parsed->path.back() == '/') parsed->path.pop_back();
    return true;
}

int Connect(const ParsedUrl& url, int timeout_ms, std::string* err_msg) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* addresses = nullptr;
    const int gai = getaddrinfo(url.host.c_str(), url.port.c_str(), &hints,
                                &addresses);
    if (gai != 0) {
        if (err_msg) *err_msg = "getaddrinfo failed: " +
                                 std::string(gai_strerror(gai));
        return -1;
    }

    int fd = -1;
    for (addrinfo* item = addresses; item; item = item->ai_next) {
        fd = ::socket(item->ai_family, item->ai_socktype, item->ai_protocol);
        if (fd < 0) continue;
        timeval timeout{};
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        if (::connect(fd, item->ai_addr, item->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(addresses);
    if (fd < 0 && err_msg) {
        *err_msg = "connect to Hermes failed: " + url.host + ":" + url.port +
                   ": " + std::strerror(errno);
    }
    return fd;
}

bool SendAll(int fd, const std::string& value, std::string* err_msg) {
    size_t sent = 0;
    while (sent < value.size()) {
        const ssize_t n = ::send(fd, value.data() + sent, value.size() - sent,
                                 MSG_NOSIGNAL);
        if (n <= 0) {
            if (err_msg) *err_msg = std::string("send Hermes request failed: ") +
                                     std::strerror(errno);
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool ReadAll(int fd, std::string* output, std::string* err_msg) {
    if (!output) return false;
    output->clear();
    char buffer[8192];
    while (true) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n == 0) return true;
        if (n < 0) {
            if (errno == EINTR) continue;
            if (err_msg) *err_msg = std::string("read Hermes response failed: ") +
                                     std::strerror(errno);
            return false;
        }
        output->append(buffer, static_cast<size_t>(n));
        if (output->size() > 16 * 1024 * 1024) {
            if (err_msg) *err_msg = "Hermes response exceeds 16 MiB";
            return false;
        }
    }
}

std::string DecodeChunkedBody(const std::string& body) {
    std::string decoded;
    size_t cursor = 0;
    while (cursor < body.size()) {
        const size_t line_end = body.find("\r\n", cursor);
        if (line_end == std::string::npos) return {};
        size_t chunk_size = 0;
        try {
            chunk_size = std::stoul(body.substr(cursor, line_end - cursor),
                                    nullptr, 16);
        } catch (...) {
            return {};
        }
        cursor = line_end + 2;
        if (chunk_size == 0) return decoded;
        if (cursor + chunk_size + 2 > body.size()) return {};
        decoded.append(body, cursor, chunk_size);
        cursor += chunk_size + 2;
    }
    return decoded;
}

bool ExtractResponseBody(const std::string& raw, int* status,
                         std::string* body, std::string* err_msg) {
    const size_t header_end = raw.find("\r\n\r\n");
    if (header_end == std::string::npos) {
        if (err_msg) *err_msg = "invalid HTTP response from Hermes";
        return false;
    }
    const std::string headers = raw.substr(0, header_end);
    const std::string first_line_end = raw.substr(0, raw.find("\r\n"));
    std::istringstream status_stream(first_line_end);
    std::string http_version;
    int parsed_status = 0;
    status_stream >> http_version >> parsed_status;
    if (status) *status = parsed_status;
    std::string response_body = raw.substr(header_end + 4);
    if (headers.find("Transfer-Encoding: chunked") != std::string::npos ||
        headers.find("transfer-encoding: chunked") != std::string::npos) {
        response_body = DecodeChunkedBody(response_body);
        if (response_body.empty() && parsed_status != 204) {
            if (err_msg) *err_msg = "invalid chunked response from Hermes";
            return false;
        }
    }
    if (body) *body = std::move(response_body);
    return parsed_status > 0;
}

std::string ToLower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value;
}

bool ReadHttpHeaders(int fd, HttpResponseHeaders* response,
                     std::string* err_msg) {
    if (!response) return false;
    std::string raw;
    char buffer[8192];
    while (raw.find("\r\n\r\n") == std::string::npos) {
        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);
        if (n == 0) {
            if (err_msg) *err_msg = "Hermes closed connection before headers";
            return false;
        }
        if (n < 0) {
            if (errno == EINTR) continue;
            if (err_msg) *err_msg = std::string("read Hermes headers failed: ") +
                                     std::strerror(errno);
            return false;
        }
        raw.append(buffer, static_cast<size_t>(n));
        if (raw.size() > 1024 * 1024) {
            if (err_msg) *err_msg = "Hermes response headers exceed 1 MiB";
            return false;
        }
    }

    const size_t header_end = raw.find("\r\n\r\n");
    const std::string header_text = raw.substr(0, header_end);
    response->initial_body = raw.substr(header_end + 4);
    const size_t first_line_end = header_text.find("\r\n");
    std::istringstream status_stream(header_text.substr(0, first_line_end));
    std::string version;
    status_stream >> version >> response->status;
    if (response->status <= 0) {
        if (err_msg) *err_msg = "invalid Hermes HTTP status";
        return false;
    }

    const std::string lower_headers = ToLower(header_text);
    response->chunked = lower_headers.find("transfer-encoding: chunked") !=
                        std::string::npos;
    const std::string length_header = "content-length:";
    const size_t length_pos = lower_headers.find(length_header);
    if (length_pos != std::string::npos) {
        const size_t value_begin = length_pos + length_header.size();
        const size_t value_end = lower_headers.find("\r\n", value_begin);
        const std::string value = lower_headers.substr(
            value_begin, value_end == std::string::npos
                            ? std::string::npos
                            : value_end - value_begin);
        try {
            response->content_length = std::stoull(value);
            response->has_content_length = true;
        } catch (...) {
            if (err_msg) *err_msg = "invalid Hermes Content-Length";
            return false;
        }
    }
    return true;
}

class BufferedSocketReader {
   public:
    BufferedSocketReader(int fd, std::string initial)
        : fd_(fd), buffer_(std::move(initial)) {}

    bool ReadMore(std::string* err_msg) {
        if (eof_) return false;
        char buffer[8192];
        while (true) {
            const ssize_t n = ::recv(fd_, buffer, sizeof(buffer), 0);
            if (n > 0) {
                buffer_.append(buffer, static_cast<size_t>(n));
                return true;
            }
            if (n == 0) {
                eof_ = true;
                return false;
            }
            if (errno == EINTR) continue;
            if (err_msg) *err_msg = std::string("read Hermes body failed: ") +
                                     std::strerror(errno);
            return false;
        }
    }

    bool ReadLine(std::string* line, std::string* err_msg) {
        if (!line) return false;
        while (true) {
            const size_t end = buffer_.find("\r\n");
            if (end != std::string::npos) {
                *line = buffer_.substr(0, end);
                buffer_.erase(0, end + 2);
                return true;
            }
            if (eof_) {
                if (buffer_.empty()) return false;
                *line = std::move(buffer_);
                buffer_.clear();
                return true;
            }
            if (!ReadMore(err_msg)) {
                if (err_msg && err_msg->empty()) *err_msg = "unexpected EOF";
                return false;
            }
        }
    }

    bool ReadExact(size_t size, std::string* output, std::string* err_msg) {
        if (!output) return false;
        output->clear();
        while (output->size() < size) {
            if (buffer_.empty() && !ReadMore(err_msg)) {
                if (err_msg && err_msg->empty()) *err_msg = "unexpected EOF";
                return false;
            }
            const size_t take = std::min(size - output->size(), buffer_.size());
            output->append(buffer_, 0, take);
            buffer_.erase(0, take);
        }
        return true;
    }

    bool Drain(const std::function<bool(const std::string&)>& on_chunk,
               std::string* err_msg) {
        while (true) {
            if (!buffer_.empty()) {
                const std::string chunk = std::move(buffer_);
                buffer_.clear();
                if (!on_chunk(chunk)) return false;
            }
            if (eof_) return true;
            if (!ReadMore(err_msg) && !eof_) return false;
        }
    }

   private:
    int fd_;
    std::string buffer_;
    bool eof_{false};
};

bool StreamHttpBody(int fd, const HttpResponseHeaders& response,
                    const std::function<bool(const std::string&)>& on_chunk,
                    std::string* err_msg) {
    BufferedSocketReader reader(fd, response.initial_body);
    if (response.chunked) {
        while (true) {
            std::string line;
            if (!reader.ReadLine(&line, err_msg)) return false;
            const size_t extension = line.find(';');
            if (extension != std::string::npos) line.resize(extension);
            size_t chunk_size = 0;
            try {
                chunk_size = std::stoull(line, nullptr, 16);
            } catch (...) {
                if (err_msg) *err_msg = "invalid Hermes chunk size";
                return false;
            }
            if (chunk_size == 0) {
                // 消费 chunk trailer，直到空行。
                do {
                    if (!reader.ReadLine(&line, err_msg)) return false;
                } while (!line.empty());
                return true;
            }
            while (chunk_size > 0) {
                const size_t take = std::min<size_t>(chunk_size, 8192);
                std::string piece;
                if (!reader.ReadExact(take, &piece, err_msg)) return false;
                if (!on_chunk(piece)) return false;
                chunk_size -= take;
            }
            std::string crlf;
            if (!reader.ReadExact(2, &crlf, err_msg) || crlf != "\r\n") {
                if (err_msg) *err_msg = "invalid Hermes chunk terminator";
                return false;
            }
        }
    }

    if (response.has_content_length) {
        size_t remaining = response.content_length;
        while (remaining > 0) {
            const size_t take = std::min<size_t>(remaining, 8192);
            std::string piece;
            if (!reader.ReadExact(take, &piece, err_msg)) return false;
            if (!on_chunk(piece)) return false;
            remaining -= take;
        }
        return true;
    }
    return reader.Drain(on_chunk, err_msg);
}

std::string JoinPath(std::string base, const std::string& suffix) {
    while (!base.empty() && base.back() == '/') base.pop_back();
    return base + suffix;
}

std::string ExtractText(const nlohmann::json& value) {
    if (value.is_string()) return value.get<std::string>();
    if (!value.is_array()) return {};
    std::string result;
    for (const auto& part : value) {
        if (part.is_string()) {
            result += part.get<std::string>();
        } else if (part.is_object() && part.value("type", "") == "text") {
            result += part.value("text", "");
        } else if (part.is_object() && part.value("type", "") == "output_text") {
            result += part.value("text", "");
        }
    }
    return result;
}


}  // namespace

bool HermesClient::Chat(const nlohmann::json& messages,
                        const HermesChatOptions& options,
                        HermesChatResult* result, std::string* err_msg) const {
    if (!result || (options.control.is_null() && !options.retry && (!messages.is_array() || messages.empty()))) {
        if (err_msg) *err_msg = "Hermes chat messages are empty";
        return false;
    }
    *result = HermesChatResult{};
    ParsedUrl base;
    if (!ParseHttpUrl(config_.hermes_base_url, &base, err_msg)) return false;
    const std::string path = JoinPath(base.path,
        options.control.is_null() ? "/chat/completions" : "/agent/control");

    nlohmann::json request_body = {
        {"model", options.model.empty() ? "pi-agent"
                                         : options.model},
        {"messages", messages},
        {"stream", false},
    };
    if (options.retry) request_body["turn_operation"] = "retry";
    if (!options.provider.empty()) {
        request_body["provider"] = options.provider;
    }
    if (!options.session_id.empty()) {
        request_body["session_id"] = options.session_id;
        request_body["context_start_seq"] = options.context_start_seq;
    }
    if (!options.control.is_null()) {
        request_body = options.control;
        request_body["session_id"] = options.session_id;
        request_body["context_start_seq"] = options.context_start_seq;
    }
    const std::string body = request_body.dump();
    const std::string request =
        "POST " + path + " HTTP/1.1\r\n" +
        "Host: " + base.host + "\r\n" +
        "Authorization: Bearer " + config_.hermes_api_key + "\r\n" +
        "Content-Type: application/json\r\n" +
        "Connection: close\r\n" +
        "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n" +
        body;

    const int fd = Connect(base, config_.request_timeout_ms, err_msg);
    if (fd < 0) return false;
    std::string raw_response;
    bool ok = SendAll(fd, request, err_msg) && ReadAll(fd, &raw_response, err_msg);
    ::close(fd);
    if (!ok) return false;

    int status = 0;
    std::string response_body;
    if (!ExtractResponseBody(raw_response, &status, &response_body, err_msg)) {
        return false;
    }
    nlohmann::json response_json;
    try {
        response_json = nlohmann::json::parse(response_body);
    } catch (const std::exception& e) {
        if (err_msg) *err_msg = "parse Hermes response failed: " +
                                 std::string(e.what());
        return false;
    }
    if (status < 200 || status >= 300) {
        if (err_msg) *err_msg = "HTTP " + std::to_string(status) + ": Pi gateway request failed";
        return false;
    }

    try {
        if (!options.control.is_null()) {
            nlohmann::json event;
            if (!NormalizeAgentEvent(response_json, &event) || event["type"] != "assistant_final") {
                if (err_msg) *err_msg = "Invalid Agent control result";
                return false;
            }
            result->text = event["data"]["text"].get<std::string>();
            result->metadata = {{"agent_event", event}};
            for (const auto* key : {"model_state", "thinking_state", "context_start_seq"})
                if (response_json["data"].contains(key)) result->metadata[key] = response_json["data"][key];
            return true;
        }
        const auto& choice = response_json.at("choices").at(0);
        if (response_json.contains("error") || choice.value("finish_reason", "") != "stop") {
            if (err_msg) *err_msg = "Pi gateway did not return a successful answer";
            return false;
        }
        const auto& message_json = choice.at("message");
        result->text = ExtractText(message_json.at("content"));
        if (response_json.contains("hermes") &&
            response_json["hermes"].is_object()) {
            const auto& hermes = response_json["hermes"];
            if (hermes.contains("response_metadata") &&
                hermes["response_metadata"].is_object()) {
                result->metadata = hermes["response_metadata"];
                if (result->metadata.contains("citations") &&
                    result->metadata["citations"].is_array()) {
                    result->citations = result->metadata["citations"];
                }
            }
        }
    } catch (const std::exception& e) {
        if (err_msg) *err_msg = "Hermes response has no assistant content: " +
                                 std::string(e.what());
        return false;
    }
    if (result->text.empty()) {
        if (err_msg) *err_msg = "Hermes returned empty assistant content";
        return false;
    }
    return true;
}

bool HermesClient::ChatStream(
    const nlohmann::json& messages,
    const HermesChatOptions& options,
    const std::function<void(const std::string&)>& on_delta,
    HermesChatResult* result, std::string* err_msg) const {
    if (!result || (!options.retry && (!messages.is_array() || messages.empty()))) {
        if (err_msg) *err_msg = "Hermes chat messages are empty";
        return false;
    }
    *result = HermesChatResult{};
    ParsedUrl base;
    if (!ParseHttpUrl(config_.hermes_base_url, &base, err_msg)) return false;
    const std::string path = JoinPath(base.path, "/chat/completions");
    nlohmann::json request_body = {
        {"model", options.model.empty() ? "pi-agent"
                                         : options.model},
        {"messages", messages},
        {"stream", true},
        {"agent_events", true},
    };
    if (options.retry) request_body["turn_operation"] = "retry";
    if (!options.provider.empty()) {
        request_body["provider"] = options.provider;
    }
    if (!options.session_id.empty()) {
        request_body["session_id"] = options.session_id;
        request_body["context_start_seq"] = options.context_start_seq;
    }
    const std::string body = request_body.dump();
    const std::string request =
        "POST " + path + " HTTP/1.1\r\n" +
        "Host: " + base.host + "\r\n" +
        "Authorization: Bearer " + config_.hermes_api_key + "\r\n" +
        "Content-Type: application/json\r\n" +
        "Accept: text/event-stream\r\n" +
        "Cache-Control: no-cache\r\n" +
        "Connection: close\r\n" +
        "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n" +
        body;

    const int fd = Connect(base, config_.request_timeout_ms, err_msg);
    if (fd < 0) return false;
    if (!SendAll(fd, request, err_msg)) {
        ::close(fd);
        return false;
    }

    HttpResponseHeaders response;
    if (!ReadHttpHeaders(fd, &response, err_msg)) {
        ::close(fd);
        return false;
    }

    if (response.status < 200 || response.status >= 300) {
        std::string error_body;
        const bool body_ok = StreamHttpBody(
            fd, response,
            [&error_body](const std::string& chunk) {
                if (error_body.size() + chunk.size() > 4 * 1024 * 1024) {
                    return false;
                }
                error_body += chunk;
                return true;
            },
            err_msg);
        ::close(fd);
        if (!body_ok) return false;
        try {
            const auto error_json = nlohmann::json::parse(error_body);
            std::string message = error_json.value("error", "Hermes HTTP error");
            if (error_json.contains("error") && error_json["error"].is_object()) {
                message = error_json["error"].value("message", message);
            }
            if (err_msg) *err_msg = "HTTP " + std::to_string(response.status) +
                                     ": " + message;
        } catch (...) {
            if (err_msg) *err_msg = "HTTP " + std::to_string(response.status) +
                                     ": Hermes streaming request failed";
        }
        return false;
    }

    HermesSseParser parser(options.on_progress);
    std::string parser_error;
    const bool body_ok = StreamHttpBody(
        fd, response,
        [&parser, result, &on_delta, &parser_error](const std::string& chunk) {
            return parser.Feed(chunk, result, on_delta, &parser_error);
        },
        err_msg);
    const bool parser_ok = body_ok && parser.Finish(result, on_delta, &parser_error);
    ::close(fd);
    if (!body_ok || !parser_ok) {
        if (err_msg && !parser_error.empty()) *err_msg = parser_error;
        return false;
    }
    if (result->text.empty()) {
        if (err_msg) *err_msg = "Hermes streaming response has no assistant content";
        return false;
    }
    return true;
}

}  // namespace sparkpush
