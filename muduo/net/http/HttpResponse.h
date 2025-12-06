#pragma once

#include <map>
#include <string>
#include "muduo/net/Buffer.h"
#include <functional>

namespace muduo {
namespace net {

class HttpResponse {
public:
    // 定义响应回调函数类型
    using ResponseCallback = std::function<void(const HttpResponse&)>;

    enum HttpStatusCode {
        kUnknown = 0,
        k200Ok = 200,
        k206PartialContent = 206,
        k301MovedPermanently = 301,
        k400BadRequest = 400,
        k401Unauthorized = 401,
        k403Forbidden = 403,
        k404NotFound = 404,
        k500InternalServerError = 500,
    };

    explicit HttpResponse(bool close)
        : statusCode_(kUnknown),
          closeConnection_(close),
          async_(false)
    {
    }
    ~HttpResponse() {
        LOG_INFO << "HttpResponse::~HttpResponse()";
    }

    void setStatusCode(HttpStatusCode code) { statusCode_ = code; }
    void setStatusMessage(const std::string& message) { statusMessage_ = message; }
    void setCloseConnection(bool on) { closeConnection_ = on; }
    bool closeConnection() const { return closeConnection_; }

    void setContentType(const std::string& contentType) { addHeader("Content-Type", contentType); }
    void addHeader(const std::string& key, const std::string& value) { headers_[key] = value; }
    void setBody(const std::string& body) { body_ = body; }

    // 设置为异步响应
    void setAsync(bool async) { async_ = async; }
    bool isAsync() const { return async_; }

    // 设置响应回调
    void setResponseCallback(const ResponseCallback& cb) { responseCallback_ = cb; }
    const ResponseCallback& getResponseCallback() const { return responseCallback_; }

    void appendToBuffer(Buffer* output) const {
        char buf[32];

        // 如果业务侧没有显式设置 statusMessage，则根据常见状态码补一个默认文案
        std::string statusMessage = statusMessage_;
        if (statusMessage.empty()) {
            switch (statusCode_) {
            case k200Ok:
                statusMessage = "OK";
                break;
            case k206PartialContent:
                statusMessage = "Partial Content";
                break;
            case k301MovedPermanently:
                statusMessage = "Moved Permanently";
                break;
            case k400BadRequest:
                statusMessage = "Bad Request";
                break;
            case k401Unauthorized:
                statusMessage = "Unauthorized";
                break;
            case k403Forbidden:
                statusMessage = "Forbidden";
                break;
            case k404NotFound:
                statusMessage = "Not Found";
                break;
            case k500InternalServerError:
                statusMessage = "Internal Server Error";
                break;
            default:
                statusMessage = "Unknown";
                break;
            }
        }

        snprintf(buf, sizeof(buf), "HTTP/1.1 %d ", statusCode_);
        output->append(buf);
        output->append(statusMessage);
        output->append("\r\n");

        if (closeConnection_) {
            output->append("Connection: close\r\n");
        } else {
            output->append("Connection: Keep-Alive\r\n");
        }

        // 如果没有显式设置 Content-Length，则自动根据 body 长度补一个
        if (headers_.find("Content-Length") == headers_.end()) {
            snprintf(buf, sizeof(buf), "%zu", body_.size());
            output->append("Content-Length: ");
            output->append(buf);
            output->append("\r\n");
        }

        for (const auto& header : headers_) {
            output->append(header.first);
            output->append(": ");
            output->append(header.second);
            output->append("\r\n");
        }

        output->append("\r\n");
        output->append(body_);
    }

private:
    std::map<std::string, std::string> headers_;
    HttpStatusCode statusCode_;
    std::string statusMessage_;
    bool closeConnection_;
    std::string body_;
    bool async_;                    // 是否为异步响应
    ResponseCallback responseCallback_;  // 响应回调函数
}; // class HttpResponse

} // namespace net
} // namespace muduo 