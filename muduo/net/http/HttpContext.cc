#include "HttpContext.h"
#include <algorithm>
#include <string.h>
#include <cstring>
#include <cctype>
#include "muduo/base/Timestamp.h"
#include "muduo/base/Logging.h"
using namespace muduo;
using namespace muduo::net;
bool HttpContext::processRequestLine(const char* begin, const char* end)
{
  bool succeed = false;
  const char* start = begin;
  const char* space = std::find(start, end, ' ');
  if (space != end && request_.setMethod(start, space))
  {
    start = space+1;
    space = std::find(start, end, ' ');
    if (space != end)
    {
      const char* question = std::find(start, space, '?');
      if (question != space)
      {
        request_.setPath(start, question);
        request_.setQuery(question, space);
      }
      else
      {
        request_.setPath(start, space);
      }
      start = space+1;
      succeed = end-start == 8 && std::equal(start, end-1, "HTTP/1.");
      if (succeed)
      {
        if (*(end-1) == '1')
        {
          request_.setVersion(HttpRequest::kHttp11);
        }
        else if (*(end-1) == '0')
        {
          request_.setVersion(HttpRequest::kHttp10);
        }
        else
        {
          succeed = false;
        }
      }
    }
  }
  return succeed;
}

bool HttpContext::processHeaders(Buffer* buf) {
    constexpr size_t kMaxHeaders = 32 * 1024;
    constexpr size_t kMaxBody = 8 * 1024 * 1024;
    while (const char* crlf = buf->findCRLF()) {
        const size_t size = crlf - buf->peek();
        if (size + 2 > kMaxHeaders - headerBytes_) return false;
        headerBytes_ += size + 2;
        if (size == 0) {
            buf->retrieve(2);
            state_ = kExpectBody;
            return true;
        }
        const char* colon = std::find(buf->peek(), crlf, ':');
        if (colon == buf->peek() || colon == crlf) return false;
        for (const char* c = buf->peek(); c != colon; ++c) {
            if (!std::isalnum(static_cast<unsigned char>(*c)) &&
                std::strchr("!#$%&'*+-.^_`|~", *c) == nullptr) return false;
        }
        std::string field(buf->peek(), colon);
        std::string value(colon + 1, crlf);
        const auto first = value.find_first_not_of(" \t");
        const auto last = value.find_last_not_of(" \t");
        value = first == std::string::npos ? "" : value.substr(first, last - first + 1);
        if (strcasecmp(field.c_str(), "Content-Length") == 0) {
            // Reject duplicates, signs, overflow and lengths beyond our buffer budget.
            if (hasContentLength_ || value.empty()) return false;
            size_t length = 0;
            for (unsigned char c : value) {
                if (c < '0' || c > '9' || length > (kMaxBody - (c - '0')) / 10)
                    return false;
                length = length * 10 + c - '0';
            }
            contentLength_ = length;
            hasContentLength_ = true;
        } else if (strcasecmp(field.c_str(), "Transfer-Encoding") == 0) {
            // Chunked decoding is unsupported. Fail at the headers, never wait forever.
            return false;
        }
        request_.addHeader(buf->peek(), colon, crlf);
        buf->retrieveUntil(crlf + 2);
    }
    return buf->readableBytes() <= kMaxHeaders - headerBytes_;
}

bool HttpContext::processBody(Buffer* buf) {
    if (isChunked_) {
        // TODO: 处理 chunked 传输
        return false;
    } else {
        size_t readable = buf->readableBytes();
        LOG_DEBUG << "processBody readable: " << readable;
        size_t toRead = std::min(readable, remainingLength());
        if (toRead > 0) {
            request_.appendToBody(buf->peek(), toRead);
            bodyReceived_ += toRead;
            buf->retrieve(toRead);
        }
        LOG_DEBUG << "bodyReceived_: " << bodyReceived_ << ", contentLength_: " << contentLength_;
        return bodyReceived_ >= contentLength_;
    }
}

// return false for error, true for success (got all or need more data)
HttpContext::ParseResult HttpContext::parseRequest(Buffer* buf, Timestamp receiveTime) {
    bool ok = true;
    bool hasMore = true;
    ParseResult result = kNeedMore;

    LOG_DEBUG << "parseRequest state_: " << state_ << ", result: " << result;
    inputStarted_ = true;
    
    while (hasMore) {
        if (state_ == kExpectRequestLine) {
            const char* crlf = buf->findCRLF();
            if (crlf && crlf - buf->peek() <= 8192) {
                ok = processRequestLine(buf->peek(), crlf);
                if (ok) {
                    request_.setReceiveTime(receiveTime);
                    buf->retrieveUntil(crlf + 2);
                    state_ = kExpectHeaders;
                } else {
                    result = kError;
                    hasMore = false;
                }
            } else {
                if (crlf || buf->readableBytes() > 8192) result = kError;
                hasMore = false;
            }
        } else if (state_ == kExpectHeaders) {
            ok = processHeaders(buf);
            if (ok) {
                if (state_ == kExpectHeaders) {
                    // No complete header remains: yield until the next read.
                    hasMore = false;
                } else if (state_ == kExpectBody) {
                    if (contentLength_ == 0 && !isChunked_) {
                        // 没有请求体
                        state_ = kGotAll;
                        result = kGotRequest;
                        hasMore = false;
                    } else {
                        // 有请求体，继续处理
                        result = kHeadersComplete;
                        if (buf->readableBytes() > 0) {
                            // 如果缓冲区还有数据，继续处理body
                            continue;
                        }
                        hasMore = false;
                    }
                }
            } else {
                result = kError;
                hasMore = false;
            }
        } else if (state_ == kExpectBody) {
            // 处理请求体
            if (processBody(buf)) {
                state_ = kGotAll;
                result = kGotRequest;
            } else if (bodyReceived_ < contentLength_) {
                // 还需要更多数据
                result = kHeadersComplete;
            }
            hasMore = false;
        }
    }

    return result;  // 只要不是错误状态都返回true
}