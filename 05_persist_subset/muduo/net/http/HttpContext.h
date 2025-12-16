#pragma once

#include "muduo/net/Buffer.h"
#include "muduo/net/http/HttpRequest.h"
#include "muduo/base/Logging.h"
#include "muduo/base/copyable.h"
#include "muduo/base/Timestamp.h"
#include <memory>
#include <unordered_map>

namespace muduo {
namespace net {

class HttpContext : public muduo::copyable
{
 public:
  enum HttpRequestParseState
  {
    kExpectRequestLine, //0 解析请求行
    kExpectHeaders,     //1 解析头部
    kExpectBody,        //2 解析body
    kGotAll,           //3 整个请求解析完成
  };

  enum ParseResult
  {
    kError = -1,           // 解析出错
    kNeedMore = 0,         // 需要更多数据
    kHeadersComplete = 1,  // 头部解析完成
    kGotRequest = 2        // 整个请求解析完成
  };

  HttpContext()
    : state_(kExpectRequestLine),
      contentLength_(0),
      bodyReceived_(0),
      isChunked_(false)
  {
  }

  ~HttpContext()
  {
    LOG_INFO << "HttpContext destroyed";
    if(customContext_)
      customContext_.reset();
  }

  // default copy-ctor, dtor and assignment are fine

  // 返回false表示解析出错，true表示解析成功（包括需要更多数据和解析完成的情况）
  HttpContext::ParseResult parseRequest(Buffer* buf, Timestamp receiveTime);
  bool gotAll() const
  { return state_ == kGotAll; }

   bool expectBody() const
  { return state_ == kExpectBody; }


  bool headersComplete() const
  { return state_ == kExpectBody || state_ == kGotAll; }

  size_t remainingLength() const
  { return contentLength_ - bodyReceived_; }

  bool isChunked() const
  { return isChunked_; }

  void reset()
  {
    state_ = kExpectRequestLine;
    HttpRequest dummy;
    request_.swap(dummy);
    contentLength_ = 0;
    bodyReceived_ = 0;
    isChunked_ = false;
    customContext_.reset();
  }

  const HttpRequest& request() const
  { return request_; }

  HttpRequest& request()
  { return request_; }

  HttpRequestParseState state() const { return state_; }

  template<typename T>
  std::shared_ptr<T> getContext() const {
    return std::static_pointer_cast<T>(customContext_);
  }

  void setContext(const std::shared_ptr<void>& context) {
    customContext_ = context;
  }

 private:
  bool processRequestLine(const char* begin, const char* end);
  bool processHeaders(Buffer* buf);
  bool processBody(Buffer* buf);

  HttpRequestParseState state_ = HttpRequestParseState::kExpectRequestLine;
  HttpRequest request_;
  size_t contentLength_;  // 用于存储 Content-Length 的值
  size_t bodyReceived_;   // 已接收的 body 长度
  bool isChunked_;        // 是否为 chunked 传输
  std::shared_ptr<void> customContext_;  // 自定义上下文存储
};

} // namespace net
} // namespace muduo 