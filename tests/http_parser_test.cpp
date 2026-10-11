#include <stdexcept>
#include <string>
#include "muduo/net/http/HttpContext.h"
using namespace muduo::net;
void Require(bool value) { if (!value) throw std::runtime_error("HTTP parser regression"); }
int main() {
    const std::string request = "POST /api/test HTTP/1.1\r\nHost: example\r\nContent-Length: 5\r\n\r\nhello";
    // Every possible TCP split, including a complete request line and partial header.
    for (size_t split = 1; split < request.size(); ++split) {
        HttpContext context;
        Buffer input;
        input.append(request.data(), split);
        Require(context.parseRequest(&input, muduo::Timestamp::now()) != HttpContext::kError);
        Require(!context.gotAll());
        input.append(request.data() + split, request.size() - split);
        Require(context.parseRequest(&input, muduo::Timestamp::now()) == HttpContext::kGotRequest);
        Require(context.request().body() == "hello" && input.readableBytes() == 0);
    }
    for (const auto& headers : {
        "Content-Length: -1\r\n", "Content-Length: 8x\r\n",
        "Content-Length: 999999999999999999999\r\n", "Content-Length: 8388609\r\n",
        "Content-Length: 0\r\nContent-Length: 0\r\n", "Transfer-Encoding: chunked\r\n",
        "Invalid header\r\n"}) {
        HttpContext context;
        Buffer input;
        input.append(std::string("POST / HTTP/1.1\r\n") + headers + "\r\n");
        Require(context.parseRequest(&input, muduo::Timestamp::now()) == HttpContext::kError);
    }
    HttpContext context;
    Buffer input;
    input.append("GET / HTTP/1.1\r\nHeader: " + std::string(32768, 'a'));
    Require(context.parseRequest(&input, muduo::Timestamp::now()) == HttpContext::kError);
}
