// 简单的基于 muduo 的静态文件 HTTP Server，用于加载 web_demo/static 下的页面

#include <muduo/base/Logging.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

using namespace muduo;
using namespace muduo::net;

namespace {

enum class ArgsParseResult { kOk, kHelp, kError };

void PrintUsage(const char* prog) {
  std::fprintf(stderr,
               "用法: %s [--port <端口>] [--doc-root <目录>] "
               "[port] [doc_root]\n"
               "  -p, --port       指定 HTTP 监听端口 (默认 9001)\n"
               "  -d, --doc-root   指定静态文件目录 (默认 ./static)\n"
               "可以继续支持旧格式：第一个位置参数为端口，第二个位置参数为目录。\n",
               prog);
}

bool ParsePortValue(const std::string& value, uint16_t* port) {
  char* end = nullptr;
  long parsed = std::strtol(value.c_str(), &end, 10);
  if (end == value.c_str() || *end != '\0' || parsed < 0 || parsed > 65535) {
    return false;
  }
  *port = static_cast<uint16_t>(parsed);
  return true;
}

ArgsParseResult ParseArgs(int argc,
                          char* argv[],
                          uint16_t* port,
                          std::string* doc_root) {
  bool port_set = false;
  bool doc_root_set = false;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-p" || arg == "--port") {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "%s 需要一个端口参数\n", arg.c_str());
        return ArgsParseResult::kError;
      }
      if (!ParsePortValue(argv[++i], port)) {
        std::fprintf(stderr, "非法端口号: %s\n", argv[i]);
        return ArgsParseResult::kError;
      }
      port_set = true;
    } else if (arg == "-d" || arg == "--doc-root") {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "%s 需要一个目录参数\n", arg.c_str());
        return ArgsParseResult::kError;
      }
      *doc_root = argv[++i];
      doc_root_set = true;
    } else if (arg == "-h" || arg == "--help") {
      return ArgsParseResult::kHelp;
    } else if (!arg.empty() && arg[0] == '-') {
      std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
      return ArgsParseResult::kError;
    } else {
      if (!port_set) {
        if (!ParsePortValue(arg, port)) {
          std::fprintf(stderr, "非法端口号: %s\n", arg.c_str());
          return ArgsParseResult::kError;
        }
        port_set = true;
      } else if (!doc_root_set) {
        *doc_root = arg;
        doc_root_set = true;
      } else {
        std::fprintf(stderr, "多余的位置参数: %s\n", arg.c_str());
        return ArgsParseResult::kError;
      }
    }
  }
  return ArgsParseResult::kOk;
}

std::string GuessContentType(const std::string& path) {
  if (path.size() >= 5 &&
      path.substr(path.size() - 5) == ".html") {
    return "text/html; charset=utf-8";
  } else if (path.size() >= 3 &&
             path.substr(path.size() - 3) == ".js") {
    return "application/javascript; charset=utf-8";
  } else if (path.size() >= 4 &&
             path.substr(path.size() - 4) == ".css") {
    return "text/css; charset=utf-8";
  } else if (path.size() >= 4 &&
             path.substr(path.size() - 4) == ".png") {
    return "image/png";
  } else if (path.size() >= 4 &&
             path.substr(path.size() - 4) == ".jpg") {
    return "image/jpeg";
  } else if (path.size() >= 5 &&
             path.substr(path.size() - 5) == ".jpeg") {
    return "image/jpeg";
  }
  return "text/plain; charset=utf-8";
}

std::string JoinPath(const std::string& root, const std::string& rel) {
  if (root.empty()) return rel;
  if (rel.empty()) return root;
  if (root.back() == '/') {
    if (rel.front() == '/') {
      return root + rel.substr(1);
    }
    return root + rel;
  }
  if (rel.front() == '/') {
    return root + rel;
  }
  return root + "/" + rel;
}

}  // namespace

class StaticFileHandler {
 public:
  explicit StaticFileHandler(const std::string& doc_root)
      : doc_root_(doc_root) {}

  void onRequest(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kGet &&
        req.method() != HttpRequest::kHead) {
      // muduo 的 HttpResponse 没有 405 枚举，使用 400 代替
      resp->setStatusCode(HttpResponse::k400BadRequest);
      resp->setContentType("text/plain; charset=utf-8");
      resp->setBody("Method Not Allowed");
      return;
    }

    std::string path = req.path();
    if (path.empty() || path == "/") {
      path = "/index.html";
    }

    // 简单防止目录穿越
    if (path.find("..") != std::string::npos) {
      // muduo 的 HttpResponse 没有 403 枚举，使用 400 代替
      resp->setStatusCode(HttpResponse::k400BadRequest);
      resp->setContentType("text/plain; charset=utf-8");
      resp->setBody("Forbidden");
      return;
    }

    std::string rel = path;
    if (!rel.empty() && rel.front() == '/') {
      rel.erase(0, 1);
    }

    std::string full_path = JoinPath(doc_root_, rel);
    std::ifstream ifs(full_path, std::ios::binary);
    if (!ifs.good()) {
      LOG_WARN << "File not found: " << full_path;
      resp->setStatusCode(HttpResponse::k404NotFound);
      resp->setContentType("text/plain; charset=utf-8");
      resp->setBody("Not Found");
      return;
    }

    std::ostringstream oss;
    oss << ifs.rdbuf();
    std::string content = oss.str();

    resp->setStatusCode(HttpResponse::k200Ok);
    resp->setContentType(GuessContentType(full_path));
    if (req.method() != HttpRequest::kHead) {
      resp->setBody(content);
    }
  }

 private:
  std::string doc_root_;
};

int main(int argc, char* argv[]) {
  Logger::setLogLevel(Logger::INFO);

  uint16_t port = 9001;
  std::string doc_root = "./static";

  ArgsParseResult parse_result =
      ParseArgs(argc, argv, &port, &doc_root);
  if (parse_result == ArgsParseResult::kHelp) {
    PrintUsage(argv[0]);
    return 0;
  }
  if (parse_result == ArgsParseResult::kError) {
    PrintUsage(argv[0]);
    return 1;
  }

  LOG_INFO << "Starting web_demo_server on port " << port
           << ", doc_root=" << doc_root;

  EventLoop loop;
  InetAddress listenAddr(port);
  HttpServer server(&loop, listenAddr, "web_demo_server");

  StaticFileHandler handler(doc_root);
  // 适配新的 HttpServer 回调签名：bool (const TcpConnectionPtr&, HttpRequest&, HttpResponse*)
  server.setHttpCallback(
      [&handler](const TcpConnectionPtr&,
                 HttpRequest& req,
                 HttpResponse* resp) -> bool {
        handler.onRequest(req, resp);
        return true;
      });
  server.setThreadNum(2);

  server.start();
  loop.loop();

  return 0;
}


