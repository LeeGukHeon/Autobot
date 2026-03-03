#include "grpc_server.h"

#include <cstdlib>
#include <exception>
#include <cctype>
#include <algorithm>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {
autobot::executor::ExecutorGrpcServer::Options ParseArgs(int argc, char** argv) {
  autobot::executor::ExecutorGrpcServer::Options options;
  if (const char* mode_env = std::getenv("AUTOBOT_EXECUTOR_MODE"); mode_env != nullptr) {
    std::string mode = mode_env;
    std::transform(mode.begin(), mode.end(), mode.begin(), [](unsigned char ch) {
      return static_cast<char>(std::tolower(ch));
    });
    if (mode == "live") {
      options.order_test_mode = false;
    } else if (mode == "order_test" || mode == "order-test") {
      options.order_test_mode = true;
    }
  }
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--host" && i + 1 < argc) {
      options.host = argv[++i];
    } else if (arg == "--port" && i + 1 < argc) {
      options.port = std::stoi(argv[++i]);
    } else if (arg == "--mode" && i + 1 < argc) {
      std::string mode = argv[++i];
      std::transform(mode.begin(), mode.end(), mode.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
      });
      if (mode == "order_test" || mode == "order-test") {
        options.order_test_mode = true;
      } else if (mode == "live") {
        options.order_test_mode = false;
      } else {
        throw std::runtime_error("unsupported --mode value, use order_test or live");
      }
    } else if (arg == "--live-mode") {
      options.order_test_mode = false;
    } else if (arg == "--help" || arg == "-h") {
      std::cout << "autobot_executor options:\n"
                << "  --host <value>       default: 0.0.0.0\n"
                << "  --port <value>       default: 50051\n"
                << "  --mode <value>       order_test (default) | live\n"
                << "  AUTOBOT_EXECUTOR_MODE env also supported (live/order_test)\n"
                << "  --live-mode          disable order-test mode\n";
      std::exit(0);
    }
  }
  return options;
}
}  // namespace

int main(int argc, char** argv) {
  try {
    autobot::executor::ExecutorGrpcServer server(ParseArgs(argc, argv));
    return server.Run();
  } catch (const std::exception& exc) {
    std::cerr << "[executor] fatal: " << exc.what() << std::endl;
    return 2;
  }
}
