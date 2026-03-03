#pragma once

#include <optional>
#include <string>

namespace autobot::executor::upbit {

struct RemainingReqInfo {
  bool valid = false;
  std::string group = "default";
  int sec = 0;
  std::optional<int> min;
  std::string raw;
};

RemainingReqInfo ParseRemainingReqHeader(const std::string& value);

}  // namespace autobot::executor::upbit
