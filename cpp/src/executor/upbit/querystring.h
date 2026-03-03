#pragma once

#include <string>
#include <utility>
#include <vector>

namespace autobot::executor::upbit {

using QueryParam = std::pair<std::string, std::string>;

std::string BuildQueryString(const std::vector<QueryParam>& params);

}  // namespace autobot::executor::upbit
