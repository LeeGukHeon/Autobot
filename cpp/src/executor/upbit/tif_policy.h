#pragma once

#include <optional>
#include <string>

namespace autobot::executor::upbit {

std::optional<std::string> NormalizeTimeInForce(
    const std::string& ord_type, const std::string& raw_time_in_force, std::string* validation_error);

}  // namespace autobot::executor::upbit
