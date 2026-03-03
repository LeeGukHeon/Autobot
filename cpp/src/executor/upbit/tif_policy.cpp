#include "upbit/tif_policy.h"

#include <algorithm>
#include <cctype>
#include <utility>

namespace autobot::executor::upbit {

namespace {

std::string Trim(std::string value) {
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

std::string Lower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

void SetValidationError(std::string* validation_error, std::string message) {
  if (validation_error == nullptr) {
    return;
  }
  *validation_error = "validation_error: " + std::move(message);
}

}  // namespace

std::optional<std::string> NormalizeTimeInForce(
    const std::string& ord_type, const std::string& raw_time_in_force, std::string* validation_error) {
  if (validation_error != nullptr) {
    validation_error->clear();
  }

  const std::string ord_type_value = Lower(Trim(ord_type));
  const std::string tif_value = Lower(Trim(raw_time_in_force));
  const bool omit_tif = tif_value.empty() || tif_value == "gtc";

  if (ord_type_value == "price" || ord_type_value == "market") {
    return std::nullopt;
  }

  if (ord_type_value == "best") {
    if (omit_tif) {
      SetValidationError(
          validation_error,
          "time_in_force is required for ord_type=best (allowed: ioc, fok)");
      return std::nullopt;
    }
    if (tif_value == "ioc" || tif_value == "fok") {
      return tif_value;
    }
    SetValidationError(validation_error, "time_in_force for ord_type=best must be ioc or fok");
    return std::nullopt;
  }

  if (ord_type_value.empty() || ord_type_value == "limit") {
    if (omit_tif) {
      return std::nullopt;
    }
    if (tif_value == "ioc" || tif_value == "fok" || tif_value == "post_only") {
      return tif_value;
    }
    SetValidationError(
        validation_error,
        "time_in_force for ord_type=limit must be omitted, ioc, fok, or post_only");
    return std::nullopt;
  }

  SetValidationError(validation_error, "unsupported ord_type for time_in_force: " + ord_type_value);
  return std::nullopt;
}

}  // namespace autobot::executor::upbit
