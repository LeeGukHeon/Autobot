#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace autobot::executor::upbit {

using OrderedParams = std::vector<std::pair<std::string, std::string>>;

enum class KeyEncodingPolicy {
  kEncodeAll = 0,
  kPreserveArrayBrackets = 1,
};

struct RequestSpec {
  std::string method;
  std::string path;
  OrderedParams query_params;
  OrderedParams body_params;
  std::unordered_map<std::string, std::string> headers;
};

struct PreparedRequest {
  std::string method;
  std::string path;
  std::string url_query;
  std::string query_string_for_hash;
  std::string body_json;
  std::unordered_map<std::string, std::string> headers;
  bool has_body = false;
};

std::string BuildUnencodedQueryString(const OrderedParams& params);
std::string BuildUrlEncodedQueryString(
    const OrderedParams& params,
    KeyEncodingPolicy key_policy = KeyEncodingPolicy::kPreserveArrayBrackets);
nlohmann::ordered_json BuildJsonBodyFromParams(const OrderedParams& params);
PreparedRequest BuildPreparedRequest(const RequestSpec& spec);

}  // namespace autobot::executor::upbit
