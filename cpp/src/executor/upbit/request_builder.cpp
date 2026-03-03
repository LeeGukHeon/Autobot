#include "upbit/request_builder.h"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <sstream>
#include <string_view>

namespace autobot::executor::upbit {

namespace {

bool IsUnreserved(char ch) {
  return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') ||
         ch == '-' || ch == '_' || ch == '.' || ch == '~';
}

std::string ToUpper(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::toupper(ch));
  });
  return value;
}

std::string PercentEncode(std::string_view raw) {
  std::ostringstream encoded;
  encoded << std::uppercase << std::hex;
  for (const unsigned char ch : raw) {
    if (IsUnreserved(static_cast<char>(ch))) {
      encoded << static_cast<char>(ch);
    } else if (ch == ' ') {
      encoded << '+';
    } else {
      encoded << '%' << std::setw(2) << std::setfill('0') << static_cast<int>(ch);
    }
  }
  return encoded.str();
}

void ReplaceAll(std::string* text, std::string_view from, std::string_view to) {
  if (text == nullptr || from.empty()) {
    return;
  }
  std::size_t pos = 0;
  while ((pos = text->find(from.data(), pos, from.size())) != std::string::npos) {
    text->replace(pos, from.size(), to.data(), to.size());
    pos += to.size();
  }
}

std::string EncodeKey(std::string_view key, KeyEncodingPolicy policy) {
  std::string encoded = PercentEncode(key);
  if (policy == KeyEncodingPolicy::kPreserveArrayBrackets) {
    ReplaceAll(&encoded, "%5B", "[");
    ReplaceAll(&encoded, "%5D", "]");
  }
  return encoded;
}

}  // namespace

std::string BuildUnencodedQueryString(const OrderedParams& params) {
  if (params.empty()) {
    return "";
  }

  std::ostringstream query;
  bool first = true;
  for (const auto& [key, value] : params) {
    if (key.empty()) {
      continue;
    }
    if (!first) {
      query << '&';
    }
    first = false;
    query << key << '=' << value;
  }
  return query.str();
}

std::string BuildUrlEncodedQueryString(const OrderedParams& params, KeyEncodingPolicy key_policy) {
  if (params.empty()) {
    return "";
  }

  std::ostringstream query;
  bool first = true;
  for (const auto& [key, value] : params) {
    if (key.empty()) {
      continue;
    }
    if (!first) {
      query << '&';
    }
    first = false;
    query << EncodeKey(key, key_policy) << '=' << PercentEncode(value);
  }
  return query.str();
}

nlohmann::json BuildJsonBodyFromParams(const OrderedParams& params) {
  nlohmann::json body = nlohmann::json::object();
  for (const auto& [key, value] : params) {
    if (key.empty()) {
      continue;
    }
    auto found = body.find(key);
    if (found == body.end()) {
      body[key] = value;
      continue;
    }
    if (found->is_array()) {
      found->push_back(value);
      continue;
    }
    nlohmann::json list = nlohmann::json::array();
    list.push_back(*found);
    list.push_back(value);
    body[key] = std::move(list);
  }
  return body;
}

PreparedRequest BuildPreparedRequest(const RequestSpec& spec) {
  PreparedRequest prepared;
  prepared.method = ToUpper(spec.method);
  prepared.path = spec.path.empty() ? "/" : spec.path;
  if (!prepared.path.empty() && prepared.path.front() != '/') {
    prepared.path = "/" + prepared.path;
  }

  prepared.headers = spec.headers;
  prepared.url_query = BuildUrlEncodedQueryString(spec.query_params);

  if (prepared.method == "GET" || prepared.method == "DELETE") {
    prepared.query_string_for_hash = BuildUnencodedQueryString(spec.query_params);
    return prepared;
  }

  if (prepared.method == "POST" || prepared.method == "PUT" || prepared.method == "PATCH") {
    if (!spec.body_params.empty()) {
      prepared.has_body = true;
      prepared.body_json = BuildJsonBodyFromParams(spec.body_params).dump();
      prepared.query_string_for_hash = BuildUnencodedQueryString(spec.body_params);
      prepared.headers["Content-Type"] = "application/json; charset=utf-8";
    }
  }
  return prepared;
}

}  // namespace autobot::executor::upbit

