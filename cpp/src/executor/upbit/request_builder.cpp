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

int HexToInt(char ch) {
  if (ch >= '0' && ch <= '9') {
    return ch - '0';
  }
  if (ch >= 'a' && ch <= 'f') {
    return 10 + (ch - 'a');
  }
  if (ch >= 'A' && ch <= 'F') {
    return 10 + (ch - 'A');
  }
  return -1;
}

std::string PercentDecodePreservingPlus(std::string_view encoded) {
  std::string decoded;
  decoded.reserve(encoded.size());
  for (std::size_t i = 0; i < encoded.size(); ++i) {
    const char ch = encoded[i];
    if (ch != '%' || i + 2 >= encoded.size()) {
      decoded.push_back(ch);
      continue;
    }
    const int hi = HexToInt(encoded[i + 1]);
    const int lo = HexToInt(encoded[i + 2]);
    if (hi < 0 || lo < 0) {
      decoded.push_back(ch);
      continue;
    }
    decoded.push_back(static_cast<char>((hi << 4) | lo));
    i += 2;
  }
  return decoded;
}

OrderedParams FilterBodyParams(const OrderedParams& params) {
  OrderedParams filtered;
  filtered.reserve(params.size());
  for (const auto& [key, value] : params) {
    if (key.empty() || value.empty()) {
      continue;
    }
    filtered.emplace_back(key, value);
  }
  return filtered;
}

}  // namespace

std::string BuildUnencodedQueryString(const OrderedParams& params) {
  if (params.empty()) {
    return "";
  }
  // Python parity: unquote(urlencode(params, doseq=True)).
  const std::string encoded = BuildUrlEncodedQueryString(params, KeyEncodingPolicy::kEncodeAll);
  return PercentDecodePreservingPlus(encoded);
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

nlohmann::ordered_json BuildJsonBodyFromParams(const OrderedParams& params) {
  nlohmann::ordered_json body = nlohmann::ordered_json::object();
  for (const auto& [key, value] : params) {
    if (key.empty() || value.empty()) {
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
    nlohmann::ordered_json list = nlohmann::ordered_json::array();
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

  if (prepared.method == "GET" || prepared.method == "DELETE") {
    prepared.url_query = BuildUrlEncodedQueryString(spec.query_params);
    prepared.query_string_for_hash = BuildUnencodedQueryString(spec.query_params);
    return prepared;
  }

  if (prepared.method == "POST" || prepared.method == "PUT" || prepared.method == "PATCH") {
    const OrderedParams body_params = FilterBodyParams(spec.body_params);
    if (!body_params.empty()) {
      prepared.has_body = true;
      prepared.body_json = BuildJsonBodyFromParams(body_params).dump();
      prepared.query_string_for_hash = BuildUnencodedQueryString(body_params);
      prepared.headers["Content-Type"] = "application/json; charset=utf-8";
    }
    prepared.url_query.clear();
    return prepared;
  }

  prepared.url_query = BuildUrlEncodedQueryString(spec.query_params);
  return prepared;
}

}  // namespace autobot::executor::upbit
