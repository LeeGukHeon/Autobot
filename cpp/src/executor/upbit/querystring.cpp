#include "upbit/querystring.h"

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

std::string EncodeKey(std::string_view key) {
  std::string encoded = PercentEncode(key);
  ReplaceAll(&encoded, "%5B", "[");
  ReplaceAll(&encoded, "%5D", "]");
  return encoded;
}

}  // namespace

std::string BuildQueryString(const std::vector<QueryParam>& params) {
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
    query << EncodeKey(key) << '=' << PercentEncode(value);
  }
  return query.str();
}

}  // namespace autobot::executor::upbit
