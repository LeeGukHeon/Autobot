#include "upbit/remaining_req.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <unordered_map>

namespace autobot::executor::upbit {

namespace {

std::string Trim(std::string value) {
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

std::string ToLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::optional<int> ParseInt(const std::string& raw) {
  try {
    std::size_t parsed = 0;
    const int value = std::stoi(raw, &parsed);
    if (parsed != raw.size()) {
      return std::nullopt;
    }
    return value;
  } catch (...) {
    return std::nullopt;
  }
}

}  // namespace

RemainingReqInfo ParseRemainingReqHeader(const std::string& value) {
  RemainingReqInfo info;
  info.raw = Trim(value);
  if (info.raw.empty()) {
    return info;
  }

  std::unordered_map<std::string, std::string> parsed;
  std::stringstream stream(info.raw);
  std::string token;
  while (std::getline(stream, token, ';')) {
    const std::string trimmed = Trim(token);
    const std::size_t sep = trimmed.find('=');
    if (sep == std::string::npos) {
      continue;
    }
    std::string key = ToLower(Trim(trimmed.substr(0, sep)));
    std::string val = Trim(trimmed.substr(sep + 1));
    if (!key.empty()) {
      parsed[key] = val;
    }
  }

  if (parsed.count("group") > 0) {
    const std::string group = Trim(parsed["group"]);
    if (!group.empty()) {
      info.group = group;
    }
  }

  const auto sec_it = parsed.find("sec");
  if (sec_it == parsed.end()) {
    return info;
  }

  const std::optional<int> sec = ParseInt(sec_it->second);
  if (!sec.has_value()) {
    return info;
  }
  info.sec = *sec;

  const auto min_it = parsed.find("min");
  if (min_it != parsed.end()) {
    info.min = ParseInt(min_it->second);
  }
  info.valid = true;
  return info;
}

}  // namespace autobot::executor::upbit
