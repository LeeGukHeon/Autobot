#include "upbit/ws_private_parsers.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <sstream>
#include <unordered_set>
#include <utility>
#include <vector>

#include "upbit/number_string.h"

namespace autobot::executor::upbit {

namespace {

std::string Trim(std::string value) {
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(), std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(), value.end());
  return value;
}

std::string ToUpper(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::toupper(ch));
  });
  return value;
}

std::string ToLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::int64_t NowMs() {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

const nlohmann::json* Coalesce(const nlohmann::json& object, std::initializer_list<const char*> keys) {
  if (!object.is_object()) {
    return nullptr;
  }
  for (const char* key : keys) {
    if (key == nullptr) {
      continue;
    }
    const auto found = object.find(key);
    if (found != object.end()) {
      return &(*found);
    }
  }
  return nullptr;
}

std::string JsonToString(const nlohmann::json* value) {
  if (value == nullptr || value->is_null()) {
    return "";
  }
  if (value->is_string()) {
    return Trim(value->get<std::string>());
  }
  if (value->is_number_integer()) {
    return std::to_string(value->get<long long>());
  }
  if (value->is_number_unsigned()) {
    return std::to_string(value->get<unsigned long long>());
  }
  if (value->is_number_float()) {
    return FormatNumberString(value->get<double>(), 16);
  }
  if (value->is_boolean()) {
    return value->get<bool>() ? "true" : "false";
  }
  return "";
}

std::int64_t JsonToInt64(const nlohmann::json* value, std::int64_t fallback) {
  if (value == nullptr || value->is_null()) {
    return fallback;
  }
  if (value->is_number_integer() || value->is_number_unsigned()) {
    return value->get<std::int64_t>();
  }
  if (value->is_number_float()) {
    return static_cast<std::int64_t>(value->get<double>());
  }
  if (value->is_string()) {
    try {
      return std::stoll(Trim(value->get<std::string>()));
    } catch (...) {
      return fallback;
    }
  }
  return fallback;
}

std::vector<std::string> NormalizeCodes(const std::vector<std::string>& codes) {
  std::vector<std::string> out;
  std::unordered_set<std::string> seen;
  out.reserve(codes.size());
  for (std::string code : codes) {
    code = ToUpper(Trim(std::move(code)));
    if (code.empty()) {
      continue;
    }
    if (seen.insert(code).second) {
      out.push_back(std::move(code));
    }
  }
  return out;
}

std::string NormalizeFormat(std::string value) {
  value = ToUpper(Trim(std::move(value)));
  if (value.empty() || value == "DEFAULT") {
    return "DEFAULT";
  }
  if (value == "JSON_LIST" || value == "SIMPLE" || value == "SIMPLE_LIST") {
    return value;
  }
  return "";
}

bool ParsePrivateObject(const nlohmann::json& object, WsPrivateEvent* out) {
  if (!object.is_object() || out == nullptr) {
    return false;
  }

  const std::int64_t ts_ms =
      JsonToInt64(Coalesce(object, {"timestamp", "tms", "trade_timestamp"}), NowMs());
  const std::string status = ToUpper(JsonToString(Coalesce(object, {"status"})));
  if (status == "UP") {
    out->kind = WsPrivateEventKind::kHealthUp;
    out->health_up.ts_ms = ts_ms;
    out->health_up.status = "UP";
    out->health_up.raw = object;
    return true;
  }

  const std::string stream_type = ToLower(JsonToString(Coalesce(object, {"type", "ty"})));
  if (stream_type.empty()) {
    return false;
  }

  if (stream_type == "myorder") {
    out->kind = WsPrivateEventKind::kMyOrder;
    out->order.ts_ms = ts_ms;
    out->order.uuid = JsonToString(Coalesce(object, {"uuid", "uid"}));
    out->order.identifier = JsonToString(Coalesce(object, {"identifier", "i"}));
    out->order.market = ToUpper(JsonToString(Coalesce(object, {"code", "cd", "market"})));
    out->order.side = ToLower(JsonToString(Coalesce(object, {"side", "sd"})));
    out->order.ord_type = ToLower(JsonToString(Coalesce(object, {"ord_type", "ot"})));
    out->order.state = JsonToString(Coalesce(object, {"state", "st"}));
    out->order.price_str = JsonToString(Coalesce(object, {"price", "p"}));
    out->order.volume_str = JsonToString(Coalesce(object, {"volume", "v"}));
    out->order.executed_volume_str = JsonToString(Coalesce(object, {"executed_volume", "ev"}));
    out->order.remaining_volume_str = JsonToString(Coalesce(object, {"remaining_volume", "rv"}));
    out->order.avg_price_str = JsonToString(Coalesce(object, {"avg_price", "ap"}));
    out->order.raw = object;
    if (out->order.executed_volume_str.empty()) {
      out->order.executed_volume_str = "0";
    }
    return true;
  }

  if (stream_type == "myasset") {
    out->kind = WsPrivateEventKind::kMyAsset;
    out->asset.ts_ms = ts_ms;
    out->asset.currency = ToUpper(JsonToString(Coalesce(object, {"currency", "cy"})));
    out->asset.balance_str = JsonToString(Coalesce(object, {"balance", "bl"}));
    out->asset.locked_str = JsonToString(Coalesce(object, {"locked", "lk"}));
    out->asset.avg_buy_price_str = JsonToString(Coalesce(object, {"avg_buy_price", "abp"}));
    out->asset.raw = object;
    return true;
  }

  return false;
}

}  // namespace

nlohmann::json BuildMyOrderSubscribeObject(const std::vector<std::string>& codes) {
  nlohmann::json out = nlohmann::json::object();
  out["type"] = "myOrder";
  const std::vector<std::string> normalized_codes = NormalizeCodes(codes);
  if (!normalized_codes.empty()) {
    out["codes"] = normalized_codes;
  }
  return out;
}

bool BuildMyAssetSubscribeObject(
    const std::vector<std::string>& codes, nlohmann::json* out, std::string* error) {
  if (out == nullptr) {
    return false;
  }
  const std::vector<std::string> normalized_codes = NormalizeCodes(codes);
  if (!normalized_codes.empty()) {
    if (error != nullptr) {
      *error = "myAsset subscription codes_not_allowed";
    }
    return false;
  }
  *out = nlohmann::json::object({{"type", "myAsset"}});
  if (error != nullptr) {
    error->clear();
  }
  return true;
}

std::string BuildPrivateSubscribePayload(
    const std::string& ticket,
    bool subscribe_my_order,
    bool subscribe_my_asset,
    const std::string& format,
    const std::vector<std::string>& my_order_codes,
    std::string* error) {
  const std::string ticket_value = Trim(ticket);
  if (ticket_value.empty()) {
    if (error != nullptr) {
      *error = "ticket is required";
    }
    return "";
  }
  if (!subscribe_my_order && !subscribe_my_asset) {
    if (error != nullptr) {
      *error = "at least one private stream type is required";
    }
    return "";
  }
  const std::string normalized_format = NormalizeFormat(format);
  if (normalized_format.empty()) {
    if (error != nullptr) {
      *error = "unsupported format (allowed: DEFAULT, JSON_LIST, SIMPLE, SIMPLE_LIST)";
    }
    return "";
  }

  nlohmann::json payload = nlohmann::json::array();
  payload.push_back({{"ticket", ticket_value}});

  if (subscribe_my_order) {
    payload.push_back(BuildMyOrderSubscribeObject(my_order_codes));
  }
  if (subscribe_my_asset) {
    nlohmann::json asset_sub;
    std::string asset_error;
    if (!BuildMyAssetSubscribeObject({}, &asset_sub, &asset_error)) {
      if (error != nullptr) {
        *error = asset_error;
      }
      return "";
    }
    payload.push_back(asset_sub);
  }
  if (normalized_format != "DEFAULT") {
    payload.push_back({{"format", normalized_format}});
  }

  if (error != nullptr) {
    error->clear();
  }
  return payload.dump();
}

bool ParsePrivateWsPayload(const nlohmann::json& payload, WsPrivateEvent* out) {
  if (out == nullptr) {
    return false;
  }
  *out = WsPrivateEvent{};

  if (payload.is_object()) {
    return ParsePrivateObject(payload, out);
  }
  if (!payload.is_array()) {
    return false;
  }

  for (const auto& item : payload) {
    if (!item.is_object()) {
      continue;
    }
    if (ParsePrivateObject(item, out)) {
      return true;
    }
  }
  return false;
}

bool ParsePrivateWsMessage(const std::string& message_text, WsPrivateEvent* out) {
  if (out == nullptr) {
    return false;
  }
  nlohmann::json payload = nlohmann::json::parse(message_text, nullptr, false);
  if (payload.is_discarded()) {
    return false;
  }
  return ParsePrivateWsPayload(payload, out);
}

}  // namespace autobot::executor::upbit
