#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace autobot::executor::upbit {

struct WsMyOrderEvent {
  std::int64_t ts_ms = 0;
  std::string uuid;
  std::string identifier;
  std::string market;
  std::string side;
  std::string ord_type;
  std::string state;
  std::string price_str;
  std::string volume_str;
  std::string executed_volume_str;
  std::string remaining_volume_str;
  std::string avg_price_str;
  nlohmann::json raw = nlohmann::json::object();
};

struct WsMyAssetEvent {
  std::int64_t ts_ms = 0;
  std::string currency;
  std::string balance_str;
  std::string locked_str;
  std::string avg_buy_price_str;
  nlohmann::json raw = nlohmann::json::object();
};

struct WsHealthUpEvent {
  std::int64_t ts_ms = 0;
  std::string status;
  nlohmann::json raw = nlohmann::json::object();
};

enum class WsPrivateEventKind {
  kNone = 0,
  kMyOrder = 1,
  kMyAsset = 2,
  kHealthUp = 3,
};

struct WsPrivateEvent {
  WsPrivateEventKind kind = WsPrivateEventKind::kNone;
  WsMyOrderEvent order;
  WsMyAssetEvent asset;
  WsHealthUpEvent health_up;
};

nlohmann::json BuildMyOrderSubscribeObject(const std::vector<std::string>& codes);
bool BuildMyAssetSubscribeObject(
    const std::vector<std::string>& codes, nlohmann::json* out, std::string* error);
std::string BuildPrivateSubscribePayload(
    const std::string& ticket,
    bool subscribe_my_order,
    bool subscribe_my_asset,
    const std::string& format,
    const std::vector<std::string>& my_order_codes,
    std::string* error);

bool ParsePrivateWsPayload(const nlohmann::json& payload, WsPrivateEvent* out);
bool ParsePrivateWsMessage(const std::string& message_text, WsPrivateEvent* out);

}  // namespace autobot::executor::upbit
